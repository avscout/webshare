/**
 * webshare-transfer.js
 *
 * Transfer arbitrary JSON-serializable payloads between two browsers, peer to
 * peer, via either PeerJS (works across networks) or Raw WebRTC + QR handshake
 * (same Wi-Fi only, but no third-party signaling).
 *
 * The library has NO opinion about what the payload contains. The consumer
 * decides how to handle incoming payloads (merge, replace, prompt, log, etc.).
 *
 * Required globals: qrcode (qrcode-generator), jsQR, Peer (peerjs).
 *
 * USAGE — Receiver:
 *
 *   const t = new SessionTransfer({ backend: 'peerjs' });
 *   t.on('log', ({ level, message }) => console.log(level, message));
 *   t.on('show-qr', ({ kind, text, chunks }) => {
 *     // PeerJS: kind='peerid', text=peer id, render single QR + show text
 *     // Raw:    kind='offer-chunks', chunks=[...], cycle them as animated QR
 *   });
 *   t.on('need-scan', ({ kind }) => {
 *     // Raw only: kind='answer'  — activate webcam, feed chunks back
 *   });
 *   t.on('connected', () => { ... });
 *   t.on('progress', ({ received, total }) => { ... });
 *   t.onPayload = async (payload) => {
 *     // YOUR app decides what to do here. Return value (any) is sent back as ack.
 *     return { applied: true };
 *   };
 *   await t.startReceiving();
 *   // For raw mode, feed scanned QR chunks back:
 *   //   t.feedScannedChunk(scannedText);
 *
 * USAGE — Sender:
 *
 *   const t = new SessionTransfer({ backend: 'peerjs' });
 *   t.on('log', ...);
 *   t.onAck = (response) => { ... };
 *   // PeerJS: connect directly with a peer id obtained from the receiver
 *   await t.connect(targetPeerId);
 *   await t.send(myPayload);
 *   // Raw: feed scanned offer chunks first, then send is auto-triggered;
 *   //   t.on('show-qr', ({chunks}) => cycle as QR);  // for answer chunks
 *   //   t.feedScannedChunk(scannedText);             // for offer chunks
 *   //   await t.send(myPayload);                     // resolves when channel opens
 *
 * Both sides should call `t.close()` when done.
 */

(function (global) {
  'use strict';

  // ============================================================
  // Tiny event emitter
  // ============================================================
  class Emitter {
    constructor() { this._listeners = {}; }
    on(event, fn) {
      (this._listeners[event] = this._listeners[event] || []).push(fn);
      return () => this.off(event, fn);
    }
    off(event, fn) {
      const arr = this._listeners[event]; if (!arr) return;
      const i = arr.indexOf(fn); if (i >= 0) arr.splice(i, 1);
    }
    emit(event, payload) {
      (this._listeners[event] || []).slice().forEach(fn => {
        try { fn(payload); } catch (e) { console.error('Listener error:', e); }
      });
    }
  }

  // ============================================================
  // Friendly ID generator (local, no network)
  // ============================================================
  const ADJ = ['tomato','rabbit','cactus','river','copper','silent','mango','swift','velvet','lemon','crimson','golden','quiet','amber','linen','rusty','silken','marble'];
  const NOUN = ['fox','sparrow','oak','meadow','harbor','comet','reef','prairie','glacier','willow','flint','arrow','ember','thicket','quill','beacon','lattice','pebble'];
  function generateFriendlyId() {
    const n = Math.floor(Math.random() * 90) + 10;
    return `idb-${ADJ[Math.floor(Math.random()*ADJ.length)]}-${NOUN[Math.floor(Math.random()*NOUN.length)]}-${n}`;
  }

  // ============================================================
  // Chunking for QR handshake
  // ============================================================
  const QR_CHUNK_SIZE = 350;

  function makeChunks(id, str) {
    const chunks = [];
    const total = Math.ceil(str.length / QR_CHUNK_SIZE);
    for (let i = 0; i < total; i++) {
      chunks.push(`${id}:${i + 1}/${total}:${str.slice(i * QR_CHUNK_SIZE, (i + 1) * QR_CHUNK_SIZE)}`);
    }
    return chunks;
  }

  class ChunkReassembler {
    constructor() { this.id = null; this.total = 0; this.parts = new Map(); }
    feed(chunk) {
      const m = chunk.match(/^([A-Z]+):(\d+)\/(\d+):(.*)$/s);
      if (!m) return null;
      const [, id, idxStr, totalStr, data] = m;
      const idx = +idxStr, total = +totalStr;
      if (this.id !== id) { this.id = id; this.total = total; this.parts.clear(); }
      const isNew = !this.parts.has(idx);
      this.parts.set(idx, data);
      if (this.parts.size === this.total) {
        let out = ''; for (let i = 1; i <= this.total; i++) out += this.parts.get(i);
        return { id, data: out, complete: true };
      }
      const missing = []; for (let i = 1; i <= this.total; i++) if (!this.parts.has(i)) missing.push(i);
      return { id, progress: this.parts.size / this.total, isNew, idx, missing };
    }
  }

  // ============================================================
  // SDP compression for smaller QRs
  // ============================================================
  async function compressString(str) {
    if (!self.CompressionStream) return btoa(unescape(encodeURIComponent(str)));
    const stream = new Blob([str]).stream().pipeThrough(new CompressionStream('gzip'));
    const buf = await new Response(stream).arrayBuffer();
    let bin = ''; const bytes = new Uint8Array(buf);
    for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
    return 'G:' + btoa(bin);
  }
  async function decompressString(s) {
    if (!s.startsWith('G:')) return decodeURIComponent(escape(atob(s)));
    const bin = atob(s.slice(2));
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip'));
    return await new Response(stream).text();
  }

  async function gatherCompleteSDP(pc) {
    if (pc.iceGatheringState === 'complete') return;
    await new Promise((resolve) => {
      pc.addEventListener('icegatheringstatechange', () => { if (pc.iceGatheringState === 'complete') resolve(); });
      setTimeout(resolve, 4000);
    });
  }

  // ============================================================
  // PeerJS — register with retry on ID collision
  // ============================================================
  function createPeerWithRetry(emitter, maxAttempts = 5) {
    return new Promise((resolve, reject) => {
      let attempt = 0;
      const tryOnce = () => {
        attempt++;
        const peerId = generateFriendlyId();
        emitter.emit('log', { level: 'info', message: `Registering "${peerId}" (attempt ${attempt}/${maxAttempts})…` });
        const peer = new Peer(peerId, { debug: 1 });
        const cleanup = () => { peer.off('open', onOpen); peer.off('error', onError); };
        const onOpen = () => { cleanup(); resolve(peer); };
        const onError = (err) => {
          if (err.type === 'unavailable-id' && attempt < maxAttempts) {
            emitter.emit('log', { level: 'info', message: `ID taken, retrying…` });
            cleanup(); peer.destroy(); setTimeout(tryOnce, 100);
          } else {
            cleanup(); peer.destroy();
            const friendly =
              err.type === 'unavailable-id' ? 'all retries hit ID collisions' :
              err.type === 'server-error'   ? "PeerJS cloud isn't responding" :
              err.type === 'network'        ? 'no internet' :
              err.type === 'browser-incompatible' ? 'browser does not support WebRTC' :
              err.message || err.type;
            reject(new Error(friendly));
          }
        };
        peer.on('open', onOpen);
        peer.on('error', onError);
      };
      tryOnce();
    });
  }

  // ============================================================
  // SessionTransfer — the public class
  // ============================================================
  const DEFAULT_RTC_CONFIG = {
    iceServers: [
      { urls: 'stun:stun.l.google.com:19302' },
      { urls: 'stun:stun1.l.google.com:19302' }
    ]
  };

  class SessionTransfer extends Emitter {
    constructor({ backend = 'peerjs', iceServers } = {}) {
      super();
      if (backend !== 'peerjs' && backend !== 'raw') {
        throw new Error(`Unknown backend "${backend}". Use 'peerjs' or 'raw'.`);
      }
      this.backend = backend;
      this.rtcConfig = iceServers ? { iceServers } : DEFAULT_RTC_CONFIG;

      // role gets set by startReceiving() or connect()
      this.role = null;       // 'receiver' or 'sender'
      this.peerId = null;     // PeerJS: assigned peer ID; Raw: not used
      this.onPayload = null;  // receiver handler
      this.onAck = null;      // sender ack handler

      // Backend-specific state
      this._peer = null;          // PeerJS Peer
      this._conn = null;          // PeerJS DataConnection
      this._pc = null;            // Raw WebRTC PeerConnection
      this._channel = null;       // Raw WebRTC DataChannel
      this._reassembler = new ChunkReassembler();
      this._pendingSend = null;   // payload waiting for raw channel to open
      this._sendResolve = null;   // resolves when raw send completes
    }

    _log(level, message) { this.emit('log', { level, message }); }
    _status(s) { this.emit('status', s); }

    // ----- RECEIVER -----
    async startReceiving() {
      if (this.role) throw new Error('Already started.');
      this.role = 'receiver';
      this._status('registering');
      if (this.backend === 'peerjs') await this._startPeerJSReceive();
      else                            await this._startRawReceive();
    }

    async _startPeerJSReceive() {
      this._peer = await createPeerWithRetry(this);
      this.peerId = this._peer.id;
      this._log('ok', `Peer ID assigned: ${this.peerId}`);
      this._status('waiting');
      this.emit('show-qr', { kind: 'peerid', text: this.peerId });

      this._peer.on('connection', (conn) => {
        this._conn = conn;
        this._log('info', `Incoming connection from ${conn.peer}`);
        this._status('connecting');
        conn.on('open', () => {
          this._log('ok', 'Data channel open.');
          this._status('connected');
          this.emit('connected');
        });
        conn.on('data', async (msg) => {
          if (msg && msg.type === 'payload') {
            this._status('transferring');
            this.emit('progress', { received: 100, total: 100 });
            try {
              const response = this.onPayload ? await this.onPayload(msg.payload) : null;
              conn.send({ type: 'ack', response });
              this._status('done');
              this._log('ok', 'Payload handled, ack sent.');
            } catch (err) {
              conn.send({ type: 'ack', error: err.message });
              this._status('error');
              this._log('err', 'onPayload threw: ' + err.message);
            }
          }
        });
        conn.on('close', () => this._log('info', 'Sender disconnected.'));
        conn.on('error', (e) => this._log('err', 'Conn error: ' + e.message));
      });
      this._peer.on('error', (err) => {
        this._log('err', `PeerJS error: ${err.type}`);
        this._status('error');
      });
      this._peer.on('disconnected', () => {
        this._log('info', 'Disconnected from signaling, reconnecting…');
        try { this._peer.reconnect(); } catch {}
      });
    }

    async _startRawReceive() {
      this._pc = new RTCPeerConnection(this.rtcConfig);
      this._instrumentPC('recv');
      this._channel = this._pc.createDataChannel('webshare', { ordered: true });
      this._setupRawReceiverChannel();
      const offer = await this._pc.createOffer();
      await this._pc.setLocalDescription(offer);
      await gatherCompleteSDP(this._pc);
      const compressed = await compressString(JSON.stringify(this._pc.localDescription));
      const chunks = makeChunks('OFFER', compressed);
      this._status('waiting');
      this.emit('show-qr', { kind: 'offer-chunks', chunks });
      this.emit('need-scan', { kind: 'answer' });
    }

    _setupRawReceiverChannel() {
      const ch = this._channel;
      ch.binaryType = 'arraybuffer';
      let receivedChunks = [], expectedSize = null, receivedSize = 0;
      ch.onopen = () => {
        this._log('ok', 'Data channel open.');
        this._status('connected');
        this.emit('connected');
      };
      ch.onmessage = async (e) => {
        if (typeof e.data === 'string') {
          const msg = JSON.parse(e.data);
          if (msg.type === 'header') {
            expectedSize = msg.size; receivedChunks = []; receivedSize = 0;
            this._status('transferring');
            this.emit('progress', { received: 0, total: expectedSize });
          } else if (msg.type === 'done') {
            try {
              const text = await new Blob(receivedChunks).text();
              const payload = JSON.parse(text);
              const response = this.onPayload ? await this.onPayload(payload) : null;
              ch.send(JSON.stringify({ type: 'ack', response }));
              this._status('done');
              this._log('ok', 'Payload handled, ack sent.');
            } catch (err) {
              ch.send(JSON.stringify({ type: 'ack', error: err.message }));
              this._status('error');
              this._log('err', 'onPayload threw: ' + err.message);
            }
          }
        } else {
          receivedChunks.push(e.data);
          receivedSize += e.data.byteLength;
          this.emit('progress', { received: receivedSize, total: expectedSize });
        }
      };
    }

    // ----- SENDER -----
    /**
     * PeerJS: connect to a known peer ID. Resolves when channel is open.
     * Raw: not used directly; sender begins by feeding the scanned offer chunk.
     */
    async connect(targetPeerId) {
      if (this.role) throw new Error('Already started.');
      this.role = 'sender';
      if (this.backend !== 'peerjs') throw new Error('connect() is only for PeerJS. For raw, feedScannedChunk() the offer.');
      this._status('connecting');
      this._log('info', `Connecting to "${targetPeerId}"…`);
      this._peer = new Peer({ debug: 1 });
      await new Promise((resolve, reject) => {
        this._peer.on('open', resolve);
        this._peer.on('error', (err) => reject(new Error(err.type + ': ' + (err.message || ''))));
      });
      this._conn = this._peer.connect(targetPeerId, { reliable: true });
      await new Promise((resolve, reject) => {
        this._conn.on('open', resolve);
        this._conn.on('error', reject);
        setTimeout(() => reject(new Error('Connection timed out')), 15000);
      });
      this._log('ok', 'Connected.');
      this._status('connected');
      this.emit('connected');

      this._conn.on('data', (msg) => {
        if (msg && msg.type === 'ack') {
          if (this.onAck) try { this.onAck(msg.response, msg.error); } catch (e) { console.error(e); }
          this._status('done');
        }
      });
    }

    /**
     * Send a payload. Resolves when bytes have been written to the channel
     * (NOT when the receiver acknowledges — listen for onAck for that).
     */
    async send(payload) {
      if (this.role !== 'sender' && this.backend === 'raw') {
        // raw mode: sender role gets set automatically when offer is fed in
        this.role = 'sender';
      }
      if (this.backend === 'peerjs') {
        if (!this._conn) throw new Error('Not connected. Call connect() first.');
        this._status('transferring');
        this.emit('progress', { sent: 0, total: 100 });
        this._conn.send({ type: 'payload', payload });
        this.emit('progress', { sent: 100, total: 100 });
        this._log('ok', 'Payload sent.');
      } else {
        // raw — channel may not be open yet (offer chunks still arriving)
        if (this._channel && this._channel.readyState === 'open') {
          await this._rawSendNow(payload);
        } else {
          this._pendingSend = payload;
          this._log('info', 'Payload queued, waiting for raw channel to open…');
        }
      }
    }

    async _rawSendNow(payload) {
      const ch = this._channel;
      this._status('transferring');
      const bytes = new TextEncoder().encode(JSON.stringify(payload));
      const CHUNK = 16 * 1024;
      ch.send(JSON.stringify({ type: 'header', size: bytes.byteLength }));
      let sent = 0;
      while (sent < bytes.byteLength) {
        while (ch.bufferedAmount > 1024 * 1024) await new Promise(r => setTimeout(r, 20));
        const slice = bytes.slice(sent, sent + CHUNK);
        ch.send(slice); sent += slice.byteLength;
        this.emit('progress', { sent, total: bytes.byteLength });
      }
      ch.send(JSON.stringify({ type: 'done' }));
      this._log('ok', `Sent ${(bytes.byteLength/1024).toFixed(1)} KB.`);
    }

    /**
     * Raw mode only: feed a scanned QR chunk to the library. Handles both
     * roles — the library figures out whether it's an OFFER (we're sender)
     * or ANSWER (we're receiver) based on the chunk header.
     */
    async feedScannedChunk(text) {
      if (this.backend !== 'raw') {
        this._log('err', 'feedScannedChunk() is for raw backend only.');
        return;
      }
      const res = this._reassembler.feed(text);
      if (!res) return;
      if (res.complete) {
        if (res.id === 'OFFER') await this._onOfferComplete(res.data);
        else if (res.id === 'ANSWER') await this._onAnswerComplete(res.data);
      } else if (res.isNew) {
        this._log('info', `Got chunk ${res.idx} · ${res.missing.length} missing`);
        this.emit('handshake-progress', { progress: res.progress, missing: res.missing });
      }
    }

    async _onOfferComplete(compressed) {
      // We're the sender, just finished receiving the offer
      this.role = 'sender';
      try {
        const offer = JSON.parse(await decompressString(compressed));
        this._pc = new RTCPeerConnection(this.rtcConfig);
        this._instrumentPC('send');
        this._pc.ondatachannel = (e) => {
          this._channel = e.channel;
          this._setupRawSenderChannel();
        };
        await this._pc.setRemoteDescription(offer);
        const answer = await this._pc.createAnswer();
        await this._pc.setLocalDescription(answer);
        await gatherCompleteSDP(this._pc);
        const compressedAnswer = await compressString(JSON.stringify(this._pc.localDescription));
        const chunks = makeChunks('ANSWER', compressedAnswer);
        this.emit('show-qr', { kind: 'answer-chunks', chunks });
        this._log('ok', 'Answer ready, show to receiver.');
      } catch (err) {
        this._log('err', 'Offer apply failed: ' + err.message);
        this._status('error');
      }
    }

    async _onAnswerComplete(compressed) {
      // We're the receiver, just finished scanning the answer
      try {
        const answer = JSON.parse(await decompressString(compressed));
        await this._pc.setRemoteDescription(answer);
        this._log('ok', 'Answer applied, awaiting connection.');
      } catch (err) {
        this._log('err', 'Answer apply failed: ' + err.message);
        this._status('error');
      }
    }

    _setupRawSenderChannel() {
      const ch = this._channel;
      ch.binaryType = 'arraybuffer';
      ch.onopen = async () => {
        this._log('ok', 'Data channel open.');
        this._status('connected');
        this.emit('connected');
        if (this._pendingSend) {
          const p = this._pendingSend; this._pendingSend = null;
          await this._rawSendNow(p);
        }
      };
      ch.onmessage = (e) => {
        if (typeof e.data === 'string') {
          const msg = JSON.parse(e.data);
          if (msg.type === 'ack') {
            if (this.onAck) try { this.onAck(msg.response, msg.error); } catch (err) { console.error(err); }
            this._status('done');
          }
        }
      };
    }

    _instrumentPC(label) {
      const pc = this._pc;
      pc.addEventListener('iceconnectionstatechange', () => {
        const s = pc.iceConnectionState;
        const level = (s === 'connected' || s === 'completed') ? 'ok' : (s === 'failed' || s === 'disconnected') ? 'err' : 'info';
        this._log(level, `${label} ICE: ${s}`);
        if (s === 'failed' || s === 'disconnected') {
          this._log('err', 'Tip: raw mode needs same Wi-Fi. Try PeerJS for cross-network.');
        }
      });
    }

    // ----- CLEANUP -----
    close() {
      try { this._conn?.close(); } catch {}
      try { this._peer?.destroy(); } catch {}
      try { this._channel?.close(); } catch {}
      try { this._pc?.close(); } catch {}
      this._conn = null; this._peer = null; this._channel = null; this._pc = null;
      this._status('idle');
    }
  }

  // Export
  global.SessionTransfer = SessionTransfer;
  global.generateFriendlyId = generateFriendlyId;

})(typeof window !== 'undefined' ? window : globalThis);
