/**
 * webshare-transfer-raw.js
 *
 * Raw WebRTC transfer backend. Extends CoreTransfer with:
 *  - SDP offer/answer exchange via animated QR codes (no signaling server)
 *  - Binary chunked transfer over the native RTCDataChannel
 *  - QR chunk reassembly via ChunkReassembler
 *
 * Limitation: requires both devices on the same network (no TURN relay),
 * because no ICE server is involved in the NAT traversal.
 *
 * Depends on CoreTransfer (webshare-transfer-core.js).
 * Also depends on qrcode-generator (global `qrcode`) and jsQR (global `jsQR`)
 * for the animated QR handshake — but those are consumed by index.html,
 * not by this library.
 *
 * Exported global: RawTransfer
 */
(function (global) {
  'use strict';

  // -----------------------------------------------------------------------
  // QR chunk helpers
  // -----------------------------------------------------------------------
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

  // -----------------------------------------------------------------------
  // SDP compression — gzip if available, base64 otherwise
  // -----------------------------------------------------------------------
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
    await new Promise(resolve => {
      pc.addEventListener('icegatheringstatechange', () => {
        if (pc.iceGatheringState === 'complete') resolve();
      });
      setTimeout(resolve, 4000);
    });
  }

  // -----------------------------------------------------------------------
  // RawTransfer
  // -----------------------------------------------------------------------
  class RawTransfer extends CoreTransfer {
    constructor({ peerInfo, iceServers } = {}) {
      super({ iceServers, peerInfo });
      this._pc           = null;   // RTCPeerConnection
      this._channel      = null;   // RTCDataChannel
      this._reassembler  = new ChunkReassembler();
      this._pendingSend  = null;   // payload waiting for channel to open
    }

    // -----------------------------------------------------------------------
    // Transport — CoreTransfer abstract interface
    // -----------------------------------------------------------------------

    // Send a JSON-encoded message over the raw data channel.
    // (Raw DataChannels carry strings; PeerJS auto-serialises objects.)
    _sendMessage(msg) {
      if (!this._channel || this._channel.readyState !== 'open') {
        throw new Error('Raw channel not open');
      }
      this._channel.send(JSON.stringify(msg));
    }

    isAlive() {
      if (!this._channel || this._channel.readyState !== 'open') return false;
      const sinceLastPong = this._lastPongAt ? (Date.now() - this._lastPongAt) : 0;
      return !this._lastPongAt || sinceLastPong < (global.HEARTBEAT_DEAD_MS || 9000);
    }

    // Raw mode cannot reconnect — it would need a fresh QR handshake.
    // checkAndRecover() deliberately does nothing (inherits no-op from CoreTransfer).

    // -----------------------------------------------------------------------
    // Receiver flow
    // -----------------------------------------------------------------------

    async startReceiving() {
      if (this.role) throw new Error('Already started.');
      this.role = 'receiver';
      this._status('registering');
      await this._startRawReceive();
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
        this._sendPeerInfo();
      };

      ch.onmessage = async (e) => {
        if (typeof e.data === 'string') {
          const msg = JSON.parse(e.data);

          // Binary transfer framing (header/done) handled here; everything
          // else dispatched through the shared protocol handler.
          if (msg.type === 'header') {
            expectedSize = msg.size;
            receivedChunks = []; receivedSize = 0;
            this._status('transferring');
            this.emit('progress', { received: 0, total: expectedSize });
            return;
          }
          if (msg.type === 'done') {
            try {
              const text    = await new Blob(receivedChunks).text();
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
            return;
          }

          // Common protocol messages (peer-info, note, lamp, ping, pong, bye, ack)
          this._handleMessage(msg);
        } else {
          // Binary chunk
          receivedChunks.push(e.data);
          receivedSize += e.data.byteLength;
          this.emit('progress', { received: receivedSize, total: expectedSize });
        }
      };
    }

    // -----------------------------------------------------------------------
    // Sender flow
    // -----------------------------------------------------------------------

    // Raw mode doesn't use connect() — the sender starts by scanning the
    // receiver's offer QR and calling feedScannedChunk().
    async connect() {
      throw new Error('Raw mode does not use connect(). Scan the receiver\'s QR to start.');
    }

    async send(payload) {
      // Channel may not be open yet if the sender is still negotiating.
      if (this._channel && this._channel.readyState === 'open') {
        await this._rawSendNow(payload);
      } else {
        this._pendingSend = payload;
        this._log('info', 'Payload queued; waiting for raw channel to open…');
      }
    }

    // -----------------------------------------------------------------------
    // QR handshake — called from index.html when chunks are scanned
    // -----------------------------------------------------------------------

    async feedScannedChunk(text) {
      const res = this._reassembler.feed(text);
      if (!res) return;
      if (res.complete) {
        if (res.id === 'OFFER')  await this._onOfferComplete(res.data);
        else if (res.id === 'ANSWER') await this._onAnswerComplete(res.data);
      } else if (res.isNew) {
        this._log('info', `Got chunk ${res.idx} · ${res.missing.length} missing`);
        this.emit('handshake-progress', { progress: res.progress, missing: res.missing });
      }
    }

    async _onOfferComplete(compressed) {
      this.role = 'sender';
      try {
        const offer = JSON.parse(await decompressString(compressed));
        this._pc = new RTCPeerConnection(this.rtcConfig);
        this._instrumentPC('send');
        this._pc.ondatachannel = e => {
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
        this._log('ok', 'Answer ready; show to receiver.');
      } catch (err) {
        this._log('err', 'Offer apply failed: ' + err.message);
        this._status('error');
      }
    }

    async _onAnswerComplete(compressed) {
      try {
        const answer = JSON.parse(await decompressString(compressed));
        await this._pc.setRemoteDescription(answer);
        this._log('ok', 'Answer applied; awaiting connection.');
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
        this._sendPeerInfo();
        if (this._pendingSend) {
          const p = this._pendingSend; this._pendingSend = null;
          await this._rawSendNow(p);
        }
      };
      ch.onmessage = e => {
        if (typeof e.data === 'string') {
          this._handleMessage(JSON.parse(e.data));
        }
      };
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
      this._log('ok', `Sent ${(bytes.byteLength / 1024).toFixed(1)} KB.`);
    }

    // -----------------------------------------------------------------------
    // ICE logging helper
    // -----------------------------------------------------------------------

    _instrumentPC(label) {
      const pc = this._pc;
      pc.addEventListener('iceconnectionstatechange', () => {
        const s = pc.iceConnectionState;
        const level = (s === 'connected' || s === 'completed') ? 'ok'
                    : (s === 'failed' || s === 'disconnected') ? 'err' : 'info';
        this._log(level, `${label} ICE: ${s}`);
        if (s === 'failed' || s === 'disconnected') {
          this._log('err', 'Tip: raw mode needs same Wi-Fi. Try PeerJS for cross-network.');
        }
      });
    }

    // -----------------------------------------------------------------------
    // Cleanup
    // -----------------------------------------------------------------------

    close() {
      this._stopHeartbeat();
      try {
        if (this._channel && this._channel.readyState === 'open') {
          this._channel.send(JSON.stringify({ type: 'bye' }));
        }
      } catch {}
      try { this._channel?.close(); } catch {}
      try { this._pc?.close();      } catch {}
      this._channel = null;
      this._pc      = null;
      this._status('idle');
    }
  }

  // -----------------------------------------------------------------------
  // Export
  // -----------------------------------------------------------------------
  global.RawTransfer = RawTransfer;

})(typeof window !== 'undefined' ? window : globalThis);
