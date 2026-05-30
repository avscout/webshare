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
  function createPeerWithRetry(emitter, peerServerConfig, maxAttempts = 5) {
    return new Promise((resolve, reject) => {
      let attempt = 0;
      const tryOnce = () => {
        attempt++;
        const peerId = generateFriendlyId();
        emitter.emit('log', { level: 'info', message: `Registering "${peerId}" (attempt ${attempt}/${maxAttempts})…` });
        // Merge peerServerConfig (if any) with the debug option. PeerJS uses
        // {host, port, path, secure, key} to point at a custom server. When
        // peerServerConfig is null/undefined, no host is passed and PeerJS
        // defaults to its public cloud (0.peerjs.com).
        const peerOptions = Object.assign({ debug: 1 }, peerServerConfig || {});
        const peer = new Peer(peerId, peerOptions);
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
              err.type === 'server-error'   ? "signaling server isn't responding" :
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

  // Application-level heartbeat. ICE has its own keepalives but those are
  // invisible to JS and get throttled to nothing on mobile when a tab is
  // backgrounded. Our own ping/pong gives the JS layer a way to notice
  // when the channel has actually died and emit 'disconnected'.
  //   INTERVAL: how often we send a ping (and check liveness)
  //   DEAD_MS:  how long without a pong before we declare the peer gone.
  // The ratio matters: DEAD_MS should comfortably exceed a couple of
  // INTERVALs so a single dropped ping doesn't trigger a false positive.
  const HEARTBEAT_INTERVAL_MS = 3000;
  const HEARTBEAT_DEAD_MS = 9000;

  // Auto-reconnect: when a previously-established channel dies (mobile
  // freeze, network blip), try to bring it back rather than giving up
  // immediately. The receiver re-registers its signaling peer and waits;
  // the sender additionally re-issues peer.connect() to the same target.
  // Only applies to PeerJS — raw mode would require a fresh QR handshake.
  //
  // The reconnect strategy is destroy-and-recreate: PeerJS' peer.reconnect()
  // is unreliable after a mobile tab freeze (the underlying WebSocket was
  // killed by the OS but PeerJS' internal state thinks it's still healthy),
  // so we tear down the peer object entirely and create a new one with the
  // same id (receiver) or a fresh id (sender). PeerJS' signaling server
  // keeps released ids reserved for ~60s, so re-registering usually
  // succeeds within that window.
  const RECONNECT_TOTAL_DEADLINE_MS = 60000;        // give up after 60s wall-clock
  const RECONNECT_RETRY_INTERVAL_MS = 2500;         // gap between attempts
  const RECONNECT_PEER_OPEN_TIMEOUT_MS = 8000;      // each peer-create attempt
  const RECONNECT_CONN_OPEN_TIMEOUT_MS = 6000;      // each conn-open attempt

  class SessionTransfer extends Emitter {
    /**
     * @param {Object} opts
     * @param {'peerjs'|'raw'} [opts.backend='peerjs']
     * @param {Array} [opts.iceServers] - Custom ICE servers for raw WebRTC
     * @param {Object} [opts.peerServer] - PeerJS signaling server config. When
     *   omitted, PeerJS defaults to its public cloud (0.peerjs.com).
     *   Shape: { host, port, path, secure, key } — only `host` is typically
     *   required. Example for a self-hosted server at TU Delft:
     *     peerServer: {
     *       host: 'webshare-signal.tudelft.nl',
     *       port: 443,
     *       path: '/peerjs',
     *       secure: true
     *     }
     */
    constructor({ backend = 'peerjs', iceServers, peerServer, peerInfo } = {}) {
      super();
      if (backend !== 'peerjs' && backend !== 'raw') {
        throw new Error(`Unknown backend "${backend}". Use 'peerjs' or 'raw'.`);
      }
      this.backend = backend;
      this.rtcConfig = iceServers ? { iceServers } : DEFAULT_RTC_CONFIG;
      this.peerServerConfig = peerServer || null;

      // role gets set by startReceiving() or connect()
      this.role = null;       // 'receiver' or 'sender'
      this.peerId = null;     // PeerJS: assigned peer ID; Raw: not used
      this.onPayload = null;  // receiver handler
      this.onAck = null;      // sender ack handler

      // Identity exchange. The library doesn't decide what the local info
      // looks like — that's the consumer's job (DEVICE_TYPE detection,
      // user-set emoji/nickname, etc). The library just guarantees both
      // sides receive each other's info before the payload completes.
      this.peerInfo = peerInfo || null;       // OUR info, sent to remote
      this._remotePeerInfo = null;            // their info, received from remote
      this._peerInfoSent = false;             // flag: have we sent ours yet?

      // Per-instance random session token, used to verify reconnects come
      // from the SAME peer. If someone else got hold of our peer ID (e.g.
      // via a copied QR) and tries to connect during a reconnect window,
      // their peer-info will carry a different token and we reject. Not a
      // cryptographic auth, just a "is this the same browser tab as
      // before" check — enough for the threat model of a casual QR copy.
      this._sessionToken = Math.random().toString(36).slice(2) + Math.random().toString(36).slice(2);
      this._remoteSessionToken = null;        // peer's token, learned at first peer-info

      // Backend-specific state
      this._peer = null;          // PeerJS Peer
      this._conn = null;          // PeerJS DataConnection
      this._pc = null;            // Raw WebRTC PeerConnection
      this._channel = null;       // Raw WebRTC DataChannel
      this._reassembler = new ChunkReassembler();
      this._pendingSend = null;   // payload waiting for raw channel to open
      this._sendResolve = null;   // resolves when raw send completes

      // Once the data channel is open, the signaling peer (PeerJS) is no
      // longer needed — the data flows directly via WebRTC. We destroy
      // the signaling peer at that point so the QR code (which encodes
      // our peer ID) is no longer a valid connection target. Anyone who
      // captured the QR mid-transfer or later cannot use it to reach us.
      // This flag tells the 'disconnected' handler not to attempt a
      // reconnect, since the disconnection was deliberate.
      this._releasedSignalingPeer = false;

      // ---- Heartbeat / liveness ----
      // Mobile WebRTC quirk: when a tab is backgrounded, the OS freezes
      // its JS. ICE keepalives stop, the channel dies on the OTHER side,
      // but the frozen side never sees a 'close' event because no code
      // ran. When it resumes, _conn.open may still falsely report true.
      // The fix: a small application-level heartbeat that times out if
      // the peer goes silent, surfaced via the existing 'disconnected'
      // event so the rest of the app reacts identically to a clean
      // close.
      this._heartbeatTimer = null;       // interval handle while alive
      this._lastPongAt = 0;              // ms; last time we heard from peer
      this._heartbeatDead = false;       // latch to avoid multi-emit

      // ---- Reconnect state ----
      this._targetPeerId = null;         // sender: receiver's id, for re-issuing peer.connect()
      this._reconnecting = false;        // are we currently in a reconnect loop?
      this._reconnectDeadline = 0;       // ms; absolute wall-clock time after which we give up
      this._reconnectTimer = null;
      this._reconnectAttempt = 0;        // for logging
      // Set when close() is called locally so the close handler on the
      // dying conn doesn't kick off a reconnect.
      this._userInitiatedDisconnect = false;
      // Set when an incoming connection during reconnect failed the
      // identity check; the close handler uses this to surface the right
      // reason instead of attempting another reconnect on that conn.
      this._identityMismatch = false;

      // Wire start/stop to the connection lifecycle. Listening on our own
      // events means a single hook covers every code path that emits
      // 'connected' (peerjs sender, peerjs receiver, raw both sides).
      this.on('connected', () => {
        this._startHeartbeat();
        // If we got here via reconnect, clear the loop state and let any
        // external observers know we're back. The 'reconnected' event is
        // a convenience for consumers that want to push state back.
        if (this._reconnecting) {
          this._reconnecting = false;
          this._reconnectAttempt = 0;
          this._reconnectDeadline = 0;
          if (this._reconnectTimer) { clearTimeout(this._reconnectTimer); this._reconnectTimer = null; }
          this._log('ok', 'Reconnect successful.');
          this.emit('reconnected');
        }
      });
      this.on('disconnected', () => this._stopHeartbeat());
    }

    // Disconnect the signaling WebSocket (PeerJS) WITHOUT closing the
    // active data channel. After this, the peer ID is no longer reachable
    // for NEW connections, but the already-established WebRTC connection
    // continues to flow because PeerJS only needs the signaling server
    // for setup, not for ongoing transport.
    //
    // IMPORTANT: We must use peer.disconnect() here, NOT peer.destroy().
    // Calling .destroy() tears down the DataConnection too, which would
    // break the transfer mid-flight. .disconnect() only closes the
    // WebSocket to peerjs.com, which is precisely what we want.
    _releaseSignalingPeer() {
      if (this._releasedSignalingPeer) return;
      if (!this._peer) return;
      this._releasedSignalingPeer = true;
      try { this._peer.disconnect(); } catch {}
      this._log('info', 'Signaling peer released; QR code is no longer reachable.');
    }

    _log(level, message) { this.emit('log', { level, message }); }
    _status(s) { this.emit('status', s); }

    // ----- Peer identity exchange -----
    // Called from each backend's channel-open path. Sends our peerInfo over
    // the just-opened data channel. The message format intentionally mirrors
    // the payload/ack envelope (`{type, ...}`) so both backends route it
    // through their existing message handling.
    _sendPeerInfo() {
      // Always allow re-send on reconnect. The _peerInfoSent flag only
      // matters for first-connect to avoid double-sends; on reconnect we
      // explicitly clear it (see _resetForReconnect) so this method
      // re-sends. Both sides include their session token so the peer can
      // verify identity across reconnects.
      if (this._peerInfoSent) return;
      if (!this.peerInfo) return;  // nothing to send
      const msg = { type: 'peer-info', info: this.peerInfo, token: this._sessionToken };
      try {
        if (this.backend === 'peerjs') {
          this._conn.send(msg);
        } else if (this._channel && this._channel.readyState === 'open') {
          this._channel.send(JSON.stringify(msg));
        } else {
          return;  // channel not ready; will retry on next open event
        }
        this._peerInfoSent = true;
        this._log('info', 'Sent peer-info to remote.');
      } catch (e) {
        this._log('err', 'Failed to send peer-info: ' + e.message);
      }
    }

    // Called from each backend's message handler when a peer-info message
    // arrives. Stores the remote info, emits the 'peer-info' event so the
    // consumer can react (e.g. render the device icons).
    //
    // On reconnect: if we already have a remote token from a previous
    // connection and the incoming token doesn't match, this is NOT the
    // same peer — reject the connection. Returns true if accepted, false
    // if rejected (so callers can stop further processing).
    _handlePeerInfo(info, token) {
      if (this._remoteSessionToken && token && token !== this._remoteSessionToken) {
        this._log('err', 'Identity mismatch on reconnect — rejecting connection.');
        // Close the offending conn/channel. Don't emit our own
        // 'disconnected' here: the close handler on the conn will do that
        // (or the reconnect retry loop will keep going). We just need to
        // not adopt this conn as a legit reconnect.
        try {
          if (this._conn) this._conn.close();
          if (this._channel) this._channel.close();
        } catch {}
        // Mark this so the close handler knows the disconnect reason.
        this._identityMismatch = true;
        this.emit('identity-mismatch');
        return false;
      }
      this._remotePeerInfo = info || null;
      if (token) this._remoteSessionToken = token;
      this._log('info', 'Received peer-info from remote.');
      this.emit('peer-info', this._remotePeerInfo);
      return true;
    }

    // Public getter for consumers who want to query state directly
    // instead of listening for the event.
    getRemotePeerInfo() { return this._remotePeerInfo; }

    // ----- RECEIVER -----
    async startReceiving() {
      if (this.role) throw new Error('Already started.');
      this.role = 'receiver';
      this._status('registering');
      if (this.backend === 'peerjs') await this._startPeerJSReceive();
      else                            await this._startRawReceive();
    }

    async _startPeerJSReceive() {
      this._peer = await createPeerWithRetry(this, this.peerServerConfig);
      this.peerId = this._peer.id;
      this._log('ok', `Peer ID assigned: ${this.peerId}`);
      this._status('waiting');
      this.emit('show-qr', { kind: 'peerid', text: this.peerId });
      this._wireReceiverPeerEvents();
    }

    // Wire peer-level handlers (incoming connection, error, signaling
    // auto-reconnect) on the current this._peer. Extracted so the
    // reconnect path can re-wire a fresh peer object with the same
    // handlers.
    _wireReceiverPeerEvents() {
      this._peer.on('connection', (conn) => {
        this._conn = conn;
        this._log('info', `Incoming connection from ${conn.peer}`);
        this._status('connecting');
        this._wireReceiverConn(conn);
      });
      this._peer.on('error', (err) => {
        this._log('err', `PeerJS error: ${err.type}`);
        // During a reconnect, peer errors are expected (the remote may
        // not be back yet) — don't surface them as fatal here. The
        // reconnect loop's deadline decides when to give up.
        if (!this._reconnecting) this._status('error');
      });
      this._peer.on('disconnected', () => {
        // If we deliberately released the signaling peer, this is a normal
        // shutdown — don't try to reconnect (that would re-register our
        // peer ID, defeating the whole point of releasing it).
        if (this._releasedSignalingPeer) return;
        // While in our application-level reconnect loop, the loop itself
        // handles re-registration via _restartPeer; PeerJS' built-in
        // reconnect would race against ours.
        if (this._reconnecting) return;
        this._log('info', 'Disconnected from signaling, reconnecting…');
        try { this._peer.reconnect(); } catch {}
      });
    }

    // Wire per-conn handlers for a receiver. Extracted from the inline
    // peer.on('connection') body so a fresh conn (e.g. from a reconnect)
    // gets the same wiring.
    _wireReceiverConn(conn) {
      conn.on('open', () => {
        this._log('ok', 'Data channel open.');
        this._status('connected');
        this.emit('connected');
        // Send our peerInfo immediately on channel open. Don't block —
        // the payload can still flow before peer-info round-trips.
        this._sendPeerInfo();
        // Drop the signaling peer now that the data channel is up.
        // The QR code's peer ID is no longer reachable after this —
        // important so that anyone who photographed the QR mid-transfer
        // or later can't connect using it. The data channel itself
        // survives because WebRTC doesn't need the signaling server
        // once the connection is established.
        this._releaseSignalingPeer();
      });
      conn.on('data', async (msg) => {
        // Stale-conn guard: a successful reconnect replaced this._conn
        // with a fresh one; ignore late messages on the old conn.
        if (this._conn !== conn) return;
        if (msg && msg.type === 'peer-info') {
          this._handlePeerInfo(msg.info, msg.token);
          return;
        }
        if (msg && msg.type === 'note') {
          this.emit('note', { text: String(msg.text == null ? '' : msg.text) });
          return;
        }
        if (msg && msg.type === 'lamp') {
          this.emit('lamp', { on: !!msg.on });
          return;
        }
        if (msg && msg.type === 'ping') {
          try {
            if (this._conn && this._conn.open !== false) this._conn.send({ type: 'pong' });
          } catch {}
          return;
        }
        if (msg && msg.type === 'pong') {
          this._lastPongAt = Date.now();
          return;
        }
        if (msg && msg.type === 'bye') {
          this._log('info', 'Peer said bye (intentional disconnect).');
          this._handleDeath('peer-bye');
          return;
        }
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
      conn.on('close', () => {
        if (this._conn !== conn) {
          this._log('info', 'Stale conn closed (ignored).');
          return;
        }
        if (this._identityMismatch) {
          this._identityMismatch = false;
          this._handleDeath('identity-mismatch');
          return;
        }
        this._log('info', 'Sender disconnected.');
        this._handleDeath('channel-closed');
      });
      conn.on('error', (e) => {
        if (this._conn !== conn) return;
        this._log('err', 'Conn error: ' + e.message);
        this._handleDeath('channel-error');
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
        this._sendPeerInfo();
      };
      ch.onmessage = async (e) => {
        if (typeof e.data === 'string') {
          const msg = JSON.parse(e.data);
          if (msg.type === 'peer-info') {
            this._handlePeerInfo(msg.info, msg.token);
            return;
          }
          if (msg.type === 'note') {
            this.emit('note', { text: String(msg.text == null ? '' : msg.text) });
            return;
          }
          if (msg && msg.type === 'lamp') {
            this.emit('lamp', { on: !!msg.on });
            return;
          }
          if (msg && msg.type === 'ping') {
            // Heartbeat ping from peer — echo a pong. No UI emit.
            try {
              if (this.backend === 'peerjs') {
                if (this._conn && this._conn.open !== false) this._conn.send({ type: 'pong' });
              } else if (this._channel && this._channel.readyState === 'open') {
                this._channel.send(JSON.stringify({ type: 'pong' }));
              }
            } catch {}
            return;
          }
          if (msg && msg.type === 'pong') {
            // Heartbeat pong from peer — record liveness, no UI emit.
            this._lastPongAt = Date.now();
            return;
          }
          if (msg.type === 'bye') {
            this._log('info', 'Peer said bye (intentional disconnect).');
            this.emit('disconnected', { reason: 'peer-bye' });
            return;
          }
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
      // Remember the target so we can re-issue peer.connect() if the
      // channel later dies and we need to reconnect.
      this._targetPeerId = targetPeerId;
      this._status('connecting');
      this._log('info', `Connecting to "${targetPeerId}"…`);
      const peerOptions = Object.assign({ debug: 1 }, this.peerServerConfig || {});
      this._peer = new Peer(peerOptions);
      await new Promise((resolve, reject) => {
        this._peer.on('open', resolve);
        this._peer.on('error', (err) => reject(new Error(err.type + ': ' + (err.message || ''))));
      });
      this._conn = this._peer.connect(targetPeerId, { reliable: true });
      // CRITICAL: Register the data handler BEFORE awaiting the channel
      // open. PeerJS does not buffer messages received between channel-open
      // and handler registration — so if the receiver's peer-info arrives
      // during that gap (the receiver fires its own peer-info immediately
      // on its open event), the message is dropped and we end up showing
      // the receiver as "unknown" in the UI.
      this._wireSenderConn(this._conn);
      await new Promise((resolve, reject) => {
        this._conn.on('open', resolve);
        this._conn.on('error', reject);
        setTimeout(() => reject(new Error('Connection timed out')), 15000);
      });
      this._log('ok', 'Connected.');
      this._status('connected');
      this.emit('connected');
      // Send our peerInfo right after the channel opens.
      this._sendPeerInfo();
      // Drop the signaling peer — same rationale as the receiver. The
      // sender's auto-generated peer ID isn't shown to the user, but
      // it's still registered with peerjs.com and would persist for as
      // long as the WebSocket stays open. No reason to keep it alive.
      this._releaseSignalingPeer();
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

    /**
     * Send a short, live-updating "note" to the peer over the existing data
     * channel. Best-effort: silently no-ops if the channel isn't open yet.
     * Independent of the payload protocol — does not change transfer state.
     * The peer receives this as a 'note' event with { text }.
     */
    sendNote(text) {
      const msg = { type: 'note', text: String(text == null ? '' : text) };
      if (this.backend === 'peerjs') {
        if (!this._conn || this._conn.open === false) return;
        try { this._conn.send(msg); } catch {}
      } else {
        if (this._channel && this._channel.readyState === 'open') {
          try { this._channel.send(JSON.stringify(msg)); } catch {}
        }
      }
    }

    /**
     * Send a shared lamp on/off state to the peer. Same delivery semantics
     * as sendNote — best-effort, no impact on the transfer protocol. The
     * peer receives this as a 'lamp' event with { on: boolean }.
     */
    sendLamp(on) {
      const msg = { type: 'lamp', on: !!on };
      if (this.backend === 'peerjs') {
        if (!this._conn || this._conn.open === false) return;
        try { this._conn.send(msg); } catch {}
      } else {
        if (this._channel && this._channel.readyState === 'open') {
          try { this._channel.send(JSON.stringify(msg)); } catch {}
        }
      }
    }

    // ----- Heartbeat / liveness -----
    // Public: is the channel believed to be alive right now?
    //  - PeerJS: needs _conn.open === true AND a recent pong.
    //  - Raw:    needs _channel.readyState === 'open' AND a recent pong.
    // The pong recency check is what catches the mobile-freeze case: the
    // underlying flag may still claim 'open' after a frozen resume.
    isAlive() {
      const now = Date.now();
      // Grace window for the case where we *just* connected but no pong
      // has had time to round-trip yet. The first heartbeat tick happens
      // 1s after connect; allow ~3x that before declaring stale.
      const sinceLastPong = this._lastPongAt ? (now - this._lastPongAt) : 0;
      const pongOk = !this._lastPongAt || sinceLastPong < HEARTBEAT_DEAD_MS;
      if (this.backend === 'peerjs') {
        if (!this._conn || this._conn.open === false) return false;
        return pongOk;
      } else {
        if (!this._channel || this._channel.readyState !== 'open') return false;
        return pongOk;
      }
    }

    _startHeartbeat() {
      // Idempotent — replace any existing timer.
      this._stopHeartbeat();
      // Reset state at the start of a fresh connection. The pong timestamp
      // is seeded to now so the first tick (1s away) doesn't immediately
      // declare us dead before any pong had a chance to arrive.
      this._lastPongAt = Date.now();
      this._heartbeatDead = false;
      this._heartbeatTimer = setInterval(() => {
        this._heartbeatTick();
      }, HEARTBEAT_INTERVAL_MS);
    }

    _stopHeartbeat() {
      if (this._heartbeatTimer) {
        clearInterval(this._heartbeatTimer);
        this._heartbeatTimer = null;
      }
    }

    _heartbeatTick() {
      const now = Date.now();
      const sinceLastPong = now - this._lastPongAt;
      if (sinceLastPong > HEARTBEAT_DEAD_MS) {
        if (this._heartbeatDead) return;  // already emitted, wait for cleanup
        this._heartbeatDead = true;
        this._log('err', `Heartbeat timeout (${sinceLastPong}ms since last pong) — declaring dead.`);
        // Tear down the underlying connection as a best-effort cleanup,
        // then route to the central death handler. _handleDeath decides
        // whether to reconnect or emit 'disconnected'.
        try {
          if (this._conn) this._conn.close();
          if (this._channel) this._channel.close();
        } catch {}
        this._handleDeath('heartbeat-timeout');
        return;
      }
      // Send the next ping. Best-effort: if send fails, the next tick will
      // notice the silence and time out.
      const msg = { type: 'ping' };
      try {
        if (this.backend === 'peerjs') {
          if (this._conn && this._conn.open !== false) this._conn.send(msg);
        } else {
          if (this._channel && this._channel.readyState === 'open') {
            this._channel.send(JSON.stringify(msg));
          }
        }
      } catch {}
    }

    // -----------------------------------------------------------------
    // Central death handler & reconnect machinery
    // -----------------------------------------------------------------
    // Every "connection is dead" code path funnels through here so the
    // policy is in one place: try to reconnect when it makes sense, give
    // up otherwise. Idempotent — repeated calls during a reconnect loop
    // are harmless.
    _handleDeath(reason) {
      if (this._reconnecting) return;  // already trying
      if (this._userInitiatedDisconnect) {
        // We closed locally. No reconnect — just surface the close.
        this.emit('disconnected', { reason });
        return;
      }
      // Fatal reasons skip reconnect entirely.
      if (reason === 'peer-bye' || reason === 'identity-mismatch') {
        this.emit('disconnected', { reason });
        return;
      }
      // Reconnect is PeerJS-only; raw needs a fresh QR handshake.
      if (this.backend !== 'peerjs') {
        this.emit('disconnected', { reason });
        return;
      }
      // Sender needs to know who to reconnect to. (Set in connect().)
      if (this.role === 'sender' && !this._targetPeerId) {
        this.emit('disconnected', { reason });
        return;
      }
      // Peer must still be alive enough to either reconnect signaling or
      // accept a new conn.
      if (!this._peer || this._peer.destroyed) {
        this.emit('disconnected', { reason });
        return;
      }
      this._beginReconnect(reason);
    }

    _beginReconnect(reason) {
      this._reconnecting = true;
      this._reconnectAttempt = 0;
      this._reconnectDeadline = Date.now() + RECONNECT_TOTAL_DEADLINE_MS;
      this._stopHeartbeat();
      // Tear down the dying conn immediately. The new conn (from the
      // recreated peer) will replace it.
      try { if (this._conn) this._conn.close(); } catch {}
      this._conn = null;
      // peer-info needs to be re-sent on the new conn.
      this._peerInfoSent = false;
      // We'll release signaling again after the new conn opens.
      this._releasedSignalingPeer = false;
      this._log('info', `Reconnecting (trigger: ${reason}). Deadline ${Math.round(RECONNECT_TOTAL_DEADLINE_MS/1000)}s.`);
      this.emit('reconnecting', { reason });
      this._attemptReconnect();
    }

    // Each attempt rebuilds the Peer object from scratch and (for senders)
    // tries to re-open the data connection. PeerJS' peer.reconnect() is
    // unreliable after a mobile freeze — the WebSocket was killed by the
    // OS but the library's internal state still claims healthy. Recreating
    // the Peer sidesteps that stale state entirely.
    _attemptReconnect() {
      if (!this._reconnecting) return;
      this._reconnectAttempt++;
      if (Date.now() > this._reconnectDeadline) {
        this._log('err', `Reconnect deadline reached (${this._reconnectAttempt - 1} attempts). Giving up.`);
        this._reconnecting = false;
        this.emit('disconnected', { reason: 'reconnect-failed' });
        return;
      }
      const remainingMs = this._reconnectDeadline - Date.now();
      this._log('info', `Reconnect attempt ${this._reconnectAttempt} (${Math.round(remainingMs/1000)}s left)…`);
      this._recreatePeerForReconnect()
        .then((ok) => {
          if (!this._reconnecting) return;
          if (!ok) {
            // Peer recreation failed — schedule the next try.
            this._reconnectTimer = setTimeout(() => this._attemptReconnect(), RECONNECT_RETRY_INTERVAL_MS);
            return;
          }
          if (this.role === 'sender') {
            this._attemptSenderDataConn();
          } else {
            // Receiver is now re-registered and listening. If the sender
            // reconnects within our remaining deadline, the
            // peer.on('connection') handler picks it up and emits
            // 'connected', which resets _reconnecting via the
            // constructor's listener. Otherwise, we re-check in a moment
            // — the peer may have silently disconnected again, in which
            // case we'll rebuild it on the next attempt.
            this._reconnectTimer = setTimeout(() => this._attemptReconnect(), RECONNECT_RETRY_INTERVAL_MS);
          }
        })
        .catch((e) => {
          this._log('err', 'Reconnect attempt threw: ' + (e && e.message ? e.message : e));
          if (!this._reconnecting) return;
          this._reconnectTimer = setTimeout(() => this._attemptReconnect(), RECONNECT_RETRY_INTERVAL_MS);
        });
    }

    // Tear down the old Peer (if any) and create a fresh one. For the
    // receiver we re-register the SAME id so the sender can still find
    // us; for the sender we let PeerJS pick a new id. Resolves true on
    // success, false on registration failure (e.g. id taken).
    async _recreatePeerForReconnect() {
      // Tear down old peer if it exists. peer.destroy() is idempotent.
      if (this._peer) {
        try { this._peer.destroy(); } catch {}
        this._peer = null;
      }
      const peerOptions = Object.assign({ debug: 1 }, this.peerServerConfig || {});
      let peer;
      try {
        if (this.role === 'receiver' && this.peerId) {
          peer = new Peer(this.peerId, peerOptions);
        } else {
          peer = new Peer(peerOptions);
        }
      } catch (e) {
        this._log('err', 'new Peer threw: ' + e.message);
        return false;
      }
      // Wait for open or error, with a timeout.
      const opened = await new Promise((resolve) => {
        let done = false;
        const finish = (result) => {
          if (done) return;
          done = true;
          try { peer.off('open', onOpen); peer.off('error', onError); } catch {}
          resolve(result);
        };
        const onOpen = () => finish(true);
        const onError = (err) => {
          this._log('err', `Peer registration error: ${err && err.type ? err.type : err}`);
          finish(false);
        };
        peer.on('open', onOpen);
        peer.on('error', onError);
        setTimeout(() => finish(false), RECONNECT_PEER_OPEN_TIMEOUT_MS);
      });
      if (!opened) {
        try { peer.destroy(); } catch {}
        return false;
      }
      this._peer = peer;
      // The receiver needs peer-level handlers wired on every new peer.
      if (this.role === 'receiver') {
        this._wireReceiverPeerEvents();
      }
      return true;
    }

    // Sender-specific: after the fresh peer is open, try to re-establish
    // the data connection to the same target.
    _attemptSenderDataConn() {
      if (!this._peer || this._peer.destroyed) {
        this._reconnectTimer = setTimeout(() => this._attemptReconnect(), RECONNECT_RETRY_INTERVAL_MS);
        return;
      }
      let conn;
      try {
        conn = this._peer.connect(this._targetPeerId, { reliable: true });
      } catch (e) {
        this._log('err', 'peer.connect threw on reconnect: ' + e.message);
        this._reconnectTimer = setTimeout(() => this._attemptReconnect(), RECONNECT_RETRY_INTERVAL_MS);
        return;
      }
      this._conn = conn;
      this._wireSenderConn(conn);
      let settled = false;
      const giveUpThisAttempt = (whyLog) => {
        if (settled) return;
        settled = true;
        if (whyLog) this._log('info', whyLog);
        try { conn.close(); } catch {}
        if (this._conn === conn) this._conn = null;
        this._reconnectTimer = setTimeout(() => this._attemptReconnect(), RECONNECT_RETRY_INTERVAL_MS);
      };
      conn.on('open', () => {
        if (settled) return;
        settled = true;
        this._log('ok', 'Reconnect: data channel open.');
        this._status('connected');
        this.emit('connected');
        this._sendPeerInfo();
        this._releaseSignalingPeer();
      });
      conn.on('error', (e) => {
        giveUpThisAttempt('Reconnect conn error: ' + (e && e.type ? e.type : 'unknown'));
      });
      setTimeout(() => giveUpThisAttempt('Reconnect conn open timeout.'), RECONNECT_CONN_OPEN_TIMEOUT_MS);
    }

    // Public API: called by the UI when the tab regains focus. If the
    // channel is silently dead (frozen-then-resumed case), kicks off
    // reconnect. Returns true if a recovery flow was started, false if
    // we're already alive (no-op) or in another state.
    checkAndRecover() {
      if (!this.role) return false;
      if (this._reconnecting) return true;
      if (this.isAlive()) return false;
      this._handleDeath('visibility-detected-dead');
      return true;
    }

    // Refresh the shared conn handler bindings for the sender. Extracted
    // from connect() so the reconnect path can reuse the same wiring on
    // a fresh DataConnection.
    _wireSenderConn(conn) {
      conn.on('data', (msg) => {
        // If this conn was already replaced by a successful reconnect,
        // any late-arriving message on the stale conn is ignored.
        if (this._conn !== conn) return;
        if (msg && msg.type === 'peer-info') {
          this._handlePeerInfo(msg.info, msg.token);
          return;
        }
        if (msg && msg.type === 'note') {
          this.emit('note', { text: String(msg.text == null ? '' : msg.text) });
          return;
        }
        if (msg && msg.type === 'lamp') {
          this.emit('lamp', { on: !!msg.on });
          return;
        }
        if (msg && msg.type === 'ping') {
          try {
            if (this._conn && this._conn.open !== false) this._conn.send({ type: 'pong' });
          } catch {}
          return;
        }
        if (msg && msg.type === 'pong') {
          this._lastPongAt = Date.now();
          return;
        }
        if (msg && msg.type === 'bye') {
          this._log('info', 'Peer said bye (intentional disconnect).');
          this._handleDeath('peer-bye');
          return;
        }
        if (msg && msg.type === 'ack') {
          if (this.onAck) try { this.onAck(msg.response, msg.error); } catch (e) { console.error(e); }
          this._status('done');
        }
      });
      conn.on('close', () => {
        // Stale-conn guard: a successful reconnect replaced this._conn
        // with a fresh one. The old conn's belated close should NOT
        // re-trigger the death machinery.
        if (this._conn !== conn) {
          this._log('info', 'Stale conn closed (ignored).');
          return;
        }
        if (this._identityMismatch) {
          this._identityMismatch = false;
          this._handleDeath('identity-mismatch');
          return;
        }
        this._log('info', 'Channel closed.');
        this._handleDeath('channel-closed');
      });
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
        this._sendPeerInfo();
        if (this._pendingSend) {
          const p = this._pendingSend; this._pendingSend = null;
          await this._rawSendNow(p);
        }
      };
      ch.onmessage = (e) => {
        if (typeof e.data === 'string') {
          const msg = JSON.parse(e.data);
          if (msg.type === 'peer-info') {
            this._handlePeerInfo(msg.info, msg.token);
            return;
          }
          if (msg.type === 'note') {
            this.emit('note', { text: String(msg.text == null ? '' : msg.text) });
            return;
          }
          if (msg && msg.type === 'lamp') {
            this.emit('lamp', { on: !!msg.on });
            return;
          }
          if (msg && msg.type === 'ping') {
            // Heartbeat ping from peer — echo a pong. No UI emit.
            try {
              if (this.backend === 'peerjs') {
                if (this._conn && this._conn.open !== false) this._conn.send({ type: 'pong' });
              } else if (this._channel && this._channel.readyState === 'open') {
                this._channel.send(JSON.stringify({ type: 'pong' }));
              }
            } catch {}
            return;
          }
          if (msg && msg.type === 'pong') {
            // Heartbeat pong from peer — record liveness, no UI emit.
            this._lastPongAt = Date.now();
            return;
          }
          if (msg.type === 'bye') {
            this._log('info', 'Peer said bye (intentional disconnect).');
            this.emit('disconnected', { reason: 'peer-bye' });
            return;
          }
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
      // Tell the close-handler not to try a reconnect — this is a
      // local-initiated teardown.
      this._userInitiatedDisconnect = true;
      // Stop the heartbeat first so no late tick fires after we've torn
      // down. The 'disconnected' listener in our own constructor also
      // stops it, but that only fires if some path emits — close() can
      // be called without emitting (e.g. local Disconnect button).
      this._stopHeartbeat();
      // Cancel any in-flight reconnect.
      if (this._reconnectTimer) { clearTimeout(this._reconnectTimer); this._reconnectTimer = null; }
      this._reconnecting = false;
      // Send a "bye" message to the peer if the channel is still open. This
      // lets the remote side distinguish an intentional disconnect from a
      // network blip — though for now both are treated the same in the
      // app, this is useful information to surface. Best-effort: if the
      // channel is already closed (or send throws for any reason) we just
      // move on with the teardown.
      try {
        if (this._conn && this._conn.open) {
          this._conn.send({ type: 'bye' });
        } else if (this._channel && this._channel.readyState === 'open') {
          this._channel.send(JSON.stringify({ type: 'bye' }));
        }
      } catch {}
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
