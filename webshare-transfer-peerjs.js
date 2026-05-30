/**
 * webshare-transfer-peerjs.js
 *
 * PeerJS transfer backend. Extends CoreTransfer with:
 *  - Friendly peer-ID generation and signaling-server registration
 *  - PeerJS DataConnection setup for both receiver and sender roles
 *  - Signaling-peer release after channel open (so QR codes expire)
 *  - Full reconnect machinery (destroy+recreate Peer objects)
 *
 * Depends on CoreTransfer (webshare-transfer-core.js) and the global
 * Peer constructor from PeerJS.
 *
 * Exported global: PeerJSTransfer, generateFriendlyId
 */
(function (global) {
  'use strict';

  // -----------------------------------------------------------------------
  // Friendly peer-ID generator
  // -----------------------------------------------------------------------
  const ADJ  = ['tomato','rabbit','cactus','river','copper','silent','mango','swift',
                 'velvet','lemon','crimson','golden','quiet','amber','linen','rusty',
                 'silken','marble'];
  const NOUN = ['fox','sparrow','oak','meadow','harbor','comet','reef','prairie',
                'glacier','willow','flint','arrow','ember','thicket','quill',
                'beacon','lattice','pebble'];

  function generateFriendlyId() {
    const n = Math.floor(Math.random() * 90) + 10;
    return `idb-${ADJ[Math.floor(Math.random()*ADJ.length)]}-${NOUN[Math.floor(Math.random()*NOUN.length)]}-${n}`;
  }

  // Register a new Peer with the PeerJS signaling server, retrying on ID
  // collision. Returns a Promise that resolves to the open Peer object.
  function createPeerWithRetry(emitter, peerServerConfig, maxAttempts = 5) {
    return new Promise((resolve, reject) => {
      let attempt = 0;
      const tryOnce = () => {
        attempt++;
        const peerId = generateFriendlyId();
        emitter.emit('log', { level: 'info', message: `Registering "${peerId}" (attempt ${attempt}/${maxAttempts})…` });
        const peerOptions = Object.assign({ debug: 1 }, peerServerConfig || {});
        const peer = new Peer(peerId, peerOptions);
        const cleanup = () => { peer.off('open', onOpen); peer.off('error', onError); };
        const onOpen = () => { cleanup(); resolve(peer); };
        const onError = (err) => {
          if (err.type === 'unavailable-id' && attempt < maxAttempts) {
            emitter.emit('log', { level: 'info', message: 'ID taken, retrying…' });
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

  // -----------------------------------------------------------------------
  // Reconnect constants
  // -----------------------------------------------------------------------
  // The reconnect strategy is destroy-and-recreate: PeerJS' peer.reconnect()
  // is unreliable after a mobile tab freeze, so we tear down the peer object
  // entirely and create a new one with the same id (receiver) or a fresh id
  // (sender).
  const RECONNECT_TOTAL_DEADLINE_MS  = 60000;  // give up after 60 s wall-clock
  const RECONNECT_RETRY_INTERVAL_MS  = 2500;   // gap between attempts
  const RECONNECT_PEER_OPEN_TIMEOUT_MS = 8000; // each peer-create attempt
  const RECONNECT_CONN_OPEN_TIMEOUT_MS = 6000; // each conn-open attempt

  // -----------------------------------------------------------------------
  // PeerJSTransfer
  // -----------------------------------------------------------------------
  class PeerJSTransfer extends CoreTransfer {
    /**
     * @param {Object} opts
     * @param {Object} [opts.peerServer] - Custom PeerJS signaling server config:
     *   { host, port, path, secure, key }. Defaults to peerjs.com cloud.
     * @param {Object} [opts.peerInfo]   - Identity blob sent to the remote peer.
     * @param {Array}  [opts.iceServers] - Custom ICE servers (for NAT traversal).
     */
    constructor({ peerServer, peerInfo, iceServers } = {}) {
      super({ iceServers, peerInfo });
      this.peerServerConfig = peerServer || null;

      // PeerJS state
      this._peer                  = null;  // Peer signaling object
      this._conn                  = null;  // DataConnection to remote
      this._releasedSignalingPeer = false;
      this._targetPeerId          = null;  // sender: where to reconnect to

      // Identity check flag (set by _handlePeerInfo on mismatch)
      this._identityMismatch = false;

      // Reconnect state
      this._userInitiatedDisconnect = false;
      this._reconnecting            = false;
      this._reconnectDeadline       = 0;
      this._reconnectTimer          = null;
      this._reconnectAttempt        = 0;

      // Reset reconnect state when a (re)connection succeeds, and let any
      // external observers know we're back.
      this.on('connected', () => {
        if (this._reconnecting) {
          this._reconnecting     = false;
          this._reconnectAttempt = 0;
          this._reconnectDeadline = 0;
          if (this._reconnectTimer) { clearTimeout(this._reconnectTimer); this._reconnectTimer = null; }
          this._log('ok', 'Reconnect successful.');
          this.emit('reconnected');
        }
      });
    }

    // -----------------------------------------------------------------------
    // Transport — CoreTransfer abstract interface
    // -----------------------------------------------------------------------

    // Send a plain-object message over the open PeerJS DataConnection.
    // PeerJS handles JSON serialisation for us, so we pass the object directly.
    _sendMessage(msg) {
      if (!this._conn || this._conn.open === false) {
        throw new Error('PeerJS channel not open');
      }
      this._conn.send(msg);
    }

    isAlive() {
      if (!this._conn || this._conn.open === false) return false;
      const sinceLastPong = this._lastPongAt ? (Date.now() - this._lastPongAt) : 0;
      // Grace: no pong yet → still true; stale pong → dead.
      return !this._lastPongAt || sinceLastPong < (global.HEARTBEAT_DEAD_MS || 9000);
    }

    checkAndRecover() {
      if (!this.role)            return false;
      if (this._reconnecting)    return true;
      if (this.isAlive())        return false;
      this._handleDeath('visibility-detected-dead');
      return true;
    }

    // -----------------------------------------------------------------------
    // Death handler & reconnect machinery (PeerJS-specific)
    // -----------------------------------------------------------------------

    // Override CoreTransfer._handleDeath — try to reconnect if possible.
    _handleDeath(reason) {
      if (this._reconnecting) return;
      if (this._userInitiatedDisconnect) { this.emit('disconnected', { reason }); return; }
      if (reason === 'peer-bye' || reason === 'identity-mismatch') {
        this.emit('disconnected', { reason });
        return;
      }
      if (this.role === 'sender' && !this._targetPeerId) {
        this.emit('disconnected', { reason });
        return;
      }
      if (!this._peer || this._peer.destroyed) {
        this.emit('disconnected', { reason });
        return;
      }
      this._beginReconnect(reason);
    }

    _beginReconnect(reason) {
      this._reconnecting      = true;
      this._reconnectAttempt  = 0;
      this._reconnectDeadline = Date.now() + RECONNECT_TOTAL_DEADLINE_MS;
      this._stopHeartbeat();
      try { if (this._conn) this._conn.close(); } catch {}
      this._conn                  = null;
      this._peerInfoSent          = false;
      this._releasedSignalingPeer = false;
      this._log('info', `Reconnecting (${reason}). Deadline ${Math.round(RECONNECT_TOTAL_DEADLINE_MS/1000)}s.`);
      this.emit('reconnecting', { reason });
      this._attemptReconnect();
    }

    _attemptReconnect() {
      if (!this._reconnecting) return;
      this._reconnectAttempt++;
      if (Date.now() > this._reconnectDeadline) {
        this._log('err', `Reconnect gave up after ${this._reconnectAttempt - 1} attempts.`);
        this._reconnecting = false;
        this.emit('disconnected', { reason: 'reconnect-failed' });
        return;
      }
      const left = Math.round((this._reconnectDeadline - Date.now()) / 1000);
      this._log('info', `Reconnect attempt ${this._reconnectAttempt} (${left}s left)…`);
      this._recreatePeerForReconnect()
        .then(ok => {
          if (!this._reconnecting) return;
          if (!ok) { this._reconnectTimer = setTimeout(() => this._attemptReconnect(), RECONNECT_RETRY_INTERVAL_MS); return; }
          if (this.role === 'sender') {
            this._attemptSenderDataConn();
          } else {
            // Receiver: peer is re-registered; wait for sender's incoming connection.
            this._reconnectTimer = setTimeout(() => this._attemptReconnect(), RECONNECT_RETRY_INTERVAL_MS);
          }
        })
        .catch(e => {
          this._log('err', 'Reconnect error: ' + (e && e.message ? e.message : e));
          if (this._reconnecting) {
            this._reconnectTimer = setTimeout(() => this._attemptReconnect(), RECONNECT_RETRY_INTERVAL_MS);
          }
        });
    }

    async _recreatePeerForReconnect() {
      if (this._peer) { try { this._peer.destroy(); } catch {} this._peer = null; }
      const opts = Object.assign({ debug: 1 }, this.peerServerConfig || {});
      let peer;
      try {
        peer = (this.role === 'receiver' && this.peerId)
          ? new Peer(this.peerId, opts)
          : new Peer(opts);
      } catch (e) {
        this._log('err', 'new Peer threw: ' + e.message);
        return false;
      }
      const opened = await new Promise(resolve => {
        let done = false;
        const finish = ok => {
          if (done) return;
          done = true;
          try { peer.off('open', onOpen); peer.off('error', onErr); } catch {}
          resolve(ok);
        };
        const onOpen = () => finish(true);
        const onErr  = err => { this._log('err', 'Peer error: ' + (err && err.type ? err.type : err)); finish(false); };
        peer.on('open', onOpen);
        peer.on('error', onErr);
        setTimeout(() => finish(false), RECONNECT_PEER_OPEN_TIMEOUT_MS);
      });
      if (!opened) { try { peer.destroy(); } catch {} return false; }
      this._peer = peer;
      if (this.role === 'receiver') this._wireReceiverPeerEvents();
      return true;
    }

    _attemptSenderDataConn() {
      if (!this._peer || this._peer.destroyed) {
        this._reconnectTimer = setTimeout(() => this._attemptReconnect(), RECONNECT_RETRY_INTERVAL_MS);
        return;
      }
      let conn;
      try {
        conn = this._peer.connect(this._targetPeerId, { reliable: true });
      } catch (e) {
        this._log('err', 'peer.connect threw: ' + e.message);
        this._reconnectTimer = setTimeout(() => this._attemptReconnect(), RECONNECT_RETRY_INTERVAL_MS);
        return;
      }
      this._conn = conn;
      this._wireSenderConn(conn);
      let settled = false;
      const give = why => {
        if (settled) return;
        settled = true;
        if (why) this._log('info', why);
        try { conn.close(); } catch {}
        if (this._conn === conn) this._conn = null;
        this._reconnectTimer = setTimeout(() => this._attemptReconnect(), RECONNECT_RETRY_INTERVAL_MS);
      };
      conn.on('open', () => {
        if (settled) return;
        settled = true;
        this._log('ok', 'Reconnect: channel open.');
        this._status('connected');
        this.emit('connected');
        this._sendPeerInfo();
        this._releaseSignalingPeer();
      });
      conn.on('error', e => give('Reconnect conn error: ' + (e && e.type ? e.type : 'unknown')));
      setTimeout(() => give('Reconnect conn open timeout.'), RECONNECT_CONN_OPEN_TIMEOUT_MS);
    }

    // -----------------------------------------------------------------------
    // Receiver flow
    // -----------------------------------------------------------------------

    async startReceiving() {
      if (this.role) throw new Error('Already started.');
      this.role = 'receiver';
      this._status('registering');
      await this._startPeerJSReceive();
    }

    async _startPeerJSReceive() {
      this._peer = await createPeerWithRetry(this, this.peerServerConfig);
      this.peerId = this._peer.id;
      this._log('ok', `Peer ID assigned: ${this.peerId}`);
      this._status('waiting');
      this.emit('show-qr', { kind: 'peerid', text: this.peerId });
      this._wireReceiverPeerEvents();
    }

    // Wire peer-level handlers on this._peer. Re-called on every new peer
    // object created during reconnect so the handlers stay attached.
    _wireReceiverPeerEvents() {
      this._peer.on('connection', conn => {
        this._conn = conn;
        this._log('info', `Incoming connection from ${conn.peer}`);
        this._status('connecting');
        this._wireReceiverConn(conn);
      });
      this._peer.on('error', err => {
        this._log('err', `PeerJS error: ${err.type}`);
        if (!this._reconnecting) this._status('error');
      });
      this._peer.on('disconnected', () => {
        if (this._releasedSignalingPeer) return;  // deliberate release — don't reconnect
        if (this._reconnecting) return;           // reconnect loop handles signaling
        this._log('info', 'Disconnected from signaling, reconnecting…');
        try { this._peer.reconnect(); } catch {}
      });
    }

    // Wire conn-level handlers on a receiver DataConnection. Extracted so
    // reconnect and initial connect use the same wiring.
    _wireReceiverConn(conn) {
      conn.on('open', () => {
        this._log('ok', 'Data channel open.');
        this._status('connected');
        this.emit('connected');
        this._sendPeerInfo();
        this._releaseSignalingPeer();
      });
      conn.on('data', msg => {
        if (this._conn !== conn) return;  // stale-conn guard
        this._handleMessage(msg);
      });
      conn.on('close', () => {
        if (this._conn !== conn) { this._log('info', 'Stale conn closed (ignored).'); return; }
        if (this._identityMismatch) { this._identityMismatch = false; this._handleDeath('identity-mismatch'); return; }
        this._log('info', 'Sender disconnected.');
        this._handleDeath('channel-closed');
      });
      conn.on('error', e => {
        if (this._conn !== conn) return;
        this._log('err', 'Conn error: ' + e.message);
        this._handleDeath('channel-error');
      });
    }

    // -----------------------------------------------------------------------
    // Sender flow
    // -----------------------------------------------------------------------

    async connect(targetPeerId) {
      if (this.role) throw new Error('Already started.');
      this.role = 'sender';
      this._targetPeerId = targetPeerId;
      this._status('connecting');
      this._log('info', `Connecting to "${targetPeerId}"…`);
      const opts = Object.assign({ debug: 1 }, this.peerServerConfig || {});
      this._peer = new Peer(opts);
      await new Promise((resolve, reject) => {
        this._peer.on('open', resolve);
        this._peer.on('error', err => reject(new Error(err.type + ': ' + (err.message || ''))));
      });
      this._conn = this._peer.connect(targetPeerId, { reliable: true });
      // Wire BEFORE awaiting open so we don't miss messages arriving in the gap.
      this._wireSenderConn(this._conn);
      await new Promise((resolve, reject) => {
        this._conn.on('open', resolve);
        this._conn.on('error', reject);
        setTimeout(() => reject(new Error('Connection timed out')), 15000);
      });
      this._log('ok', 'Connected.');
      this._status('connected');
      this.emit('connected');
      this._sendPeerInfo();
      this._releaseSignalingPeer();
    }

    // Wire the sender's data-conn handlers. Extracted from connect() so
    // the reconnect path can reuse it on a fresh DataConnection.
    _wireSenderConn(conn) {
      conn.on('data', msg => {
        if (this._conn !== conn) return;  // stale-conn guard
        this._handleMessage(msg);
      });
      conn.on('close', () => {
        if (this._conn !== conn) { this._log('info', 'Stale conn closed (ignored).'); return; }
        if (this._identityMismatch) { this._identityMismatch = false; this._handleDeath('identity-mismatch'); return; }
        this._log('info', 'Channel closed.');
        this._handleDeath('channel-closed');
      });
    }

    // Override CoreTransfer._handleMessage to intercept the 'payload' case:
    // the payload arrives on the receiver side, but we want to call
    // the receiver-specific path. CoreTransfer already handles this correctly
    // via _handlePayload — no override needed. The ack is sent via
    // _sendMessage which maps to this._conn.send(). ✓

    async send(payload) {
      if (!this._conn) throw new Error('Not connected. Call connect() first.');
      this._status('transferring');
      this.emit('progress', { sent: 0, total: 100 });
      this._conn.send({ type: 'payload', payload });
      this.emit('progress', { sent: 100, total: 100 });
      this._log('ok', 'Payload sent.');
    }

    // -----------------------------------------------------------------------
    // Signaling release
    // -----------------------------------------------------------------------

    // Disconnect the signaling WebSocket WITHOUT closing the data channel.
    // After this the peer ID is no longer reachable for new connections,
    // but the existing WebRTC channel continues. Side-effect: the QR code
    // encoded our peer ID, so dropping signaling makes the QR invalid.
    _releaseSignalingPeer() {
      if (this._releasedSignalingPeer) return;
      if (!this._peer) return;
      this._releasedSignalingPeer = true;
      try { this._peer.disconnect(); } catch {}
      this._log('info', 'Signaling peer released; QR code is no longer reachable.');
    }

    // -----------------------------------------------------------------------
    // Cleanup
    // -----------------------------------------------------------------------

    close() {
      this._userInitiatedDisconnect = true;
      this._stopHeartbeat();
      if (this._reconnectTimer) { clearTimeout(this._reconnectTimer); this._reconnectTimer = null; }
      this._reconnecting = false;
      try { if (this._conn && this._conn.open) this._conn.send({ type: 'bye' }); } catch {}
      try { this._conn?.close();   } catch {}
      try { this._peer?.destroy(); } catch {}
      this._conn = null;
      this._peer = null;
      this._status('idle');
    }
  }

  // -----------------------------------------------------------------------
  // Exports
  // -----------------------------------------------------------------------
  global.PeerJSTransfer    = PeerJSTransfer;
  global.generateFriendlyId = generateFriendlyId;

})(typeof window !== 'undefined' ? window : globalThis);
