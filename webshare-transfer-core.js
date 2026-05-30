/**
 * webshare-transfer-core.js
 *
 * Base class for all transfer backends. Handles the shared protocol layer:
 * peer-info exchange, application-level heartbeat, identity verification,
 * and the message dispatch loop. Each backend (PeerJS, Raw WebRTC, Trystero)
 * extends CoreTransfer and implements the transport-specific parts.
 *
 * ABSTRACT INTERFACE — subclasses MUST implement:
 *   _sendMessage(msg)     Send a plain-object message to the connected peer.
 *                         Throw if the channel is not ready.
 *   startReceiving()      Start listening for an incoming peer.
 *   connect(...)          Initiate a connection to a remote peer.
 *   send(payload)         Send the application payload.
 *   isAlive()             Return true if the channel is currently healthy.
 *   close()               Tear down all connections and cancel timers.
 *
 * OPTIONAL OVERRIDES:
 *   _handleDeath(reason)  Default just emits 'disconnected'. PeerJS overrides
 *                         with its reconnect machinery.
 *   _startHeartbeat()     Trystero overrides with a no-op (Nostr handles it).
 *   _stopHeartbeat()      Same.
 *   sendNote(text)        Trystero overrides with typed Nostr actions.
 *   sendLamp(on)          Same.
 *   checkAndRecover()     PeerJS overrides; Trystero is a no-op.
 */
(function (global) {
  'use strict';

  // -----------------------------------------------------------------------
  // Tiny event emitter
  // -----------------------------------------------------------------------
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
        try { fn(payload); } catch (e) { console.error('Emitter error:', e); }
      });
    }
  }

  // -----------------------------------------------------------------------
  // Shared constants
  // -----------------------------------------------------------------------
  const DEFAULT_RTC_CONFIG = {
    iceServers: [
      { urls: 'stun:stun.l.google.com:19302' },
      { urls: 'stun:stun1.l.google.com:19302' }
    ]
  };

  // Application-level heartbeat. ICE has its own keepalives but those are
  // invisible to JS and get throttled when a tab is backgrounded. Our own
  // ping/pong gives the JS layer a way to notice when the channel has died.
  const HEARTBEAT_INTERVAL_MS = 3000;
  const HEARTBEAT_DEAD_MS     = 9000;  // ~3 missed pings → declare dead

  // -----------------------------------------------------------------------
  // CoreTransfer — the shared base class
  // -----------------------------------------------------------------------
  class CoreTransfer extends Emitter {
    constructor({ iceServers, peerInfo } = {}) {
      super();
      this.rtcConfig = iceServers ? { iceServers } : DEFAULT_RTC_CONFIG;

      // Lifecycle
      this.role      = null;   // 'receiver' | 'sender' — set by subclass
      this.peerId    = null;   // our identifier (peer-ID or room code)
      this.onPayload = null;   // async fn(payload) → response value
      this.onAck     = null;   // fn(response, error)

      // Identity exchange
      this.peerInfo           = peerInfo || null;
      this._remotePeerInfo    = null;
      this._peerInfoSent      = false;
      this._sessionToken      = Math.random().toString(36).slice(2) +
                                 Math.random().toString(36).slice(2);
      this._remoteSessionToken = null;

      // Heartbeat state
      this._heartbeatTimer = null;
      this._lastPongAt     = 0;
      this._heartbeatDead  = false;

      // Wire heartbeat start/stop to connection events so every subclass
      // gets it for free.
      this.on('connected',    () => this._startHeartbeat());
      this.on('disconnected', () => this._stopHeartbeat());
    }

    // -----------------------------------------------------------------------
    // Protocol — message dispatch (called by each backend's data handler)
    // -----------------------------------------------------------------------
    //
    // Backends call this._handleMessage(msg) from their data handler with the
    // already-decoded plain object. CoreTransfer routes it to the right handler.
    _handleMessage(msg) {
      if (!msg || typeof msg.type !== 'string') return;
      switch (msg.type) {
        case 'peer-info':
          this._handlePeerInfo(msg.info, msg.token);
          break;
        case 'note':
          this.emit('note', { text: String(msg.text == null ? '' : msg.text) });
          break;
        case 'lamp':
          this.emit('lamp', { on: !!msg.on });
          break;
        case 'ping':
          // Heartbeat ping from peer — echo a pong. Silently swallow if the
          // channel isn't ready (shouldn't happen if we received a ping at all).
          try { this._sendMessage({ type: 'pong' }); } catch {}
          break;
        case 'pong':
          this._lastPongAt = Date.now();
          break;
        case 'bye':
          this._log('info', 'Peer said bye (intentional disconnect).');
          this._handleDeath('peer-bye');
          break;
        case 'ack':
          this._status('done');
          if (this.onAck) {
            try { this.onAck(msg.response, msg.error); } catch (e) { console.error(e); }
          }
          break;
        case 'payload':
          this._handlePayload(msg.payload);
          break;
      }
    }

    // Receiver-side payload handler — called by _handleMessage.
    async _handlePayload(payload) {
      this._status('transferring');
      this.emit('progress', { received: 100, total: 100 });
      try {
        const response = this.onPayload ? await this.onPayload(payload) : null;
        this._sendMessage({ type: 'ack', response });
        this._status('done');
        this._log('ok', 'Payload received and acknowledged.');
      } catch (err) {
        try { this._sendMessage({ type: 'ack', error: err.message }); } catch {}
        this._status('error');
        this._log('err', 'onPayload threw: ' + err.message);
      }
    }

    // -----------------------------------------------------------------------
    // Peer-info — identity exchange at channel open
    // -----------------------------------------------------------------------
    _sendPeerInfo() {
      if (this._peerInfoSent) return;
      if (!this.peerInfo) return;
      try {
        this._sendMessage({ type: 'peer-info', info: this.peerInfo, token: this._sessionToken });
        this._peerInfoSent = true;
        this._log('info', 'Sent peer-info to remote.');
      } catch {
        // Channel not ready yet — the backend will retry on the next 'open' event.
      }
    }

    // Verify + store incoming peer-info. Returns false and sets the
    // _identityMismatch flag if the token doesn't match a previous session.
    _handlePeerInfo(info, token) {
      if (this._remoteSessionToken && token && token !== this._remoteSessionToken) {
        this._log('err', 'Identity mismatch — rejecting connection.');
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

    getRemotePeerInfo() { return this._remotePeerInfo; }

    // -----------------------------------------------------------------------
    // Shared messaging helpers — delegated to the transport layer
    // -----------------------------------------------------------------------
    sendNote(text) {
      try { this._sendMessage({ type: 'note', text: String(text == null ? '' : text) }); } catch {}
    }

    sendLamp(on) {
      try { this._sendMessage({ type: 'lamp', on: !!on }); } catch {}
    }

    // -----------------------------------------------------------------------
    // Heartbeat
    // -----------------------------------------------------------------------
    _startHeartbeat() {
      this._stopHeartbeat();
      this._lastPongAt    = Date.now();
      this._heartbeatDead = false;
      this._heartbeatTimer = setInterval(() => this._heartbeatTick(), HEARTBEAT_INTERVAL_MS);
    }

    _stopHeartbeat() {
      if (this._heartbeatTimer) { clearInterval(this._heartbeatTimer); this._heartbeatTimer = null; }
    }

    _heartbeatTick() {
      const sinceLastPong = Date.now() - this._lastPongAt;
      if (sinceLastPong > HEARTBEAT_DEAD_MS) {
        if (this._heartbeatDead) return;
        this._heartbeatDead = true;
        this._log('err', `Heartbeat timeout (${sinceLastPong}ms) — declaring channel dead.`);
        try { this._sendMessage({ type: 'bye' }); } catch {} // best-effort
        this._handleDeath('heartbeat-timeout');
        return;
      }
      try { this._sendMessage({ type: 'ping' }); } catch {}
    }

    // -----------------------------------------------------------------------
    // Death handler — override in PeerJSTransfer for reconnect logic.
    // Base implementation: emit 'disconnected' and stop.
    // -----------------------------------------------------------------------
    _handleDeath(reason) {
      this.emit('disconnected', { reason });
    }

    // -----------------------------------------------------------------------
    // No-op stubs for methods that only some backends implement
    // -----------------------------------------------------------------------
    isAlive()           { return false; }
    checkAndRecover()   { return false; }

    // -----------------------------------------------------------------------
    // Abstract — subclasses MUST override
    // -----------------------------------------------------------------------
    _sendMessage(/* msg */) {
      throw new Error('_sendMessage() not implemented by this backend');
    }

    // -----------------------------------------------------------------------
    // Utilities
    // -----------------------------------------------------------------------
    _log(level, message) { this.emit('log', { level, message }); }
    _status(s)           { this.emit('status', s); }
  }

  // -----------------------------------------------------------------------
  // Exports
  // -----------------------------------------------------------------------
  global.CoreTransfer      = CoreTransfer;
  global.Emitter           = Emitter;            // shared by backends that need it
  global.DEFAULT_RTC_CONFIG = DEFAULT_RTC_CONFIG;
  global.HEARTBEAT_INTERVAL_MS = HEARTBEAT_INTERVAL_MS;
  global.HEARTBEAT_DEAD_MS     = HEARTBEAT_DEAD_MS;

})(typeof window !== 'undefined' ? window : globalThis);
