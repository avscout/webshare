/**
 * webshare-transfer-trystero.js
 *
 * Trystero/Nostr transfer backend. Extends CoreTransfer with:
 *  - Serverless peer discovery via the Nostr decentralised relay network
 *  - Room-based pairing (shared code like "ruby-maple-42") instead of
 *    peer-ID-based pairing — so both sides just join the same named room
 *  - Native reconnect: Trystero's Nostr relay keep-alives handle connection
 *    recovery automatically; the grace window only expires if the peer truly
 *    goes away (user closes the app, not just screen-off)
 *  - Persistent rooms: a room can be kept open across UI navigation so that
 *    known devices reconnect automatically without re-scanning the QR code
 *  - Gated entry: an onPeerAccept callback decides whether an incoming peer
 *    is accepted; unknown peers are only accepted while the QR is on screen
 *
 * Depends on CoreTransfer (webshare-transfer-core.js).
 * Loads Trystero lazily via dynamic import() from esm.run — no bundler needed.
 *
 * Exported global: TrysteroTransfer
 */
(function (global) {
  'use strict';

  const TRYSTERO_APP_ID = 'webshare-tudelft-v1';
  // esm.run is jsDelivr's ESM CDN — the officially recommended way to load
  // Trystero in a browser without a bundler. esm.sh caused peer discovery
  // issues. 0.21.8/nostr is pinned to the version confirmed working.
  const TRYSTERO_CDN    = 'https://esm.run/trystero@0.21.8/nostr';

  // Pre-fetch the Trystero module as soon as this script loads so the
  // first _joinRoom() call doesn't have to wait for a network round-trip.
  let _trysteroModulePromise = null;
  function _prefetchTrystero() {
    if (!_trysteroModulePromise) {
      _trysteroModulePromise = import(TRYSTERO_CDN).catch(() => {
        _trysteroModulePromise = null;
      });
    }
    return _trysteroModulePromise;
  }
  _prefetchTrystero();

  // Public Nostr relays — all fully open, no signup or payment required.
  const NOSTR_RELAY_URLS = [
    'wss://nos.lol',
    'wss://relay.snort.social',
    'wss://relay.nostr.band',
    'wss://relay.primal.net',
    'wss://nostr.mom',
  ];

  // TURN servers for WebRTC NAT traversal. Using turnConfig (not rtcConfig)
  // so Trystero's own default STUN servers are preserved alongside these.
  const TURN_CONFIG = [
    {
      urls: [
        'turn:openrelay.metered.ca:80',
        'turn:openrelay.metered.ca:443',
        'turn:openrelay.metered.ca:443?transport=tcp',
      ],
      username: 'openrelayproject',
      credential: 'openrelayproject',
    },
  ];
  // Must cover: ICE timeout (~30s) + reconnect time after screen-on.
  // 5 minutes handles typical field use (set phone down briefly, pick back up).
  const LEAVE_GRACE_MS = 5 * 60 * 1000;

  // Room-code word lists — short enough to be legible in a QR label.
  const ADJ  = ['amber','azure','bright','cedar','clean','coral','crisp','early',
                 'frost','golden','green','ivory','jade','lemon','lime','maple',
                 'misty','navy','olive','peach','pearl','pine','rosy','ruby',
                 'sage','sandy','slate','solar','sunny','swift','teal','vivid'];
  const NOUN = ['anchor','anvil','apple','arch','beacon','birch','bloom','bolt',
                'brook','cloud','coast','comet','creek','dawn','delta','dome',
                'drift','dune','eagle','echo','ember','falcon','fern','field',
                'finch','fjord','flame','flash','fleet','flint','forest','gate',
                'glade','globe','grove','haven','hawk','heath','heron','hill'];

  function generateRoomCode() {
    const pick = arr => arr[Math.floor(Math.random() * arr.length)];
    return `${pick(ADJ)}-${pick(NOUN)}-${Math.floor(Math.random() * 90) + 10}`;
  }

  // -----------------------------------------------------------------------
  // TrysteroTransfer
  // -----------------------------------------------------------------------
  class TrysteroTransfer extends CoreTransfer {
    /**
     * @param {object}   options
     * @param {object}   [options.peerInfo]       - Local identity sent to remote.
     * @param {object[]} [options.iceServers]      - Custom ICE servers.
     * @param {boolean}  [options.persistent]      - If true, close() is a soft
     *   reset that leaves the room open. Use forceClose() to actually leave.
     * @param {Function} [options.onPeerAccept]   - (deviceId, peerInfo) => boolean.
     *   Called after peer-info exchange to decide whether to accept the peer.
     *   If omitted, all peers are accepted (first-pairing flow).
     */
    constructor({ peerInfo, iceServers, persistent = false, onPeerAccept = null } = {}) {
      super({ iceServers, peerInfo });

      // Trystero room state
      this._room           = null;
      this._remotePeerId   = null;   // Trystero's ephemeral ID for the other side
      this._leaveTimer     = null;   // grace-window timer after onPeerLeave
      this._isReconnecting = false;
      this._pendingPeerJoin = null;  // { peerId, isReconnect } — held until peer-info accepted
      this._rejectedPeers  = new Set(); // ephemeral IDs of rejected peers

      // Typed Trystero action senders (populated once _joinRoom resolves)
      this._sendPeerInfoAction = null;
      this._sendNoteAction     = null;
      this._sendLampAction     = null;
      this._sendPayloadAction  = null;
      this._sendAckAction      = null;

      // Persistence + gating
      this.persistent    = persistent;
      this.onPeerAccept  = onPeerAccept;  // (deviceId, info) => boolean
    }

    // -----------------------------------------------------------------------
    // Transport overrides
    // -----------------------------------------------------------------------

    // Trystero uses typed actions rather than a single message channel, so
    // _sendMessage isn't used here. We override sendNote and sendLamp directly.
    _sendMessage(/* msg */) {
      throw new Error('TrysteroTransfer does not use _sendMessage; use typed actions.');
    }

    // Override CoreTransfer.sendNote — use the Trystero note action.
    sendNote(text) {
      if (!this._sendNoteAction) return;
      try { this._sendNoteAction(String(text == null ? '' : text)); } catch {}
    }

    // Override CoreTransfer.sendLamp — use the Trystero lamp action.
    sendLamp(on) {
      if (!this._sendLampAction) return;
      try { this._sendLampAction({ on: !!on }); } catch {}
    }

    // Override CoreTransfer._sendPeerInfo — use the Trystero peer-info action.
    _sendPeerInfo() {
      if (this._peerInfoSent || !this._sendPeerInfoAction || !this.peerInfo) return;
      try {
        this._sendPeerInfoAction({ info: this.peerInfo, token: this._sessionToken });
        this._peerInfoSent = true;
        this._log('info', 'Sent peer-info to remote (Trystero action).');
      } catch {}
    }

    // Trystero's Nostr relay keep-alives act as the heartbeat — no need for
    // our own application-level ping/pong. onPeerLeave signals disconnect.
    _startHeartbeat() { /* Trystero handles keep-alives internally */ }
    _stopHeartbeat()  { /* nothing to stop */ }

    isAlive() {
      // For Trystero, "alive" means the room is open and can receive peers.
      // Waiting for a first peer, or being in the reconnect grace window, are
      // both valid states — not stale. Only return false if the room is gone.
      return !!this._room;
    }

    // Trystero reconnect is automatic via Nostr relay keep-alives.
    checkAndRecover() { return false; }

    // -----------------------------------------------------------------------
    // Public API — starting sessions
    // -----------------------------------------------------------------------

    // Receiver flow: generate a new room code and emit show-qr.
    // Used for first-time pairing.
    async startReceiving() {
      if (this.role) throw new Error('Already started.');
      this.role = 'receiver';
      const code = generateRoomCode();
      this.peerId = code;
      this._log('info', `Trystero room code: ${code}`);
      await this._joinRoom(code);
      this.emit('show-qr', { kind: 'peerid', text: code });
    }

    // Sender flow: join an existing room and wait for the receiver peer.
    // Used for first-time pairing (sender side).
    async connect(roomCode) {
      if (this.role) throw new Error('Already started.');
      this.role = 'sender';
      this.peerId = roomCode;
      this._log('info', `Joining Trystero room: ${roomCode}`);
      return new Promise(async (resolve, reject) => {
        const timer = setTimeout(() => {
          reject(new Error('Connection timed out — is the receiver open on the same room code?'));
        }, 60000);
        const unsub = this.on('connected', () => {
          clearTimeout(timer);
          unsub();
          resolve();
        });
        try {
          await this._joinRoom(roomCode);
        } catch (err) {
          clearTimeout(timer);
          unsub();
          reject(err);
        }
      });
    }

    // Persistent room: join a known room and wait indefinitely for a peer.
    // Does not emit show-qr. Uses onPeerAccept gating.
    async joinPersistentRoom(roomCode) {
      if (this.role) throw new Error('Already started.');
      this.role = 'persistent';
      this.peerId = roomCode;
      this._log('info', `Joining persistent room: ${roomCode}`);
      await this._joinRoom(roomCode);
    }

    async send(payload) {
      if (!this._sendPayloadAction) throw new Error('Not connected to a peer.');
      this._status('outgoing');
      this._sendPayloadAction(payload);
      this.emit('progress', { received: 100, total: 100 });
    }

    // -----------------------------------------------------------------------
    // Trystero room setup
    // -----------------------------------------------------------------------

    async _joinRoom(roomCode) {
      this._log('info', 'Connecting to Nostr relays…');
      let joinRoom;
      try {
        ({ joinRoom } = await _prefetchTrystero());
      } catch (e) {
        throw new Error('Failed to load Trystero: ' + (e.message || e));
      }
      this._log('ok', 'Trystero loaded — joining room…');

      this._room = joinRoom({
        appId      : TRYSTERO_APP_ID,
        relayUrls  : NOSTR_RELAY_URLS,
        turnConfig : TURN_CONFIG,
      }, roomCode);

      // Log relay connection status after a short delay so sockets have
      // had time to connect or fail. getRelaySockets() returns a Map of
      // url → WebSocket; we check .readyState (1 = OPEN, 3 = CLOSED).
      setTimeout(() => {
        if (!this._room) return;
        try {
          const sockets = this._room.getRelaySockets();
          sockets.forEach((ws, url) => {
            const state = ws.readyState === 1 ? 'connected'
                        : ws.readyState === 0 ? 'connecting'
                        : ws.readyState === 2 ? 'closing'
                        : 'closed';
            const level = ws.readyState === 1 ? 'ok' : ws.readyState === 0 ? 'info' : 'err';
            this._log(level, `Relay ${url} — ${state}`);
          });
        } catch {}
      }, 3000);

      // Each makeAction returns [senderFn, receiverHandlerRegistrar].
      // Guard against API shape changes in future Trystero versions.
      const _makeAction = (name) => {
        const result = this._room.makeAction(name);
        if (!Array.isArray(result)) {
          throw new Error(
            `Trystero makeAction('${name}') returned ${typeof result} — ` +
            'expected [sender, receiver]. Check Trystero version compatibility.'
          );
        }
        return result;
      };

      const [sendPayload, onPayload]   = _makeAction('payload');
      const [sendAck,     onAck]       = _makeAction('ack');
      const [sendPeerInfo, onPeerInfo] = _makeAction('peerinfo');
      const [sendNote,    onNote]      = _makeAction('note');
      const [sendLamp,    onLamp]      = _makeAction('lamp');

      this._sendPayloadAction  = sendPayload;
      this._sendAckAction      = sendAck;
      this._sendPeerInfoAction = (data, targetId) => sendPeerInfo(data, targetId);
      this._sendNoteAction     = sendNote;
      this._sendLampAction     = sendLamp;

      // Peer lifecycle
      this._room.onPeerJoin(id  => this._onPeerJoin(id));
      this._room.onPeerLeave(id => this._onPeerLeave(id));

      // Incoming peer-info — gate on onPeerAccept before accepting.
      onPeerInfo(({ info, token } = {}, peerId) => {
        if (this._rejectedPeers.has(peerId)) return;

        const deviceId = info && info.deviceId;

        // If gating is active, check acceptance before emitting 'connected'.
        if (this.onPeerAccept && deviceId) {
          const accepted = this.onPeerAccept(deviceId, info);
          if (!accepted) {
            this._rejectedPeers.add(peerId);
            // If this was the tentatively accepted peer, undo that.
            if (peerId === this._remotePeerId) {
              this._remotePeerId   = null;
              this._isReconnecting = false;
            }
            this._pendingPeerJoin = null;
            this._log('info', `Unknown device rejected (QR not visible).`);
            return;
          }
        }

        // Peer is accepted — emit the deferred connected / reconnected event.
        if (this._pendingPeerJoin && this._pendingPeerJoin.peerId === peerId) {
          const { isReconnect } = this._pendingPeerJoin;
          this._pendingPeerJoin = null;
          if (isReconnect) {
            this._log('ok', `Peer reconnected (${peerId.slice(0, 8)}…)`);
            this.emit('reconnected');
          } else {
            this._log('ok', `Peer joined room (${peerId.slice(0, 8)}…)`);
            this.emit('connected');
          }
        }

        this._handlePeerInfo(info, token);
      });

      onNote((text, peerId) => {
        if (this._rejectedPeers.has(peerId)) return;
        this.emit('note', { text: String(text == null ? '' : text) });
      });

      onLamp(({ on } = {}, peerId) => {
        if (this._rejectedPeers.has(peerId)) return;
        this.emit('lamp', { on: !!on });
      });

      onPayload(async (payload, peerId) => {
        if (this._rejectedPeers.has(peerId)) return;
        this._status('transferring');
        this.emit('progress', { received: 50, total: 100 });
        try {
          const response = this.onPayload ? await this.onPayload(payload) : null;
          sendAck({ response }, peerId);
          this._status('done');
          this.emit('progress', { received: 100, total: 100 });
          this._log('ok', 'Payload received and acknowledged.');
        } catch (err) {
          sendAck({ error: err.message }, peerId);
          this._status('error');
          this._log('err', 'onPayload threw: ' + err.message);
        }
      });

      onAck(({ response, error } = {}, peerId) => {
        if (this._rejectedPeers.has(peerId)) return;
        this._status('done');
        if (this.onAck) {
          try { this.onAck(response, error); } catch (e) { console.error(e); }
        }
      });
    }

    // -----------------------------------------------------------------------
    // Peer lifecycle
    // -----------------------------------------------------------------------

    _onPeerJoin(peerId) {
      const isReconnect = this._isReconnecting || (this._leaveTimer !== null);
      if (this._leaveTimer) { clearTimeout(this._leaveTimer); this._leaveTimer = null; }
      this._remotePeerId   = peerId;
      this._isReconnecting = false;
      this._log('info', `Nostr: peer found in room (${peerId.slice(0, 8)}…) — establishing WebRTC…`);
      // Send our peer-info immediately so the other side can gate on our deviceId.
      this._sendPeerInfo();

      if (this.onPeerAccept) {
        // Defer the 'connected' / 'reconnected' event until peer-info arrives
        // and onPeerAccept has made its decision.
        this._pendingPeerJoin = { peerId, isReconnect };
      } else {
        // No gating — emit immediately (first-pairing flow).
        if (isReconnect) {
          this._log('ok', `Peer reconnected (${peerId.slice(0, 8)}…)`);
          this.emit('reconnected');
        } else {
          this._log('ok', `Peer joined room (${peerId.slice(0, 8)}…)`);
          this.emit('connected');
        }
      }
    }

    _onPeerLeave(peerId) {
      if (peerId !== this._remotePeerId) return;
      this._log('info', `Peer left room — waiting up to ${LEAVE_GRACE_MS / 1000}s for reconnect…`);
      this._isReconnecting = true;
      this._peerInfoSent   = false;  // will re-send on rejoin
      this._pendingPeerJoin = null;
      this.emit('reconnecting', { reason: 'peer-left' });
      this._leaveTimer = setTimeout(() => {
        this._leaveTimer     = null;
        this._remotePeerId   = null;
        this._isReconnecting = false;
        this._log('info', `Peer did not return within ${LEAVE_GRACE_MS / 1000}s — disconnecting.`);
        this.emit('disconnected', { reason: 'peer-left' });
      }, LEAVE_GRACE_MS);
    }

    // -----------------------------------------------------------------------
    // Cleanup
    // -----------------------------------------------------------------------

    // Soft reset — clears peer state but keeps the room open.
    // For persistent rooms this is what gets called when the user navigates
    // away from the transfer screen; the room stays joined in the background.
    close() {
      if (this.persistent) {
        if (this._leaveTimer) { clearTimeout(this._leaveTimer); this._leaveTimer = null; }
        this._remotePeerId   = null;
        this._isReconnecting = false;
        this._peerInfoSent   = false;
        this._pendingPeerJoin = null;
        this._status('idle');
        return;
      }
      this._doClose();
    }

    // Hard close — leaves the room. Called on explicit unpair.
    forceClose() {
      this.persistent = false;
      this._doClose();
    }

    _doClose() {
      if (this._leaveTimer) { clearTimeout(this._leaveTimer); this._leaveTimer = null; }
      if (this._room) { try { this._room.leave(); } catch {} this._room = null; }
      this._remotePeerId        = null;
      this._isReconnecting      = false;
      this._pendingPeerJoin     = null;
      this._sendPeerInfoAction  = null;
      this._sendNoteAction      = null;
      this._sendLampAction      = null;
      this._sendPayloadAction   = null;
      this._sendAckAction       = null;
      this._status('idle');
    }
  }

  // -----------------------------------------------------------------------
  // Export
  // -----------------------------------------------------------------------
  global.TrysteroTransfer = TrysteroTransfer;

})(typeof window !== 'undefined' ? window : globalThis);
