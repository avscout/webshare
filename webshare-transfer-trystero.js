/**
 * webshare-transfer-trystero.js
 *
 * TrysteroTransfer — persistent, multi-peer WebRTC via Nostr signaling.
 *
 * Key differences from PeerJS/Raw:
 *  - Persistent rooms: roomcode never expires, auto-reconnect after any outage
 *  - Multi-peer: multiple devices can share the same room simultaneously
 *  - Group password: room access requires knowing the shared secret (in QR)
 *  - Sync actions: sync-full / sync-delta / sync-request for automatic data sync
 *
 * Loads Trystero lazily via dynamic import() from esm.run (jsDelivr) — the
 * officially recommended CDN. Pre-fetched at script load to avoid cold-start
 * delay when the user first connects.
 *
 * Exported global: TrysteroTransfer
 */
(function (global) {
  'use strict';

  // Neutral, non-identifying app namespace. This string is visible to relay
  // operators in the signaling traffic, so it deliberately reveals nothing
  // about the app or organisation — it's just a random opaque identifier.
  // All devices must share the same value to find each other.
  const TRYSTERO_APP_ID = 'b9a1c5f51a800c69';
  // esm.run is jsDelivr's ESM CDN — the officially recommended way to load
  // Trystero in a browser without a bundler.
  //
  // Trystero split each signaling strategy into its own package. The legacy
  // `trystero/mqtt` subpath now throws a deprecation error, so MQTT must be
  // loaded from the dedicated @trystero-p2p/mqtt package. Nostr still works
  // via the main package's /nostr subpath.
  const TRYSTERO_VERSION = '0.23.0';
  const TRYSTERO_CDN_NOSTR = `https://esm.run/trystero@${TRYSTERO_VERSION}/nostr`;
  const TRYSTERO_CDN_MQTT  = `https://esm.run/@trystero-p2p/mqtt`;

  // Pre-fetch the Trystero modules. Each strategy is a separate module;
  // we cache them by strategy name so switching backends is instant.
  // On failure we clear the cache entry so a later attempt can retry,
  // and resolve to null so callers can detect the failure cleanly.
  const _trysteroModules = {};
  function _prefetchTrystero(strategy = 'nostr') {
    if (!_trysteroModules[strategy]) {
      const url = strategy === 'mqtt' ? TRYSTERO_CDN_MQTT : TRYSTERO_CDN_NOSTR;
      _trysteroModules[strategy] = import(url).catch((e) => {
        _trysteroModules[strategy] = null;  // allow retry next time
        return null;                         // resolve to null, not undefined
      });
    }
    return _trysteroModules[strategy];
  }
  _prefetchTrystero('mqtt');
  _prefetchTrystero('nostr');

  // Relay diagnostics should run at most once per strategy per page load —
  // repeated joins (reconnects) must not stack extra test WebSockets, which
  // would get the app rate-limited. Switching strategy (MQTT↔Nostr) tests
  // the new strategy's relays once.
  const _relayDiagnosticsRun = {};

  // Public Nostr relays — all fully open, no signup or payment required.
  const NOSTR_RELAY_URLS = [
    'wss://nos.lol',
    'wss://relay.snort.social',
    'wss://relay.primal.net',
    'wss://nostr.mom',
  ];

  // Public MQTT brokers with WebSocket support — used by the MQTT strategy.
  // These mirror the @trystero-p2p/mqtt package's own default broker list,
  // including the correct /mqtt paths each broker expects.
  const MQTT_BROKER_URLS = [
    'wss://test.mosquitto.org:8081/mqtt',
    'wss://broker.hivemq.com:8884/mqtt',
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

  // How long to wait after all peers have left before declaring a fatal disconnect.
  // 5 minutes handles typical field use (set phone down briefly, pick back up).
  const LEAVE_GRACE_MS = 5 * 60 * 1000;

  // Room-code word lists — short enough to be legible in a QR label.
  const ADJ  = ['amber','azure','bright','cedar','clean','coral','crisp','early',
                 'frost','golden','green','ivory','jade','lemon','lime','maple',
                 'misty','noble','ocean','plain','quiet','rapid','sandy','shady',
                 'sharp','silky','snowy','solar','still','stone','storm','sunny',
                 'swift','tawny','urban','valid','vivid','warm','wild','windy'];
  const NOUN = ['birch','brook','cliff','cloud','creek','cross','delta','drift',
                 'dune','eagle','falls','field','flame','flint','frost','glade',
                 'grove','haven','heath','hill','inlet','knoll','lagoon','lake',
                 'marsh','meadow','mist','moon','moss','mound','moor','peak',
                 'pine','plain','pond','pool','range','ridge','river','rock',
                 'shore','slope','snow','spire','spring','stone','storm','vale'];

  function generateRoomCode() {
    const pick = arr => arr[Math.floor(Math.random() * arr.length)];
    const n = Math.floor(10 + Math.random() * 90);
    return `${pick(ADJ)}-${pick(NOUN)}-${n}`;
  }

  function generatePassword() {
    const arr = new Uint8Array(16);
    crypto.getRandomValues(arr);
    return Array.from(arr, b => b.toString(16).padStart(2, '0')).join('');
  }

  // -----------------------------------------------------------------------
  // TrysteroTransfer
  // -----------------------------------------------------------------------
  class TrysteroTransfer extends CoreTransfer {
    /**
     * @param {object}   options
     * @param {object}   [options.peerInfo]       - Local identity sent to remote.
     * @param {object[]} [options.iceServers]      - Custom ICE servers.
     * @param {boolean}  [options.persistent]      - If true, close() is a soft reset.
     * @param {string}   [options.password]        - Shared group password for Trystero encryption.
     * @param {Function} [options.onPeerAccept]   - (deviceId, peerInfo) => boolean.
     */
    constructor({ peerInfo, iceServers, persistent = false, password = null, onPeerAccept = null, strategy = 'mqtt' } = {}) {
      super({ iceServers, peerInfo });

      this._strategy = strategy;  // 'nostr' or 'mqtt'

      // Trystero room state
      this._room            = null;
      this._remotePeerId    = null;    // most recently connected peer (for compat)
      this._connectedPeers  = new Set(); // all currently connected peer IDs
      this._leaveTimer      = null;
      this._isReconnecting  = false;
      this._pendingPeerJoins = new Map(); // peerId → { isReconnect }
      this._rejectedPeers   = new Set();
      // When true, ALL inbound data messages are dropped at the central gate
      // (see _makeAction). Set when this device has left / been removed from
      // the group. Default-closed: any action, present or future, is blocked
      // without needing a per-handler check.
      this._inboundSealed   = false;

      // Action senders (populated once _joinRoom resolves)
      this._sendPeerInfoAction    = null;
      this._sendNoteAction        = null;
      this._sendLampAction        = null;
      this._sendPayloadAction     = null;
      this._sendAckAction         = null;
      this._sendSyncFullAction    = null;
      this._sendSyncDeltaAction   = null;
      this._sendSyncRequestAction = null;
      this._sendSourceInfoAction  = null;
      this._sendSourceReqAction   = null;
      this._sendSourceDataAction  = null;
      this._sendMbrSyncAction     = null;

      // Config
      this.persistent   = persistent;
      this.password     = password;
      this.onPeerAccept = onPeerAccept;

      // Sync callbacks — set externally by TrysteroRoomManager / SyncManager
      this.onSyncFull     = null;  // (sessions, fromPeerId) => void
      this.onSyncDelta    = null;  // (session,  fromPeerId) => void
      this.onSyncRequest  = null;  // (fromPeerId) => void
      this.onSourceInfo   = null;  // ({ hash, uploadedAt }, fromPeerId) => void
      this.onSourceRequest = null; // (fromPeerId) => void
      this.onSourceData   = null;  // ({ filename, uploadedAt, hash, rows }, fromPeerId) => void
      this.onMbrSync      = null;  // (member, fromPeerId) => void
      this.onCollectionDelta = null; // (name, record, fromPeerId) => void
      this.onCollectionFull  = null; // (name, records, fromPeerId) => void
    }

    // -----------------------------------------------------------------------
    // Transport overrides
    // -----------------------------------------------------------------------

    _sendMessage(/* msg */) {
      throw new Error('TrysteroTransfer does not use _sendMessage; use typed actions.');
    }

    sendNote(text) {
      if (!this._sendNoteAction) return;
      try { this._sendNoteAction(String(text == null ? '' : text)); } catch {}
    }

    sendLamp(on) {
      if (!this._sendLampAction) return;
      try { this._sendLampAction({ on: !!on }); } catch {}
    }

    // Generic collection sync (used by SyncEngine). Delta broadcasts to all
    // peers; full seeds a specific peer (or all, if no target).
    sendCollectionDelta(name, record) {
      if (!this._sendColDeltaAction) return;
      try { this._sendColDeltaAction(name, record); } catch {}
    }
    sendCollectionFull(name, records, targetPeerId) {
      if (!this._sendColFullAction) return;
      try { this._sendColFullAction(name, records, targetPeerId || undefined); } catch {}
    }

    // Seal/unseal the central inbound gate. When sealed, every inbound data
    // message is dropped (used when this device has left/been removed from the
    // group). Default-closed for all actions, present and future.
    sealInbound()   { this._inboundSealed = true; }
    unsealInbound() { this._inboundSealed = false; }

    // Watch for the case where ALL signaling brokers/relays drop. Brokers are
    // only needed to find peers and to reconnect; an already-established P2P
    // connection keeps working when they're gone. But with zero brokers, no
    // new peers or reconnects are possible — worth a warning. Logs only the
    // TRANSITION (all-down, and recovery), not every flap, and survives
    // Trystero swapping sockets on reconnect because it re-reads the live set.
    _startRelayWatch(label) {
      if (this._relayWatch) return;            // already watching
      if (!this._getRelaySockets) return;      // no live socket info available
      this._relayAllDown = false;
      this._relayWatch = setInterval(() => {
        if (!this._room) { clearInterval(this._relayWatch); this._relayWatch = null; return; }
        let open = 0;
        try {
          const sockets = this._getRelaySockets() || {};
          for (const ws of Object.values(sockets)) {
            if (ws && ws.readyState === 1) open++;
          }
        } catch { return; }
        if (open === 0 && !this._relayAllDown) {
          this._relayAllDown = true;
          this._log('err', `⚠ All ${label}s are down — new connections and reconnects are not possible until a ${label} returns. Existing direct connections keep working.`);
          this.emit('relay-status', { connected: 0, allDown: true });
        } else if (open > 0 && this._relayAllDown) {
          this._relayAllDown = false;
          this._log('ok', `${label.charAt(0).toUpperCase() + label.slice(1)} connection restored (${open} reachable).`);
          this.emit('relay-status', { connected: open, allDown: false });
        }
      }, 5000);
    }

    // Send peer-info to a specific peer, or broadcast if no targetId given.
    _sendPeerInfo(targetPeerId) {
      if (!this._sendPeerInfoAction || !this.peerInfo) return;
      try {
        this._sendPeerInfoAction(
          { info: this.peerInfo, token: this._sessionToken },
          targetPeerId || undefined
        );
        this._peerInfoSent = true;
        if (!targetPeerId) {
          this._log('info', 'Sent peer-info to all peers.');
        }
      } catch {}
    }

    // Sync methods — called by SyncManager
    sendSyncFull(sessions, targetPeerId) {
      if (!this._sendSyncFullAction) return;
      try { this._sendSyncFullAction(sessions, targetPeerId || undefined); } catch {}
    }

    sendSyncDelta(session) {
      if (!this._sendSyncDeltaAction || this._connectedPeers.size === 0) return;
      try { this._sendSyncDeltaAction(session); } catch {}
    }

    sendSyncRequest(targetPeerId) {
      if (!this._sendSyncRequestAction) return;
      try { this._sendSyncRequestAction({}, targetPeerId || undefined); } catch {}
    }

    sendSourceInfo(data, targetPeerId) {
      if (!this._sendSourceInfoAction) return;
      try { this._sendSourceInfoAction(data, targetPeerId || undefined); } catch {}
    }

    sendSourceRequest(targetPeerId) {
      if (!this._sendSourceReqAction) return;
      try { this._sendSourceReqAction({}, targetPeerId || undefined); } catch {}
    }

    sendSourceData(data, targetPeerId) {
      if (!this._sendSourceDataAction) return;
      try { this._sendSourceDataAction(data, targetPeerId || undefined); } catch {}
    }

    sendMbrSync(member, targetPeerId) {
      if (!this._sendMbrSyncAction || this._connectedPeers.size === 0) return;
      try { this._sendMbrSyncAction(member, targetPeerId || undefined); } catch {}
    }

    _startHeartbeat() { /* Trystero handles keep-alives internally */ }
    _stopHeartbeat()  { /* nothing to stop */ }

    isAlive() { return !!this._room; }
    checkAndRecover() { return false; }

    // -----------------------------------------------------------------------
    // Public API — starting sessions
    // -----------------------------------------------------------------------

    // Receiver (group owner showing invitation QR).
    // If this is the very first pairing (no group yet), generates a fresh roomCode.
    // Returns the roomCode so the caller can build the invitation QR.
    async startReceiving(roomCode) {
      if (this.role) throw new Error('Already started.');
      this.role = 'receiver';
      const code = roomCode || generateRoomCode();
      this.peerId = code;
      this._log('info', `Trystero room code: ${code}`);
      await this._joinRoom(code);
      this.emit('show-qr', { kind: 'peerid', text: code });
      return code;
    }

    // Sender flow: join an existing room and wait for the receiver peer.
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

    // Persistent room: join a known room and wait indefinitely for peers.
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
      const isMqtt = this._strategy === 'mqtt';
      this._log('info', isMqtt ? 'Connecting to MQTT brokers…' : 'Connecting to Nostr relays…');
      const mod = await _prefetchTrystero(this._strategy);
      if (!mod || typeof mod.joinRoom !== 'function') {
        throw new Error(`Failed to load Trystero ${this._strategy} module (CDN unreachable?)`);
      }
      const { joinRoom } = mod;
      // getRelaySockets is a MODULE-level export in Trystero (not a room
      // method). Capture it so the relay diagnostics can report which brokers
      // we're actually connected to.
      this._getRelaySockets = (typeof mod.getRelaySockets === 'function')
        ? mod.getRelaySockets
        : null;
      this._log('ok', 'Trystero loaded — joining room…');

      // Defensive: if a room is somehow already open, leave it before
      // opening a new one. Stacked rooms keep their relay sockets alive
      // and get the app rate-limited.
      if (this._room) {
        try { this._room.leave(); } catch {}
        this._room = null;
        if (this._relayWatch) { clearInterval(this._relayWatch); this._relayWatch = null; }
      }

      const config = {
        appId      : TRYSTERO_APP_ID,
        relayConfig: { urls: isMqtt ? MQTT_BROKER_URLS : NOSTR_RELAY_URLS },
        turnConfig : TURN_CONFIG,
      };
      if (this.password) config.password = this.password;

      this._room = joinRoom(config, roomCode);
      const RELAY_URLS = isMqtt ? MQTT_BROKER_URLS : NOSTR_RELAY_URLS;

      // Log relay status after 3s — only in dev mode, and only once per
      // strategy per page load. The guard is set NOW (at schedule time), not
      // inside the timer, so a second join within the 3s window doesn't
      // schedule a second diagnostics run (which would double the log).
      if (this._devMode && !_relayDiagnosticsRun[this._strategy]) {
        _relayDiagnosticsRun[this._strategy] = true;
        setTimeout(() => {
        if (!this._room) return;

        const label = isMqtt ? 'broker' : 'relay';

        try {
          // Preferred: ask Trystero which signaling sockets are actually open.
          // getRelaySockets() returns a map of URL → WebSocket for the sockets
          // Trystero itself is using right now (not a separate test socket).
          if (this._getRelaySockets) {
            const sockets = this._getRelaySockets();
            const entries = Object.entries(sockets || {});
            let connected = 0;
            const connectedUrls = [];
            for (const [url, ws] of entries) {
              const open = ws && ws.readyState === 1;
              if (open) { connected++; connectedUrls.push(url); }
            }
            const total = entries.length;
            if (total === 0) {
              this._log('err', `No ${label}s found — Trystero may not be loaded correctly.`);
            } else {
              this._log(connected ? 'ok' : 'err',
                `Connected to ${connected}/${total} ${label}s${connectedUrls.length ? ': ' + connectedUrls.join(', ') : ''}.`);
              // Split-risk interpretation: peer discovery only works between two
              // devices if they share at least one broker. Fewer connected
              // brokers = higher chance two devices have no broker in common.
              if (connected === 0) {
                this._log('err', `⚠ No ${label} reachable — peer discovery will not work.`);
              } else if (connected < total) {
                this._log('info',
                  `Note: not all ${label}s are reachable. Two devices can only find each other if they share at least one ${label}. ` +
                  `On this network only: ${connectedUrls.join(', ')}.`);
              }
              this.emit('relay-status', { connected, total, connectedUrls });
            }
            this._startRelayWatch(label);
            return;
          }

          // Fallback when the module doesn't expose getRelaySockets: probe each
          // URL with a short-lived test socket. This reports reachability, not
          // the live signaling socket — logged to the dev-log all the same.
          const results = [];
          let pending = RELAY_URLS.length;
          const summarise = () => {
            const reachable = results.filter(r => r.ok).map(r => r.url);
            this._log(reachable.length ? 'ok' : 'err',
              `Connected to ${reachable.length}/${RELAY_URLS.length} ${label}s${reachable.length ? ': ' + reachable.join(', ') : ''}.`);
            if (reachable.length && reachable.length < RELAY_URLS.length) {
              this._log('info',
                `Note: two devices can only find each other if they share at least one ${label}. On this network reachable: ${reachable.join(', ')}.`);
            }
            this.emit('relay-status', { connected: reachable.length, total: RELAY_URLS.length, connectedUrls: reachable });
          };
          RELAY_URLS.forEach(url => {
            let settled = false;
            const ws = new WebSocket(url);
            const done = (ok) => {
              if (settled) return; settled = true;
              results.push({ url, ok });
              try { ws.close(); } catch {}
              if (--pending === 0) summarise();
            };
            const timer = setTimeout(() => done(false), 5000);
            ws.onopen  = () => { clearTimeout(timer); done(true); };
            ws.onerror = () => { clearTimeout(timer); done(false); };
          });
        } catch (e) {
          this._log('err', `Could not determine ${label} status: ` + e.message);
        }
      }, 3000);
      }

      // makeAction's return shape differs across Trystero versions:
      //   • Classic (≤0.23 array builds): returns [send, onReceive(, onProgress)]
      //     where send(data, targetPeerId) and onReceive(cb) registers a handler.
      //   • New @trystero-p2p/core (0.25+): returns an object
      //     { send(data, {target, metadata}), set onMessage(cb), ... }.
      // We normalise both to a legacy [send, onReceive] tuple, where the
      // returned `send` accepts (data, targetPeerId) and `onReceive` accepts a
      // callback (cb(data, peerId)). The rest of this file is written against
      // that tuple, so callers don't need to know which Trystero is underneath.
      // Central inbound gate. EVERY action's receive callback passes through
      // here, so a new action added in the future is blocked-by-default
      // without touching its handler.
      //   • _inboundSealed → drop everything (device left/removed the group).
      //   • _rejectedPeers → drop everything from that peer, EXCEPT actions
      //     flagged allowWhenRejected (peerinfo), which must still flow so a
      //     previously-rejected peer can be re-admitted.
      const _gate = (peerId, opts) => {
        if (this._inboundSealed) return false;
        if (this._rejectedPeers.has(peerId) && !(opts && opts.allowWhenRejected)) return false;
        return true;
      };

      const _makeAction = (name, opts) => {
        const result = this._room.makeAction(name);

        // Classic array form.
        if (Array.isArray(result)) {
          if (this._devMode && !this._loggedApiShape) {
            this._loggedApiShape = true;
            console.log(`[FieldSync] ${this._strategy} uses classic array makeAction API`);
          }
          const [rawSend, rawOnReceive] = result;
          const onReceive = (cb) => rawOnReceive((data, peerId) => {
            if (!_gate(peerId, opts)) return;
            cb(data, peerId);
          });
          return [rawSend, onReceive];
        }

        // New object form — adapt to the tuple.
        if (result && typeof result === 'object' && typeof result.send === 'function') {
          if (this._devMode && !this._loggedApiShape) {
            this._loggedApiShape = true;
            console.log(`[FieldSync] ${this._strategy} uses new object makeAction API (adapted)`);
          }
          const send = (data, targetPeerId) => {
            if (targetPeerId == null) return result.send(data);
            return result.send(data, { target: targetPeerId });
          };
          const onReceive = (cb) => {
            result.onMessage = (data, meta) => {
              const peerId = meta && typeof meta === 'object' ? meta.peerId : meta;
              if (!_gate(peerId, opts)) return;
              cb(data, peerId);
            };
          };
          return [send, onReceive];
        }

        throw new Error(
          `Trystero makeAction('${name}') returned an unexpected shape — ` +
          'cannot adapt to [send, onReceive]. Check Trystero version compatibility.'
        );
      };

      // Existing actions
      const [sendPayload,  onPayload]  = _makeAction('payload');
      const [sendAck,      onAck]      = _makeAction('ack');
      const [sendPeerInfo, onPeerInfo] = _makeAction('peerinfo', { allowWhenRejected: true });
      const [sendNote,     onNote]     = _makeAction('note');
      const [sendLamp,     onLamp]     = _makeAction('lamp');

      // Sync actions
      const [sendSyncFull,    onSyncFull]    = _makeAction('sync-full');
      const [sendSyncDelta,   onSyncDelta]   = _makeAction('sync-delta');
      const [sendSyncRequest, onSyncRequest] = _makeAction('sync-request');
      const [sendSourceInfo,  onSourceInfo]  = _makeAction('src-info');
      const [sendSourceReq,   onSourceReq]   = _makeAction('src-req');
      const [sendSourceData,  onSourceData]  = _makeAction('src-data');
      const [sendMbrSync,     onMbrSync]     = _makeAction('mbr-sync');

      // Generic collection sync — one action pair carries every collection;
      // the collection name travels inside the payload. Used by SyncEngine.
      const [sendColDelta,    onColDelta]    = _makeAction('col-delta');
      const [sendColFull,     onColFull]     = _makeAction('col-full');

      this._sendPayloadAction     = sendPayload;
      this._sendAckAction         = sendAck;
      this._sendPeerInfoAction    = (data, targetId) => sendPeerInfo(data, targetId);
      this._sendNoteAction        = sendNote;
      this._sendLampAction        = sendLamp;
      this._sendSyncFullAction    = (data, targetId) => sendSyncFull(data, targetId);
      this._sendSyncDeltaAction   = (data) => sendSyncDelta(data);
      this._sendSyncRequestAction = (data, targetId) => sendSyncRequest(data, targetId);
      this._sendSourceInfoAction  = (data, targetId) => sendSourceInfo(data, targetId);
      this._sendSourceReqAction   = (data, targetId) => sendSourceReq(data, targetId);
      this._sendSourceDataAction  = (data, targetId) => sendSourceData(data, targetId);
      this._sendMbrSyncAction     = (data) => sendMbrSync(data);
      this._sendColDeltaAction    = (name, record) => sendColDelta({ name, record });
      this._sendColFullAction     = (name, records, targetId) => sendColFull({ name, records }, targetId);

      // Peer lifecycle — the room exposes onPeerJoin/onPeerLeave as a callable
      // function in classic builds (core 0.23) but as a settable property in
      // newer builds (core 0.25). Support both shapes.
      const _wireRoomCallback = (name, handler) => {
        const current = this._room[name];
        if (typeof current === 'function') {
          // Classic: call it with the handler. (A bound listener-registrar.)
          // Heuristic: registrars take the handler as an argument; property
          // getters return undefined/non-function. We already checked it's a
          // function, so call it.
          try {
            this._room[name](handler);
            return;
          } catch (e) {
            // Fall through to assignment if calling fails.
          }
        }
        // New: assign as a property (setter).
        try { this._room[name] = handler; } catch (e) {
          this._log('err', `Could not wire ${name}: ` + (e.message || e));
        }
      };
      _wireRoomCallback('onPeerJoin',  id => this._onPeerJoin(id));
      _wireRoomCallback('onPeerLeave', id => this._onPeerLeave(id));

      // Incoming peer-info — gate on onPeerAccept before accepting.
      onPeerInfo(({ info, token } = {}, peerId) => {
        // A peer may have been rejected earlier (e.g. before our group
        // existed, or before an mbr-sync taught us they're legitimate).
        // Don't treat rejection as permanent: re-run onPeerAccept, which now
        // accepts known members and any peer in an existing group. Only stay
        // rejected if onPeerAccept still says no.
        const wasRejected = this._rejectedPeers.has(peerId);

        const deviceId = info && info.deviceId;

        if (this.onPeerAccept && deviceId) {
          const accepted = this.onPeerAccept(deviceId, info);
          if (!accepted) {
            this._rejectedPeers.add(peerId);
            this._connectedPeers.delete(peerId);
            if (this._remotePeerId === peerId) {
              this._remotePeerId = this._connectedPeers.size > 0
                ? [...this._connectedPeers][0] : null;
            }
            this._pendingPeerJoins.delete(peerId);
            this._log('info', `Unknown device rejected (QR not visible).`);
            return;
          }
          // Accepted now — clear any stale rejection so the rest of the
          // pipeline (sync, source, mbr) stops ignoring this peer.
          if (wasRejected) {
            this._rejectedPeers.delete(peerId);
            this._log('ok', `Previously-rejected peer now accepted (${peerId.slice(0, 8)}…)`);
          }
        } else if (wasRejected) {
          // No acceptance callback but previously rejected — keep ignoring.
          return;
        }

        // Peer accepted — emit connected / reconnected
        const pending = this._pendingPeerJoins.get(peerId);
        if (pending) {
          this._pendingPeerJoins.delete(peerId);
          if (pending.isReconnect) {
            this._log('ok', `Peer reconnected (${peerId.slice(0, 8)}…)`);
            this.emit('reconnected', peerId);
          } else {
            this._log('ok', `Peer joined room (${peerId.slice(0, 8)}…)`);
            this.emit('connected', peerId);
          }
        }

        // Emit peer-info event with peerId attached
        if (info) this.emit('peer-info', { ...info, _peerId: peerId });
      });

      onNote((text, peerId) => {
        this.emit('note', { text: String(text == null ? '' : text) });
      });

      onLamp(({ on } = {}, peerId) => {
        this.emit('lamp', { on: !!on });
      });

      onPayload(async (payload, peerId) => {
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
        this._status('done');
        if (this.onAck) {
          try { this.onAck(response, error); } catch (e) { console.error(e); }
        }
      });

      // Sync handlers
      onSyncFull((sessions, peerId) => {
        if (this.onSyncFull) this.onSyncFull(sessions, peerId);
      });

      onSyncDelta((session, peerId) => {
        if (this.onSyncDelta) this.onSyncDelta(session, peerId);
      });

      onSyncRequest((_, peerId) => {
        if (this.onSyncRequest) this.onSyncRequest(peerId);
      });

      onSourceInfo((data, peerId) => {
        if (this.onSourceInfo) this.onSourceInfo(data, peerId);
      });

      onSourceReq((_, peerId) => {
        if (this.onSourceRequest) this.onSourceRequest(peerId);
      });

      onSourceData((data, peerId) => {
        if (this.onSourceData) this.onSourceData(data, peerId);
      });

      onMbrSync((data, peerId) => {
        if (this.onMbrSync) this.onMbrSync(data, peerId);
      });

      onColDelta((data, peerId) => {
        if (data && data.name && this.onCollectionDelta) {
          this.onCollectionDelta(data.name, data.record, peerId);
        }
      });

      onColFull((data, peerId) => {
        if (data && data.name && this.onCollectionFull) {
          this.onCollectionFull(data.name, data.records, peerId);
        }
      });
    }

    // -----------------------------------------------------------------------
    // Peer lifecycle — multi-peer aware
    // -----------------------------------------------------------------------

    _onPeerJoin(peerId) {
      const wasReconnecting = this._isReconnecting || (this._leaveTimer !== null);
      if (this._leaveTimer) { clearTimeout(this._leaveTimer); this._leaveTimer = null; }

      this._connectedPeers.add(peerId);
      this._remotePeerId   = peerId;
      this._isReconnecting = false;

      this._log('info', `Nostr: peer found in room (${peerId.slice(0, 8)}…) — establishing WebRTC…`);

      // Send peer-info specifically to this new peer
      if (this._sendPeerInfoAction && this.peerInfo) {
        try {
          this._sendPeerInfoAction(
            { info: this.peerInfo, token: this._sessionToken },
            peerId
          );
        } catch {}
      }

      if (this.onPeerAccept) {
        this._pendingPeerJoins.set(peerId, { isReconnect: wasReconnecting });
      } else {
        if (wasReconnecting) {
          this._log('ok', `Peer reconnected (${peerId.slice(0, 8)}…)`);
          this.emit('reconnected', peerId);
        } else {
          this._log('ok', `Peer joined room (${peerId.slice(0, 8)}…)`);
          this.emit('connected', peerId);
        }
      }
    }

    _onPeerLeave(peerId) {
      this._connectedPeers.delete(peerId);
      this._pendingPeerJoins.delete(peerId);

      if (this._remotePeerId === peerId) {
        this._remotePeerId = this._connectedPeers.size > 0
          ? [...this._connectedPeers][0] : null;
      }

      this._log('info', `Peer left (${peerId.slice(0, 8)}…) — ${this._connectedPeers.size} remaining.`);
      this.emit('peer-left', peerId);

      // Only declare disconnect when ALL peers have left
      if (this._connectedPeers.size === 0) {
        this._peerInfoSent   = false;
        this._isReconnecting = true;
        this.emit('reconnecting', { reason: 'peer-left' });
        this._leaveTimer = setTimeout(() => {
          this._leaveTimer     = null;
          this._isReconnecting = false;
          this._remotePeerId   = null;
          this._log('info', `All peers gone — disconnected.`);
          this.emit('disconnected', { reason: 'peer-left' });
        }, LEAVE_GRACE_MS);
      }
    }

    // -----------------------------------------------------------------------
    // Cleanup
    // -----------------------------------------------------------------------

    close() {
      if (this.persistent) {
        if (this._leaveTimer) { clearTimeout(this._leaveTimer); this._leaveTimer = null; }
        this._remotePeerId   = null;
        this._isReconnecting = false;
        this._peerInfoSent   = false;
        this._pendingPeerJoins.clear();
        this._status('idle');
        return;
      }
      this._doClose();
    }

    forceClose() {
      this.persistent = false;
      this._doClose();
    }

    _doClose() {
      if (this._leaveTimer) { clearTimeout(this._leaveTimer); this._leaveTimer = null; }
      if (this._relayWatch) { clearInterval(this._relayWatch); this._relayWatch = null; }
      if (this._room) { try { this._room.leave(); } catch {} this._room = null; }
      this._remotePeerId          = null;
      this._connectedPeers.clear();
      this._pendingPeerJoins.clear();
      this._isReconnecting        = false;
      this._sendPeerInfoAction    = null;
      this._sendNoteAction        = null;
      this._sendLampAction        = null;
      this._sendPayloadAction     = null;
      this._sendAckAction         = null;
      this._sendSyncFullAction    = null;
      this._sendSyncDeltaAction   = null;
      this._sendSyncRequestAction = null;
      this._sendSourceInfoAction  = null;
      this._sendSourceReqAction   = null;
      this._sendSourceDataAction  = null;
      this._sendMbrSyncAction     = null;
      this._status('idle');
    }
  }

  // Expose generatePassword for use in index.html (group creation)
  global.generateTrysteroPassword = generatePassword;

  // -----------------------------------------------------------------------
  // Export
  // -----------------------------------------------------------------------
  global.TrysteroTransfer = TrysteroTransfer;

})(typeof window !== 'undefined' ? window : globalThis);
