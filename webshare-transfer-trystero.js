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

  const TRYSTERO_APP_ID = 'webshare-tudelft-v1';
  // esm.run is jsDelivr's ESM CDN — the officially recommended way to load
  // Trystero in a browser without a bundler.
  // 0.21.8/nostr is pinned to the version confirmed working.
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
    constructor({ peerInfo, iceServers, persistent = false, password = null, onPeerAccept = null } = {}) {
      super({ iceServers, peerInfo });

      // Trystero room state
      this._room            = null;
      this._remotePeerId    = null;    // most recently connected peer (for compat)
      this._connectedPeers  = new Set(); // all currently connected peer IDs
      this._leaveTimer      = null;
      this._isReconnecting  = false;
      this._pendingPeerJoins = new Map(); // peerId → { isReconnect }
      this._rejectedPeers   = new Set();

      // Action senders (populated once _joinRoom resolves)
      this._sendPeerInfoAction    = null;
      this._sendNoteAction        = null;
      this._sendLampAction        = null;
      this._sendPayloadAction     = null;
      this._sendAckAction         = null;
      this._sendSyncFullAction    = null;
      this._sendSyncDeltaAction   = null;
      this._sendSyncRequestAction = null;

      // Config
      this.persistent   = persistent;
      this.password     = password;
      this.onPeerAccept = onPeerAccept;

      // Sync callbacks — set externally by TrysteroRoomManager / SyncManager
      this.onSyncFull    = null;  // (sessions, fromPeerId) => void
      this.onSyncDelta   = null;  // (session,  fromPeerId) => void
      this.onSyncRequest = null;  // (fromPeerId) => void
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
      this._log('info', 'Connecting to Nostr relays…');
      let joinRoom;
      try {
        ({ joinRoom } = await _prefetchTrystero());
      } catch (e) {
        throw new Error('Failed to load Trystero: ' + (e.message || e));
      }
      this._log('ok', 'Trystero loaded — joining room…');

      const config = {
        appId      : TRYSTERO_APP_ID,
        relayUrls  : NOSTR_RELAY_URLS,
        turnConfig : TURN_CONFIG,
      };
      if (this.password) config.password = this.password;

      this._room = joinRoom(config, roomCode);

      // Log relay status after 3s with a clear summary.
      // Shows which relays are up/down and warns loudly if none connect.
      setTimeout(() => {
        if (!this._room) return;
        try {
          const sockets = this._room.getRelaySockets();
          let connected = 0, total = 0;
          sockets.forEach((ws, url) => {
            total++;
            const state = ws.readyState === 1 ? 'connected'
                        : ws.readyState === 0 ? 'connecting'
                        : ws.readyState === 2 ? 'closing' : 'closed';
            const level = ws.readyState === 1 ? 'ok' : ws.readyState === 0 ? 'info' : 'err';
            if (ws.readyState === 1) connected++;
            this._log(level, `Relay ${url} — ${state}`);
          });
      if (total === 0) {
            this._log('err', 'No relays found — Trystero may not be loaded correctly.');
          } else if (connected === 0) {
            this._log('err', `⚠ No relays reachable (0/${total}) — check network. Peer discovery will not work.`);
            this.emit('relay-status', { connected: 0, total });
          } else {
            this._log('ok', `${connected}/${total} relays connected — room ready.`);
            this.emit('relay-status', { connected, total });
          }
        } catch (e) {
          this._log('err', 'Could not check relay status: ' + e.message);
        }
      }, 3000);

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

      // Existing actions
      const [sendPayload,  onPayload]  = _makeAction('payload');
      const [sendAck,      onAck]      = _makeAction('ack');
      const [sendPeerInfo, onPeerInfo] = _makeAction('peerinfo');
      const [sendNote,     onNote]     = _makeAction('note');
      const [sendLamp,     onLamp]     = _makeAction('lamp');

      // Sync actions
      const [sendSyncFull,    onSyncFull]    = _makeAction('sync-full');
      const [sendSyncDelta,   onSyncDelta]   = _makeAction('sync-delta');
      const [sendSyncRequest, onSyncRequest] = _makeAction('sync-request');

      this._sendPayloadAction     = sendPayload;
      this._sendAckAction         = sendAck;
      this._sendPeerInfoAction    = (data, targetId) => sendPeerInfo(data, targetId);
      this._sendNoteAction        = sendNote;
      this._sendLampAction        = sendLamp;
      this._sendSyncFullAction    = (data, targetId) => sendSyncFull(data, targetId);
      this._sendSyncDeltaAction   = (data) => sendSyncDelta(data);
      this._sendSyncRequestAction = (data, targetId) => sendSyncRequest(data, targetId);

      // Peer lifecycle
      this._room.onPeerJoin(id  => this._onPeerJoin(id));
      this._room.onPeerLeave(id => this._onPeerLeave(id));

      // Incoming peer-info — gate on onPeerAccept before accepting.
      onPeerInfo(({ info, token } = {}, peerId) => {
        if (this._rejectedPeers.has(peerId)) return;

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

      // Sync handlers
      onSyncFull((sessions, peerId) => {
        if (this._rejectedPeers.has(peerId)) return;
        if (this.onSyncFull) this.onSyncFull(sessions, peerId);
      });

      onSyncDelta((session, peerId) => {
        if (this._rejectedPeers.has(peerId)) return;
        if (this.onSyncDelta) this.onSyncDelta(session, peerId);
      });

      onSyncRequest((_, peerId) => {
        if (this._rejectedPeers.has(peerId)) return;
        if (this.onSyncRequest) this.onSyncRequest(peerId);
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
