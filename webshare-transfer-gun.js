/**
 * webshare-transfer-gun.js
 *
 * GunTransport — experimental P2P sync backend using GunDB.
 *
 * Key differences from Trystero:
 *  - Gossip by default: Gun propagates data through all known peers,
 *    even ones that have never directly connected to each other.
 *  - CRDT conflict resolution: Gun uses HAM (Hypothetical Amnesia Machine)
 *    to resolve conflicts automatically — last-write-wins by wall clock.
 *  - No explicit connect/disconnect: Gun syncs continuously whenever
 *    peers are reachable. No handshake required.
 *  - Persistent graph: data is stored in IndexedDB and survives page reloads.
 *
 * Architecture:
 *  - All FieldSync data lives under gun.get('fieldsync/<roomCode>')
 *  - Sessions stored as gun.get('fieldsync/<roomCode>/sessions/<id>')
 *  - Source info stored as gun.get('fieldsync/<roomCode>/source-info')
 *  - Members stored as gun.get('fieldsync/<roomCode>/members/<deviceId>')
 *
 * Limitations vs Trystero:
 *  - No explicit peer-info exchange (no avatar/emoji in real-time)
 *  - No note/lamp ephemeral messaging (Gun is persistent, not ephemeral)
 *  - Gun relay servers are different from Nostr relays
 *
 * Exported global: GunTransport, GunRoomManager
 */
(function (global) {
  'use strict';

  const GUN_CDN = 'https://cdn.jsdelivr.net/npm/gun/gun.js';
  const SEA_CDN = 'https://cdn.jsdelivr.net/npm/gun/sea.js';

  // Public Gun relay peers — community-hosted super peers
  // gun.eco is the official Gun relay maintained by the Gun team
  const GUN_RELAY_PEERS = [
    'https://gun.eco/gun',
    'https://peer.wallie.io/gun',
  ];

  let _gunModulePromise = null;

  function _loadGun() {
    if (_gunModulePromise) return _gunModulePromise;
    _gunModulePromise = new Promise((resolve, reject) => {
      const finish = () => {
        if (global.Gun || global.GUN) resolve(global.Gun || global.GUN);
        else reject(new Error('GUN not found after script load'));
      };
      const loadScript = (src) => new Promise((res, rej) => {
        const s = document.createElement('script');
        s.src = src; s.onload = res; s.onerror = rej;
        document.head.appendChild(s);
      });
      if (global.Gun || global.GUN) { finish(); return; }
      // Load Gun first, then SEA (SEA extends Gun)
      loadScript(GUN_CDN)
        .then(() => loadScript(SEA_CDN))
        .then(finish)
        .catch(() => reject(new Error('Failed to load Gun.js or SEA')));
    });
    return _gunModulePromise;
  }

  // Pre-fetch Gun on script load
  _loadGun().catch(() => { _gunModulePromise = null; });

  // -----------------------------------------------------------------------
  // GunTransport
  //
  // Implements the same callback interface as TrysteroTransfer so it
  // can be used as a drop-in with SyncManager and SourceSyncManager.
  // -----------------------------------------------------------------------
  class GunTransport {
    constructor({ roomCode, password, deviceId, peerInfo } = {}) {
      this.roomCode  = roomCode;
      this.password  = password;
      this.deviceId  = deviceId;
      this.peerInfo  = peerInfo;

      this._gun      = null;
      this._room     = null;        // gun.get('fieldsync/<roomCode>')
      this._sessions = null;        // gun.get('.../sessions')
      this._members  = null;        // gun.get('.../members')
      this._source   = null;        // gun.get('.../source-info')
      this._running  = false;

      // Seen message IDs to prevent duplicate processing
      this._seenSessionIds = new Set();

      // Callbacks — same interface as TrysteroTransfer
      this.onSyncFull      = null;  // (sessions, fromPeerId) => void
      this.onSyncDelta     = null;  // (session,  fromPeerId) => void
      this.onSyncRequest   = null;  // not used by Gun (pull model)
      this.onSourceInfo    = null;  // ({ hash, uploadedAt }, fromPeerId) => void
      this.onSourceRequest = null;  // not used by Gun
      this.onSourceData    = null;  // ({ filename, uploadedAt, hash, rows }, fromPeerId) => void
      this.onMbrSync       = null;  // (data) => void
      this._onNote         = null;  // (text) => void  — wired by GunRoomManager
      this._onLamp         = null;  // (on) => void    — wired by GunRoomManager

      // Connection state — Gun doesn't have explicit connect events
      // so we simulate "connected" after a short delay
      this._connected = false;
      this._connectedPeers = new Set(); // compat with Trystero checks
      this._peerInfo = new Map();       // compat with SourceSyncManager
    }

    async start() {
      if (this._running) return;
      const GUN = await _loadGun();
      if (!GUN) throw new Error('Gun.js failed to load');

      this._gun = GUN({
        peers: GUN_RELAY_PEERS,
        localStorage: false,  // use IndexedDB via Radisk
        radisk: true,
      });

      const ns = `fieldsync-${this.roomCode}`;
      this._room     = this._gun.get(ns);
      this._sessions = this._room.get('sessions');
      this._members  = this._room.get('members');
      this._source   = this._room.get('source');

      this._running = true;
      this._subscribeToSessions();
      this._subscribeToMembers();
      this._subscribeToSource();
      this._subscribeToEphemeral();
      this._announcePresence();

      // Simulate "connected" after 2s — Gun doesn't have explicit handshakes
      setTimeout(() => {
        if (!this._connected) {
          this._connected = true;
          if (this.onSyncRequest) this.onSyncRequest(null); // trigger full sync
        }
      }, 2000);
    }

    // -----------------------------------------------------------------------
    // Session sync
    // -----------------------------------------------------------------------

    _subscribeToSessions() {
      this._sessions.map().on(async (data, id) => {
        if (!data || !id || id === '_') return;
        if (data.deviceId === this.deviceId) return; // skip own echoed writes
        if (!data.enc) return;
        const session = await this._decrypt(data.enc);
        if (session && session.id && this.onSyncDelta) this.onSyncDelta(session, null);
      });
    }

    // Send a single session delta (called by SyncManager.broadcastDelta)
    async sendSyncDelta(session) {
      if (!this._sessions || !session || !session.id) return;
      try {
        const enc = await this._encrypt(session);
        if (!enc) return;
        this._sessions.get(session.id).put({ enc, deviceId: this.deviceId });
      } catch (e) {
        console.warn('[GunTransport] sendSyncDelta error:', e);
      }
    }

    // Gun already propagates everything — push all our sessions encrypted
    async sendSyncFull(sessions, targetPeerId) {
      if (!sessions || !this._sessions) return;
      for (const s of sessions) {
        if (s && s.id) {
          const enc = await this._encrypt(s);
          if (enc) {
            try { this._sessions.get(s.id).put({ enc, deviceId: this.deviceId }); } catch {}
          }
        }
      }
    }

    // Pull all sessions we don't have yet
    sendSyncRequest(targetPeerId) {
      this._sessions.map().once(async (data, id) => {
        if (!data || !id || id === '_' || !data.enc) return;
        const session = await this._decrypt(data.enc);
        if (session && session.id && this.onSyncDelta) this.onSyncDelta(session, null);
      });
    }

    // -----------------------------------------------------------------------
    // Member sync
    // -----------------------------------------------------------------------

    _subscribeToMembers() {
      this._members.map().on(async (data, deviceId) => {
        if (!data || !deviceId || deviceId === '_' || !data.enc) return;
        const member = await this._decrypt(data.enc);
        if (member && member.deviceId && this.onMbrSync) {
          this.onMbrSync({ members: [member], removed: [] });
        }
      });
    }

    async sendMbrSync(data, targetPeerId) {
      if (!this._members || !data) return;
      const members = Array.isArray(data.members) ? data.members
                    : Array.isArray(data) ? data : [data];
      for (const m of members) {
        if (m && m.deviceId) {
          const enc = await this._encrypt(m);
          if (enc) {
            try { this._members.get(m.deviceId).put({ enc, deviceId: m.deviceId }); } catch {}
          }
        }
      }
    }

    // -----------------------------------------------------------------------
    // Source sync
    // -----------------------------------------------------------------------

    _subscribeToSource() {
      // Source info (hash + metadata) — encrypted
      this._source.get('info').on(async (data) => {
        if (!data || !data.enc) return;
        const info = await this._decrypt(data.enc);
        if (info && info.hash && this.onSourceInfo) this.onSourceInfo(info, null);
      });

      // Full source data — encrypted (contains hostnames, IPs, MACs)
      this._source.get('data').once(async (data) => {
        if (!data || !data.enc) return;
        const src = await this._decrypt(data.enc);
        if (src && src.rows && this.onSourceData) this.onSourceData(src, null);
      });
    }

    async sendSourceInfo(data, targetPeerId) {
      if (!this._source || !data) return;
      const enc = await this._encrypt(data);
      if (enc) { try { this._source.get('info').put({ enc, deviceId: this.deviceId }); } catch {} }
    }

    sendSourceRequest(targetPeerId) {
      this._source.get('data').once(async (data) => {
        if (!data || !data.enc) return;
        const src = await this._decrypt(data.enc);
        if (src && src.rows && this.onSourceData) this.onSourceData(src, null);
      });
    }

    async sendSourceData(data, targetPeerId) {
      if (!this._source || !data) return;
      try {
        // Encrypt the entire source payload — rows included — before it
        // touches the relay. The relay only ever sees ciphertext.
        const encData = await this._encrypt(data);
        if (encData) this._source.get('data').put({ enc: encData, deviceId: this.deviceId });
        // Encrypt info too
        const encInfo = await this._encrypt({
          hash      : data.hash,
          uploadedAt: data.uploadedAt,
          filename  : data.filename,
          rowCount  : data.rows.length,
        });
        if (encInfo) this._source.get('info').put({ enc: encInfo, deviceId: this.deviceId });
      } catch (e) {
        console.warn('[GunTransport] sendSourceData error:', e);
      }
    }

    // -----------------------------------------------------------------------
    // Presence
    // -----------------------------------------------------------------------

    _announcePresence() {
      if (!this._members || !this.deviceId || !this.peerInfo) return;
      const announce = async () => {
        const enc = await this._encrypt({
          ...this.peerInfo,
          deviceId  : this.deviceId,
          lastSeenAt: Date.now(),
        });
        if (enc) {
          try { this._members.get(this.deviceId).put({ enc, deviceId: this.deviceId }); } catch {}
        }
      };
      announce();
      // Refresh presence every 60s — re-encrypt with fresh timestamp
      setInterval(() => { if (this._running) announce(); }, 60000);
    }

    // -----------------------------------------------------------------------
    // Note and lamp — real-time via Gun subscriptions
    // -----------------------------------------------------------------------

    _subscribeToEphemeral() {
      // Note — triggers on every keystroke from any peer
      this._room.get('note').on(async (data) => {
        if (!data || data.deviceId === this.deviceId || !data.enc) return;
        const payload = await this._decrypt(data.enc);
        if (payload && this._onNote) this._onNote(payload.text || '');
      });

      // Lamp toggle
      this._room.get('lamp').on(async (data) => {
        if (!data || data.deviceId === this.deviceId || !data.enc) return;
        const payload = await this._decrypt(data.enc);
        if (payload && this._onLamp) this._onLamp(!!payload.on);
      });
    }

    async sendNote(text) {
      if (!this._room) return;
      const enc = await this._encrypt({ text: String(text == null ? '' : text), at: Date.now() });
      if (enc) { try { this._room.get('note').put({ enc, deviceId: this.deviceId }); } catch {} }
    }

    async sendLamp(on) {
      if (!this._room) return;
      const enc = await this._encrypt({ on: !!on, at: Date.now() });
      if (enc) { try { this._room.get('lamp').put({ enc, deviceId: this.deviceId }); } catch {} }
    }
    isAlive()   { return this._running && this._connected; }

    stop() {
      this._running   = false;
      this._connected = false;
      if (this._gun) {
        try { this._gun.off(); } catch {}
        this._gun = null;
      }
    }

    // -----------------------------------------------------------------------
    // Encryption — SEA symmetric encryption using the shared group password
    //
    // Every record is encrypted to a single 'enc' blob before it touches the
    // relay. Only deviceId (a random UUID, not sensitive) stays in cleartext
    // so we can filter our own echoed writes. The relay never sees task data,
    // hostnames, IPs, MACs, or any source content — only ciphertext.
    // -----------------------------------------------------------------------

    async _encrypt(obj) {
      const SEA = global.SEA || (global.Gun && global.Gun.SEA);
      if (!SEA || !this.password) return null;
      try {
        return await SEA.encrypt(JSON.stringify(obj), this.password);
      } catch (e) {
        console.warn('[GunTransport] encrypt error:', e);
        return null;
      }
    }

    async _decrypt(blob) {
      const SEA = global.SEA || (global.Gun && global.Gun.SEA);
      if (!SEA || !this.password || !blob) return null;
      try {
        const json = await SEA.decrypt(blob, this.password);
        return json ? JSON.parse(json) : null;
      } catch (e) {
        console.warn('[GunTransport] decrypt error:', e);
        return null;
      }
    }
  }

  // -----------------------------------------------------------------------
  // GunRoomManager
  //
  // Mirrors TrysteroRoomManager's interface so index.html can use it
  // interchangeably via getActiveTransport().
  // -----------------------------------------------------------------------
  const GunRoomManager = {
    _transport: null,

    isConnected() {
      return !!this._transport && this._transport.isAlive();
    },

    async init(roomCode, password, deviceId, peerInfo) {
      if (this._transport) await this.stop();
      this._transport = new GunTransport({ roomCode, password, deviceId, peerInfo });

      // Wire callbacks to SyncManager / SourceSyncManager
      this._transport.onSyncDelta    = (s)    => SyncManager.receiveDelta(s, null);
      this._transport.onSyncRequest  = ()     => SyncManager.sendFullTo(null);
      this._transport.onSourceInfo   = (d, p) => SourceSyncManager.receiveInfo(d, p);
      this._transport.onSourceData   = (d, p) => SourceSyncManager.receiveData(d, p);
      this._transport._onNote        = (text) => setPeerNoteText(text);
      this._transport._onLamp        = (on)   => setLampState(on, { send: false });
      this._transport.onMbrSync      = (d) => {
        if (!d) return;
        const members = Array.isArray(d.members) ? d.members
                      : Array.isArray(d) ? d : [d];
        const changed = GroupStore.mergeMemberList(members);
        if (changed) {
          renderHomeGroupMembers();
          refreshMyDeviceMembers();
        }
      };

      setActiveTransport(this._transport);
      await this._transport.start();
    },

    async stop() {
      if (this._transport) {
        this._transport.stop();
        this._transport = null;
      }
      setActiveTransport(null);
    },

    getConnectedMembers() {
      // Gun doesn't track connected peers explicitly — return all known members
      return GroupStore.getMembers().map(m => ({ info: m, peerId: m.deviceId }));
    },
  };

  // -----------------------------------------------------------------------
  // Exports
  // -----------------------------------------------------------------------
  global.GunTransport    = GunTransport;
  global.GunRoomManager  = GunRoomManager;

})(typeof window !== 'undefined' ? window : globalThis);
