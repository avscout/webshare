/**
 * webshare-transfer-trystero.js
 *
 * Trystero/Nostr transfer backend. Extends CoreTransfer with:
 *  - Serverless peer discovery via the Nostr decentralised relay network
 *  - Room-based pairing (shared code like "ruby-maple-42") instead of
 *    peer-ID-based pairing — so both sides just join the same named room
 *  - Native reconnect: Trystero's Nostr relay keep-alives handle connection
 *    recovery automatically; we only need a short grace window on peer-leave
 *    before declaring a fatal disconnect
 *  - Identity verification via per-session tokens exchanged in peer-info
 *
 * Depends on CoreTransfer (webshare-transfer-core.js).
 * Loads Trystero lazily via dynamic import() from esm.sh — no bundler needed.
 *
 * Exported global: TrysteroTransfer
 */
(function (global) {
  'use strict';

  const TRYSTERO_APP_ID = 'webshare-tudelft-v1';
  const TRYSTERO_CDN    = 'https://esm.sh/trystero/nostr';

  // How long to wait after a peer leaves before declaring a fatal disconnect.
  // Trystero's Nostr relay keep-alives will bring the peer back if the
  // disconnect was transient (mobile screen-off, brief network blip).
  const LEAVE_GRACE_MS = 8000;

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
    constructor({ peerInfo, iceServers } = {}) {
      super({ iceServers, peerInfo });

      // Trystero room state
      this._room           = null;
      this._remotePeerId   = null;   // Trystero's ephemeral ID for the other side
      this._leaveTimer     = null;   // grace-window timer after onPeerLeave
      this._isReconnecting = false;

      // Typed Trystero action senders (populated once _joinRoom resolves)
      this._sendPeerInfoAction = null;
      this._sendNoteAction     = null;
      this._sendLampAction     = null;
      this._sendPayloadAction  = null;
      this._sendAckAction      = null;
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
      return !!this._remotePeerId && !this._isReconnecting;
    }

    // Trystero reconnect is automatic; checkAndRecover is a no-op.
    checkAndRecover() { return false; }

    // -----------------------------------------------------------------------
    // Receiver flow
    // -----------------------------------------------------------------------

    async startReceiving() {
      if (this.role) throw new Error('Already started.');
      this.role = 'receiver';
      const code = generateRoomCode();
      this.peerId = code;
      this._log('info', `Trystero room code: ${code}`);
      await this._joinRoom(code);
      this.emit('show-qr', { kind: 'peerid', text: code });
    }

    // -----------------------------------------------------------------------
    // Sender flow
    // -----------------------------------------------------------------------

    // Join the same room as the receiver. Resolves when the receiver is
    // detected as a peer (onPeerJoin fires).
    async connect(roomCode) {
      if (this.role) throw new Error('Already started.');
      this.role = 'sender';
      this.peerId = roomCode;
      this._log('info', `Joining Trystero room: ${roomCode}`);
      return new Promise(async (resolve, reject) => {
        const timer = setTimeout(() => {
          reject(new Error('Connection timed out — is the receiver open on the same room code?'));
        }, 30000);
        // Register the one-shot listener BEFORE joining so we cannot miss
        // the event if onPeerJoin fires synchronously (it won't, but defensive).
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
      this._log('info', 'Loading Trystero (Nostr backend)…');
      let joinRoom;
      try {
        ({ joinRoom } = await import(TRYSTERO_CDN));
      } catch (e) {
        throw new Error('Failed to load Trystero: ' + (e.message || e));
      }
      this._log('ok', 'Trystero loaded — connecting to Nostr relays…');

      this._room = joinRoom({ appId: TRYSTERO_APP_ID }, roomCode);

      // Each makeAction returns [senderFn, receiverHandlerRegistrar].
      const [sendPayload, onPayload]     = this._room.makeAction('payload');
      const [sendAck,     onAck]         = this._room.makeAction('ack');
      const [sendPeerInfo, onPeerInfo]   = this._room.makeAction('peerinfo');
      const [sendNote,    onNote]        = this._room.makeAction('note');
      const [sendLamp,    onLamp]        = this._room.makeAction('lamp');

      this._sendPayloadAction  = sendPayload;
      this._sendAckAction      = sendAck;
      this._sendPeerInfoAction = (data, targetId) => sendPeerInfo(data, targetId);
      this._sendNoteAction     = sendNote;
      this._sendLampAction     = sendLamp;

      // Peer lifecycle
      this._room.onPeerJoin(id  => this._onPeerJoin(id));
      this._room.onPeerLeave(id => this._onPeerLeave(id));

      // Incoming data
      onPeerInfo(({ info, token } = {}, peerId) => {
        // Use CoreTransfer's identity verification
        this._handlePeerInfo(info, token);
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
    }

    // -----------------------------------------------------------------------
    // Peer lifecycle
    // -----------------------------------------------------------------------

    _onPeerJoin(peerId) {
      const isReconnect = this._isReconnecting || (this._leaveTimer !== null);
      if (this._leaveTimer) { clearTimeout(this._leaveTimer); this._leaveTimer = null; }
      this._remotePeerId   = peerId;
      this._isReconnecting = false;
      // Send peer-info so the other side can display our identity.
      this._sendPeerInfo();
      if (isReconnect) {
        this._log('ok', `Peer reconnected (${peerId.slice(0, 8)}…)`);
        this.emit('reconnected');
      } else {
        this._log('ok', `Peer joined room (${peerId.slice(0, 8)}…)`);
        this.emit('connected');
      }
    }

    _onPeerLeave(peerId) {
      if (peerId !== this._remotePeerId) return;
      this._log('info', 'Peer left room — waiting for reconnect…');
      this._isReconnecting = true;
      this._peerInfoSent   = false;  // will need to re-send on rejoin
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

    close() {
      if (this._leaveTimer) { clearTimeout(this._leaveTimer); this._leaveTimer = null; }
      if (this._room) { try { this._room.leave(); } catch {} this._room = null; }
      this._remotePeerId        = null;
      this._isReconnecting      = false;
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
