# webshare

A peer-to-peer transfer demo for moving session data between two browsers — typically a phone and a desktop — without any persistent cloud storage. Built on WebRTC with two interchangeable signaling backends.

## What this is

This repository contains two things:

1. **`webshare-transfer.js`** — a small standalone JavaScript library that moves arbitrary JSON-serializable payloads peer-to-peer between two browsers. It has no opinion about what the payload contains and no DOM dependencies. Designed to be imported into other apps.
2. **`index.html`** — a demo page that consumes the library. It defines its own session data model, manages local IndexedDB storage, and handles conflict resolution when an incoming session matches an existing local one.

The split is intentional. The library handles the transfer plumbing; the consuming application handles the meaning of the data.

## How it works

### High-level flow

The receiver (typically a laptop) opens the page. As soon as Receive mode is active, a short human-readable peer ID such as `idb-tomato-rabbit-42` is generated locally and displayed as both a QR code and plain text. The sender (typically a phone) opens the same page, picks a session from its local list, then either scans the QR code or types the peer ID. The two browsers establish a direct WebRTC data channel and the session payload flows across.

When the payload arrives on the receiver, the demo checks whether a session with that ID already exists locally. If not, it stores the new session. If it does, the demo shows a dialog with both versions side by side and asks the user whether to skip, merge, or replace.

A persistent log dock at the bottom of the page shows all activity (connections, transfers, errors) as it happens. Click the header to collapse it.

### The two backends

The library supports two ways of establishing the WebRTC connection. Both produce the same encrypted peer-to-peer channel; only the signaling step differs.

**PeerJS backend (default).** Uses the free PeerJS Cloud signaling server. The receiver registers a peer ID and the sender connects to it. Works across networks — phone on cellular, desktop on Wi-Fi, both fine. The PeerJS service is involved only for the initial handshake; data transfer itself is direct between browsers.

**Raw WebRTC backend.** No third-party signaling server. Instead, the WebRTC offer/answer SDPs are exchanged optically via QR codes — the desktop displays animated chunks of the offer, the phone scans them, then the phone displays its answer chunks back for the desktop's webcam. Only works reliably when both devices are on the same local network because no TURN relay is configured. The demo shows a masked public-IP "network ID" on this mode so users can visually verify they're on the same network before attempting the handshake.

The backend toggle is at the top of the page. Both produce the same final result; choose based on your network situation.

### The transfer library API

For consuming the library from another app:

```javascript
// Receiver
const t = new SessionTransfer({ backend: 'peerjs' });
t.on('log', ({ level, message }) => console.log(level, message));
t.on('show-qr', ({ kind, text, chunks }) => {
  // kind === 'peerid'         → display single QR with `text`
  // kind === 'offer-chunks'   → cycle through `chunks` as animated QR
  // kind === 'answer-chunks'  → cycle through `chunks` as animated QR
});
t.on('connected', () => { /* peer connection open */ });
t.on('progress', ({ received, total }) => { /* update UI */ });
t.onPayload = async (payload) => {
  // YOUR application decides what to do here.
  // Return value (any) is sent back to the sender as ack.
  return { applied: true };
};
await t.startReceiving();

// For raw backend, feed scanned QR chunks back into the library:
//   t.feedScannedChunk(textFromCamera);

// Sender (PeerJS)
const t = new SessionTransfer({ backend: 'peerjs' });
await t.connect(targetPeerId);
t.onAck = (response) => { /* receiver's return value */ };
await t.send(myPayload);

// When done, on both sides:
t.close();
```

### Custom PeerJS signaling server

By default the library uses the public PeerJS Cloud (`0.peerjs.com`). To point at a self-hosted PeerServer instead — for example one running on institutional infrastructure — pass a `peerServer` config:

```javascript
new SessionTransfer({
  backend: 'peerjs',
  peerServer: {
    host: 'your-server.example.com',
    port: 443,
    path: '/peerjs',
    secure: true
  }
});
```

PeerServer is open source (`npm install peer`) and runs on any Node.js host or Docker. Switching to it requires only this config change; the rest of the code is unchanged.

### Session data model (demo only)

The demo defines a session as a JSON object with this shape:

```json
{
  "id": "idb-tomato-rabbit-42",
  "name": "Building A audit",
  "createdAt": "2026-05-20T09:00:00Z",
  "modifiedAt": "2026-05-20T18:30:00Z",
  "data": {
    "asset_001": { "category": "monitor", "room": "102", "found": true },
    "asset_002": { "category": "laptop",  "room": "201", "found": false }
  }
}
```

The `id` is generated once when the session is created and never changes. This is what makes re-transfers idempotent — the receiver always knows whether an incoming session is a new one or an update to an existing one.

The `data` field is an object keyed by record IDs. The demo uses asset-style records as example data, but the library treats `data` as opaque — any JSON-serializable shape works.

### Managing sessions

Both Send and Receive mode show a session list with **New / Edit / Delete** controls. From either mode you can review what's currently stored on the device, edit JSON data inline, or remove sessions you no longer need. On the receive side, the panel is collapsible and shows the current count.

### Conflict resolution

When an incoming session matches a local one by ID, the demo shows three options:

- **Skip** — incoming session is discarded, local copy unchanged
- **Merge (phone wins)** — `{ ...local.data, ...incoming.data }`, modified timestamps and name from the sender
- **Replace** — local copy is overwritten entirely with the incoming one

This decision is intentionally in the demo, not the library. Different applications need different conflict rules.

## Dependencies

This project uses the following external libraries and services. All are loaded from public CDNs at runtime with local fallbacks included in the repository.

### JavaScript libraries

| Library | Purpose | Source |
|---|---|---|
| **PeerJS** (`peerjs@1.5.4`) | Wraps WebRTC with peer-ID-based signaling. Provides the `Peer` and `DataConnection` classes used by the PeerJS backend. | [peerjs.com](https://peerjs.com/) · [github.com/peers/peerjs](https://github.com/peers/peerjs) |
| **qrcode-generator** (`qrcode-generator@1.4.4`) | Generates QR codes as inline SVG for both backends (peer IDs and Raw WebRTC offer/answer chunks). | [github.com/kazuhikoarase/qrcode-generator](https://github.com/kazuhikoarase/qrcode-generator) |
| **jsQR** (`jsqr@1.4.0`) | Decodes QR codes from webcam video frames. Used for the QR-handshake flow and for scanning peer IDs. | [github.com/cozmo/jsQR](https://github.com/cozmo/jsQR) |

The CDN is **jsDelivr** (`cdn.jsdelivr.net`). If a script fails to load from the CDN, the loader falls back to a local copy in the repository root (`qrcode.js`, `jsQR.js`, `peerjs.min.js`).

### External services

| Service | Purpose | When used |
|---|---|---|
| **PeerJS Cloud** (`0.peerjs.com`) | Signaling server: brokers the initial offer/answer exchange between two peers. Does not see message contents (DTLS-encrypted end-to-end). | PeerJS backend only |
| **Google STUN** (`stun.l.google.com:19302`, `stun1.l.google.com:19302`) | Helps each peer discover its public IP for NAT traversal. No data flows through STUN servers. | Both backends |
| **ipify.org** (`api.ipify.org`) | Returns the public IP of the current device as JSON. Used to display a masked "network ID" so two peers can visually verify they're on the same network before using Raw WebRTC. | Raw backend only; silently hidden if unreachable |

### Browser APIs

- **WebRTC** (`RTCPeerConnection`, `RTCDataChannel`) — peer-to-peer encrypted data channels
- **getUserMedia** — webcam access for QR scanning
- **IndexedDB** — local storage of sessions (demo only)
- **CompressionStream / DecompressionStream** — gzip compression of SDP for smaller QR codes (Raw backend)
- **fetch** with `AbortController` — for the ipify network-ID call

## File structure

```
webshare/
├── index.html               ← demo page (thin consumer of the library)
├── webshare-transfer.js     ← the standalone transfer library
├── qrcode.js                ← qrcode-generator (local fallback for CDN)
├── jsQR.js                  ← jsQR scanner (local fallback for CDN)
├── peerjs.min.js            ← PeerJS (local fallback for CDN)
├── README.md                ← this file
└── .nojekyll                ← tells GitHub Pages to skip Jekyll
```

## Requirements

- Both devices must open the same URL (same origin — IndexedDB is partitioned per origin)
- HTTPS (provided automatically by GitHub Pages)
- Camera permission on at least one device for QR scanning
- Modern browser with WebRTC and `getUserMedia` support — Chromium, Firefox, or Safari

For the PeerJS backend, both devices need internet access (for signaling). For the Raw backend, both devices need to be on the same local network.

## Trying it out

Start with the PeerJS backend since it's more forgiving:

1. Open the page on your laptop. It opens in **Receive** mode by default — a QR code and peer ID appear immediately.
2. Open the page on your phone. Switch to **Send** mode.
3. Click **New session** — a session is created with example asset data already filled in. Click **Create**.
4. The session appears in the list. Tap it to select.
5. Tap **Start camera** and point the phone at the laptop's QR code.
6. Within a couple of seconds, the laptop logs "Data channel open" and the session arrives.

To test the conflict-resolution flow, modify the session on the phone (edit some records or add new ones) and transfer again. The laptop will pop up the conflict dialog showing both versions.

## What this demo is for

This started as an experiment in moving IndexedDB data between devices and grew into a clean abstraction. The intent is to use `webshare-transfer.js` as a building block for applications where field workers collect data on a phone and sync it to a laptop for analysis.

The demo is functional but intentionally minimal. It demonstrates the transfer mechanism end-to-end with realistic-looking data. It is not a production-ready application.

## Browser support

Tested in:
- Chromium-based browsers (Chrome, Edge) on Android, macOS, Windows
- Firefox on macOS, Windows
- Safari on iOS, macOS

WebRTC and `getUserMedia` are required and well-supported in all current versions of these browsers.

## License

MIT
