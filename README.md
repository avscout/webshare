# webshare

A peer-to-peer transfer demo for moving session data between two browsers — typically a phone and a desktop — without any persistent cloud storage. Built on WebRTC with two interchangeable signaling backends.

**Live demo:** https://avscout.github.io/webshare

## What this is

This repository contains two things:

1. **`webshare-transfer.js`** — a small standalone JavaScript library that moves arbitrary JSON-serializable payloads peer-to-peer between two browsers. It has no opinion about what the payload contains and no DOM dependencies. Designed to be imported into other apps (such as AVScout).
2. **`index.html`** — a demo page that consumes the library. It defines its own session data model, manages local IndexedDB storage, and handles conflict resolution when an incoming session matches an existing local one.

The split is intentional. The library handles the transfer plumbing; the consuming application handles the meaning of the data.

## How it works

### High-level flow

The receiver (typically a laptop) opens the page and clicks **Start**. A short, human-readable peer ID such as `idb-tomato-rabbit-42` is generated locally and displayed as both a QR code and plain text. The sender (typically a phone) opens the same page, picks a session from its local list, then either scans the QR code or types the peer ID. The two browsers establish a direct WebRTC data channel and the session payload flows across.

When the payload arrives on the receiver, the demo checks whether a session with that ID already exists locally. If not, it stores the new session. If it does, the demo shows a dialog with both versions side by side and asks the user whether to skip, merge, or replace.

### The two backends

The library supports two ways of establishing the WebRTC connection. Both produce the same encrypted peer-to-peer channel; only the signaling step differs.

**PeerJS backend (default).** Uses the free PeerJS Cloud signaling server. The receiver registers a peer ID and the sender connects to it. Works across networks — phone on cellular, desktop on Wi-Fi, both fine. The PeerJS service is involved only for the initial handshake; data transfer itself is direct between browsers.

**Raw WebRTC backend.** No third-party signaling server. Instead, the WebRTC offer/answer SDPs are exchanged optically via QR codes — the desktop displays animated chunks of the offer, the phone scans them, then the phone displays its answer chunks back for the desktop's webcam. Only works reliably when both devices are on the same local network because no TURN relay is configured.

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

The library handles peer ID generation, retry-on-collision, ICE state tracking, SDP compression for QR efficiency, chunked transfer for large payloads, and reassembly. The consumer handles UI rendering and payload semantics.

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

### Conflict resolution

When an incoming session matches a local one by ID, the demo shows three options:

- **Skip** — incoming session is discarded, local copy unchanged
- **Merge (phone wins)** — `{ ...local.data, ...incoming.data }`, modified timestamps and name from the sender
- **Replace** — local copy is overwritten entirely with the incoming one

This decision is intentionally in the demo, not the library. Different applications need different conflict rules; AVScout might use per-record timestamps, an approval flow, or a different merge strategy entirely.

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

All three JS libraries are loaded from jsDelivr CDN first, with the local files as a fallback if the CDN is unreachable.

## Requirements

- Both devices must open the same URL (same origin — IndexedDB is partitioned per origin)
- HTTPS (provided automatically by GitHub Pages)
- Camera permission on at least one device for QR scanning
- Modern browser with WebRTC and `getUserMedia` support — Chromium, Firefox, or Safari

For the PeerJS backend, both devices need internet access (for signaling). For the Raw backend, both devices need to be on the same local network.

## Trying it out

Start with the PeerJS backend since it's more forgiving:

1. Open the page on your laptop. Switch to **Receive** mode. Click **Start**.
2. A QR code and a peer ID appear. Leave the laptop here.
3. Open the page on your phone. Switch to **Send** mode.
4. Click **New session** — a session is created with example asset data already filled in. Click **Create**.
5. The session appears in the list. Tap it to select.
6. Tap **Start camera** in step 2 and point the phone at the laptop's QR code.
7. Within a couple of seconds, the laptop logs "Data channel open" and the session arrives. Done.

To test the conflict-resolution flow, modify the session on the phone (edit some records or add new ones) and transfer again. The laptop will pop up the conflict dialog showing both versions.

## What this demo is for

This started as an experiment in moving IndexedDB data between devices and grew into a clean abstraction. The intent is to use `webshare-transfer.js` as a building block for **AVScout**, an asset-scouting application where field workers collect data on a phone and sync it to a laptop for analysis.

The demo is functional but intentionally minimal. It demonstrates the transfer mechanism end-to-end with realistic-looking data. It is not a production-ready application.

## Browser support

Tested in:
- Chromium-based browsers (Chrome, Edge) on Android, macOS, Windows
- Firefox on macOS, Windows
- Safari on iOS, macOS

WebRTC and `getUserMedia` are required and well-supported in all current versions of these browsers.

## License

MIT
