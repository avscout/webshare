# webshare

A single-file utility for transferring **IndexedDB** data between devices — phone to phone, phone to desktop, desktop to desktop. No server, no account, no cloud. Just a portable `.idbx` file moved by whatever channel you prefer: Bluetooth, AirDrop, Nearby Share, LocalSend, USB, email.

**Live demo:** https://avscout.github.io/webshare

## What it does

- **Export** any IndexedDB database from the current origin to a `.idbx` file
- **Share via OS** — hand the file straight to the operating system's share sheet (Web Share API)
- **Import** a `.idbx` file back into IndexedDB on another device

Schemas, indexes (with `unique` and `multiEntry` flags), and binary values (Blob, File, ArrayBuffer, TypedArrays, Date, Map, Set) survive the round trip.

## How to use it

1. Open `https://avscout.github.io/webshare` on **both devices**.
2. *Optional:* hit **Create demoDB** to seed a sample database with notes and a small PNG blob.
3. On the source device, enter the database name and hit **Export to file** (downloads a `.idbx`) or **Open share sheet** (mobile, hands off directly to AirDrop / Nearby Share / LocalSend / etc.).
4. Transfer the file to the other device by any means you like.
5. On the target device, drop the file into the import zone and hit **Import to IndexedDB**.

## Why same-origin matters

IndexedDB is sandboxed per origin. Both devices must open the page from the **same URL** (`https://avscout.github.io/webshare`) or the imported data lands in the wrong sandbox. That's why this is hosted as a static page rather than distributed as an HTML file you save locally — `file://` URLs each have their own origin.

## Browser support

| Feature | Chromium | Firefox | Safari |
|---|---|---|---|
| Export / Import | ✓ | ✓ | ✓ |
| Web Share (files) | mobile ✓, desktop partial | ✗ | mobile ✓ |

Falls back to download when Web Share isn't available.

## File format

The `.idbx` file is JSON with a small header, the full schema (including index definitions), and all records. Binary values are base64-encoded with type tags so they decode back to real `Blob` / `File` / typed array instances. Format version is embedded for forward compatibility.

## License

MIT
