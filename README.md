# ClipFlow

Send text, links, OTPs, commands, images, PDFs and any other file between your own devices over Wi-Fi.
One Java file, no dependencies, no internet, no cloud.

## Run it

You need **Java 11 or newer** (a JDK, e.g. https://adoptium.net).

| System  | How to start                                   |
|---------|------------------------------------------------|
| Windows | double-click `run.bat` (downloads Java automatically if missing) or `start.bat` |
| macOS   | double-click `start.command` (first time: right-click > Open) |
| Linux   | `./start.sh`                                   |
| Manual  | `javac ClipFlowHub.java` then `java ClipFlowHub [port]` |

The launcher compiles the source, starts the server, opens your browser and prints a box with the address
plus a QR code, for example `http://192.168.1.23:8080`.

## Connect two devices

1. Put both devices on the **same Wi-Fi** (or one phone hotspot).
2. On the second device, scan the QR code in the terminal (or the one under **Pair device** in the web page), or type the address.
3. Each device gets a name such as "Emerald Phone" and shows up on the other's **Nearby Radar**.
4. Pick **Send All** or tap one device, then use **Send Text** or **Send Files**. Received items appear in **Inbox / Feed**.

Tap your own device name to rename it.

## If devices cannot see each other

- **Windows Firewall:** the first start asks to allow Java. Allow it on *Private* networks. If you clicked Cancel, open
  Windows Security > Firewall > "Allow an app" and tick Java for Private.
- **Guest / public Wi-Fi** often blocks device-to-device traffic ("client isolation"). Use your normal Wi-Fi or a phone hotspot.
- **VPNs** can hide the LAN. Disconnect while using ClipFlow.
- **Wrong address in the QR:** the terminal lists other addresses of the machine; try one of them.
- **Port busy:** run `start.bat 9090` / `./start.sh 9090`.

## Good to know

- **Copy button on phones:** browsers only allow the modern clipboard API on HTTPS or localhost, so over plain
  `http://192.168.x.x` ClipFlow automatically uses the older copy method. It works on current iOS and Android; if a
  browser still refuses, long-press the text to copy it.
- Received files are saved in `shared_drops/` next to the program. Name clashes become `name (1).ext`. Limit: 2 GB per file, 1 MB per text.
- Feed items disappear from the server after 1 hour; saved files stay on disk until you delete them.
- A device that stops sending heartbeats for 8 seconds is removed from the radar (phones do this when the screen locks) and returns when the page is open again.
- **There is no login.** Anyone on the same network who opens the address can see the feed and send to you. Use it on networks you trust.
- Two tabs of one browser appear as two devices, which is handy for trying it out on a single computer.
- The sender does not get its own broadcasts back in its inbox.

## API

| Endpoint | Purpose |
|----------|---------|
| `GET /` | the web app |
| `POST /api/heartbeat` | `{"id","alias","type"}` every 2.5 s |
| `GET /api/peers` | online devices |
| `POST /api/send` | headers `X-Target-To` (id or `ALL`), `X-Target-From`, `X-Packet-Type` (`text`/`file`), `X-File-Name`, optional `X-Sender-Id`; body is raw text or file bytes |
| `GET /api/inbox?peerId=` | packets for `ALL` or that id |
| `GET /api/download?file=` | file as attachment (`&inline=1` previews images) |
| `GET /api/info`, `GET /api/qr` | extras: connect addresses and a QR code (SVG) |

Quick test from a terminal:

```
curl -X POST http://localhost:8080/api/send -H "X-Target-To: ALL" -H "X-Target-From: curl" -H "X-Packet-Type: text" -d "hello"
```

## Files

- `ClipFlowHub.java` - the whole program (server, QR encoder, web UI)
- `run.bat` - Windows one-click: installs a portable JDK if needed, then calls `start.bat`
- `start.bat`, `start.command`, `start.sh` - launchers
- `ClipFlow.jar` - prebuilt copy, used only if no JDK is installed (just a JRE)
