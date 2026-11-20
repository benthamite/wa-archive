# wa-archive

wa-archive links to your WhatsApp account as a companion device (like
WhatsApp Web) and keeps a local SQLite archive of your messages. It also
serves a local API on a Unix socket through which a client can send, edit,
delete and react to messages, mark chats read and fetch attachments.

It is the backend of [wsp](https://github.com/benthamite/wsp), a WhatsApp
client for Emacs, in the way that signal-cli backs Signal clients. Other
programs can read the archive directly.

It is built on [whatsmeow](https://github.com/tulir/whatsmeow). It is not
affiliated with or endorsed by WhatsApp or Meta. Using an unofficial client
may be against WhatsApp's terms of service. Use it at your own risk.

## What it does

- Archives incoming and outgoing messages, edits, deletions, reactions,
  delivery and read receipts, and the read state you set on other devices.
  It does this live and from the history your phone sends when you link it.
- Keeps 90 days of messages by default (`-retention-days`; `0` keeps
  everything).
- Downloads an attachment only when asked. Voice notes and audio are the
  exception: they are downloaded as they arrive, and can be transcribed by a
  program you supply.
- Sends only what a local client or tool asks it to send. It sends no typing
  indicator or online status unless a client asks, and it withdraws an
  "online" status when the last client disconnects. Apart from that, its
  traffic is what the protocol needs to stay linked: acknowledgements,
  "inactive" delivery receipts, retry receipts, key uploads, keepalives and
  history-sync receipts.

## Requirements

- Go 1.27.2 or newer (see `go.mod`).
- A C compiler, because the SQLite driver
  ([go-sqlite3](https://github.com/mattn/go-sqlite3)) uses cgo.
- macOS or Linux. `install.sh` is for macOS only; on Linux, build by hand and
  use the systemd unit below.
- A phone with WhatsApp that can link another device.

## Build

```sh
git clone https://github.com/benthamite/wa-archive
cd wa-archive
go test ./...
go build -o bin/wa-archive .
```

## Link your phone

```sh
bin/wa-archive link 15551234567
```

Give your number in international format, with digits only. The command
prints a pairing code. On the phone, open WhatsApp > Linked devices > Link a
device > Link with phone number instead, and enter the code. When you link
it, it asks for your full history.

To remove the device later, run `bin/wa-archive unlink`. The archive is
kept. After an unlink, or after you remove the device from the phone, run
`link` again to relink.

## Run

```sh
bin/wa-archive run [-retention-days N]
```

`run` archives until the device is unlinked. Run it under a service manager
that restarts it **only after a nonzero exit**. The daemon exits 0 on
purpose in two cases:

- when it has been unlinked;
- when WhatsApp rejects it as an outdated client. Restarting cannot fix
  that, so it leaves a marker file and refuses to start until you install a
  new build.

It exits 1 on failures that a restart can fix.

### macOS (launchd)

Save this as `~/Library/LaunchAgents/wa-archive.plist`. Replace
`/path/to/wa-archive` and `/Users/you`, then load it with
`launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/wa-archive.plist`.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>wa-archive</string>
  <key>ProgramArguments</key>
  <array>
    <string>/path/to/wa-archive/bin/wa-archive</string>
    <string>run</string>
  </array>
  <!-- Optional: see "Configuration" below.
  <key>EnvironmentVariables</key>
  <dict>
    <key>WA_TRANSCRIBE</key>
    <string>/path/to/transcribe-script</string>
    <key>WA_ARCHIVE_ALERT</key>
    <string>/path/to/notify-script</string>
  </dict>
  -->
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
  <key>ThrottleInterval</key>
  <integer>30</integer>
  <key>StandardOutPath</key>
  <string>/Users/you/.local/state/wa-archive/launchd.out.log</string>
  <key>StandardErrorPath</key>
  <string>/Users/you/.local/state/wa-archive/launchd.err.log</string>
</dict>
</plist>
```

After that, `./install.sh` updates a running installation. It runs the
tests, builds and ad-hoc signs the binary, swaps it in and restarts the
agent. Then it waits up to 60 seconds for the daemon to report that it is
connected. If the new build does not connect, it puts the previous binary
back and exits nonzero, so an automated updater can rely on its exit status.
If your agent's label is not `wa-archive`, set `WA_ARCHIVE_LABEL` to that
label.

### Linux (systemd)

Save this as `~/.config/systemd/user/wa-archive.service`. Then run
`systemctl --user daemon-reload` and
`systemctl --user enable --now wa-archive`.

```ini
[Unit]
Description=WhatsApp archive (wa-archive)
After=network-online.target

[Service]
ExecStart=%h/path/to/wa-archive/bin/wa-archive run
Restart=on-failure
RestartSec=30
# Environment=WA_TRANSCRIBE=%h/bin/transcribe
# Environment=WA_ARCHIVE_ALERT=%h/bin/notify

[Install]
WantedBy=default.target
```

To update, rebuild, delete the `outdated` marker if there is one, and run
`systemctl --user restart wa-archive`.

## Configuration

| Variable | Meaning |
|---|---|
| `XDG_DATA_HOME`, `XDG_STATE_HOME` | Base directories (see below). |
| `WA_TRANSCRIBE` | A program that the daemon runs with the path of each voice note or audio file as it arrives. By convention it writes the transcript to that path plus `.txt`. If unset, nothing is transcribed. |
| `WA_ARCHIVE_ALERT` | A shell command (run with `/bin/sh -c`) that receives an urgent alert on its stdin. At present the only alert is an outdated-client rejection. If unset, the alert goes only to stderr. |
| `WA_ARCHIVE_LABEL` | `install.sh` only: the LaunchAgent's label (default `wa-archive`). |

## Files

Data (`$XDG_DATA_HOME/wa-archive`, default `~/.local/share/wa-archive`):

| Path | Contents |
|---|---|
| `device.db` | whatsmeow's device keys and session. Anyone who has this file can act as your linked device. |
| `messages.db` | The archive. Other programs may open it read-only. |
| `api.sock` | The API socket (while `run` is running). |
| `media/` | Downloaded attachments. |
| `outbox/` | The file-based send queue (see below). |
| `fetch/` | The file-based attachment-download queue (see below). |

State (`$XDG_STATE_HOME/wa-archive`, default `~/.local/state/wa-archive`):

| Path | Contents |
|---|---|
| `heartbeat.json` | `{"event": ..., "at": ...}`, rewritten when the daemon connects, every five minutes while it is connected, and on each archive write. A monitor can treat a stale file as a failure. |
| `unlinked` | Present after the device was unlinked. |
| `outdated` | Present after WhatsApp rejected the build as outdated. `run` will not start while it exists. |

All files are created with mode 0600 and directories with mode 0700. The
archive holds decrypted messages, so protect it as you would the phone.

## The socket API

Clients connect to `api.sock`. They exchange JSON objects, one per line, and
every client receives every event. Methods: `status`, `send`, `send_file`,
`edit`, `delete`, `react`, `mark_read`, `fetch`, `typing`, `presence` and
`op_status`.
Every method that acts on WhatsApp commits its result to `messages.db`
before it answers. Sends are journaled under a client-chosen operation id.
A client that lost an answer can repeat the request or call `op_status`, and
it gets the recorded outcome instead of a second send.

The contract, covering methods, errors, events and the tables that clients
read, is in wsp's
[docs/api.md](https://github.com/benthamite/wsp/blob/main/docs/api.md).
Changing the contract means changing both projects.

## File queues for scripts

Tools that would rather not speak the socket protocol can drop files:

- **Send:** write `outbox/<name>.json` containing
  `{"to": "<jid>", "text": "...", "file": "/abs/path"?, "reply_to": "<id>"?}`.
  The daemon sends jobs in name order. It moves each one to `outbox/sent/`,
  `outbox/failed/` (nothing went out) or `outbox/uncertain/` (it may have gone
  out), with a receipt that gives the message id.
- **Fetch an attachment:** write `fetch/<name>.json` containing
  `{"chat": "<chat key>", "id": "<message id>"}`. The file is saved under
  `media/`, and a receipt with its path is written to `fetch/done/` or
  `fetch/failed/`.

The daemon handles both queues every two seconds while it is connected. Use
a write-then-rename so that the daemon never reads a partly written job.

## Licence

GPL-3.0-or-later; see [LICENSE](LICENSE). whatsmeow is under MPL-2.0, which
allows it to be combined with GPL code. Its libsignal dependency is
GPL-3.0.
