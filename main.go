// wa-archive links to WhatsApp as a companion device and archives messages to
// SQLite for read-only use by other local programs.
//
// It is also the backend of wsp, an Emacs WhatsApp client, which reads the
// archive directly and acts through the local API (api.go).
//
// It sends only what it is asked to: the messages queued in its outbox
// (outbox.go) by local tools, and what a client asks for through the API on
// the user's behalf: messages, edits, deletions, reactions, read receipts
// (mark_read), and typing and online status (only through the typing and
// presence methods, which wsp leaves off by default; an "available" set that
// way is withdrawn when the last client disconnects or the daemon stops). It
// downloads an
// attachment only when a local tool or an API client asks for it, or, for voice notes and
// audio, as it arrives (fetch.go). Its other traffic is what the protocol
// needs to stay linked:
// acknowledgements, "inactive" delivery receipts (not shown in WhatsApp's
// apps; ordinary ones while a client has set the user available), retry receipts, key uploads, keepalives, history-sync receipts, and
// requests to the user's own phone to resend a message that failed to decrypt
// (whatsmeow sends these on a first retry; nobody else sees them).
//
// Usage:
//
//	wa-archive link PHONE   pair with a phone-number code, then exit
//	wa-archive run [-retention-days N]
//	                        archive until unlinked (the service); keep
//	                        N days of messages (default 90; 0 keeps all)
//	wa-archive unlink       remove this device from the phone's linked devices
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

// Exit statuses. The service manager should restart the daemon only after a
// nonzero exit (launchd KeepAlive SuccessfulExit=false, systemd
// Restart=on-failure).
const (
	exitUnlinked = 0
	exitFailure  = 1
)

type paths struct {
	data, state string
}

func defaultPaths() paths {
	home, _ := os.UserHomeDir()
	data := os.Getenv("XDG_DATA_HOME")
	if data == "" {
		data = filepath.Join(home, ".local", "share")
	}
	state := os.Getenv("XDG_STATE_HOME")
	if state == "" {
		state = filepath.Join(home, ".local", "state")
	}
	return paths{filepath.Join(data, "wa-archive"), filepath.Join(state, "wa-archive")}
}

func (p paths) deviceDB() string   { return filepath.Join(p.data, "device.db") }
func (p paths) messagesDB() string { return filepath.Join(p.data, "messages.db") }
func (p paths) heartbeat() string  { return filepath.Join(p.state, "heartbeat.json") }
func (p paths) unlinked() string   { return filepath.Join(p.state, "unlinked") }

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: wa-archive link PHONE | run [-retention-days N] | unlink")
		os.Exit(2)
	}
	syscall.Umask(0o077)
	p := defaultPaths()
	for _, dir := range []string{p.data, p.state} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			fatal("create %s: %v", dir, err)
		}
	}
	// The name shown under Linked devices on the phone.
	store.DeviceProps.Os = proto.String("wa-archive (read-only)")
	// Ask the phone for its full history at pairing time, not just the recent
	// batch. This only takes effect when linking.
	store.DeviceProps.RequireFullSync = proto.Bool(true)
	var err error
	switch os.Args[1] {
	case "link":
		if len(os.Args) != 3 {
			fatal("usage: wa-archive link PHONE")
		}
		err = link(p, os.Args[2])
	case "run":
		var o runOptions
		if o, err = parseRunFlags(os.Args[2:]); err == nil {
			err = run(p, o)
		}
	case "unlink":
		err = unlink(p)
	default:
		fatal("unknown command %q", os.Args[1])
	}
	if err != nil {
		fatal("%v", err)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "wa-archive: "+format+"\n", args...)
	os.Exit(exitFailure)
}

func newClient(ctx context.Context, p paths, level string) (*whatsmeow.Client, error) {
	container, err := sqlstore.New(ctx, "sqlite3", "file:"+p.deviceDB()+"?_foreign_keys=on&_busy_timeout=5000",
		waLog.Stdout("store", level, false))
	if err != nil {
		return nil, fmt.Errorf("open device store: %w", err)
	}
	device, err := container.GetFirstDevice(ctx)
	if err != nil {
		return nil, fmt.Errorf("load device: %w", err)
	}
	cli := whatsmeow.NewClient(device, waLog.Stdout("client", level, false))
	// Acknowledge a message only after the handler has committed it.
	cli.SynchronousAck = true
	// History chunks are downloaded by the handler, committed, and only then
	// receipted and deleted from the server (see handleHistoryNotification).
	cli.ManualHistorySyncDownload = true
	cli.DisableManualHistorySyncReceipt = true
	// Re-requesting from the phone sends peer messages; leave it off.
	cli.AutomaticMessageRerequestFromPhone = false
	return cli, nil
}

func link(p paths, phone string) error {
	ctx := context.Background()
	cli, err := newClient(ctx, p, "WARN")
	if err != nil {
		return err
	}
	if cli.Store.ID != nil {
		// After a logout whatsmeow drops the session but keeps the device row.
		// Only a device marked unlinked may be replaced.
		if _, err := os.Stat(p.unlinked()); err != nil {
			return errors.New("already linked; run `wa-archive unlink` first")
		}
		if err := cli.Store.Delete(ctx); err != nil {
			return fmt.Errorf("delete stale device: %w", err)
		}
		if cli, err = newClient(ctx, p, "WARN"); err != nil {
			return err
		}
	}
	qr, err := cli.GetQRChannel(ctx)
	if err != nil {
		return err
	}
	if err := cli.Connect(); err != nil {
		return err
	}
	defer cli.Disconnect()
	requested := false
	for item := range qr {
		switch item.Event {
		case "code":
			if requested {
				continue
			}
			requested = true
			code, err := cli.PairPhone(ctx, phone, true, whatsmeow.PairClientChrome, "Chrome (Mac OS)")
			if err != nil {
				return fmt.Errorf("request pairing code: %w", err)
			}
			fmt.Printf("Pairing code: %s\n", code)
			fmt.Println("On the phone: WhatsApp > Linked devices > Link a device > Link with phone number instead.")
		case "success":
			fmt.Println("Linked.")
			_ = os.Remove(p.unlinked())
			return nil
		default:
			if item.Error != nil {
				return fmt.Errorf("pairing failed (%s): %w", item.Event, item.Error)
			}
			if item.Event != "code" {
				return fmt.Errorf("pairing ended: %s", item.Event)
			}
		}
	}
	return errors.New("pairing channel closed without success")
}

// unlink logs the device out, which removes it from the phone's Linked
// devices list and deletes its keys from the device store. The archive of
// messages is kept.
func unlink(p paths) error {
	ctx := context.Background()
	cli, err := newClient(ctx, p, "WARN")
	if err != nil {
		return err
	}
	if cli.Store.ID == nil {
		return errors.New("not linked")
	}
	if err := cli.Connect(); err != nil {
		return err
	}
	defer cli.Disconnect()
	if err := cli.Logout(ctx); err != nil {
		return fmt.Errorf("logout: %w", err)
	}
	_ = touch(p.unlinked())
	fmt.Println("Unlinked.")
	return nil
}

// runOptions are the flags of `wa-archive run`.
type runOptions struct {
	retentionDays int
}

func parseRunFlags(args []string) (runOptions, error) {
	var o runOptions
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.IntVar(&o.retentionDays, "retention-days", defaultRetentionDays,
		"days of messages to keep; 0 keeps all")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() > 0 {
		return o, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	if o.retentionDays < 0 {
		return o, errors.New("-retention-days must be 0 or more")
	}
	return o, nil
}

func run(p paths, o runOptions) error {
	if msg := outdatedMessage(p); msg != "" {
		fmt.Fprintln(os.Stderr, msg)
		os.Exit(exitOutdated)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cli, err := newClient(ctx, p, "INFO")
	if err != nil {
		return err
	}
	if cli.Store.ID == nil {
		_ = touch(p.unlinked())
		fmt.Fprintln(os.Stderr, "wa-archive: not linked; run `wa-archive link PHONE`")
		os.Exit(exitUnlinked)
	}
	db, err := sql.Open("sqlite3", "file:"+p.messagesDB()+"?_busy_timeout=5000")
	if err != nil {
		return err
	}
	arc, err := OpenArchive(db)
	if err != nil {
		return err
	}
	h := &handler{cli: cli, wa: liveClient{cli}, arc: arc, paths: p, ctx: ctx, alert: defaultAlertCommand()}
	// The API (for wsp) is up before connecting, so a client can already
	// ask for status and hears the "connected" event.
	srv, err := listenAPI(p.apiSocket(), h)
	if err != nil {
		return fmt.Errorf("API socket: %w", err)
	}
	defer srv.Close()
	arc.SetNotify(srv.changed)
	h.events = srv.broadcast
	cli.AddEventHandlerWithSuccessStatus(h.handle)
	if err := cli.Connect(); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer cli.Disconnect()
	// Deferred calls run last first: this one, before disconnecting, so that
	// withdrawing a presence the API set can still go out (the earlier
	// srv.Close then does nothing).
	defer srv.Close()
	prune := func() {
		cutoff, ok := retentionCutoff(time.Now(), o.retentionDays)
		if !ok {
			return
		}
		if _, err := arc.Prune(cutoff); err != nil {
			fatal("prune: %v", err)
		}
	}
	prune()
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	outbox := time.NewTicker(2 * time.Second)
	defer outbox.Stop()
	for _, dir := range []string{p.outbox(), p.fetchDir(), p.mediaDir()} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	ticks := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-outbox.C:
			if cli.IsConnected() && cli.IsLoggedIn() {
				h.processOutbox()
				h.processFetches()
			}
		case <-ticker.C:
			if cli.IsConnected() && cli.IsLoggedIn() {
				h.beat("connected")
				// Contacts arrive through app-state sync after connecting.
				if err := h.backfillNames(); err != nil {
					fatal("backfill names: %v", err)
				}
			}
			if ticks++; ticks%12 == 0 { // hourly
				prune()
			}
		}
	}
}

func touch(path string) error {
	return os.WriteFile(path, []byte(time.Now().UTC().Format(time.RFC3339)+"\n"), 0o600)
}

// writeHeartbeat atomically records the daemon's last healthy state. An
// external monitor can treat a stale file as a failure.
func writeHeartbeat(path, event string) error {
	data, err := json.Marshal(map[string]string{
		"event": event,
		"at":    time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
