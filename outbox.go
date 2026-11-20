package main

// The outbox is a file-based send path for scripts and other local tools
// (interactive clients use the API in api.go; both share the sending code in
// send.go). A tool writes a job file into <data>/outbox/; the running daemon,
// which holds the only connection, sends it, archives it as the user's own
// message, and moves the job to outbox/sent/ or outbox/failed/ with a
// receipt. Which messages may be queued is the queueing tool's rule. A send
// that may have gone out anyway (sendError.Uncertain) goes to
// outbox/uncertain/ instead, so that a tool never reports it as "nothing
// sent" and invites a duplicate.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"go.mau.fi/whatsmeow"
)

// Job is one message to send.
type Job struct {
	To   string `json:"to"`             // chat JID, e.g. 15550000000@s.whatsapp.net or ...@g.us
	Text string `json:"text"`           // message text, or the attachment's caption
	File string `json:"file,omitempty"` // absolute path of an attachment
	// ReplyTo is the ID of a message in the same chat that this one quotes,
	// as WhatsApp's "Reply" does.
	ReplyTo string `json:"reply_to,omitempty"`
}

// Receipt is written next to the job once it is handled.
type Receipt struct {
	Job
	Status    string `json:"status"` // "sent", "failed" (nothing went out) or "uncertain" (it may have)
	MessageID string `json:"message_id,omitempty"`
	SentAt    string `json:"sent_at,omitempty"`
	Error     string `json:"error,omitempty"`
}

func (p paths) outbox() string { return filepath.Join(p.data, "outbox") }

// processOutbox sends every queued job, oldest first.
func (h *handler) processOutbox() {
	dir := h.paths.outbox()
	names, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil || len(names) == 0 {
		return
	}
	sort.Strings(names)
	for _, name := range names {
		h.handleJob(name)
	}
}

func (h *handler) handleJob(name string) {
	r := Receipt{Status: "failed"}
	data, err := os.ReadFile(name)
	if err == nil {
		err = json.Unmarshal(data, &r.Job)
	}
	var unarchived *archiveError
	var failed *sendError
	if err == nil {
		var resp whatsmeow.SendResponse
		resp, err = h.send(r.Job)
		switch {
		case err == nil || errors.As(err, &unarchived):
			r.Status, r.MessageID = "sent", resp.ID
			r.SentAt = resp.Timestamp.UTC().Format(time.RFC3339)
		case errors.As(err, &failed) && failed.Uncertain:
			// It may have gone out, under the id chosen beforehand.
			r.Status, r.MessageID = "uncertain", resp.ID
		}
	}
	if err != nil {
		r.Error = err.Error()
	}
	out, _ := json.MarshalIndent(r, "", "  ")
	dest := filepath.Join(h.paths.outbox(), r.Status, filepath.Base(name))
	writeReceipt(dest, out)
	_ = os.Remove(name)
	if unarchived != nil {
		// The message went out but is not in the archive. The receipt says
		// so (status sent, with the error); journal it as well, then exit
		// like any other failed commit so launchd restarts the daemon.
		key, _ := h.arc.Canonical(r.To)
		if err := h.arc.RecordOp(Op{ID: "outbox/" + filepath.Base(name), Method: "outbox", ChatKey: key,
			MessageID: r.MessageID, State: "sent", Error: r.Error, TS: unarchived.Resp.Timestamp.Unix()}); err != nil {
			fmt.Fprintf(os.Stderr, "wa-archive: journal unarchived send %s: %v\n", r.MessageID, err)
		}
		h.die(exitFailure)
	}
}

// writeReceipt writes DATA to DEST atomically, so a reader polling for the
// receipt never sees it half written.
func writeReceipt(dest string, data []byte) {
	if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
		return
	}
	tmp := dest + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o600); err == nil {
		_ = os.Rename(tmp, dest)
	}
}

// send delivers JOB and archives it as the user's own message. The message id is
// chosen before sending, so a message that went out but could not be
// archived can still be named in its receipt.
func (h *handler) send(job Job) (whatsmeow.SendResponse, error) {
	d, err := h.prepare(job)
	if err != nil {
		return whatsmeow.SendResponse{}, err
	}
	return h.deliver(d.to, d.msg, h.wa.GenerateMessageID(), d.batch)
}
