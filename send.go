package main

// The send path shared by the outbox (outbox.go, for local tools) and the
// API (api.go, for clients such as wsp): validating the recipient, building the message,
// uploading an attachment, quoting, sending under a message id chosen
// beforehand, and archiving what was sent. Which messages may be sent is each
// caller's rule, not this file's.

import (
	"errors"
	"fmt"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

// maxAttachment bounds what may be attached (WhatsApp's document limit is far
// higher; this only stops an accidental huge file).
const maxAttachment = 64 << 20

// parseChatJID accepts the chat addresses a message may be sent to: a person
// by phone number or LID, or a group.
func parseChatJID(s string) (types.JID, error) {
	to, err := types.ParseJID(s)
	if err != nil || to.User == "" ||
		(to.Server != types.DefaultUserServer && to.Server != types.GroupServer &&
			to.Server != types.HiddenUserServer) {
		return types.EmptyJID, fmt.Errorf("invalid recipient %q", s)
	}
	return to, nil
}

// draft is a message ready to send: built (its attachment uploaded), quoted
// and addressed, with the archive row it becomes once sent.
type draft struct {
	to      types.JID
	key     string
	aliases map[string]string
	msg     *waE2E.Message
	row     Message // without ID and TS, which the send fills in
}

// prepare turns JOB into a draft. Nothing is sent; an attachment is uploaded
// (encrypted; nobody can see it without the message that carries its key).
func (h *handler) prepare(job Job) (*draft, error) {
	to, err := parseChatJID(job.To)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(job.Text) == "" && job.File == "" {
		return nil, errors.New("empty message")
	}
	msg, kind, fileName, err := h.buildMessage(job)
	if err != nil {
		return nil, err
	}
	key, aliases := h.canonical(to, types.EmptyJID)
	if job.ReplyTo != "" {
		ci, err := h.quote(to, key, job.ReplyTo)
		if err != nil {
			return nil, err
		}
		msg = withQuote(msg, ci)
	}
	ref, mimetype := mediaRef(msg)
	return &draft{to: to, key: key, aliases: aliases, msg: msg, row: Message{
		ChatKey: key, FromMe: true, Text: job.Text, QuotedID: job.ReplyTo,
		MediaType: kind, FileName: fileName, Source: "sent", MediaRef: ref, MediaMime: mimetype,
	}}, nil
}

// batch is what archiving D, sent as RESP, commits.
func (d *draft) batch(resp whatsmeow.SendResponse) Batch {
	m := d.row
	m.ID, m.TS = resp.ID, resp.Timestamp.Unix()
	return Batch{Aliases: d.aliases, Messages: []Message{m}}
}

// sendError is a failed send. Uncertain means the message may have reached
// WhatsApp anyway (the request was written but no answer came back, or the
// failure came from a stage not known to precede the write), so it must
// not be resent automatically.
type sendError struct {
	Uncertain bool
	Err       error
}

func (e *sendError) Error() string { return "send: " + e.Err.Error() }
func (e *sendError) Unwrap() error { return e.Err }

// archiveError is a message that was sent but could not be archived. The
// daemon reports it, journals it and exits, as for any failed commit.
type archiveError struct {
	Resp whatsmeow.SendResponse
	Err  error
}

func (e *archiveError) Error() string {
	return fmt.Sprintf("sent as %s, but archiving it failed: %v", e.Resp.ID, e.Err)
}
func (e *archiveError) Unwrap() error { return e.Err }

// failsBeforeSending are whatsmeow errors returned before anything is
// written to the connection, so nothing went out. Every other send error is
// treated as uncertain: whatsmeow's request may already have been written
// (timeouts, disconnects during the wait, failed retries), and resending a
// message that did go out would duplicate it.
var failsBeforeSending = []error{
	whatsmeow.ErrClientIsNil, whatsmeow.ErrNotLoggedIn, whatsmeow.ErrRecipientADJID,
	whatsmeow.ErrUnknownServer, whatsmeow.ErrNoPermissionInGroup,
	// The server answered and refused the message.
	whatsmeow.ErrServerReturnedError,
}

func sendIsUncertain(err error) bool {
	for _, before := range failsBeforeSending {
		if errors.Is(err, before) {
			return false
		}
	}
	return true
}

// deliver sends MSG to TO under message id ID and commits the batch that
// BUILD makes from WhatsApp's answer, in one transaction. It returns a
// *sendError when sending fails and an *archiveError when the message went
// out but the commit failed; the response carries ID either way.
func (h *handler) deliver(to types.JID, msg *waE2E.Message, id types.MessageID,
	build func(whatsmeow.SendResponse) Batch) (whatsmeow.SendResponse, error) {
	resp, err := h.wa.SendMessage(h.ctx, to, msg, id)
	if resp.ID == "" {
		// On failure too: a message that may have gone out went out as ID.
		resp.ID = id
	}
	if err != nil {
		return resp, &sendError{Uncertain: sendIsUncertain(err), Err: err}
	}
	if err := h.arc.Commit(build(resp)); err != nil {
		fmt.Fprintf(os.Stderr, "wa-archive: sent %s but could not archive it: %v\n", resp.ID, err)
		return resp, &archiveError{Resp: resp, Err: err}
	}
	return resp, nil
}

// quote returns the context that makes a message sent to TO a reply to
// message ID, archived under the chat KEY. A message missing from the archive
// fails the job rather than going out without its quote. The quoted author is
// named in the chat's addressing mode (phone number or LID), which WhatsApp
// needs to show who wrote it.
func (h *handler) quote(to types.JID, key, id string) (*waE2E.ContextInfo, error) {
	m, ok, err := h.arc.Lookup(key, id)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("message %s to reply to is not in the archive of %s", id, key)
	}
	author, err := h.author(to, m)
	if err != nil {
		return nil, err
	}
	return quoteContext(id, author, m), nil
}

// author returns who wrote M, addressed as the chat TO addresses its
// members.
func (h *handler) author(to types.JID, m Message) (types.JID, error) {
	var author types.JID
	if m.FromMe {
		if h.wa == nil || h.wa.OwnID().IsEmpty() {
			return types.EmptyJID, errors.New("not logged in")
		}
		author = h.wa.OwnID()
	} else if a, err := types.ParseJID(m.SenderKey); err != nil || a.User == "" {
		return types.EmptyJID, fmt.Errorf("message %s has no usable sender", m.ID)
	} else {
		author = a
	}
	return h.addressFor(to, author, m.FromMe)
}

// addressFor returns AUTHOR as the chat TO addresses its members: by LID in a
// group that uses LIDs, otherwise unchanged.
func (h *handler) addressFor(to, author types.JID, fromMe bool) (types.JID, error) {
	if to.Server != types.GroupServer {
		return author, nil
	}
	info, err := h.groupInfo(to)
	if err != nil {
		return types.EmptyJID, fmt.Errorf("group info: %w", err)
	}
	if info.AddressingMode != types.AddressingModeLID || author.Server != types.DefaultUserServer {
		return author, nil
	}
	lid := h.wa.OwnLID()
	if !fromMe {
		if lid, err = h.wa.LIDForPN(h.ctx, author); err != nil {
			return types.EmptyJID, err
		}
	}
	if lid.IsEmpty() {
		return types.EmptyJID, fmt.Errorf("no LID for %s in a LID group", author)
	}
	return lid.ToNonAD(), nil
}

// groupInfoRetry is how long a group whose info WhatsApp refused is not
// asked about again when addressing its members.
const groupInfoRetry = 15 * time.Minute

// groupInfo returns the info of group JID, remembering a refusal for
// groupInfoRetry so that repeated calls (mark_read, replies, reactions) do
// not ask WhatsApp each time.
func (h *handler) groupInfo(jid types.JID) (*types.GroupInfo, error) {
	k := jid.String()
	if t, ok := h.groupInfoRefused.failedAt(k); ok && time.Since(t) < groupInfoRetry {
		return nil, fmt.Errorf("WhatsApp refused it at %s", t.Format(time.TimeOnly))
	}
	info, err := h.wa.GroupInfo(h.ctx, jid)
	if err != nil {
		h.groupInfoRefused.record(k, time.Now())
		return nil, err
	}
	h.groupInfoRefused.forget(k)
	return info, nil
}

// messageKey refers to message ID in CHAT written by AUTHOR (the user
// when FROMME), as edits, revokes and reactions need. It is whatsmeow's
// BuildMessageKey without the client, so tests can check it.
func messageKey(chat types.JID, id string, fromMe bool, author types.JID) *waCommon.MessageKey {
	key := &waCommon.MessageKey{
		FromMe:    proto.Bool(fromMe),
		ID:        proto.String(id),
		RemoteJID: proto.String(chat.String()),
	}
	if !fromMe && chat.Server != types.DefaultUserServer && chat.Server != types.HiddenUserServer {
		key.Participant = proto.String(author.ToNonAD().String())
	}
	return key
}

// quoteContext is the reply context for message ID by AUTHOR, quoting M's
// text (or, for an attachment without a caption, its file name).
func quoteContext(id string, author types.JID, m Message) *waE2E.ContextInfo {
	text := m.Text
	if text == "" {
		text = m.FileName
	}
	return &waE2E.ContextInfo{
		StanzaID:      proto.String(id),
		Participant:   proto.String(author.String()),
		QuotedMessage: &waE2E.Message{Conversation: proto.String(text)},
	}
}

// withQuote attaches CI to MSG. Plain text becomes an extended text message,
// the only text kind that carries a context.
func withQuote(msg *waE2E.Message, ci *waE2E.ContextInfo) *waE2E.Message {
	switch {
	case msg.ImageMessage != nil:
		msg.ImageMessage.ContextInfo = ci
	case msg.DocumentMessage != nil:
		msg.DocumentMessage.ContextInfo = ci
	default:
		return &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
			Text: proto.String(msg.GetConversation()), ContextInfo: ci,
		}}
	}
	return msg
}

// buildMessage returns the message for JOB, its media kind and file name.
func (h *handler) buildMessage(job Job) (*waE2E.Message, string, string, error) {
	if job.File == "" {
		return &waE2E.Message{Conversation: proto.String(job.Text)}, "", "", nil
	}
	if !filepath.IsAbs(job.File) {
		return nil, "", "", fmt.Errorf("attachment path must be absolute: %s", job.File)
	}
	info, err := os.Stat(job.File)
	if err != nil {
		return nil, "", "", err
	}
	if info.Size() > maxAttachment {
		return nil, "", "", fmt.Errorf("attachment over %d MB", maxAttachment>>20)
	}
	data, err := os.ReadFile(job.File)
	if err != nil {
		return nil, "", "", err
	}
	name := filepath.Base(job.File)
	mimetype := mime.TypeByExtension(strings.ToLower(filepath.Ext(name)))
	if mimetype == "" {
		mimetype = http.DetectContentType(data)
	}
	if strings.HasPrefix(mimetype, "image/") && mimetype != "image/svg+xml" {
		up, err := h.wa.Upload(h.ctx, data, whatsmeow.MediaImage)
		if err != nil {
			return nil, "", "", fmt.Errorf("upload: %w", err)
		}
		return &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
			Caption: proto.String(job.Text), Mimetype: proto.String(mimetype),
			URL: proto.String(up.URL), DirectPath: proto.String(up.DirectPath),
			MediaKey: up.MediaKey, FileEncSHA256: up.FileEncSHA256,
			FileSHA256: up.FileSHA256, FileLength: proto.Uint64(up.FileLength),
		}}, "image", name, nil
	}
	up, err := h.wa.Upload(h.ctx, data, whatsmeow.MediaDocument)
	if err != nil {
		return nil, "", "", fmt.Errorf("upload: %w", err)
	}
	return &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{
		Caption: proto.String(job.Text), FileName: proto.String(name), Title: proto.String(name),
		Mimetype: proto.String(mimetype),
		URL:      proto.String(up.URL), DirectPath: proto.String(up.DirectPath),
		MediaKey: up.MediaKey, FileEncSHA256: up.FileEncSHA256,
		FileSHA256: up.FileSHA256, FileLength: proto.Uint64(up.FileLength),
	}}, "document", name, nil
}
