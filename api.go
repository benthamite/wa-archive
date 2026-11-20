package main

// The local API through which a client such as wsp (an Emacs WhatsApp
// client) acts on WhatsApp. It is the contract in wsp's docs/api.md
// (https://github.com/benthamite/wsp/blob/main/docs/api.md): JSON lines over a Unix
// socket in the data directory, any number of clients, every client gets
// every event. wsp reads the archive directly; this API is only for acting
// (sending, editing, deleting, reacting, marking read, fetching, typing,
// presence) and for status. Every acting method commits its result to the
// archive before it answers, so the client can redraw from the archive as
// soon as it has the answer, and every commit is announced as a "changed"
// event (Archive.SetNotify).
//
// Sending methods are journaled in api_ops under the client's op id before
// anything goes out, with the WhatsApp message id chosen first, so a client
// that lost an answer can ask again (the same op returns the journaled
// outcome) or ask op_status, and an answer is "uncertain" whenever the
// message may have gone out.

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

func (p paths) apiSocket() string { return filepath.Join(p.data, "api.sock") }

// apiError is an error answer. Fatal errors end the daemon once the answer
// has been written (a message sent but not archived).
type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	fatal   bool
}

func (e *apiError) Error() string { return e.Code + ": " + e.Message }

func apiErr(code, format string, args ...any) *apiError {
	return &apiError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// Error codes (api.md).
const (
	codeBadRequest   = "bad_request"
	codeNotFound     = "not_found"
	codeNotConnected = "not_connected"
	codeFailed       = "failed"    // nothing went out
	codeUncertain    = "uncertain" // it may have gone out
	codeInternal     = "internal"
)

// maxRequestLine bounds one request (a long message is far below it).
const maxRequestLine = 16 << 20

// clientQueue is how many lines may wait for a slow client before it is
// dropped, so that one stuck client cannot hold up commits.
const clientQueue = 256

// fetchTimeout is how long fetch waits for a download.
const fetchTimeout = 120 * time.Second

type apiServer struct {
	h       *handler
	ln      net.Listener
	path    string
	mu      sync.Mutex
	clients map[*apiClient]struct{}
	closed  bool
}

type apiClient struct {
	conn net.Conn
	out  chan outLine
	gone chan struct{}
	once sync.Once
	// reqs counts the client's requests still running.
	reqs sync.WaitGroup
}

// outLine is one line to write; done, when set, is closed once it is written.
type outLine struct {
	data []byte
	done chan struct{}
}

// listenAPI opens the socket at PATH, replacing a stale socket file but not
// one a running daemon still answers on.
func listenAPI(path string, h *handler) (*apiServer, error) {
	if c, err := net.DialTimeout("unix", path, time.Second); err == nil {
		c.Close()
		return nil, fmt.Errorf("another process is serving %s", path)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove stale socket: %w", err)
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		return nil, err
	}
	s := &apiServer{h: h, ln: ln, path: path, clients: map[*apiClient]struct{}{}}
	go s.serve()
	return s, nil
}

// Close stops listening, disconnects every client, withdraws any presence
// the API set and removes the socket. It may be called more than once; run
// calls it before disconnecting from WhatsApp, so the withdrawal can still go
// out.
func (s *apiServer) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	clients := make([]*apiClient, 0, len(s.clients))
	for c := range s.clients {
		clients = append(clients, c)
	}
	s.mu.Unlock()
	s.ln.Close()
	for _, c := range clients {
		c.close()
	}
	s.h.withdrawPresence()
	_ = os.Remove(s.path)
}

func (s *apiServer) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		c := &apiClient{conn: conn, out: make(chan outLine, clientQueue), gone: make(chan struct{})}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			conn.Close()
			return
		}
		s.clients[c] = struct{}{}
		s.mu.Unlock()
		go c.writeLoop()
		go s.readLoop(c)
	}
}

func (c *apiClient) close() {
	c.once.Do(func() {
		close(c.gone)
		c.conn.Close()
	})
}

func (c *apiClient) writeLoop() {
	for {
		select {
		case <-c.gone:
			return
		case l := <-c.out:
			_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			_, err := c.conn.Write(l.data)
			if l.done != nil {
				close(l.done)
			}
			if err != nil {
				c.close()
				return
			}
		}
	}
}

// send queues V for the client and returns a channel closed once it is
// written (or the client is gone).
func (c *apiClient) send(v any) <-chan struct{} {
	done := make(chan struct{})
	data, err := json.Marshal(v)
	if err != nil {
		data, _ = json.Marshal(map[string]any{"error": apiErr(codeInternal, "encode answer: %v", err)})
	}
	select {
	case c.out <- outLine{data: append(data, '\n'), done: done}:
	case <-c.gone:
		close(done)
	}
	return done
}

func (s *apiServer) readLoop(c *apiClient) {
	defer func() {
		c.close()
		// Once the client's last request has finished, so that a presence
		// request still running cannot come after the withdrawal.
		c.reqs.Wait()
		s.mu.Lock()
		delete(s.clients, c)
		last := len(s.clients) == 0 && !s.closed
		s.mu.Unlock()
		if last {
			s.h.withdrawPresence()
		}
	}()
	sc := bufio.NewScanner(c.conn)
	sc.Buffer(make([]byte, 64<<10), maxRequestLine)
	for sc.Scan() {
		line := sc.Bytes()
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if err := json.Unmarshal(line, &req); err != nil {
			c.send(map[string]any{"id": nil, "error": apiErr(codeBadRequest, "invalid JSON: %v", err)})
			continue
		}
		id := req.ID
		if len(id) == 0 {
			id = json.RawMessage("null")
		}
		// Requests run concurrently, so a slow one (fetch, a large upload)
		// does not hold up the rest; answers carry the request's id.
		c.reqs.Add(1)
		go func() {
			defer c.reqs.Done()
			result, err := s.h.call(req.Method, req.Params)
			if err == nil {
				if result == nil {
					result = map[string]any{}
				}
				c.send(map[string]any{"id": id, "result": result})
				return
			}
			var ae *apiError
			if !errors.As(err, &ae) {
				ae = apiErr(codeInternal, "%v", err)
			}
			done := c.send(map[string]any{"id": id, "error": ae})
			if ae.fatal {
				select {
				case <-done:
				case <-time.After(5 * time.Second):
				}
				fmt.Fprintf(os.Stderr, "wa-archive: %s; exiting\n", ae.Message)
				s.h.die(exitFailure)
			}
		}()
	}
}

// broadcast sends V to every client. A client too slow to keep up is
// dropped rather than allowed to block the daemon.
func (s *apiServer) broadcast(v any) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	data = append(data, '\n')
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.clients {
		select {
		case <-c.gone:
		case c.out <- outLine{data: data}:
		default:
			fmt.Fprintln(os.Stderr, "wa-archive: dropping an API client that does not read its events")
			c.close()
		}
	}
}

// changed announces that CHATS changed in the archive.
func (s *apiServer) changed(chats []string) {
	s.broadcast(map[string]any{"event": "changed", "chats": chats})
}

// announceStatus tells API clients that the connection changed.
func (h *handler) announceStatus(connected, loggedIn bool) {
	if h.events != nil {
		h.events(map[string]any{"event": "status", "connected": connected, "logged_in": loggedIn})
	}
}

type apiMethod func(h *handler, params json.RawMessage) (any, error)

var apiMethods = map[string]apiMethod{
	"status":    (*handler).apiStatus,
	"send":      (*handler).apiSend,
	"send_file": (*handler).apiSendFile,
	"edit":      (*handler).apiEdit,
	"delete":    (*handler).apiDelete,
	"react":     (*handler).apiReact,
	"mark_read": (*handler).apiMarkRead,
	"fetch":     (*handler).apiFetch,
	"typing":    (*handler).apiTyping,
	"presence":  (*handler).apiPresence,
	"op_status": (*handler).apiOpStatus,
}

// call runs one API request.
func (h *handler) call(method string, params json.RawMessage) (any, error) {
	m, ok := apiMethods[method]
	if !ok {
		return nil, apiErr(codeBadRequest, "unknown method %q", method)
	}
	return m(h, params)
}

// decode reads PARAMS into V, rejecting unknown fields so a misspelt
// parameter is an error rather than silently ignored.
func decode(params json.RawMessage, v any) error {
	if len(params) == 0 || string(params) == "null" {
		params = json.RawMessage("{}")
	}
	dec := json.NewDecoder(strings.NewReader(string(params)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return apiErr(codeBadRequest, "params: %v", err)
	}
	return nil
}

func (h *handler) requireConnected() error {
	if h.wa == nil || !h.wa.IsConnected() || !h.wa.IsLoggedIn() {
		return apiErr(codeNotConnected, "WhatsApp is not connected")
	}
	return nil
}

// resolveChat returns the canonical key of CHAT (a key or an alias) and the
// JID messages to it go to, validated as the outbox validates recipients.
func (h *handler) resolveChat(chat string) (types.JID, string, error) {
	if chat == "" {
		return types.EmptyJID, "", apiErr(codeBadRequest, "chat is required")
	}
	key, err := h.arc.Canonical(chat)
	if err != nil {
		return types.EmptyJID, "", err
	}
	to, err := parseChatJID(key)
	if err != nil {
		return types.EmptyJID, "", apiErr(codeBadRequest, "%v", err)
	}
	return to, key, nil
}

// ownMessage returns message ID of chat KEY, which must be the user's own.
func (h *handler) ownMessage(key, id string) (Message, error) {
	m, err := h.target(key, id)
	if err == nil && !m.FromMe {
		err = apiErr(codeBadRequest, "message %s is not your own", id)
	}
	return m, err
}

// target returns message ID of chat KEY, which must be archived and not
// deleted.
func (h *handler) target(key, id string) (Message, error) {
	if id == "" {
		return Message{}, apiErr(codeBadRequest, "id is required")
	}
	m, ok, err := h.arc.Lookup(key, id)
	if err != nil {
		return m, err
	}
	if !ok {
		return m, apiErr(codeNotFound, "message %s is not in the archive of %s (or was deleted)", id, key)
	}
	return m, nil
}

// action is one journaled send: what to send and what sending it commits.
type action struct {
	op, method string
	to         types.JID
	key        string
	// build makes the message (uploading an attachment if any) and the
	// batch the sent message commits. It runs after the op is journaled; a
	// failure there means nothing went out.
	build  func() (*waE2E.Message, func(whatsmeow.SendResponse) Batch, error)
	result func(Op) map[string]any
}

// journaled answers a repeated op with its recorded outcome.
func journaled(o Op, a action) (any, error) {
	if o.Method != a.method {
		return nil, apiErr(codeBadRequest, "op %s was already used for %s", o.ID, o.Method)
	}
	switch o.State {
	case "sent":
		return a.result(o), nil
	case "failed":
		return nil, apiErr(codeFailed, "%s", o.Error)
	}
	msg := "the operation is pending or its outcome is unknown; the message may have been sent"
	if o.Error != "" {
		msg += ": " + o.Error
	}
	return nil, apiErr(codeUncertain, "%s", msg)
}

// run performs A once: it journals the op as pending with the message id
// chosen beforehand, sends, and records the outcome. A repeated op returns
// the journaled outcome without acting again.
func (h *handler) run(a action) (any, error) {
	if a.op == "" {
		return nil, apiErr(codeBadRequest, "op is required")
	}
	if err := h.requireConnected(); err != nil {
		return nil, err
	}
	id := h.wa.GenerateMessageID()
	o, fresh, err := h.arc.ClaimOp(Op{ID: a.op, Method: a.method, ChatKey: a.key, MessageID: string(id), TS: time.Now().Unix()})
	if err != nil {
		return nil, err
	}
	if !fresh {
		return journaled(o, a)
	}
	record := func(state, msg string) {
		o.State, o.Error = state, msg
		if err := h.arc.RecordOp(o); err != nil {
			fmt.Fprintf(os.Stderr, "wa-archive: journal op %s (%s): %v\n", o.ID, state, err)
		}
	}
	msg, archive, err := a.build()
	if err != nil {
		record("failed", err.Error())
		return nil, apiErr(codeFailed, "%v", err)
	}
	resp, err := h.deliver(a.to, msg, id, func(resp whatsmeow.SendResponse) Batch {
		b := archive(resp)
		sent := o
		sent.State, sent.TS = "sent", resp.Timestamp.Unix()
		b.Op = &sent
		return b
	})
	var se *sendError
	var ae *archiveError
	switch {
	case errors.As(err, &ae):
		o.TS = resp.Timestamp.Unix()
		record("sent", ae.Error())
		return nil, &apiError{Code: codeUncertain, Message: ae.Error(), fatal: true}
	case errors.As(err, &se) && se.Uncertain:
		record("pending", err.Error())
		return nil, apiErr(codeUncertain, "the message may have been sent: %v", err)
	case err != nil:
		record("failed", err.Error())
		return nil, apiErr(codeFailed, "%v", err)
	}
	o.State, o.TS = "sent", resp.Timestamp.Unix()
	return a.result(o), nil
}

// firstOp looks up an op before anything else is checked, so a repeated op
// gets its journaled outcome even if the chat or message has since changed
// or the connection is down.
func (h *handler) firstOp(a action) (any, bool, error) {
	if a.op == "" {
		return nil, false, nil
	}
	o, ok, err := h.arc.GetOp(a.op)
	if err != nil || !ok {
		return nil, false, err
	}
	res, err := journaled(o, a)
	return res, true, err
}

func sentResult(o Op) map[string]any {
	return map[string]any{"op": o.ID, "message_id": o.MessageID, "ts": o.TS}
}

func (h *handler) sendAction(op, method, chat string, job Job) (any, error) {
	a := action{op: op, method: method, result: sentResult}
	if res, done, err := h.firstOp(a); done || err != nil {
		return res, err
	}
	to, key, err := h.resolveChat(chat)
	if err != nil {
		return nil, err
	}
	a.to, a.key = to, key
	job.To = key
	a.build = func() (*waE2E.Message, func(whatsmeow.SendResponse) Batch, error) {
		d, err := h.prepare(job)
		if err != nil {
			return nil, nil, err
		}
		return d.msg, d.batch, nil
	}
	return h.run(a)
}

func (h *handler) apiSend(params json.RawMessage) (any, error) {
	var p struct {
		Op      string `json:"op"`
		Chat    string `json:"chat"`
		Text    string `json:"text"`
		ReplyTo string `json:"reply_to"`
	}
	if err := decode(params, &p); err != nil {
		return nil, err
	}
	if strings.TrimSpace(p.Text) == "" {
		return nil, apiErr(codeBadRequest, "text is empty")
	}
	return h.sendAction(p.Op, "send", p.Chat, Job{Text: p.Text, ReplyTo: p.ReplyTo})
}

func (h *handler) apiSendFile(params json.RawMessage) (any, error) {
	var p struct {
		Op      string `json:"op"`
		Chat    string `json:"chat"`
		Path    string `json:"path"`
		Caption string `json:"caption"`
		ReplyTo string `json:"reply_to"`
	}
	if err := decode(params, &p); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(p.Path) {
		return nil, apiErr(codeBadRequest, "path must be absolute: %q", p.Path)
	}
	if info, err := os.Stat(p.Path); err != nil || !info.Mode().IsRegular() {
		return nil, apiErr(codeBadRequest, "no such file: %s", p.Path)
	}
	return h.sendAction(p.Op, "send_file", p.Chat, Job{Text: p.Caption, File: p.Path, ReplyTo: p.ReplyTo})
}

func (h *handler) apiEdit(params json.RawMessage) (any, error) {
	var p struct {
		Op   string `json:"op"`
		Chat string `json:"chat"`
		ID   string `json:"id"`
		Text string `json:"text"`
	}
	if err := decode(params, &p); err != nil {
		return nil, err
	}
	a := action{op: p.Op, method: "edit", result: func(o Op) map[string]any {
		return map[string]any{"op": o.ID, "message_id": o.MessageID}
	}}
	if res, done, err := h.firstOp(a); done || err != nil {
		return res, err
	}
	if strings.TrimSpace(p.Text) == "" {
		return nil, apiErr(codeBadRequest, "text is empty")
	}
	to, key, err := h.resolveChat(p.Chat)
	if err != nil {
		return nil, err
	}
	m, err := h.ownMessage(key, p.ID)
	if err != nil {
		return nil, err
	}
	// Only text can be replaced with text; a caption edit would need the
	// attachment's message kind.
	if m.MediaType != "" {
		return nil, apiErr(codeBadRequest, "message %s is a %s; only text messages can be edited", p.ID, m.MediaType)
	}
	// WhatsApp ignores edits after its edit window; refusing is better than
	// archiving an edit nobody else sees.
	if time.Since(time.Unix(m.TS, 0)) > whatsmeow.EditWindow {
		return nil, apiErr(codeBadRequest, "message %s is older than WhatsApp's %v edit window", p.ID, whatsmeow.EditWindow)
	}
	a.to, a.key = to, key
	a.build = func() (*waE2E.Message, func(whatsmeow.SendResponse) Batch, error) {
		msg := &waE2E.Message{EditedMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{
			ProtocolMessage: &waE2E.ProtocolMessage{
				Key:           messageKey(to, p.ID, true, types.EmptyJID),
				Type:          waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(),
				EditedMessage: &waE2E.Message{Conversation: proto.String(p.Text)},
				TimestampMS:   proto.Int64(time.Now().UnixMilli()),
			},
		}}}
		return msg, func(resp whatsmeow.SendResponse) Batch {
			return Batch{Mutations: []Mutation{{ChatKey: key, TargetID: p.ID, EventID: resp.ID, Kind: "edit",
				TS: resp.Timestamp.Unix(), Text: p.Text}}}
		}, nil
	}
	return h.run(a)
}

func (h *handler) apiDelete(params json.RawMessage) (any, error) {
	var p struct {
		Op   string `json:"op"`
		Chat string `json:"chat"`
		ID   string `json:"id"`
	}
	if err := decode(params, &p); err != nil {
		return nil, err
	}
	a := action{op: p.Op, method: "delete", result: func(o Op) map[string]any { return map[string]any{"op": o.ID} }}
	if res, done, err := h.firstOp(a); done || err != nil {
		return res, err
	}
	to, key, err := h.resolveChat(p.Chat)
	if err != nil {
		return nil, err
	}
	if _, err := h.ownMessage(key, p.ID); err != nil {
		return nil, err
	}
	a.to, a.key = to, key
	a.build = func() (*waE2E.Message, func(whatsmeow.SendResponse) Batch, error) {
		msg := &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
			Type: waE2E.ProtocolMessage_REVOKE.Enum(),
			Key:  messageKey(to, p.ID, true, types.EmptyJID),
		}}
		return msg, func(resp whatsmeow.SendResponse) Batch {
			return Batch{Mutations: []Mutation{{ChatKey: key, TargetID: p.ID, EventID: resp.ID, Kind: "revoke",
				TS: resp.Timestamp.Unix()}}}
		}, nil
	}
	return h.run(a)
}

func (h *handler) apiReact(params json.RawMessage) (any, error) {
	var p struct {
		Op    string  `json:"op"`
		Chat  string  `json:"chat"`
		ID    string  `json:"id"`
		Emoji *string `json:"emoji"`
	}
	if err := decode(params, &p); err != nil {
		return nil, err
	}
	a := action{op: p.Op, method: "react", result: func(o Op) map[string]any { return map[string]any{"op": o.ID} }}
	if res, done, err := h.firstOp(a); done || err != nil {
		return res, err
	}
	if p.Emoji == nil {
		return nil, apiErr(codeBadRequest, `emoji is required ("" removes the reaction)`)
	}
	to, key, err := h.resolveChat(p.Chat)
	if err != nil {
		return nil, err
	}
	m, err := h.target(key, p.ID)
	if err != nil {
		return nil, err
	}
	a.to, a.key = to, key
	a.build = func() (*waE2E.Message, func(whatsmeow.SendResponse) Batch, error) {
		author := types.EmptyJID
		if !m.FromMe {
			var err error
			if author, err = h.author(to, m); err != nil {
				return nil, nil, err
			}
		}
		ms := time.Now().UnixMilli()
		msg := &waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{
			Key:               messageKey(to, p.ID, m.FromMe, author),
			Text:              proto.String(*p.Emoji),
			SenderTimestampMS: proto.Int64(ms),
		}}
		return msg, func(resp whatsmeow.SendResponse) Batch {
			return Batch{Reactions: []Reaction{h.reactionRow(key, "", true, p.ID, *p.Emoji, ms, resp.Timestamp)}}
		}, nil
	}
	return h.run(a)
}

func (h *handler) apiMarkRead(params json.RawMessage) (any, error) {
	var p struct {
		Chat string `json:"chat"`
	}
	if err := decode(params, &p); err != nil {
		return nil, err
	}
	to, key, err := h.resolveChat(p.Chat)
	if err != nil {
		return nil, err
	}
	if err := h.requireConnected(); err != nil {
		return nil, err
	}
	marked, skipped, err := h.markRead(to, key)
	if err != nil {
		var ae *apiError
		if !errors.As(err, &ae) {
			err = apiErr(codeFailed, "marked %d and skipped %d, then: %v", marked, skipped, err)
		}
		return nil, err
	}
	return map[string]any{"marked": marked, "skipped": skipped}, nil
}

// markRead sends read receipts for the chat's unread incoming messages and
// records them as read. WhatsApp takes one receipt per sender, so a group's
// messages are grouped by sender, named as the group addresses its members.
// Only the ids selected at the start are marked: a message arriving
// meanwhile stays unread until the next call, and no receipt is ever sent
// for a message at or below the watermark (UnreadIncoming excludes them).
// Each sender's ids are recorded as soon as their receipt has gone out, so a
// failure part-way leaves the rest for the next call. Failing to record ids
// whose receipt went out is fatal, as for any failed commit.
//
// A group message whose sender cannot be addressed (no sender, the group's
// info refused, no LID for the sender in a LID group) can get no receipt; it
// is recorded read without one and counted as skipped, so that it does not
// stay unread for ever with every later call failing on it.
func (h *handler) markRead(to types.JID, key string) (marked, skipped int, err error) {
	unread, err := h.arc.UnreadIncoming(key)
	if err != nil || len(unread) == 0 {
		return 0, 0, err
	}
	var order []string
	bySender := map[string][]types.MessageID{}
	for _, u := range unread {
		if _, ok := bySender[u.SenderKey]; !ok {
			order = append(order, u.SenderKey)
		}
		bySender[u.SenderKey] = append(bySender[u.SenderKey], u.ID)
	}
	type receipt struct {
		sender types.JID
		ids    []types.MessageID
	}
	var receipts []receipt
	var unmarkable []types.MessageID
	for _, senderKey := range order {
		ids := bySender[senderKey]
		sender, err := h.receiptSender(to, senderKey)
		if err != nil {
			fmt.Fprintf(os.Stderr, "wa-archive: mark_read %s: no receipt for %d message(s) from %q: %v\n",
				key, len(ids), senderKey, err)
			unmarkable = append(unmarkable, ids...)
			continue
		}
		receipts = append(receipts, receipt{sender, ids})
	}
	var typ types.ReceiptType
	if len(receipts) > 0 {
		if typ, err = h.readReceiptType(); err != nil {
			return 0, 0, err
		}
	}
	if len(unmarkable) > 0 {
		if err := h.arc.Commit(readBatch(key, unmarkable)); err != nil {
			return 0, 0, err
		}
		skipped = len(unmarkable)
	}
	for _, r := range receipts {
		if err := h.wa.MarkRead(h.ctx, r.ids, time.Now(), to, r.sender, typ); err != nil {
			return marked, skipped, err
		}
		if err := h.arc.Commit(readBatch(key, r.ids)); err != nil {
			// The receipts went out but are not recorded. As for a sent
			// message whose commit failed: answer, then exit so the service
			// manager restarts the daemon rather than carrying on.
			fmt.Fprintf(os.Stderr, "wa-archive: sent read receipts for %s %v but could not record them: %v\n", key, r.ids, err)
			return marked, skipped, &apiError{Code: codeUncertain, fatal: true, Message: fmt.Sprintf(
				"read receipts for %s went out, but recording them failed: %v", strings.Join(r.ids, ", "), err)}
		}
		marked += len(r.ids)
	}
	return marked, skipped, nil
}

// receiptSender is whom a receipt for messages from SENDERKEY in chat TO
// names: nobody in a direct chat (the chat itself), the sender as the group
// addresses its members in a group.
func (h *handler) receiptSender(to types.JID, senderKey string) (types.JID, error) {
	if to.Server != types.GroupServer {
		return to, nil
	}
	sj, err := types.ParseJID(senderKey)
	if err != nil || sj.User == "" {
		return types.EmptyJID, errors.New("no usable sender")
	}
	return h.addressFor(to, sj, false)
}

func readBatch(key string, ids []types.MessageID) Batch {
	b := Batch{}
	for _, id := range ids {
		b.Reads = append(b.Reads, ReadMark{ChatKey: key, ID: id})
	}
	return b
}

// readReceiptType is the receipt mark_read sends under the user's
// read-receipts setting: "read" when it is on, "read-self" (seen only by
// their own devices) when it is off. When the setting cannot be read nothing
// may be sent, since guessing wrong would show blue ticks they have turned off.
func (h *handler) readReceiptType() (types.ReceiptType, error) {
	ctx, cancel := context.WithTimeout(h.ctx, 15*time.Second)
	defer cancel()
	switch setting := h.wa.ReadReceipts(ctx); setting {
	case "all":
		return types.ReceiptTypeRead, nil
	case "none":
		return types.ReceiptTypeReadSelf, nil
	default:
		return "", apiErr(codeFailed, "your read-receipts setting could not be read (%s), so no receipts were sent and nothing was marked read", setting)
	}
}

func (h *handler) apiFetch(params json.RawMessage) (any, error) {
	var p struct {
		Chat string `json:"chat"`
		ID   string `json:"id"`
	}
	if err := decode(params, &p); err != nil {
		return nil, err
	}
	if p.Chat == "" || p.ID == "" {
		return nil, apiErr(codeBadRequest, "chat and id are required")
	}
	key, err := h.arc.Canonical(p.Chat)
	if err != nil {
		return nil, err
	}
	if err := h.requireConnected(); err != nil {
		return nil, err
	}
	type outcome struct {
		path string
		err  error
	}
	done := make(chan outcome, 1)
	go func() {
		path, err := h.fetch(FetchRequest{Chat: key, ID: p.ID})
		done <- outcome{path, err}
	}()
	select {
	case o := <-done:
		if errors.Is(o.err, errNoMediaRef) {
			return nil, apiErr(codeNotFound, "%v", o.err)
		}
		if o.err != nil {
			return nil, apiErr(codeFailed, "%v", o.err)
		}
		h.arc.announce(map[string]bool{key: true})
		return map[string]any{"path": o.path}, nil
	case <-time.After(fetchTimeout):
		return nil, apiErr(codeFailed, "download not finished after %v", fetchTimeout)
	}
}

func (h *handler) apiTyping(params json.RawMessage) (any, error) {
	var p struct {
		Chat  string `json:"chat"`
		State string `json:"state"`
	}
	if err := decode(params, &p); err != nil {
		return nil, err
	}
	state := types.ChatPresence(p.State)
	if state != types.ChatPresenceComposing && state != types.ChatPresencePaused {
		return nil, apiErr(codeBadRequest, "state must be composing or paused")
	}
	to, _, err := h.resolveChat(p.Chat)
	if err != nil {
		return nil, err
	}
	if err := h.requireConnected(); err != nil {
		return nil, err
	}
	if err := h.wa.SendChatPresence(h.ctx, to, state); err != nil {
		return nil, apiErr(codeFailed, "%v", err)
	}
	return map[string]any{}, nil
}

func (h *handler) apiPresence(params json.RawMessage) (any, error) {
	var p struct {
		State string `json:"state"`
	}
	if err := decode(params, &p); err != nil {
		return nil, err
	}
	state := types.Presence(p.State)
	if state != types.PresenceAvailable && state != types.PresenceUnavailable {
		return nil, apiErr(codeBadRequest, "state must be available or unavailable")
	}
	if err := h.requireConnected(); err != nil {
		return nil, err
	}
	h.presenceMu.Lock()
	defer h.presenceMu.Unlock()
	if err := h.wa.SendPresence(h.ctx, state); err != nil {
		return nil, apiErr(codeFailed, "%v", err)
	}
	h.presence = state
	return map[string]any{}, nil
}

// withdrawPresence sends "unavailable" if the API last set the user available.
// It runs when the last API client disconnects and when the daemon shuts
// down, so a client that dies or quits without saying so cannot leave the
// user shown online. It is the only presence the daemon sends on its own, and only
// ever to undo what a client asked for.
func (h *handler) withdrawPresence() {
	h.presenceMu.Lock()
	defer h.presenceMu.Unlock()
	if h.presence != types.PresenceAvailable || h.wa == nil {
		return
	}
	// Not h.ctx alone: at shutdown it is already cancelled.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(h.ctx), 10*time.Second)
	defer cancel()
	if err := h.wa.SendPresence(ctx, types.PresenceUnavailable); err != nil {
		// Kept as available, so the next trigger tries again.
		fmt.Fprintf(os.Stderr, "wa-archive: withdraw presence: %v\n", err)
		return
	}
	h.presence = types.PresenceUnavailable
}

func (h *handler) apiStatus(params json.RawMessage) (any, error) {
	if err := decode(params, &struct{}{}); err != nil {
		return nil, err
	}
	res := map[string]any{"connected": false, "logged_in": false, "version": daemonVersion(),
		"read_receipts": "unknown", "self": ""}
	if h.wa != nil {
		res["connected"], res["logged_in"] = h.wa.IsConnected(), h.wa.IsLoggedIn()
		if own := h.wa.OwnID(); !own.IsEmpty() {
			res["self"] = own.String()
		}
		ctx, cancel := context.WithTimeout(h.ctx, 15*time.Second)
		defer cancel()
		res["read_receipts"] = h.wa.ReadReceipts(ctx)
	}
	return res, nil
}

func (h *handler) apiOpStatus(params json.RawMessage) (any, error) {
	var p struct {
		Op string `json:"op"`
	}
	if err := decode(params, &p); err != nil {
		return nil, err
	}
	if p.Op == "" {
		return nil, apiErr(codeBadRequest, "op is required")
	}
	o, ok, err := h.arc.GetOp(p.Op)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, apiErr(codeNotFound, "no op %s", p.Op)
	}
	res := map[string]any{"state": o.State}
	if o.MessageID != "" {
		res["message_id"] = o.MessageID
	}
	if o.Error != "" {
		res["error"] = o.Error
	}
	return res, nil
}

// daemonVersion names this build: wa-archive's commit and whatsmeow's
// version, which is what decides whether WhatsApp accepts the client.
func daemonVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	rev, dirty := "", ""
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			if s.Value == "true" {
				dirty = "+dirty"
			}
		}
	}
	if len(rev) > 12 {
		rev = rev[:12]
	}
	if rev == "" {
		rev = "unknown"
	}
	wm := "unknown"
	for _, d := range info.Deps {
		if d.Path == "go.mau.fi/whatsmeow" {
			wm = d.Version
		}
	}
	return fmt.Sprintf("wa-archive %s%s, whatsmeow %s", rev, dirty, wm)
}
