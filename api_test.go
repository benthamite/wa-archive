package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// apiHarness is a daemon's API on a temporary socket, over a temporary
// archive and a fake WhatsApp.
type apiHarness struct {
	*testHandler
	srv  *apiServer
	sock string
}

func newAPIHarness(t *testing.T) *apiHarness {
	t.Helper()
	th := newTestHandler(t)
	// Unix socket paths are limited to about 100 bytes; the default test
	// directory on macOS is too deep.
	dir, err := os.MkdirTemp("/tmp", "wa-api-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "api.sock")
	srv, err := listenAPI(sock, th.handler)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)
	th.arc.SetNotify(srv.changed)
	th.events = srv.broadcast
	return &apiHarness{testHandler: th, srv: srv, sock: sock}
}

type apiConn struct {
	t      *testing.T
	conn   net.Conn
	r      *bufio.Reader
	nextID int
	events []map[string]any
}

func (a *apiHarness) dial(t *testing.T) *apiConn {
	t.Helper()
	c, err := net.Dial("unix", a.sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return &apiConn{t: t, conn: c, r: bufio.NewReader(c)}
}

func (c *apiConn) line() map[string]any {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	l, err := c.r.ReadBytes('\n')
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	var v map[string]any
	if err := json.Unmarshal(l, &v); err != nil {
		c.t.Fatalf("answer %q: %v", l, err)
	}
	return v
}

func (c *apiConn) write(s string) {
	c.t.Helper()
	if _, err := c.conn.Write([]byte(s + "\n")); err != nil {
		c.t.Fatal(err)
	}
}

// call sends one request and returns its result or error code, keeping any
// events that arrive first.
func (c *apiConn) call(method string, params any) (map[string]any, string) {
	c.t.Helper()
	c.nextID++
	req, _ := json.Marshal(map[string]any{"id": c.nextID, "method": method, "params": params})
	c.write(string(req))
	for {
		v := c.line()
		if _, ok := v["event"]; ok {
			c.events = append(c.events, v)
			continue
		}
		if id, _ := v["id"].(float64); int(id) != c.nextID {
			c.t.Fatalf("answer for id %v, want %d: %v", v["id"], c.nextID, v)
		}
		if e, ok := v["error"].(map[string]any); ok {
			return e, e["code"].(string)
		}
		return v["result"].(map[string]any), ""
	}
}

func (c *apiConn) ok(method string, params any) map[string]any {
	c.t.Helper()
	res, code := c.call(method, params)
	if code != "" {
		c.t.Fatalf("%s: error %s: %v", method, code, res["message"])
	}
	return res
}

func (c *apiConn) fails(method string, params any, want string) map[string]any {
	c.t.Helper()
	res, code := c.call(method, params)
	if code != want {
		c.t.Fatalf("%s: got %q (%v), want error %s", method, code, res, want)
	}
	return res
}

func (c *apiConn) changed() []string {
	var chats []string
	for _, e := range c.events {
		if e["event"] == "changed" {
			for _, k := range e["chats"].([]any) {
				chats = append(chats, k.(string))
			}
		}
	}
	return chats
}

func TestSocketIsPrivateAndReplacesAStaleFile(t *testing.T) {
	a := newAPIHarness(t)
	info, err := os.Stat(a.sock)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode %v (%v)", info.Mode(), err)
	}
	// A second daemon must not take over a socket that is being served.
	if _, err := listenAPI(a.sock, a.handler); err == nil {
		t.Fatal("a live socket was replaced")
	}
	a.srv.Close()
	if _, err := os.Stat(a.sock); !os.IsNotExist(err) {
		t.Fatal("socket left behind on close")
	}
	// A stale socket file (a daemon that died) is replaced.
	ln, err := net.Listen("unix", a.sock)
	if err != nil {
		t.Fatal(err)
	}
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	ln.Close()
	srv, err := listenAPI(a.sock, a.handler)
	if err != nil {
		t.Fatalf("stale socket not replaced: %v", err)
	}
	srv.Close()
}

func TestBadRequestsAreAnsweredAndTheConnectionSurvives(t *testing.T) {
	a := newAPIHarness(t)
	c := a.dial(t)
	c.write(`{not json`)
	v := c.line()
	if v["id"] != nil || v["error"].(map[string]any)["code"] != codeBadRequest {
		t.Fatalf("bad JSON answer %v", v)
	}
	c.fails("frobnicate", nil, codeBadRequest)
	c.fails("send", map[string]any{"op": "o", "chat": alice, "text": "x", "colour": "red"}, codeBadRequest)
	c.fails("send", map[string]any{"op": "o", "chat": "not-a-jid", "text": "x"}, codeBadRequest)
	c.fails("send", map[string]any{"op": "o", "chat": alice, "text": "  "}, codeBadRequest)
	c.fails("send", map[string]any{"chat": alice, "text": "x"}, codeBadRequest)
	c.fails("op_status", map[string]any{"op": "never"}, codeNotFound)
	if res := c.ok("status", nil); res["connected"] != true {
		t.Fatalf("status after bad requests: %v", res)
	}
	if len(a.wa.sent) != 0 {
		t.Fatal("a bad request sent something")
	}
}

func TestConcurrentClientsAllGetEvents(t *testing.T) {
	a := newAPIHarness(t)
	sender, watcher := a.dial(t), a.dial(t)
	// Make sure both are registered before anything is committed.
	sender.ok("status", nil)
	watcher.ok("status", nil)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := net.Dial("unix", a.sock)
			if err != nil {
				t.Error(err)
				return
			}
			defer c.Close()
			c.Write([]byte(`{"id":1,"method":"status"}` + "\n"))
			c.SetReadDeadline(time.Now().Add(5 * time.Second))
			if l, err := bufio.NewReader(c).ReadString('\n'); err != nil || !strings.Contains(l, `"connected":true`) {
				t.Errorf("concurrent status: %q %v", l, err)
			}
		}()
	}
	wg.Wait()
	res := sender.ok("send", map[string]any{"op": "op-1", "chat": alice, "text": "invented"})
	if got := sender.changed(); len(got) != 1 || got[0] != alice {
		t.Fatalf("sender's events %v (the changed event precedes the answer)", sender.events)
	}
	if v := watcher.line(); v["event"] != "changed" || v["chats"].([]any)[0] != alice {
		t.Fatalf("watcher got %v", v)
	}
	if res["message_id"] == "" {
		t.Fatal("no message id")
	}
	a.handleOK(t, &events.Connected{})
	if v := watcher.line(); v["event"] != "status" || v["connected"] != true || v["logged_in"] != true {
		t.Fatalf("status event %v", v)
	}
}

func TestSendJournalsAndARepeatedOpDoesNotSendAgain(t *testing.T) {
	a := newAPIHarness(t)
	c := a.dial(t)
	mustCommit(t, a.arc, Batch{Messages: []Message{incoming(alice, "Q1", alice, 5)}})
	res := c.ok("send", map[string]any{"op": "op-1", "chat": alice, "text": "invented", "reply_to": "Q1"})
	id := res["message_id"].(string)
	if res["op"] != "op-1" || id != "3EB0FAKE0001" || res["ts"].(float64) == 0 {
		t.Fatalf("result %v", res)
	}
	if len(a.wa.sent) != 1 || a.wa.sent[0].id != types.MessageID(id) ||
		a.wa.sent[0].msg.GetExtendedTextMessage().GetContextInfo().GetStanzaID() != "Q1" {
		t.Fatalf("sent %+v", a.wa.sent)
	}
	if count(t, a.db, `SELECT COUNT(*) FROM messages WHERE id = ? AND quoted_id = 'Q1' AND from_me = 1`, id) != 1 {
		t.Fatal("sent message not archived before the answer")
	}
	again := c.ok("send", map[string]any{"op": "op-1", "chat": alice, "text": "invented"})
	if again["message_id"] != id || again["ts"] != res["ts"] || len(a.wa.sent) != 1 {
		t.Fatalf("repeat answered %v after %d sends", again, len(a.wa.sent))
	}
	if st := c.ok("op_status", map[string]any{"op": "op-1"}); st["state"] != "sent" || st["message_id"] != id {
		t.Fatalf("op_status %v", st)
	}
	c.fails("edit", map[string]any{"op": "op-1", "chat": alice, "id": id, "text": "y"}, codeBadRequest)
}

func TestFailedAndUncertainSends(t *testing.T) {
	a := newAPIHarness(t)
	c := a.dial(t)
	// Refused before anything was written: failed, and the op stays failed.
	a.wa.sendErr = whatsmeow.ErrNotLoggedIn
	c.fails("send", map[string]any{"op": "f", "chat": alice, "text": "invented"}, codeFailed)
	a.wa.sendErr = nil
	c.fails("send", map[string]any{"op": "f", "chat": alice, "text": "invented"}, codeFailed)
	if st := c.ok("op_status", map[string]any{"op": "f"}); st["state"] != "failed" || st["error"] == nil {
		t.Fatalf("op_status %v", st)
	}
	// Written but never acknowledged: uncertain, the op stays pending, and
	// asking again never resends.
	a.wa.sendErr = whatsmeow.ErrMessageTimedOut
	c.fails("send", map[string]any{"op": "u", "chat": alice, "text": "invented"}, codeUncertain)
	a.wa.sendErr = nil
	c.fails("send", map[string]any{"op": "u", "chat": alice, "text": "invented"}, codeUncertain)
	st := c.ok("op_status", map[string]any{"op": "u"})
	if st["state"] != "pending" || st["message_id"] == nil {
		t.Fatalf("op_status %v", st)
	}
	if len(a.wa.sent) != 0 {
		t.Fatalf("resent: %+v", a.wa.sent)
	}
	// Before acknowledgement the op is already journaled with its message id.
	a.wa.beforeSend = func() {
		o, ok, _ := a.arc.GetOp("p")
		if !ok || o.State != "pending" || o.MessageID == "" {
			t.Errorf("not journaled before sending: %+v", o)
		}
	}
	c.ok("send", map[string]any{"op": "p", "chat": alice, "text": "invented"})
	// Not connected: nothing journaled, so the same op may be retried.
	a.wa.connected = false
	c.fails("send", map[string]any{"op": "n", "chat": alice, "text": "invented"}, codeNotConnected)
	if _, ok, _ := a.arc.GetOp("n"); ok {
		t.Fatal("a request refused for want of a connection was journaled")
	}
}

func TestSentButNotArchivedIsUncertainJournaledAndExits(t *testing.T) {
	a := newAPIHarness(t)
	c := a.dial(t)
	failMessageInserts(t, a.db)
	res := c.fails("send", map[string]any{"op": "x", "chat": alice, "text": "invented"}, codeUncertain)
	if !strings.Contains(res["message"].(string), "archiving it failed") {
		t.Fatalf("message %v", res["message"])
	}
	o, ok, _ := a.arc.GetOp("x")
	if !ok || o.State != "sent" || o.Error == "" || o.MessageID != "3EB0FAKE0001" {
		t.Fatalf("journal %+v", o)
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(a.exitCodes()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := a.exitCodes(); len(got) != 1 || got[0] != exitFailure {
		t.Fatalf("exits %v", got)
	}
}

func TestSendFileUploadsAndArchivesTheAttachment(t *testing.T) {
	a := newAPIHarness(t)
	c := a.dial(t)
	path := filepath.Join(t.TempDir(), "photo.png")
	if err := os.WriteFile(path, []byte("\x89PNG\r\n\x1a\ninvented"), 0o600); err != nil {
		t.Fatal(err)
	}
	c.fails("send_file", map[string]any{"op": "f0", "chat": alice, "path": "photo.png"}, codeBadRequest)
	res := c.ok("send_file", map[string]any{"op": "f1", "chat": group, "path": path, "caption": "invented"})
	if a.wa.uploads != 1 || a.wa.sent[0].msg.GetImageMessage().GetCaption() != "invented" {
		t.Fatalf("sent %+v", a.wa.sent)
	}
	if count(t, a.db, `SELECT COUNT(*) FROM messages m JOIN media_refs r USING (chat_key, id)
		WHERE m.id = ? AND m.media_type = 'image' AND m.file_name = 'photo.png'`, res["message_id"]) != 1 {
		t.Fatal("attachment not archived with its reference")
	}
}

func TestEditAndDeleteOwnMessages(t *testing.T) {
	a := newAPIHarness(t)
	c := a.dial(t)
	now := time.Now().Unix()
	own := Message{ChatKey: alice, ID: "OWN", FromMe: true, TS: now, Text: "invented", Source: "sent"}
	old := Message{ChatKey: alice, ID: "OLD", FromMe: true, TS: now - 3600, Text: "invented", Source: "sent"}
	mustCommit(t, a.arc, Batch{Messages: []Message{own, old, incoming(alice, "THEIRS", alice, now)}})
	c.fails("edit", map[string]any{"op": "e0", "chat": alice, "id": "THEIRS", "text": "y"}, codeBadRequest)
	c.fails("edit", map[string]any{"op": "e1", "chat": alice, "id": "OLD", "text": "y"}, codeBadRequest)
	c.fails("edit", map[string]any{"op": "e2", "chat": alice, "id": "NOPE", "text": "y"}, codeNotFound)
	res := c.ok("edit", map[string]any{"op": "e3", "chat": alice, "id": "OWN", "text": "edited"})
	pm := a.wa.sent[0].msg.GetEditedMessage().GetMessage().GetProtocolMessage()
	if pm.GetKey().GetID() != "OWN" || !pm.GetKey().GetFromMe() || pm.GetEditedMessage().GetConversation() != "edited" {
		t.Fatalf("edit message %v", a.wa.sent[0].msg)
	}
	if m, _, _ := a.arc.Lookup(alice, "OWN"); m.Text != "edited" {
		t.Fatalf("archived text %q", m.Text)
	}
	if res["message_id"] != string(a.wa.sent[0].id) {
		t.Fatalf("result %v", res)
	}
	c.fails("delete", map[string]any{"op": "d0", "chat": alice, "id": "THEIRS"}, codeBadRequest)
	c.ok("delete", map[string]any{"op": "d1", "chat": alice, "id": "OWN"})
	rv := a.wa.sent[1].msg.GetProtocolMessage()
	if rv.GetType().String() != "REVOKE" || rv.GetKey().GetID() != "OWN" || !rv.GetKey().GetFromMe() {
		t.Fatalf("revoke %v", rv)
	}
	if _, ok, _ := a.arc.Lookup(alice, "OWN"); ok {
		t.Fatal("deleted message still shows")
	}
	// Repeating the delete returns its journaled outcome although the
	// message is now gone.
	c.ok("delete", map[string]any{"op": "d1", "chat": alice, "id": "OWN"})
	if len(a.wa.sent) != 2 {
		t.Fatal("repeated delete sent again")
	}
}

func TestReactAddsAndRemovesOwnReaction(t *testing.T) {
	a := newAPIHarness(t)
	c := a.dial(t)
	g := jid(t, group)
	a.wa.groups[group] = &types.GroupInfo{JID: g, AddressingMode: types.AddressingModeLID}
	a.wa.lids[bob] = jid(t, "99000000000002@lid")
	mustCommit(t, a.arc, Batch{Messages: []Message{incoming(group, "G1", bob, 10)}})
	c.ok("react", map[string]any{"op": "r1", "chat": group, "id": "G1", "emoji": "👍"})
	key := a.wa.sent[0].msg.GetReactionMessage().GetKey()
	if key.GetFromMe() || key.GetParticipant() != "99000000000002@lid" || key.GetRemoteJID() != group {
		t.Fatalf("reaction key %v (a LID group names the author by LID)", key)
	}
	if e, fromMe := reactionEmoji(t, a.testHandler, group, "G1", selfJID); e != "👍" || fromMe != 1 {
		t.Fatalf("archived %q %d", e, fromMe)
	}
	c.fails("react", map[string]any{"op": "r2", "chat": group, "id": "G1"}, codeBadRequest)
	c.ok("react", map[string]any{"op": "r3", "chat": group, "id": "G1", "emoji": ""})
	if e, _ := reactionEmoji(t, a.testHandler, group, "G1", selfJID); e != "" {
		t.Fatalf("after removal %q", e)
	}
}

func TestMarkReadDirectChat(t *testing.T) {
	a := newAPIHarness(t)
	c := a.dial(t)
	mine := Message{ChatKey: alice, ID: "MINE", FromMe: true, TS: 40, Source: "sent"}
	mustCommit(t, a.arc, Batch{
		Messages: []Message{incoming(alice, "OLD", alice, 10), incoming(alice, "TIE", alice, 20),
			incoming(alice, "N1", alice, 30), incoming(alice, "N2", alice, 30), mine},
		Watermarks: []Watermark{{alice, 20}},
	})
	if res := c.ok("mark_read", map[string]any{"chat": alice}); res["marked"].(float64) != 2 {
		t.Fatalf("marked %v", res)
	}
	if len(a.wa.reads) != 1 || strings.Join(a.wa.reads[0].ids, ",") != "N1,N2" ||
		a.wa.reads[0].chat.String() != alice || a.wa.reads[0].sender.String() != alice {
		t.Fatalf("receipts %+v (never for messages at or below the watermark, nor the user's own)", a.wa.reads)
	}
	if res := c.ok("mark_read", map[string]any{"chat": alice}); res["marked"].(float64) != 0 || len(a.wa.reads) != 1 {
		t.Fatalf("second mark_read %v, %d receipts", res, len(a.wa.reads))
	}
	if got := c.changed(); len(got) == 0 || got[0] != alice {
		t.Fatalf("no changed event: %v", c.events)
	}
}

func TestMarkReadGroupsBySender(t *testing.T) {
	a := newAPIHarness(t)
	c := a.dial(t)
	mustCommit(t, a.arc, Batch{Messages: []Message{incoming(group, "B1", bob, 10), incoming(group, "A1", alice, 11),
		incoming(group, "B2", bob, 12)}})
	if res := c.ok("mark_read", map[string]any{"chat": group}); res["marked"].(float64) != 3 {
		t.Fatalf("marked %v", res)
	}
	got := map[string]string{}
	for _, r := range a.wa.reads {
		if r.chat.String() != group {
			t.Fatalf("receipt to %s", r.chat)
		}
		got[r.sender.String()] = strings.Join(r.ids, ",")
	}
	if len(got) != 2 || got[bob] != "B1,B2" || got[alice] != "A1" {
		t.Fatalf("receipts by sender %v", got)
	}
	if u, _ := a.arc.UnreadIncoming(group); len(u) != 0 {
		t.Fatalf("still unread %v", u)
	}
}

func TestMarkReadLeavesAMessageThatArrivesMeanwhileUnread(t *testing.T) {
	a := newAPIHarness(t)
	c := a.dial(t)
	mustCommit(t, a.arc, Batch{Messages: []Message{incoming(alice, "M1", alice, 10)}})
	a.wa.duringRead = func() {
		a.wa.duringRead = nil
		mustCommit(t, a.arc, Batch{Messages: []Message{incoming(alice, "LATE", alice, 10)}})
	}
	if res := c.ok("mark_read", map[string]any{"chat": alice}); res["marked"].(float64) != 1 {
		t.Fatalf("marked %v", res)
	}
	if u, _ := a.arc.UnreadIncoming(alice); len(u) != 1 || u[0].ID != "LATE" {
		t.Fatalf("unread %v; the late message got no receipt, so it must stay unread", u)
	}
}

func TestMarkReadFailureKeepsMessagesUnread(t *testing.T) {
	a := newAPIHarness(t)
	c := a.dial(t)
	mustCommit(t, a.arc, Batch{Messages: []Message{incoming(alice, "M1", alice, 10)}})
	a.wa.markReadErr = errors.New("connection lost")
	c.fails("mark_read", map[string]any{"chat": alice}, codeFailed)
	if u, _ := a.arc.UnreadIncoming(alice); len(u) != 1 {
		t.Fatal("marked read without a receipt")
	}
}

func TestStatusReportsReadReceiptsSetting(t *testing.T) {
	a := newAPIHarness(t)
	c := a.dial(t)
	for _, setting := range []string{"all", "none", "unknown"} {
		a.wa.privacy = setting
		res := c.ok("status", nil)
		if res["read_receipts"] != setting || res["self"] != selfJID || res["logged_in"] != true ||
			!strings.Contains(res["version"].(string), "whatsmeow") {
			t.Fatalf("status %v", res)
		}
	}
}

func TestFetchDownloadsAndAnnounces(t *testing.T) {
	a := newAPIHarness(t)
	c := a.dial(t)
	m := incoming(alice, "DOC", alice, 10)
	m.MediaType, m.FileName, m.MediaRef, m.MediaMime = "document", "invented.pdf", []byte{}, "application/pdf"
	mustCommit(t, a.arc, Batch{Messages: []Message{m}})
	if _, err := a.db.Exec(`INSERT INTO media_refs (chat_key, id, kind, mimetype, ref) VALUES (?, 'DOC', 'document', 'application/pdf', x'')`, alice); err != nil {
		t.Fatal(err)
	}
	c.fails("fetch", map[string]any{"chat": alice, "id": "NOPE"}, codeNotFound)
	a.wa.download = []byte("invented")
	c.events = nil
	res := c.ok("fetch", map[string]any{"chat": alice, "id": "DOC"})
	data, err := os.ReadFile(res["path"].(string))
	if err != nil || string(data) != "invented" {
		t.Fatalf("file %q %v", data, err)
	}
	if got := c.changed(); len(got) != 1 || got[0] != alice {
		t.Fatalf("changed %v", c.events)
	}
}

func TestTypingAndPresenceSendOnlyWhatTheyAreAsked(t *testing.T) {
	a := newAPIHarness(t)
	c := a.dial(t)
	c.fails("typing", map[string]any{"chat": alice, "state": "recording"}, codeBadRequest)
	c.fails("presence", map[string]any{"state": "away"}, codeBadRequest)
	c.ok("typing", map[string]any{"chat": alice, "state": "composing"})
	c.ok("presence", map[string]any{"state": "unavailable"})
	if len(a.wa.chatPresence) != 1 || a.wa.chatPresence[0].state != types.ChatPresenceComposing ||
		len(a.wa.presence) != 1 || a.wa.presence[0] != types.PresenceUnavailable {
		t.Fatalf("chat presence %+v, presence %+v", a.wa.chatPresence, a.wa.presence)
	}
}

// Every other method and every event the daemon handles sends no presence
// and no chat state.
func TestNothingSendsPresenceUnasked(t *testing.T) {
	a := newAPIHarness(t)
	c := a.dial(t)
	now := time.Now().Unix()
	mustCommit(t, a.arc, Batch{Messages: []Message{incoming(alice, "IN", alice, now),
		{ChatKey: alice, ID: "OWN", FromMe: true, TS: now, Text: "invented", Source: "sent"}}})
	a.handleOK(t, &events.Connected{})
	a.handleOK(t, reactionEvent(t, alice, alice, false, "OWN", "👍", now*1000))
	a.handleOK(t, receiptEvent(t, alice, alice, false, types.ReceiptTypeRead, now, "OWN"))
	a.handleOK(t, receiptEvent(t, alice, selfJID, true, types.ReceiptTypeReadSelf, now, "IN"))
	c.ok("status", nil)
	c.ok("send", map[string]any{"op": "1", "chat": alice, "text": "invented"})
	c.ok("react", map[string]any{"op": "2", "chat": alice, "id": "IN", "emoji": "👍"})
	c.ok("edit", map[string]any{"op": "3", "chat": alice, "id": "OWN", "text": "invented"})
	c.ok("delete", map[string]any{"op": "4", "chat": alice, "id": "OWN"})
	mustCommit(t, a.arc, Batch{Messages: []Message{incoming(alice, "IN2", alice, now)}})
	c.ok("mark_read", map[string]any{"chat": alice})
	c.ok("op_status", map[string]any{"op": "1"})
	queueJob(t, a.handler, "j.json", Job{To: alice, Text: "invented"})
	a.processOutbox()
	if len(a.wa.presence) != 0 || len(a.wa.chatPresence) != 0 {
		t.Fatalf("presence %v, chat state %v sent unasked", a.wa.presence, a.wa.chatPresence)
	}
}

func (f *fakeWA) presenceSent() []types.Presence {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]types.Presence(nil), f.presence...)
}

// waitPresence waits until exactly WANT has been sent.
func waitPresence(t *testing.T, f *fakeWA, want ...types.Presence) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		got := f.presenceSent()
		if fmt.Sprint(got) == fmt.Sprint(want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("presence sent %v, want %v", got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Presence set available through the API is withdrawn when the last client
// disconnects, so a client that dies cannot leave the user shown online.
func TestPresenceIsWithdrawnWhenTheLastClientLeaves(t *testing.T) {
	a := newAPIHarness(t)
	c1, c2 := a.dial(t), a.dial(t)
	c1.ok("presence", map[string]any{"state": "available"})
	c2.ok("status", nil)
	c1.conn.Close()
	time.Sleep(100 * time.Millisecond)
	waitPresence(t, a.wa, types.PresenceAvailable)
	c2.conn.Close()
	waitPresence(t, a.wa, types.PresenceAvailable, types.PresenceUnavailable)
	// Nothing more once withdrawn: a new client that sets nothing and leaves
	// sends nothing.
	c3 := a.dial(t)
	c3.ok("status", nil)
	c3.conn.Close()
	time.Sleep(100 * time.Millisecond)
	waitPresence(t, a.wa, types.PresenceAvailable, types.PresenceUnavailable)
}

func TestPresenceUnavailableIsNotSentAgain(t *testing.T) {
	a := newAPIHarness(t)
	c := a.dial(t)
	c.ok("presence", map[string]any{"state": "available"})
	c.ok("presence", map[string]any{"state": "unavailable"})
	c.conn.Close()
	time.Sleep(100 * time.Millisecond)
	waitPresence(t, a.wa, types.PresenceAvailable, types.PresenceUnavailable)
}

func TestPresenceIsWithdrawnOnShutdown(t *testing.T) {
	a := newAPIHarness(t)
	c := a.dial(t)
	c.ok("presence", map[string]any{"state": "available"})
	a.srv.Close()
	waitPresence(t, a.wa, types.PresenceAvailable, types.PresenceUnavailable)
}

// mark_read decides the receipt type from the user's read-receipts setting
// itself, rather than letting whatsmeow fall back to "read" (blue ticks)
// when it cannot fetch the setting.
func TestMarkReadFollowsTheReadReceiptsSetting(t *testing.T) {
	for _, tc := range []struct {
		privacy string
		want    types.ReceiptType
	}{{"all", types.ReceiptTypeRead}, {"none", types.ReceiptTypeReadSelf}} {
		a := newAPIHarness(t)
		c := a.dial(t)
		a.wa.privacy = tc.privacy
		mustCommit(t, a.arc, Batch{Messages: []Message{incoming(alice, "M1", alice, 10)}})
		c.ok("mark_read", map[string]any{"chat": alice})
		if len(a.wa.reads) != 1 || a.wa.reads[0].typ != tc.want {
			t.Fatalf("privacy %s: receipts %+v, want type %q", tc.privacy, a.wa.reads, tc.want)
		}
	}
}

func TestMarkReadWithUnknownSettingSendsAndRecordsNothing(t *testing.T) {
	a := newAPIHarness(t)
	c := a.dial(t)
	a.wa.privacy = "unknown"
	mustCommit(t, a.arc, Batch{Messages: []Message{incoming(alice, "M1", alice, 10)}})
	res := c.fails("mark_read", map[string]any{"chat": alice}, codeFailed)
	if !strings.Contains(res["message"].(string), "read-receipts setting") {
		t.Fatalf("message %v", res)
	}
	if len(a.wa.reads) != 0 {
		t.Fatalf("receipts sent: %+v", a.wa.reads)
	}
	if u, _ := a.arc.UnreadIncoming(alice); len(u) != 1 {
		t.Fatal("marked read without a receipt")
	}
}

// Group messages whose sender cannot be addressed get no receipt (none can
// be sent) but are recorded read, so the chat's unread count clears instead
// of wsp retrying mark_read forever.
func TestMarkReadSkipsMessagesWithoutAUsableSender(t *testing.T) {
	a := newAPIHarness(t)
	c := a.dial(t)
	mustCommit(t, a.arc, Batch{Messages: []Message{incoming(group, "B1", bob, 10),
		incoming(group, "X1", "", 11), incoming(group, "X2", "not a jid", 12)}})
	res := c.ok("mark_read", map[string]any{"chat": group})
	if res["marked"].(float64) != 1 || res["skipped"].(float64) != 2 {
		t.Fatalf("result %v", res)
	}
	if len(a.wa.reads) != 1 || a.wa.reads[0].sender.String() != bob {
		t.Fatalf("receipts %+v", a.wa.reads)
	}
	if u, _ := a.arc.UnreadIncoming(group); len(u) != 0 {
		t.Fatalf("still unread %v", u)
	}
}

func TestMarkReadSkipsASenderWithoutALIDInALIDGroup(t *testing.T) {
	a := newAPIHarness(t)
	c := a.dial(t)
	g := jid(t, group)
	a.wa.groups[group] = &types.GroupInfo{JID: g, GroupName: types.GroupName{Name: "Invented"},
		AddressingMode: types.AddressingModeLID}
	bobLID := jid(t, "88000000000002@lid")
	a.wa.lids[bob] = bobLID
	mustCommit(t, a.arc, Batch{Messages: []Message{incoming(group, "B1", bob, 10), incoming(group, "A1", alice, 11)}})
	res := c.ok("mark_read", map[string]any{"chat": group})
	if res["marked"].(float64) != 1 || res["skipped"].(float64) != 1 {
		t.Fatalf("result %v", res)
	}
	if len(a.wa.reads) != 1 || a.wa.reads[0].sender != bobLID {
		t.Fatalf("receipts %+v", a.wa.reads)
	}
	if u, _ := a.arc.UnreadIncoming(group); len(u) != 0 {
		t.Fatalf("still unread %v", u)
	}
}

// A group whose info WhatsApp refuses is not asked again on every call.
func TestMarkReadSkipsARefusedGroupAndRemembersTheRefusal(t *testing.T) {
	a := newAPIHarness(t)
	c := a.dial(t)
	a.wa.groupErr = errors.New("not a participant")
	mustCommit(t, a.arc, Batch{Messages: []Message{incoming(group, "B1", bob, 10), incoming(group, "A1", alice, 11)}})
	res := c.ok("mark_read", map[string]any{"chat": group})
	if res["marked"].(float64) != 0 || res["skipped"].(float64) != 2 || len(a.wa.reads) != 0 {
		t.Fatalf("result %v, receipts %+v", res, a.wa.reads)
	}
	mustCommit(t, a.arc, Batch{Messages: []Message{incoming(group, "B2", bob, 12)}})
	res = c.ok("mark_read", map[string]any{"chat": group})
	if res["skipped"].(float64) != 1 {
		t.Fatalf("second result %v", res)
	}
	if a.wa.groupCalls != 1 {
		t.Fatalf("GroupInfo asked %d times, want 1", a.wa.groupCalls)
	}
	if u, _ := a.arc.UnreadIncoming(group); len(u) != 0 {
		t.Fatalf("still unread %v", u)
	}
}

// Receipts that went out but could not be recorded are answered as
// uncertain, and the daemon exits as after any failed commit, rather than
// answering "failed" and carrying on.
func TestMarkReadCommitFailureAfterReceiptsIsUncertainAndExits(t *testing.T) {
	a := newAPIHarness(t)
	c := a.dial(t)
	mustCommit(t, a.arc, Batch{Messages: []Message{incoming(alice, "M1", alice, 10)}})
	if _, err := a.db.Exec(`CREATE TRIGGER fail_reads BEFORE INSERT ON read_messages
		BEGIN SELECT RAISE(ABORT, 'disk full'); END`); err != nil {
		t.Fatal(err)
	}
	res := c.fails("mark_read", map[string]any{"chat": alice}, codeUncertain)
	if !strings.Contains(res["message"].(string), "M1") || len(a.wa.reads) != 1 {
		t.Fatalf("message %v, receipts %+v", res["message"], a.wa.reads)
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(a.exitCodes()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := a.exitCodes(); len(got) != 1 || got[0] != exitFailure {
		t.Fatalf("exits %v", got)
	}
}
