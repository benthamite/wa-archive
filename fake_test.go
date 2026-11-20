package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
)

// fakeWA stands in for WhatsApp in tests: it records what the daemon would
// send and answers lookups from invented data. Nothing connects anywhere.
type fakeWA struct {
	mu        sync.Mutex
	connected bool
	own, lid  types.JID
	nextID    int
	clock     int64

	sent    []fakeSent
	sendErr error
	// beforeSend, when set, runs inside SendMessage (to simulate something
	// happening while a message is in flight).
	beforeSend func()

	reads        []fakeRead
	markReadErr  error
	duringRead   func()
	presence     []types.Presence
	chatPresence []fakeChatPresence
	uploads      int

	groups   map[string]*types.GroupInfo
	groupErr error // when set, every GroupInfo fails with it
	// groupCalls counts GroupInfo requests.
	groupCalls int
	lids       map[string]types.JID // phone-number JID -> LID
	privacy    string
	download   []byte
}

type fakeSent struct {
	to  types.JID
	msg *waE2E.Message
	id  types.MessageID
}

type fakeRead struct {
	ids          []types.MessageID
	chat, sender types.JID
	typ          types.ReceiptType
}

type fakeChatPresence struct {
	chat  types.JID
	state types.ChatPresence
}

const selfJID = "15550000000@s.whatsapp.net"

func newFakeWA() *fakeWA {
	own, _ := types.ParseJID(selfJID)
	lid, _ := types.ParseJID("99000000000000@lid")
	return &fakeWA{connected: true, own: own, lid: lid, clock: 1_700_000_000,
		groups: map[string]*types.GroupInfo{}, lids: map[string]types.JID{}, privacy: "all"}
}

func (f *fakeWA) IsConnected() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.connected }
func (f *fakeWA) IsLoggedIn() bool  { return f.IsConnected() }
func (f *fakeWA) OwnID() types.JID  { return f.own }
func (f *fakeWA) OwnLID() types.JID { return f.lid }

func (f *fakeWA) GenerateMessageID() types.MessageID {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	return types.MessageID(fmt.Sprintf("3EB0FAKE%04d", f.nextID))
}

func (f *fakeWA) SendMessage(_ context.Context, to types.JID, msg *waE2E.Message, id types.MessageID) (whatsmeow.SendResponse, error) {
	if f.beforeSend != nil {
		f.beforeSend()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sendErr != nil {
		return whatsmeow.SendResponse{}, f.sendErr
	}
	f.clock++
	f.sent = append(f.sent, fakeSent{to, msg, id})
	return whatsmeow.SendResponse{ID: id, Timestamp: time.Unix(f.clock, 0)}, nil
}

func (f *fakeWA) MarkRead(_ context.Context, ids []types.MessageID, _ time.Time, chat, sender types.JID, typ types.ReceiptType) error {
	if f.duringRead != nil {
		f.duringRead()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.markReadErr != nil {
		return f.markReadErr
	}
	f.reads = append(f.reads, fakeRead{append([]types.MessageID(nil), ids...), chat, sender, typ})
	return nil
}

func (f *fakeWA) SendPresence(_ context.Context, state types.Presence) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.presence = append(f.presence, state)
	return nil
}

func (f *fakeWA) SendChatPresence(_ context.Context, chat types.JID, state types.ChatPresence) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.chatPresence = append(f.chatPresence, fakeChatPresence{chat, state})
	return nil
}

func (f *fakeWA) Upload(_ context.Context, data []byte, _ whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.uploads++
	return whatsmeow.UploadResponse{URL: "https://example.invalid/m", DirectPath: "/m", MediaKey: []byte{1},
		FileLength: uint64(len(data))}, nil
}

func (f *fakeWA) Download(context.Context, whatsmeow.DownloadableMessage) ([]byte, error) {
	if f.download == nil {
		return nil, errors.New("no media")
	}
	return f.download, nil
}

func (f *fakeWA) GroupInfo(_ context.Context, jid types.JID) (*types.GroupInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.groupCalls++
	if f.groupErr != nil {
		return nil, f.groupErr
	}
	if g, ok := f.groups[jid.String()]; ok {
		return g, nil
	}
	return &types.GroupInfo{JID: jid, GroupName: types.GroupName{Name: "Invented group"}}, nil
}

func (f *fakeWA) PNForLID(_ context.Context, lid types.JID) (types.JID, error) {
	for pn, l := range f.lids {
		if l == lid {
			return types.ParseJID(pn)
		}
	}
	return types.EmptyJID, nil
}

func (f *fakeWA) LIDForPN(_ context.Context, pn types.JID) (types.JID, error) {
	return f.lids[pn.String()], nil
}

func (f *fakeWA) Contact(context.Context, types.JID) (types.ContactInfo, error) {
	return types.ContactInfo{}, nil
}

func (f *fakeWA) ReadReceipts(context.Context) string { return f.privacy }

// testHandler is a handler on a temporary archive and state directory, with
// a fake WhatsApp and an exit that records its code instead of exiting.
type testHandler struct {
	*handler
	wa     *fakeWA
	db     *sql.DB
	exitMu sync.Mutex
	exits  []int
}

func (th *testHandler) exitCodes() []int {
	th.exitMu.Lock()
	defer th.exitMu.Unlock()
	return append([]int(nil), th.exits...)
}

func newTestHandler(t *testing.T) *testHandler {
	t.Helper()
	a, db := openTest(t)
	dir := t.TempDir()
	th := &testHandler{wa: newFakeWA(), db: db}
	th.handler = &handler{wa: th.wa, arc: a, paths: paths{data: dir, state: dir}, ctx: context.Background(),
		exit: func(code int) {
			th.exitMu.Lock()
			defer th.exitMu.Unlock()
			th.exits = append(th.exits, code)
		}}
	return th
}

// failMessageInserts makes every later insert into messages fail, as a full
// disk would, while the rest of the database keeps working.
func failMessageInserts(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`CREATE TRIGGER fail_messages BEFORE INSERT ON messages
		BEGIN SELECT RAISE(ABORT, 'disk full'); END`); err != nil {
		t.Fatal(err)
	}
}
