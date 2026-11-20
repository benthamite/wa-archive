package main

import (
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func jid(t *testing.T, s string) types.JID {
	t.Helper()
	j, err := types.ParseJID(s)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func reactionEvent(t *testing.T, chat, sender string, fromMe bool, target, emoji string, ms int64) *events.Message {
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: jid(t, chat), Sender: jid(t, sender), IsFromMe: fromMe,
				IsGroup: jid(t, chat).Server == types.GroupServer},
			ID: "R" + emoji, Timestamp: time.UnixMilli(ms),
		},
		Message: &waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{
			Key:  &waCommon.MessageKey{ID: proto.String(target)},
			Text: proto.String(emoji), SenderTimestampMS: proto.Int64(ms),
		}},
	}
}

func (th *testHandler) handleOK(t *testing.T, evt any) {
	t.Helper()
	if !th.handle(evt) || len(th.exitCodes()) != 0 {
		t.Fatalf("handling %T failed (exits %v)", evt, th.exits)
	}
}

func reactionEmoji(t *testing.T, th *testHandler, chat, target, sender string) (string, int) {
	t.Helper()
	var e string
	var fromMe int
	if err := th.db.QueryRow(`SELECT emoji, from_me FROM reactions WHERE chat_key = ? AND target_id = ? AND sender_key = ?`,
		chat, target, sender).Scan(&e, &fromMe); err != nil {
		t.Fatalf("reaction %s/%s by %s: %v", chat, target, sender, err)
	}
	return e, fromMe
}

func TestLiveReactionsAreStoredWithTheirOrder(t *testing.T) {
	th := newTestHandler(t)
	var announced [][]string
	th.arc.SetNotify(func(c []string) { announced = append(announced, c) })
	th.handleOK(t, reactionEvent(t, group, bob, false, "T1", "👍", 10_000))
	if e, _ := reactionEmoji(t, th, group, "T1", bob); e != "👍" {
		t.Fatalf("emoji %q", e)
	}
	th.handleOK(t, reactionEvent(t, group, bob, false, "T1", "", 20_000))
	th.handleOK(t, reactionEvent(t, group, bob, false, "T1", "👍", 10_000)) // redelivered late
	if e, _ := reactionEmoji(t, th, group, "T1", bob); e != "" {
		t.Fatalf("emoji %q after removal and stale replay", e)
	}
	if count(t, th.db, `SELECT COUNT(*) FROM messages`) != 0 {
		t.Fatal("a reaction was archived as a message")
	}
	if len(announced) != 3 || announced[0][0] != group {
		t.Fatalf("announced %v", announced)
	}
	// The user's reaction from their phone is filed under their own key.
	th.handleOK(t, reactionEvent(t, alice, selfJID, true, "T2", "❤️", 30_000))
	if e, fromMe := reactionEmoji(t, th, alice, "T2", selfJID); e != "❤️" || fromMe != 1 {
		t.Fatalf("own reaction %q from_me=%d", e, fromMe)
	}
}

func receiptEvent(t *testing.T, chat, sender string, fromMe bool, typ types.ReceiptType, ts int64, ids ...string) *events.Receipt {
	return &events.Receipt{
		MessageSource: types.MessageSource{Chat: jid(t, chat), Sender: jid(t, sender), IsFromMe: fromMe,
			IsGroup: jid(t, chat).Server == types.GroupServer},
		MessageIDs: ids, Timestamp: time.Unix(ts, 0), Type: typ,
	}
}

func TestReceiptsForOwnMessagesAreRecorded(t *testing.T) {
	th := newTestHandler(t)
	th.handleOK(t, receiptEvent(t, alice, alice, false, types.ReceiptTypeDelivered, 10, "M1", "M2"))
	th.handleOK(t, receiptEvent(t, alice, alice, false, types.ReceiptTypeRead, 11, "M1"))
	th.handleOK(t, receiptEvent(t, group, bob, false, types.ReceiptTypePlayed, 12, "V1"))
	th.handleOK(t, receiptEvent(t, alice, alice, false, types.ReceiptTypeInactive, 13, "M3"))
	if n := count(t, th.db, `SELECT COUNT(*) FROM receipts WHERE chat_key = ? AND reader_key = ?`, alice, alice); n != 3 {
		t.Fatalf("alice's receipts = %d, want 3 (inactive ignored)", n)
	}
	if n := count(t, th.db, `SELECT COUNT(*) FROM receipts WHERE chat_key = ? AND reader_key = ? AND kind = 'played'`, group, bob); n != 1 {
		t.Fatal("group played receipt missing")
	}
	if n := count(t, th.db, `SELECT COUNT(*) FROM read_messages`); n != 0 {
		t.Fatal("someone else's receipt marked the user's messages read")
	}
}

// The user reading on their phone arrives as a receipt from their own account:
// "read" with read receipts on, "read-self" with them off.
func TestOwnReadsOnOtherDevicesMarkMessagesRead(t *testing.T) {
	for _, typ := range []types.ReceiptType{types.ReceiptTypeRead, types.ReceiptTypeReadSelf} {
		th := newTestHandler(t)
		mustCommit(t, th.arc, Batch{Messages: []Message{incoming(alice, "A1", alice, 10), incoming(alice, "A2", alice, 11),
			incoming(group, "G1", bob, 12), incoming(group, "G2", alice, 13)}})
		th.handleOK(t, receiptEvent(t, alice, selfJID, true, typ, 20, "A1"))
		rec := receiptEvent(t, group, selfJID, true, typ, 21, "G1")
		rec.MessageSender = jid(t, bob)
		th.handleOK(t, rec)
		if u, _ := th.arc.UnreadIncoming(alice); len(u) != 1 || u[0].ID != "A2" {
			t.Fatalf("%s: direct unread %v", typ, u)
		}
		if u, _ := th.arc.UnreadIncoming(group); len(u) != 1 || u[0].ID != "G2" {
			t.Fatalf("%s: group unread %v", typ, u)
		}
		if n := count(t, th.db, `SELECT COUNT(*) FROM receipts`); n != 0 {
			t.Fatalf("%s: own read stored as a receipt", typ)
		}
	}
}

func TestMarkChatAsReadAdvancesTheWatermark(t *testing.T) {
	th := newTestHandler(t)
	mustCommit(t, th.arc, Batch{Messages: []Message{incoming(alice, "A1", alice, 10), incoming(alice, "A2", alice, 30)}})
	th.handleOK(t, &events.MarkChatAsRead{JID: jid(t, alice), Timestamp: time.Unix(20, 0),
		Action: &waSyncAction.MarkChatAsReadAction{Read: proto.Bool(true)}})
	if u, _ := th.arc.UnreadIncoming(alice); len(u) != 1 || u[0].ID != "A2" {
		t.Fatalf("unread %v", u)
	}
	th.handleOK(t, &events.MarkChatAsRead{JID: jid(t, alice), Timestamp: time.Unix(40, 0),
		Action: &waSyncAction.MarkChatAsReadAction{Read: proto.Bool(false)}})
	if u, _ := th.arc.UnreadIncoming(alice); len(u) != 1 {
		t.Fatalf("marking unread changed read state: %v", u)
	}
}

func historyMessage(id, participant string, ts uint64, msg *waE2E.Message) *waHistorySync.HistorySyncMsg {
	key := &waCommon.MessageKey{ID: proto.String(id), RemoteJID: proto.String(group), FromMe: proto.Bool(false)}
	if participant != "" {
		key.Participant = proto.String(participant)
	}
	return &waHistorySync.HistorySyncMsg{Message: &waWeb.WebMessageInfo{Key: key, MessageTimestamp: proto.Uint64(ts), Message: msg}}
}

func TestHistoryCarriesReactionsAndReadState(t *testing.T) {
	th := newTestHandler(t)
	withReaction := historyMessage("H1", bob, 100, &waE2E.Message{Conversation: proto.String("invented")})
	withReaction.Message.Reactions = []*waWeb.Reaction{{
		Key:  &waCommon.MessageKey{ID: proto.String("RX"), FromMe: proto.Bool(false), Participant: proto.String(alice)},
		Text: proto.String("🎉"), SenderTimestampMS: proto.Int64(150_000),
	}}
	reaction := historyMessage("H2", alice, 160, &waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{
		Key: &waCommon.MessageKey{ID: proto.String("H1")}, Text: proto.String("🙏"), SenderTimestampMS: proto.Int64(160_000)}})
	read := &waHistorySync.Conversation{ID: proto.String(group), UnreadCount: proto.Uint32(0),
		Messages: []*waHistorySync.HistorySyncMsg{withReaction, reaction}}
	unread := &waHistorySync.Conversation{ID: proto.String("120363000000000002@g.us"), UnreadCount: proto.Uint32(1),
		Messages: []*waHistorySync.HistorySyncMsg{historyMessage("U1", bob, 200, &waE2E.Message{Conversation: proto.String("invented")})}}
	b, err := th.historyBatch("N1", &waHistorySync.HistorySync{Conversations: []*waHistorySync.Conversation{read, unread}})
	if err != nil {
		t.Fatal(err)
	}
	mustCommit(t, th.arc, *b)
	if e, _ := reactionEmoji(t, th, group, "H1", alice); e != "🙏" {
		t.Fatalf("alice's reaction = %q, want the later 🙏 (the reaction message replaces the annotation)", e)
	}
	if n := count(t, th.db, `SELECT COUNT(*) FROM messages WHERE id = 'H2'`); n != 0 {
		t.Fatal("a history reaction was archived as a message")
	}
	if n := count(t, th.db, `SELECT watermark_ts FROM chat_reads WHERE chat_key = ?`, group); n != 160 {
		t.Fatalf("read conversation's watermark = %d, want 160", n)
	}
	if u, _ := th.arc.UnreadIncoming("120363000000000002@g.us"); len(u) != 1 {
		t.Fatalf("a conversation with unread messages lost them: %v", u)
	}
}

func TestHistoryReceiptsOfOwnMessages(t *testing.T) {
	th := newTestHandler(t)
	own := historyMessage("O1", "", 100, &waE2E.Message{Conversation: proto.String("invented")})
	own.Message.Key.FromMe = proto.Bool(true)
	own.Message.UserReceipt = []*waWeb.UserReceipt{{UserJID: proto.String(bob),
		ReceiptTimestamp: proto.Int64(101), ReadTimestamp: proto.Int64(102)}}
	b, err := th.historyBatch("N2", &waHistorySync.HistorySync{Conversations: []*waHistorySync.Conversation{
		{ID: proto.String(group), Messages: []*waHistorySync.HistorySyncMsg{own}}}})
	if err != nil {
		t.Fatal(err)
	}
	mustCommit(t, th.arc, *b)
	if n := count(t, th.db, `SELECT COUNT(*) FROM receipts WHERE chat_key = ? AND message_id = 'O1' AND reader_key = ?
		AND kind IN ('delivered', 'read')`, group, bob); n != 2 {
		t.Fatalf("history receipts = %d, want 2", n)
	}
}

func directHistoryMessage(id string, fromMe bool, ts uint64) *waHistorySync.HistorySyncMsg {
	key := &waCommon.MessageKey{ID: proto.String(id), RemoteJID: proto.String(alice), FromMe: proto.Bool(fromMe)}
	return &waHistorySync.HistorySyncMsg{Message: &waWeb.WebMessageInfo{Key: key, MessageTimestamp: proto.Uint64(ts),
		Message: &waE2E.Message{Conversation: proto.String("invented")}}}
}

func unreadIDs(t *testing.T, a *Archive, chat string) string {
	t.Helper()
	u, err := a.UnreadIncoming(chat)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, m := range u {
		ids = append(ids, m.ID)
	}
	return strings.Join(ids, ",")
}

// A chat first seen in a history sync (say after a relink) keeps unread
// only the newest UnreadCount incoming messages, not its whole history, so
// mark_read never sends receipts for messages the phone shows as read.
func TestHistoryUnreadCountLeavesOnlyTheNewestUnread(t *testing.T) {
	th := newTestHandler(t)
	conv := &waHistorySync.Conversation{ID: proto.String(alice), UnreadCount: proto.Uint32(2),
		Messages: []*waHistorySync.HistorySyncMsg{
			directHistoryMessage("D", false, 130), directHistoryMessage("OWN", true, 125),
			directHistoryMessage("C2", false, 120), directHistoryMessage("C1", false, 120),
			directHistoryMessage("B", false, 110), directHistoryMessage("A", false, 100)}}
	b, err := th.historyBatch("N3", &waHistorySync.HistorySync{Conversations: []*waHistorySync.Conversation{conv}})
	if err != nil {
		t.Fatal(err)
	}
	mustCommit(t, th.arc, *b)
	if got := unreadIDs(t, th.arc, alice); got != "C2,D" {
		t.Fatalf("unread %q, want C2,D (C1 ties with C2 but is older in the archive's order)", got)
	}
}

// A chat the archive already tracks keeps its read state: history does not
// move it back or forward on an unread count.
func TestHistoryUnreadCountLeavesKnownChatsAlone(t *testing.T) {
	th := newTestHandler(t)
	mustCommit(t, th.arc, Batch{Watermarks: []Watermark{{alice, 50}}})
	conv := &waHistorySync.Conversation{ID: proto.String(alice), UnreadCount: proto.Uint32(1),
		Messages: []*waHistorySync.HistorySyncMsg{directHistoryMessage("B", false, 110), directHistoryMessage("A", false, 100)}}
	b, err := th.historyBatch("N4", &waHistorySync.HistorySync{Conversations: []*waHistorySync.Conversation{conv}})
	if err != nil {
		t.Fatal(err)
	}
	mustCommit(t, th.arc, *b)
	if got := unreadIDs(t, th.arc, alice); got != "A,B" {
		t.Fatalf("unread %q, want A,B", got)
	}
}

// A new chat marked unread on the phone with no unread count keeps its
// newest incoming message unread rather than its whole history.
func TestHistoryMarkedUnreadKeepsOnlyTheNewestUnread(t *testing.T) {
	th := newTestHandler(t)
	conv := &waHistorySync.Conversation{ID: proto.String(alice), MarkedAsUnread: proto.Bool(true),
		Messages: []*waHistorySync.HistorySyncMsg{directHistoryMessage("B", false, 110), directHistoryMessage("A", false, 100)}}
	b, err := th.historyBatch("N5", &waHistorySync.HistorySync{Conversations: []*waHistorySync.Conversation{conv}})
	if err != nil {
		t.Fatal(err)
	}
	mustCommit(t, th.arc, *b)
	if got := unreadIDs(t, th.arc, alice); got != "B" {
		t.Fatalf("unread %q, want B", got)
	}
}
