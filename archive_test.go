package main

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func openTest(t *testing.T) (*Archive, *sql.DB) {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+filepath.Join(t.TempDir(), "m.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	a, err := OpenArchive(db)
	if err != nil {
		t.Fatal(err)
	}
	return a, db
}

func msg(chat, id, text string, ts int64, source string) Message {
	return Message{ChatKey: chat, ID: id, Text: text, TS: ts, Source: source}
}

func mustCommit(t *testing.T, a *Archive, b Batch) {
	t.Helper()
	if err := a.Commit(b); err != nil {
		t.Fatal(err)
	}
}

func count(t *testing.T, db *sql.DB, q string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(q, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestReplayDoesNotDuplicateOrOverwrite(t *testing.T) {
	a, db := openTest(t)
	mustCommit(t, a, Batch{Messages: []Message{msg("c", "1", "live text", 10, "live")}})
	mustCommit(t, a, Batch{Messages: []Message{msg("c", "1", "history text", 10, "history")}})
	mustCommit(t, a, Batch{Messages: []Message{msg("c", "1", "history text", 10, "history")}})
	if n := count(t, db, `SELECT COUNT(*) FROM messages`); n != 1 {
		t.Fatalf("rows = %d, want 1", n)
	}
	var text string
	db.QueryRow(`SELECT text FROM messages`).Scan(&text)
	if text != "live text" {
		t.Fatalf("text = %q, replay overwrote it", text)
	}
}

func TestEditAndRevokeSurviveReplayInBothOrders(t *testing.T) {
	for _, mutationFirst := range []bool{false, true} {
		a, db := openTest(t)
		original := Batch{Messages: []Message{msg("c", "1", "v1", 10, "live")}}
		mutations := Batch{Mutations: []Mutation{
			{ChatKey: "c", TargetID: "1", EventID: "e1", Kind: "edit", TS: 20, Text: "v2"},
			{ChatKey: "c", TargetID: "1", EventID: "e2", Kind: "revoke", TS: 30},
		}}
		if mutationFirst {
			mustCommit(t, a, mutations)
			mustCommit(t, a, original)
		} else {
			mustCommit(t, a, original)
			mustCommit(t, a, mutations)
		}
		// Replayed history of the original and of the edit changes nothing.
		mustCommit(t, a, Batch{Messages: []Message{msg("c", "1", "v1", 10, "history")}})
		mustCommit(t, a, Batch{Mutations: mutations.Mutations[:1]})
		if n := count(t, db, `SELECT COUNT(*) FROM mutations WHERE target_id = '1'`); n != 2 {
			t.Fatalf("mutationFirst=%v: mutations = %d, want 2", mutationFirst, n)
		}
		if n := count(t, db, `SELECT COUNT(*) FROM mutations WHERE kind = 'revoke'`); n != 1 {
			t.Fatalf("mutationFirst=%v: revoke lost", mutationFirst)
		}
	}
}

func TestStateSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "m.db")
	open := func() (*Archive, *sql.DB) {
		db, err := sql.Open("sqlite3", "file:"+path)
		if err != nil {
			t.Fatal(err)
		}
		a, err := OpenArchive(db)
		if err != nil {
			t.Fatal(err)
		}
		return a, db
	}
	a, db := open()
	mustCommit(t, a, Batch{Mutations: []Mutation{{ChatKey: "c", TargetID: "1", EventID: "e", Kind: "revoke", TS: 30}}})
	db.Close()
	a, db = open()
	defer db.Close()
	mustCommit(t, a, Batch{Messages: []Message{msg("c", "1", "v1", 10, "history")}})
	if n := count(t, db, `SELECT COUNT(*) FROM mutations WHERE kind = 'revoke'`); n != 1 {
		t.Fatal("revoke lost across restart")
	}
}

func TestAliasMergesSplitChat(t *testing.T) {
	a, db := openTest(t)
	mustCommit(t, a, Batch{
		Chats:    []Chat{{Key: "123@lid", Name: "Dan"}},
		Messages: []Message{msg("123@lid", "1", "under lid", 10, "live"), msg("123@lid", "dup", "x", 11, "live")},
	})
	mustCommit(t, a, Batch{Messages: []Message{msg("549@s.whatsapp.net", "dup", "x", 11, "live")}})
	mustCommit(t, a, Batch{Aliases: map[string]string{"123@lid": "549@s.whatsapp.net"}})
	if n := count(t, db, `SELECT COUNT(*) FROM messages WHERE chat_key = '123@lid'`); n != 0 {
		t.Fatalf("%d rows left under the alias", n)
	}
	if n := count(t, db, `SELECT COUNT(*) FROM messages WHERE chat_key = '549@s.whatsapp.net'`); n != 2 {
		t.Fatalf("rows under canonical key = %d, want 2", n)
	}
	if n := count(t, db, `SELECT COUNT(*) FROM chats WHERE key = '549@s.whatsapp.net' AND name = 'Dan'`); n != 1 {
		t.Fatal("chat name not carried over")
	}
	if key, _ := a.Canonical("123@lid"); key != "549@s.whatsapp.net" {
		t.Fatalf("Canonical = %q", key)
	}
}

func TestChunkCommitIsAtomic(t *testing.T) {
	a, db := openTest(t)
	// A batch whose last statement fails must leave nothing behind, including
	// its sync_chunks row, so the chunk is reprocessed after a restart.
	bad := Batch{
		Messages:  []Message{msg("c", "1", "a", 10, "history")},
		Mutations: []Mutation{{ChatKey: "c", TargetID: "1", EventID: "e", Kind: "bogus", TS: 1}},
		Chunk:     &Chunk{NotificationID: "n1", SyncType: "INITIAL_BOOTSTRAP"},
	}
	if err := a.Commit(bad); err == nil {
		t.Fatal("expected the CHECK constraint to fail")
	}
	if n := count(t, db, `SELECT COUNT(*) FROM messages`); n != 0 {
		t.Fatal("partial batch committed")
	}
	if done, _ := a.HasChunk("n1"); done {
		t.Fatal("failed chunk recorded as done")
	}
}

func TestCommitFailsWhenDatabaseUnavailable(t *testing.T) {
	a, db := openTest(t)
	db.Close()
	if err := a.Commit(Batch{Messages: []Message{msg("c", "1", "a", 10, "live")}}); err == nil {
		t.Fatal("commit on a closed database succeeded")
	}
}

func TestMaskedNamesAreNotStored(t *testing.T) {
	a, db := openTest(t)
	mustCommit(t, a, Batch{Chats: []Chat{{Key: "1@s.whatsapp.net", Name: "+1∙∙∙∙00"}}})
	if n := count(t, db, `SELECT COUNT(*) FROM chats WHERE name <> ''`); n != 0 {
		t.Fatal("masked phone stored as a name")
	}
	mustCommit(t, a, Batch{Messages: []Message{{ChatKey: "1@s.whatsapp.net", ID: "m", SenderName: "Carol", TS: 5, Text: "x", Source: "live"}}})
	if name, _ := a.LatestPushName("1@s.whatsapp.net"); name != "Carol" {
		t.Fatalf("LatestPushName = %q", name)
	}
}

func TestUnnamedGroupsListsOnlyNamelessGroups(t *testing.T) {
	a, _ := openTest(t)
	mustCommit(t, a, Batch{Chats: []Chat{
		{Key: "1@g.us", IsGroup: true},
		{Key: "2@g.us", Name: "Book club", IsGroup: true},
		{Key: "3@s.whatsapp.net"},
	}})
	keys, err := a.UnnamedGroups()
	if err != nil || len(keys) != 1 || keys[0] != "1@g.us" {
		t.Fatalf("UnnamedGroups = %v, %v", keys, err)
	}
	mustCommit(t, a, Batch{Chats: []Chat{{Key: "1@g.us", Name: "Neighbours", IsGroup: true}}})
	if keys, _ := a.UnnamedGroups(); len(keys) != 0 {
		t.Fatalf("named group still listed: %v", keys)
	}
}

func TestPruneDropsOnlyOldMessages(t *testing.T) {
	a, db := openTest(t)
	now := time.Now().Unix()
	mustCommit(t, a, Batch{Messages: []Message{
		msg("c@g.us", "old", "old text", now-100*24*3600, "live"),
		msg("c@g.us", "new", "new text", now-3600, "live"),
	}})
	cutoff, ok := retentionCutoff(time.Now(), defaultRetentionDays)
	if !ok {
		t.Fatal("the default retention keeps everything")
	}
	n, err := a.Prune(cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || count(t, db, `SELECT COUNT(*) FROM messages WHERE id='new'`) != 1 {
		t.Fatalf("pruned %d; want only the old message gone", n)
	}
}

func TestMediaRefsFollowMessagesThroughMergeAndPrune(t *testing.T) {
	a, db := openTest(t)
	now := time.Now().Unix()
	m := msg("lid@lid", "doc", "", now, "live")
	m.MediaType, m.MediaRef, m.MediaMime = "document", []byte{1, 2}, "application/pdf"
	mustCommit(t, a, Batch{Messages: []Message{m}})
	if count(t, db, `SELECT COUNT(*) FROM media_refs WHERE chat_key='lid@lid'`) != 1 {
		t.Fatal("media ref not stored")
	}
	mustCommit(t, a, Batch{Aliases: map[string]string{"lid@lid": "1@s.whatsapp.net"}})
	if count(t, db, `SELECT COUNT(*) FROM media_refs WHERE chat_key='1@s.whatsapp.net'`) != 1 {
		t.Fatal("media ref did not follow the merged chat")
	}
	if _, err := a.Prune(now + 1); err != nil {
		t.Fatal(err)
	}
	if count(t, db, `SELECT COUNT(*) FROM media_refs`) != 0 {
		t.Fatal("media ref outlived its message")
	}
}

func TestLookupFindsAnArchivedMessage(t *testing.T) {
	a, _ := openTest(t)
	m := msg("g@g.us", "ID1", "hola", 10, "live")
	m.SenderKey = "15550000003@s.whatsapp.net"
	mustCommit(t, a, Batch{Messages: []Message{m}})
	got, ok, err := a.Lookup("g@g.us", "ID1")
	if err != nil || !ok || got.Text != "hola" || got.SenderKey != m.SenderKey || got.FromMe {
		t.Fatalf("got %+v %v %v", got, ok, err)
	}
	if _, ok, err := a.Lookup("g@g.us", "nope"); ok || err != nil {
		t.Fatalf("missing message: %v %v", ok, err)
	}
}

func TestLookupGivesTheEditedTextAndSkipsDeletedMessages(t *testing.T) {
	a, _ := openTest(t)
	mustCommit(t, a, Batch{Messages: []Message{msg("g@g.us", "E", "borrador", 10, "live"), msg("g@g.us", "D", "x", 11, "live")},
		Mutations: []Mutation{
			{ChatKey: "g@g.us", TargetID: "E", EventID: "e1", Kind: "edit", TS: 12, Text: "corregido"},
			{ChatKey: "g@g.us", TargetID: "D", EventID: "r1", Kind: "revoke", TS: 13},
		}})
	if got, ok, err := a.Lookup("g@g.us", "E"); err != nil || !ok || got.Text != "corregido" {
		t.Fatalf("edited: %+v %v %v", got, ok, err)
	}
	if _, ok, err := a.Lookup("g@g.us", "D"); ok || err != nil {
		t.Fatalf("deleted message found: %v %v", ok, err)
	}
}

func TestRetentionZeroKeepsEverything(t *testing.T) {
	if _, ok := retentionCutoff(time.Now(), 0); ok {
		t.Fatal("-retention-days 0 prunes")
	}
	now := time.Unix(1_000_000_000, 0)
	cutoff, ok := retentionCutoff(now, 2)
	if !ok || cutoff != now.Unix()-2*24*3600 {
		t.Fatalf("cutoff = %d, %v", cutoff, ok)
	}
}

func TestRunFlagsDefaultToNinetyDays(t *testing.T) {
	o, err := parseRunFlags(nil)
	if err != nil || o.retentionDays != 90 {
		t.Fatalf("default: %+v %v", o, err)
	}
	o, err = parseRunFlags([]string{"-retention-days", "0"})
	if err != nil || o.retentionDays != 0 {
		t.Fatalf("-retention-days 0: %+v %v", o, err)
	}
	if _, err := parseRunFlags([]string{"-retention-days", "-1"}); err == nil {
		t.Fatal("negative retention accepted")
	}
}

const (
	alice = "15550000001@s.whatsapp.net"
	bob   = "15550000002@s.whatsapp.net"
	group = "120363000000000001@g.us"
)

func incoming(chat, id, sender string, ts int64) Message {
	return Message{ChatKey: chat, ID: id, SenderKey: sender, TS: ts, Text: "x", Source: "live"}
}

func TestBootstrapCountsArchivedMessagesAsReadOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "m.db")
	db, err := sql.Open("sqlite3", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	// An archive from before the client tables existed.
	if _, err := db.Exec(schema); err != nil {
		t.Fatal(err)
	}
	for _, m := range []Message{incoming(alice, "a1", alice, 10), incoming(alice, "a2", alice, 30), incoming(group, "g1", bob, 20)} {
		if _, err := db.Exec(`INSERT INTO messages (chat_key, id, sender_key, ts, source) VALUES (?, ?, ?, ?, 'live')`,
			m.ChatKey, m.ID, m.SenderKey, m.TS); err != nil {
			t.Fatal(err)
		}
	}
	a, err := OpenArchive(db)
	if err != nil {
		t.Fatal(err)
	}
	if n := count(t, db, `SELECT watermark_ts FROM chat_reads WHERE chat_key = ?`, alice); n != 30 {
		t.Fatalf("alice watermark = %d, want 30", n)
	}
	if n := count(t, db, `SELECT watermark_ts FROM chat_reads WHERE chat_key = ?`, group); n != 20 {
		t.Fatalf("group watermark = %d, want 20", n)
	}
	if u, _ := a.UnreadIncoming(alice); len(u) != 0 {
		t.Fatalf("archived messages unread after the upgrade: %v", u)
	}
	// A later message is unread, and reopening does not run the bootstrap again.
	mustCommit(t, a, Batch{Messages: []Message{incoming(alice, "a3", alice, 40), incoming(bob, "b1", bob, 5)}})
	db.Close()
	db, err = sql.Open("sqlite3", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a, err = OpenArchive(db)
	if err != nil {
		t.Fatal(err)
	}
	if u, _ := a.UnreadIncoming(alice); len(u) != 1 || u[0].ID != "a3" {
		t.Fatalf("unread after reopen = %v, want a3", u)
	}
	if u, _ := a.UnreadIncoming(bob); len(u) != 1 {
		t.Fatalf("a chat new since the upgrade has no watermark, so its message is unread: %v", u)
	}
}

func TestReactionsAddRemoveAndIgnoreStaleReplays(t *testing.T) {
	a, db := openTest(t)
	emoji := func() string {
		var e string
		if err := db.QueryRow(`SELECT emoji FROM reactions WHERE chat_key = ? AND target_id = 'm' AND sender_key = ?`,
			group, bob).Scan(&e); err != nil {
			t.Fatal(err)
		}
		return e
	}
	react := func(e string, ts int64) {
		mustCommit(t, a, Batch{Reactions: []Reaction{{ChatKey: group, TargetID: "m", SenderKey: bob, Emoji: e, TS: ts}}})
	}
	react("👍", 10)
	if emoji() != "👍" {
		t.Fatal("reaction not stored")
	}
	react("", 20)
	if emoji() != "" || count(t, db, `SELECT COUNT(*) FROM reactions`) != 1 {
		t.Fatal("removal not kept as an empty row")
	}
	react("👍", 10) // replayed from history after the removal
	if emoji() != "" {
		t.Fatal("stale replay brought a removed reaction back")
	}
	react("❤️", 30)
	react("😂", 30) // a change within the same second
	if emoji() != "😂" {
		t.Fatalf("emoji = %q, want the later of two same-second reactions", emoji())
	}
}

func TestReceiptsAndReadMarksAreKeptOnce(t *testing.T) {
	a, db := openTest(t)
	r := ReceiptRow{ChatKey: alice, MessageID: "m", ReaderKey: alice, Kind: "read", TS: 10}
	mustCommit(t, a, Batch{Receipts: []ReceiptRow{r}, Reads: []ReadMark{{alice, "x"}}})
	r.TS = 20
	mustCommit(t, a, Batch{Receipts: []ReceiptRow{r}, Reads: []ReadMark{{alice, "x"}}})
	if n := count(t, db, `SELECT ts FROM receipts`); n != 10 {
		t.Fatalf("receipt ts = %d, want the first", n)
	}
	if n := count(t, db, `SELECT COUNT(*) FROM read_messages`); n != 1 {
		t.Fatalf("read marks = %d", n)
	}
	if err := a.Commit(Batch{Receipts: []ReceiptRow{{ChatKey: alice, MessageID: "m", ReaderKey: alice, Kind: "seen"}}}); err == nil {
		t.Fatal("unknown receipt kind accepted")
	}
}

func TestWatermarkOnlyMovesForward(t *testing.T) {
	a, db := openTest(t)
	mustCommit(t, a, Batch{Watermarks: []Watermark{{alice, 50}}})
	mustCommit(t, a, Batch{Watermarks: []Watermark{{alice, 40}}})
	if n := count(t, db, `SELECT watermark_ts FROM chat_reads WHERE chat_key = ?`, alice); n != 50 {
		t.Fatalf("watermark = %d, want 50", n)
	}
}

func TestUnreadIsByMessageNotTimestamp(t *testing.T) {
	a, _ := openTest(t)
	mine := incoming(group, "own", "", 31)
	mine.FromMe = true
	// Two messages share a timestamp; marking one read must not hide the other.
	mustCommit(t, a, Batch{
		Messages: []Message{incoming(group, "old", bob, 10), incoming(group, "t1", bob, 30),
			incoming(group, "t2", alice, 30), mine},
		Watermarks: []Watermark{{group, 10}},
		Reads:      []ReadMark{{group, "t1"}},
	})
	u, err := a.UnreadIncoming(group)
	if err != nil || len(u) != 1 || u[0].ID != "t2" || u[0].SenderKey != alice {
		t.Fatalf("unread = %v, %v; want only t2", u, err)
	}
}

func TestAliasMergeCarriesClientTables(t *testing.T) {
	a, db := openTest(t)
	const lid = "99000000000001@lid"
	mustCommit(t, a, Batch{
		Messages:   []Message{incoming(lid, "m", lid, 10)},
		Reactions:  []Reaction{{ChatKey: lid, TargetID: "m", SenderKey: lid, Emoji: "👍", TS: 20}, {ChatKey: group, TargetID: "g", SenderKey: lid, Emoji: "🙏", TS: 5}},
		Receipts:   []ReceiptRow{{ChatKey: lid, MessageID: "mine", ReaderKey: lid, Kind: "read", TS: 11}},
		Reads:      []ReadMark{{lid, "m"}},
		Watermarks: []Watermark{{lid, 15}},
	})
	if _, _, err := a.ClaimOp(Op{ID: "op1", Method: "send", ChatKey: lid, TS: 1}); err != nil {
		t.Fatal(err)
	}
	// The canonical chat already has an older reaction by the same person to
	// the same message, and a newer one in the group.
	mustCommit(t, a, Batch{
		Reactions:  []Reaction{{ChatKey: alice, TargetID: "m", SenderKey: alice, Emoji: "😮", TS: 12}, {ChatKey: group, TargetID: "g", SenderKey: alice, Emoji: "🎉", TS: 9}},
		Watermarks: []Watermark{{alice, 7}},
	})
	mustCommit(t, a, Batch{Aliases: map[string]string{lid: alice}})
	for _, table := range []string{"reactions WHERE chat_key", "reactions WHERE sender_key", "receipts WHERE chat_key",
		"receipts WHERE reader_key", "chat_reads WHERE chat_key", "read_messages WHERE chat_key", "api_ops WHERE chat_key"} {
		if n := count(t, db, `SELECT COUNT(*) FROM `+table+` = ?`, lid); n != 0 {
			t.Errorf("%d rows of %s left under the alias", n, table)
		}
	}
	var e string
	db.QueryRow(`SELECT emoji FROM reactions WHERE chat_key = ? AND target_id = 'm'`, alice).Scan(&e)
	if e != "👍" {
		t.Errorf("merged reaction = %q, want the newer one", e)
	}
	db.QueryRow(`SELECT emoji FROM reactions WHERE chat_key = ? AND target_id = 'g'`, group).Scan(&e)
	if e != "🎉" {
		t.Errorf("group reaction = %q, want the newer one", e)
	}
	if n := count(t, db, `SELECT watermark_ts FROM chat_reads WHERE chat_key = ?`, alice); n != 15 {
		t.Errorf("watermark = %d, want the later one", n)
	}
	if n := count(t, db, `SELECT COUNT(*) FROM receipts WHERE chat_key = ? AND reader_key = ?`, alice, alice); n != 1 {
		t.Error("receipt not carried over")
	}
	if u, _ := a.UnreadIncoming(alice); len(u) != 0 {
		t.Errorf("read mark lost in the merge: %v", u)
	}
	if o, ok, _ := a.GetOp("op1"); !ok || o.ChatKey != alice {
		t.Errorf("op chat = %+v", o)
	}
}

func TestCommitAnnouncesTouchedChats(t *testing.T) {
	a, db := openTest(t)
	var got [][]string
	a.SetNotify(func(chats []string) { got = append(got, chats) })
	mustCommit(t, a, Batch{Messages: []Message{incoming(alice, "1", alice, 1)}, Reactions: []Reaction{{ChatKey: group, TargetID: "x", SenderKey: bob, Emoji: "👍", TS: 1}}})
	mustCommit(t, a, Batch{Chunk: &Chunk{NotificationID: "n"}})
	mustCommit(t, a, Batch{Watermarks: []Watermark{{bob, 3}}})
	if len(got) != 2 || strings.Join(got[0], ",") != group+","+alice || strings.Join(got[1], ",") != bob {
		t.Fatalf("announced %v", got)
	}
	db.Close()
	if err := a.Commit(Batch{Messages: []Message{incoming(alice, "2", alice, 2)}}); err == nil || len(got) != 2 {
		t.Fatalf("a failed commit was announced: %v %v", err, got)
	}
}

func TestClaimOpJournalsOnce(t *testing.T) {
	a, _ := openTest(t)
	o, fresh, err := a.ClaimOp(Op{ID: "u1", Method: "send", ChatKey: alice, MessageID: "3EB0AA", TS: 5})
	if err != nil || !fresh || o.State != "pending" {
		t.Fatalf("first claim: %+v %v %v", o, fresh, err)
	}
	if err := a.RecordOp(Op{ID: "u1", Method: "send", ChatKey: alice, MessageID: "3EB0AA", State: "sent", TS: 6}); err != nil {
		t.Fatal(err)
	}
	o, fresh, err = a.ClaimOp(Op{ID: "u1", Method: "send", ChatKey: alice, MessageID: "OTHER", TS: 7})
	if err != nil || fresh || o.State != "sent" || o.MessageID != "3EB0AA" {
		t.Fatalf("repeat claim: %+v %v %v", o, fresh, err)
	}
}

func TestPruneDropsOldReactionsAndReceipts(t *testing.T) {
	a, db := openTest(t)
	var got [][]string
	a.SetNotify(func(chats []string) { got = append(got, chats) })
	mustCommit(t, a, Batch{
		Messages:  []Message{incoming(alice, "old", alice, 10), incoming(bob, "new", bob, 100)},
		Reactions: []Reaction{{ChatKey: alice, TargetID: "old", SenderKey: alice, Emoji: "👍", TS: 10}},
		Receipts:  []ReceiptRow{{ChatKey: alice, MessageID: "o", ReaderKey: alice, Kind: "read", TS: 10}},
	})
	got = nil
	if _, err := a.Prune(50); err != nil {
		t.Fatal(err)
	}
	if count(t, db, `SELECT COUNT(*) FROM reactions`)+count(t, db, `SELECT COUNT(*) FROM receipts`) != 0 {
		t.Fatal("old reactions or receipts survived the prune")
	}
	if len(got) != 1 || len(got[0]) != 1 || got[0][0] != alice {
		t.Fatalf("prune announced %v", got)
	}
}

// Prune must take the write lock before it reads, so that busy_timeout
// covers a writer that commits in between; a read-then-write transaction
// fails at once with "database is locked" when its snapshot goes stale.
func TestPruneWaitsForConcurrentWriters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "m.db")
	open := func() *Archive {
		db, err := sql.Open("sqlite3", "file:"+path+"?_busy_timeout=5000")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		a, err := OpenArchive(db)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	pruner, writer := open(), open()
	var old []Message
	for i := range 2000 {
		old = append(old, msg(fmt.Sprintf("%d@s.whatsapp.net", i%50), fmt.Sprintf("OLD%d", i), "invented", 10, "live"))
	}
	stop := make(chan struct{})
	errs := make(chan error, 1)
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				m := msg("1@s.whatsapp.net", fmt.Sprintf("NEW%d-%d", w, i), "invented", 1_000_000, "live")
				if err := writer.Commit(Batch{Messages: []Message{m}}); err != nil {
					select {
					case errs <- err:
					default:
					}
					return
				}
			}
		}()
	}
	var pruneErr error
	for range 40 {
		if err := pruner.Commit(Batch{Messages: old}); err != nil {
			pruneErr = err
			break
		}
		if _, err := pruner.Prune(100); err != nil {
			pruneErr = err
			break
		}
	}
	close(stop)
	wg.Wait()
	if pruneErr != nil {
		t.Fatalf("prune beside concurrent writers: %v", pruneErr)
	}
	select {
	case err := <-errs:
		t.Fatalf("writer: %v", err)
	default:
	}
}
