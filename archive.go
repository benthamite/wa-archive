package main

// The archive database: chats, messages, edits/revokes, reactions, receipts,
// read state, sync coverage and the API's operation journal. Everything here
// is independent of whatsmeow so it can be tested directly. Clients such as
// wsp read this database directly (read-only); the tables they read are part
// of the API contract in wsp's docs/api.md.

import (
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const schema = `
CREATE TABLE IF NOT EXISTS chats (
	key        TEXT PRIMARY KEY,
	name       TEXT NOT NULL DEFAULT '',
	is_group   INTEGER NOT NULL DEFAULT 0,
	updated_at INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS aliases (
	alias TEXT PRIMARY KEY,
	key   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
	chat_key    TEXT NOT NULL,
	id          TEXT NOT NULL,
	sender_key  TEXT NOT NULL DEFAULT '',
	sender_name TEXT NOT NULL DEFAULT '',
	from_me     INTEGER NOT NULL DEFAULT 0,
	ts          INTEGER NOT NULL,
	text        TEXT NOT NULL DEFAULT '',
	quoted_id   TEXT NOT NULL DEFAULT '',
	media_type  TEXT NOT NULL DEFAULT '',
	file_name   TEXT NOT NULL DEFAULT '',
	source      TEXT NOT NULL,
	PRIMARY KEY (chat_key, id)
);
CREATE INDEX IF NOT EXISTS messages_ts ON messages (chat_key, ts);
CREATE TABLE IF NOT EXISTS mutations (
	chat_key  TEXT NOT NULL,
	target_id TEXT NOT NULL,
	event_id  TEXT NOT NULL,
	kind      TEXT NOT NULL CHECK (kind IN ('edit', 'revoke')),
	ts        INTEGER NOT NULL,
	text      TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (chat_key, target_id, event_id)
);
CREATE TABLE IF NOT EXISTS media_refs (
	chat_key TEXT NOT NULL,
	id       TEXT NOT NULL,
	kind     TEXT NOT NULL,
	mimetype TEXT NOT NULL DEFAULT '',
	ref      BLOB NOT NULL,
	PRIMARY KEY (chat_key, id)
);
CREATE TABLE IF NOT EXISTS sync_chunks (
	notification_id TEXT PRIMARY KEY,
	sync_type       TEXT NOT NULL,
	chunk_order     INTEGER NOT NULL,
	progress        INTEGER NOT NULL,
	conversations   INTEGER NOT NULL,
	messages        INTEGER NOT NULL,
	received_at     INTEGER NOT NULL
);
`

// clientSchema holds the tables added for API clients such as wsp. They are created
// together with the read-state bootstrap in one transaction (see
// createClientTables), so a crash cannot leave them without it.
const clientSchema = `
CREATE TABLE IF NOT EXISTS reactions (
	chat_key   TEXT NOT NULL,
	target_id  TEXT NOT NULL,
	sender_key TEXT NOT NULL,
	from_me    INTEGER NOT NULL DEFAULT 0,
	emoji      TEXT NOT NULL,
	ts         INTEGER NOT NULL,
	PRIMARY KEY (chat_key, target_id, sender_key)
);
CREATE TABLE IF NOT EXISTS receipts (
	chat_key   TEXT NOT NULL,
	message_id TEXT NOT NULL,
	reader_key TEXT NOT NULL,
	kind       TEXT NOT NULL CHECK (kind IN ('delivered', 'read', 'played')),
	ts         INTEGER NOT NULL,
	PRIMARY KEY (chat_key, message_id, reader_key, kind)
);
CREATE TABLE IF NOT EXISTS chat_reads (
	chat_key     TEXT PRIMARY KEY,
	watermark_ts INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS read_messages (
	chat_key TEXT NOT NULL,
	id       TEXT NOT NULL,
	PRIMARY KEY (chat_key, id)
);
CREATE TABLE IF NOT EXISTS api_ops (
	op_id      TEXT PRIMARY KEY,
	method     TEXT NOT NULL,
	chat_key   TEXT NOT NULL,
	message_id TEXT NOT NULL DEFAULT '',
	state      TEXT NOT NULL CHECK (state IN ('pending', 'sent', 'failed')),
	error      TEXT NOT NULL DEFAULT '',
	ts         INTEGER NOT NULL
);
`

// Archive wraps the messages database.
type Archive struct {
	db *sql.DB
	// notify, when set, is told which chats each successful write touched,
	// after it is committed (the API's "changed" event).
	notify func(chats []string)
}

// SetNotify registers F to be called with the chats each commit touched.
func (a *Archive) SetNotify(f func(chats []string)) { a.notify = f }

func (a *Archive) announce(chats map[string]bool) {
	delete(chats, "")
	if a.notify == nil || len(chats) == 0 {
		return
	}
	keys := make([]string, 0, len(chats))
	for k := range chats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	a.notify(keys)
}

func OpenArchive(db *sql.DB) (*Archive, error) {
	for _, pragma := range []string{"PRAGMA journal_mode=WAL", "PRAGMA busy_timeout=5000"} {
		if _, err := db.Exec(pragma); err != nil {
			return nil, fmt.Errorf("%s: %w", pragma, err)
		}
	}
	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("create schema: %w", err)
	}
	if err := createClientTables(db); err != nil {
		return nil, err
	}
	// Masked phone numbers ("+1∙∙∙00") are placeholders, not names.
	if _, err := db.Exec(`UPDATE chats SET name = '' WHERE name LIKE '%∙%'`); err != nil {
		return nil, fmt.Errorf("clear masked names: %w", err)
	}
	return &Archive{db: db}, nil
}

// createClientTables creates the tables wsp needs and, the first time, counts
// every message already archived as read: historical read state is not
// available, and sending read receipts for months-old messages would be
// wrong. It sets each chat's watermark to its newest
// message, in the same transaction as the tables, so the bootstrap runs
// exactly once.
func createClientTables(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var exists int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'chat_reads'`).
		Scan(&exists); err != nil {
		return err
	}
	if _, err := tx.Exec(clientSchema); err != nil {
		return fmt.Errorf("create client tables: %w", err)
	}
	if exists == 0 {
		if _, err := tx.Exec(`INSERT OR IGNORE INTO chat_reads (chat_key, watermark_ts)
			SELECT chat_key, MAX(ts) FROM messages GROUP BY chat_key`); err != nil {
			return fmt.Errorf("bootstrap read state: %w", err)
		}
	}
	return tx.Commit()
}

// Message is one stored message in canonical form.
type Message struct {
	ChatKey    string
	ID         string
	SenderKey  string
	SenderName string
	FromMe     bool
	TS         int64
	Text       string
	QuotedID   string
	MediaType  string
	FileName   string
	Source     string // "live", "history" or "sent"
	// MediaRef is the attachment's download reference (the marshaled image,
	// video, audio or document submessage), kept so a file can be fetched on
	// request (fetch.go). Nothing is downloaded unless asked for.
	MediaRef  []byte
	MediaMime string
}

// Mutation is an edit or revoke of an earlier message.
type Mutation struct {
	ChatKey  string
	TargetID string
	EventID  string
	Kind     string // "edit" or "revoke"
	TS       int64
	Text     string
}

// Chunk records one committed history-sync chunk.
type Chunk struct {
	NotificationID string
	SyncType       string
	ChunkOrder     int
	Progress       int
	Conversations  int
	Messages       int
	ReceivedAt     int64
}

// Reaction is one person's current reaction to a message. An empty Emoji
// records a removal, so a replayed older reaction cannot bring it back.
type Reaction struct {
	ChatKey   string
	TargetID  string
	SenderKey string
	FromMe    bool
	Emoji     string
	TS        int64
}

// ReceiptRow records that READERKEY received, read or played one of the
// user's own messages.
type ReceiptRow struct {
	ChatKey   string
	MessageID string
	ReaderKey string
	Kind      string // "delivered", "read" or "played"
	TS        int64
}

// ReadMark records that the user has read incoming message ID (on any device).
type ReadMark struct {
	ChatKey string
	ID      string
}

// Watermark says every message of a chat up to TS counts as read. It only
// ever moves forward.
type Watermark struct {
	ChatKey string
	TS      int64
}

// InitialRead is the read state of a chat the archive has none for yet:
// everything up to WatermarkTS counts as read, and so do ReadIDs (messages
// at the watermark's next second that the source says were read). It is
// applied only if the chat has no chat_reads row, so it never overrides read
// state the archive already tracks.
type InitialRead struct {
	ChatKey     string
	WatermarkTS int64
	ReadIDs     []string
}

// Op is one journaled API action (api_ops).
type Op struct {
	ID        string
	Method    string
	ChatKey   string
	MessageID string
	State     string // "pending", "sent" or "failed"
	Error     string
	TS        int64
}

// Batch is everything one event contributes. It is committed atomically.
type Batch struct {
	Chats      []Chat
	Aliases    map[string]string // alias -> canonical key
	Messages   []Message
	Mutations  []Mutation
	Reactions  []Reaction
	Receipts   []ReceiptRow
	Reads      []ReadMark
	Watermarks []Watermark
	// InitialReads apply before Watermarks.
	InitialReads []InitialRead
	Chunk        *Chunk
	// Op, when set, updates a journaled operation in the same transaction
	// as the message it sent, so the two cannot disagree.
	Op *Op
}

// touched returns the chats whose contents B changes.
func (b Batch) touched() map[string]bool {
	chats := map[string]bool{}
	for alias, key := range b.Aliases {
		chats[alias], chats[key] = true, true
	}
	for _, c := range b.Chats {
		chats[c.Key] = true
	}
	for _, m := range b.Messages {
		chats[m.ChatKey] = true
	}
	for _, m := range b.Mutations {
		chats[m.ChatKey] = true
	}
	for _, r := range b.Reactions {
		chats[r.ChatKey] = true
	}
	for _, r := range b.Receipts {
		chats[r.ChatKey] = true
	}
	for _, r := range b.Reads {
		chats[r.ChatKey] = true
	}
	for _, w := range b.Watermarks {
		chats[w.ChatKey] = true
	}
	for _, r := range b.InitialReads {
		chats[r.ChatKey] = true
	}
	return chats
}

type Chat struct {
	Key       string
	Name      string
	IsGroup   bool
	UpdatedAt int64
}

// Commit writes a batch in one transaction. Merge rules:
//   - a message row keeps the first content stored for it; replays (history
//     after live, or history twice) never change it, but may fill an empty
//     sender name;
//   - edits and revokes are separate rows, so no replay can undo them, and one
//     that arrives before its target is kept until the target arrives;
//   - a reaction replaces the stored one only if it is not older, so a
//     replayed reaction never overrides a newer one (ties go to the incoming
//     row: timestamps are in seconds, and a quick change of mind within one
//     second is likelier than a replay from the same second);
//   - receipts and read marks are kept once; a watermark only moves forward;
//     an initial read state applies only to a chat with none;
//   - a new alias moves rows filed under the alias to the canonical key.
//
// After the commit, the chats it touched are announced (SetNotify).
func (a *Archive) Commit(b Batch) error {
	if err := a.commit(b); err != nil {
		return err
	}
	a.announce(b.touched())
	return nil
}

func (a *Archive) commit(b Batch) error {
	tx, err := a.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for alias, key := range b.Aliases {
		if err := mergeAlias(tx, alias, key); err != nil {
			return fmt.Errorf("alias %s: %w", alias, err)
		}
	}
	for _, c := range b.Chats {
		if strings.Contains(c.Name, "∙") {
			c.Name = ""
		}
		if _, err := tx.Exec(`INSERT INTO chats (key, name, is_group, updated_at) VALUES (?, ?, ?, ?)
			ON CONFLICT (key) DO UPDATE SET
				name = CASE WHEN excluded.name <> '' THEN excluded.name ELSE chats.name END,
				is_group = excluded.is_group,
				updated_at = MAX(chats.updated_at, excluded.updated_at)`,
			c.Key, c.Name, boolInt(c.IsGroup), c.UpdatedAt); err != nil {
			return fmt.Errorf("chat %s: %w", c.Key, err)
		}
	}
	for _, m := range b.Messages {
		if _, err := tx.Exec(`INSERT INTO messages
			(chat_key, id, sender_key, sender_name, from_me, ts, text, quoted_id, media_type, file_name, source)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (chat_key, id) DO UPDATE SET
				sender_name = CASE WHEN messages.sender_name = '' THEN excluded.sender_name ELSE messages.sender_name END`,
			m.ChatKey, m.ID, m.SenderKey, m.SenderName, boolInt(m.FromMe), m.TS, m.Text,
			m.QuotedID, m.MediaType, m.FileName, m.Source); err != nil {
			return fmt.Errorf("message %s/%s: %w", m.ChatKey, m.ID, err)
		}
		if _, err := tx.Exec(`INSERT INTO chats (key, updated_at) VALUES (?, ?)
			ON CONFLICT (key) DO UPDATE SET updated_at = MAX(chats.updated_at, excluded.updated_at)`,
			m.ChatKey, m.TS); err != nil {
			return err
		}
		if len(m.MediaRef) > 0 {
			if _, err := tx.Exec(`INSERT INTO media_refs (chat_key, id, kind, mimetype, ref)
				VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`,
				m.ChatKey, m.ID, m.MediaType, m.MediaMime, m.MediaRef); err != nil {
				return fmt.Errorf("media ref %s/%s: %w", m.ChatKey, m.ID, err)
			}
		}
	}
	for _, mu := range b.Mutations {
		if _, err := tx.Exec(`INSERT INTO mutations (chat_key, target_id, event_id, kind, ts, text)
			VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`,
			mu.ChatKey, mu.TargetID, mu.EventID, mu.Kind, mu.TS, mu.Text); err != nil {
			return fmt.Errorf("mutation %s/%s: %w", mu.ChatKey, mu.TargetID, err)
		}
	}
	for _, r := range b.Reactions {
		if _, err := tx.Exec(`INSERT INTO reactions (chat_key, target_id, sender_key, from_me, emoji, ts)
			VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT (chat_key, target_id, sender_key) DO UPDATE SET
				from_me = excluded.from_me, emoji = excluded.emoji, ts = excluded.ts
			WHERE excluded.ts >= reactions.ts`,
			r.ChatKey, r.TargetID, r.SenderKey, boolInt(r.FromMe), r.Emoji, r.TS); err != nil {
			return fmt.Errorf("reaction %s/%s: %w", r.ChatKey, r.TargetID, err)
		}
	}
	for _, r := range b.Receipts {
		if _, err := tx.Exec(`INSERT INTO receipts (chat_key, message_id, reader_key, kind, ts)
			VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`,
			r.ChatKey, r.MessageID, r.ReaderKey, r.Kind, r.TS); err != nil {
			return fmt.Errorf("receipt %s/%s: %w", r.ChatKey, r.MessageID, err)
		}
	}
	for _, r := range b.Reads {
		if _, err := tx.Exec(`INSERT INTO read_messages (chat_key, id) VALUES (?, ?) ON CONFLICT DO NOTHING`,
			r.ChatKey, r.ID); err != nil {
			return fmt.Errorf("read mark %s/%s: %w", r.ChatKey, r.ID, err)
		}
	}
	for _, r := range b.InitialReads {
		res, err := tx.Exec(`INSERT INTO chat_reads (chat_key, watermark_ts) VALUES (?, ?) ON CONFLICT DO NOTHING`,
			r.ChatKey, r.WatermarkTS)
		if err != nil {
			return fmt.Errorf("initial read state %s: %w", r.ChatKey, err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			continue
		}
		for _, id := range r.ReadIDs {
			if _, err := tx.Exec(`INSERT INTO read_messages (chat_key, id) VALUES (?, ?) ON CONFLICT DO NOTHING`,
				r.ChatKey, id); err != nil {
				return fmt.Errorf("initial read mark %s/%s: %w", r.ChatKey, id, err)
			}
		}
	}
	for _, w := range b.Watermarks {
		if _, err := tx.Exec(`INSERT INTO chat_reads (chat_key, watermark_ts) VALUES (?, ?)
			ON CONFLICT (chat_key) DO UPDATE SET watermark_ts = MAX(chat_reads.watermark_ts, excluded.watermark_ts)`,
			w.ChatKey, w.TS); err != nil {
			return fmt.Errorf("watermark %s: %w", w.ChatKey, err)
		}
	}
	if o := b.Op; o != nil {
		if err := updateOp(tx, *o); err != nil {
			return err
		}
	}
	if c := b.Chunk; c != nil {
		if _, err := tx.Exec(`INSERT INTO sync_chunks
			(notification_id, sync_type, chunk_order, progress, conversations, messages, received_at)
			VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`,
			c.NotificationID, c.SyncType, c.ChunkOrder, c.Progress, c.Conversations, c.Messages, c.ReceivedAt); err != nil {
			return fmt.Errorf("chunk %s: %w", c.NotificationID, err)
		}
	}
	return tx.Commit()
}

// defaultRetentionDays is how long message text stays in the archive unless
// `run -retention-days` says otherwise (the copy holds decrypted WhatsApp
// content, so by default it should not grow forever). A deployment that backs
// a client such as wsp may pass 0 to keep everything.
const defaultRetentionDays = 90

// retentionCutoff is the Unix time before which messages are pruned when
// keeping DAYS days, and false when DAYS is 0 (keep everything).
func retentionCutoff(now time.Time, days int) (int64, bool) {
	if days <= 0 {
		return 0, false
	}
	return now.Add(-time.Duration(days) * 24 * time.Hour).Unix(), true
}

// Prune deletes messages and edits/revokes older than cutoff (Unix seconds).
// A later history-sync replay may re-insert old messages; the next prune
// removes them again.
func (a *Archive) Prune(cutoff int64) (int64, error) {
	tx, err := a.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	// The first statement writes, so the transaction takes the write lock
	// before it reads anything and busy_timeout covers waiting for it. A
	// transaction that read first would fail at once ("database is locked")
	// if another connection committed before its first write.
	rows, err := tx.Query(`DELETE FROM messages WHERE ts < ? RETURNING chat_key`, cutoff)
	if err != nil {
		return 0, err
	}
	chats := map[string]bool{}
	var n int64
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			rows.Close()
			return 0, err
		}
		chats[k] = true
		n++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	// Reactions and receipts age out with the messages they annotate. Read
	// marks and watermarks hold no content and stay.
	for _, table := range []string{"mutations", "reactions", "receipts"} {
		if _, err := tx.Exec(`DELETE FROM `+table+` WHERE ts < ?`, cutoff); err != nil {
			return 0, err
		}
	}
	if _, err := tx.Exec(`DELETE FROM media_refs WHERE NOT EXISTS (SELECT 1 FROM messages m
		WHERE m.chat_key = media_refs.chat_key AND m.id = media_refs.id)`); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	a.announce(chats)
	return n, nil
}

// UnnamedDirectChats returns the keys of direct chats that have no name yet.
func (a *Archive) UnnamedDirectChats() ([]string, error) {
	return a.unnamedChats(false)
}

// UnnamedGroups returns the keys of group chats that have no name, such as
// groups created after linking, whose name no history sync carried.
func (a *Archive) UnnamedGroups() ([]string, error) {
	return a.unnamedChats(true)
}

func (a *Archive) unnamedChats(groups bool) ([]string, error) {
	rows, err := a.db.Query(`SELECT key FROM chats WHERE name = '' AND is_group = ?`, boolInt(groups))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

// LatestPushName returns the most recent display name a contact used in a
// direct chat, or "".
func (a *Archive) LatestPushName(chatKey string) (string, error) {
	var name string
	err := a.db.QueryRow(`SELECT sender_name FROM messages WHERE chat_key = ? AND from_me = 0
		AND sender_name <> '' ORDER BY ts DESC LIMIT 1`, chatKey).Scan(&name)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return name, err
}

// HasChunk reports whether a history notification was already committed.
// Lookup returns message ID of the chat CHATKEY as it now reads (its latest
// edit), and whether the archive has it. A deleted message counts as missing.
func (a *Archive) Lookup(chatKey, id string) (Message, bool, error) {
	m := Message{ChatKey: chatKey, ID: id}
	var fromMe, revoked int
	err := a.db.QueryRow(`SELECT sender_key, sender_name, from_me, ts,
		COALESCE((SELECT u.text FROM mutations u WHERE u.chat_key = m.chat_key AND u.target_id = m.id
			AND u.kind = 'edit' ORDER BY u.ts DESC LIMIT 1), text),
		media_type, file_name,
		EXISTS (SELECT 1 FROM mutations u WHERE u.chat_key = m.chat_key AND u.target_id = m.id
			AND u.kind = 'revoke')
		FROM messages m WHERE chat_key = ? AND id = ?`, chatKey, id).
		Scan(&m.SenderKey, &m.SenderName, &fromMe, &m.TS, &m.Text, &m.MediaType, &m.FileName, &revoked)
	if errors.Is(err, sql.ErrNoRows) || revoked != 0 {
		return m, false, nil
	}
	if err != nil {
		return m, false, err
	}
	m.FromMe = fromMe != 0
	return m, true, nil
}

func (a *Archive) HasChunk(notificationID string) (bool, error) {
	var n int
	err := a.db.QueryRow(`SELECT COUNT(*) FROM sync_chunks WHERE notification_id = ?`, notificationID).Scan(&n)
	return n > 0, err
}

// Canonical returns the canonical key recorded for an alias, or the input.
func (a *Archive) Canonical(key string) (string, error) {
	var canon string
	err := a.db.QueryRow(`SELECT key FROM aliases WHERE alias = ?`, key).Scan(&canon)
	if err == sql.ErrNoRows {
		return key, nil
	}
	return canon, err
}

func mergeAlias(tx *sql.Tx, alias, key string) error {
	if alias == key || alias == "" || key == "" {
		return nil
	}
	if _, err := tx.Exec(`INSERT INTO aliases (alias, key) VALUES (?, ?)
		ON CONFLICT (alias) DO UPDATE SET key = excluded.key`, alias, key); err != nil {
		return err
	}
	// Move anything filed under the alias. Rows already present under the
	// canonical key win; duplicates under the alias are dropped.
	stmts := []string{
		`INSERT OR IGNORE INTO messages SELECT ?, id, sender_key, sender_name, from_me, ts, text, quoted_id, media_type, file_name, source FROM messages WHERE chat_key = ?`,
		`DELETE FROM messages WHERE chat_key = ?`,
		`INSERT OR IGNORE INTO mutations SELECT ?, target_id, event_id, kind, ts, text FROM mutations WHERE chat_key = ?`,
		`DELETE FROM mutations WHERE chat_key = ?`,
		`INSERT OR IGNORE INTO media_refs SELECT ?, id, kind, mimetype, ref FROM media_refs WHERE chat_key = ?`,
		`DELETE FROM media_refs WHERE chat_key = ?`,
		`UPDATE messages SET sender_key = ? WHERE sender_key = ?`,
		// Of two reactions by the same person to the same message, the newer wins.
		`INSERT INTO reactions (chat_key, target_id, sender_key, from_me, emoji, ts)
			SELECT ?, target_id, sender_key, from_me, emoji, ts FROM reactions WHERE chat_key = ? AND true
			ON CONFLICT (chat_key, target_id, sender_key) DO UPDATE SET
				from_me = excluded.from_me, emoji = excluded.emoji, ts = excluded.ts
			WHERE excluded.ts > reactions.ts`,
		`DELETE FROM reactions WHERE chat_key = ?`,
		`INSERT INTO reactions (chat_key, target_id, sender_key, from_me, emoji, ts)
			SELECT chat_key, target_id, ?, from_me, emoji, ts FROM reactions WHERE sender_key = ? AND true
			ON CONFLICT (chat_key, target_id, sender_key) DO UPDATE SET
				from_me = excluded.from_me, emoji = excluded.emoji, ts = excluded.ts
			WHERE excluded.ts > reactions.ts`,
		`DELETE FROM reactions WHERE sender_key = ?`,
		`INSERT OR IGNORE INTO receipts SELECT ?, message_id, reader_key, kind, ts FROM receipts WHERE chat_key = ?`,
		`DELETE FROM receipts WHERE chat_key = ?`,
		`INSERT OR IGNORE INTO receipts SELECT chat_key, message_id, ?, kind, ts FROM receipts WHERE reader_key = ?`,
		`DELETE FROM receipts WHERE reader_key = ?`,
		`INSERT INTO chat_reads (chat_key, watermark_ts)
			SELECT ?, watermark_ts FROM chat_reads WHERE chat_key = ? AND true
			ON CONFLICT (chat_key) DO UPDATE SET watermark_ts = MAX(chat_reads.watermark_ts, excluded.watermark_ts)`,
		`DELETE FROM chat_reads WHERE chat_key = ?`,
		`INSERT OR IGNORE INTO read_messages SELECT ?, id FROM read_messages WHERE chat_key = ?`,
		`DELETE FROM read_messages WHERE chat_key = ?`,
		`UPDATE api_ops SET chat_key = ? WHERE chat_key = ?`,
	}
	for i, s := range stmts {
		var err error
		switch strings.Count(s, "?") {
		case 2:
			_, err = tx.Exec(s, key, alias)
		default:
			_, err = tx.Exec(s, alias)
		}
		if err != nil {
			return fmt.Errorf("merge step %d: %w", i, err)
		}
	}
	if _, err := tx.Exec(`INSERT INTO chats (key, name, is_group, updated_at)
		SELECT ?, name, is_group, updated_at FROM chats WHERE key = ?
		ON CONFLICT (key) DO UPDATE SET
			name = CASE WHEN chats.name = '' THEN excluded.name ELSE chats.name END,
			updated_at = MAX(chats.updated_at, excluded.updated_at)`, key, alias); err != nil {
		return err
	}
	_, err := tx.Exec(`DELETE FROM chats WHERE key = ?`, alias)
	return err
}

// Unread is an incoming message the user has not read.
type Unread struct {
	ID        string
	SenderKey string
	TS        int64
}

// UnreadIncoming lists the chat's incoming messages above its watermark that
// are not marked read, oldest first.
func (a *Archive) UnreadIncoming(chatKey string) ([]Unread, error) {
	rows, err := a.db.Query(`SELECT id, sender_key, ts FROM messages m
		WHERE chat_key = ? AND from_me = 0
		AND ts > COALESCE((SELECT watermark_ts FROM chat_reads WHERE chat_key = m.chat_key), 0)
		AND NOT EXISTS (SELECT 1 FROM read_messages r WHERE r.chat_key = m.chat_key AND r.id = m.id)
		ORDER BY ts, id`, chatKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Unread
	for rows.Next() {
		var u Unread
		if err := rows.Scan(&u.ID, &u.SenderKey, &u.TS); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// ClaimOp journals O as pending unless an op with its id already exists, in
// which case it returns that one and false. The insert is atomic, so two
// requests with the same op id cannot both act.
func (a *Archive) ClaimOp(o Op) (Op, bool, error) {
	res, err := a.db.Exec(`INSERT INTO api_ops (op_id, method, chat_key, message_id, state, error, ts)
		VALUES (?, ?, ?, ?, 'pending', '', ?) ON CONFLICT (op_id) DO NOTHING`,
		o.ID, o.Method, o.ChatKey, o.MessageID, o.TS)
	if err != nil {
		return Op{}, false, err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		o.State = "pending"
		return o, true, nil
	}
	prev, _, err := a.GetOp(o.ID)
	return prev, false, err
}

// GetOp returns the journaled op ID and whether it exists.
func (a *Archive) GetOp(id string) (Op, bool, error) {
	o := Op{ID: id}
	err := a.db.QueryRow(`SELECT method, chat_key, message_id, state, error, ts FROM api_ops WHERE op_id = ?`, id).
		Scan(&o.Method, &o.ChatKey, &o.MessageID, &o.State, &o.Error, &o.TS)
	if errors.Is(err, sql.ErrNoRows) {
		return o, false, nil
	}
	return o, err == nil, err
}

// RecordOp writes O to the journal on its own: inserting it when it is new,
// otherwise updating its outcome.
func (a *Archive) RecordOp(o Op) error {
	tx, err := a.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := updateOp(tx, o); err != nil {
		return err
	}
	return tx.Commit()
}

func updateOp(tx *sql.Tx, o Op) error {
	_, err := tx.Exec(`INSERT INTO api_ops (op_id, method, chat_key, message_id, state, error, ts)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (op_id) DO UPDATE SET message_id = excluded.message_id, state = excluded.state,
			error = excluded.error, ts = excluded.ts`,
		o.ID, o.Method, o.ChatKey, o.MessageID, o.State, o.Error, o.TS)
	if err != nil {
		return fmt.Errorf("op %s: %w", o.ID, err)
	}
	return nil
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
