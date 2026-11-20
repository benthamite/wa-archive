package main

// Event handling: turning whatsmeow events into archive batches.

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type handler struct {
	// cli is used only for what the daemon does with WhatsApp as part of
	// event handling (history downloads and receipts); everything else goes
	// through wa, which tests replace with a fake.
	cli   *whatsmeow.Client
	wa    waClient
	arc   *Archive
	paths paths
	ctx   context.Context
	// noGroupInfo holds groups whose info WhatsApp refused (e.g. groups the
	// user left), so the periodic backfill does not ask again until a restart.
	noGroupInfo groupFailures
	// groupInfoRefused holds groups whose info WhatsApp refused when
	// addressing their members (send.go, groupInfo), for a while.
	groupInfoRefused groupFailures
	// backfillMu serializes backfillNames, which runs both on connecting
	// (the event goroutine) and from the main loop's ticker.
	backfillMu sync.Mutex
	// exit ends the daemon (os.Exit unless a test replaces it).
	exit func(code int)
	// presence is the presence the API last sent (empty if none), guarded by
	// presenceMu (withdrawPresence).
	presenceMu sync.Mutex
	presence   types.Presence
	// events, when set, sends an event to every API client.
	events func(v any)
	// alert is the command that sends its stdin as an immediate alert to
	// the user (set by run from WA_ARCHIVE_ALERT; tests use a fake).
	alert []string
}

// die exits with CODE. In tests exit returns, so callers return after it.
func (h *handler) die(code int) {
	if h.exit != nil {
		h.exit(code)
		return
	}
	os.Exit(code)
}

// handle returns false when an event could not be committed. With
// SynchronousAck that withholds the acknowledgement, so WhatsApp redelivers
// the message; the daemon then exits so launchd restarts it cleanly.
func (h *handler) handle(evt any) bool {
	var err error
	switch e := evt.(type) {
	case *events.Message:
		if n := e.Message.GetProtocolMessage().GetHistorySyncNotification(); n != nil && e.Info.IsFromMe {
			err = h.handleHistoryNotification(e.Info.ID, n)
		} else {
			err = h.commitMessage(e, "live")
		}
	case *events.Receipt:
		err = h.commitBatch(h.receiptBatch(e))
	case *events.MarkChatAsRead:
		err = h.commitBatch(h.markChatAsReadBatch(e))
	case *events.GroupInfo:
		if e.Name != nil {
			err = h.commitChat(e.JID, e.Name.Name, true, e.Timestamp)
		}
	case *events.Connected:
		h.beat("connected")
		h.announceStatus(true, true)
		err = h.backfillNames()
	case *events.Disconnected:
		h.announceStatus(false, h.wa != nil && h.wa.IsLoggedIn())
	case *events.ClientOutdated:
		h.clientOutdated()
		return true
	case *events.LoggedOut:
		// Unlinked from the phone or by the server. Leave a persistent marker
		// and exit 0 so the service manager does not restart into a loop; the
		// heartbeat then goes stale, which an external monitor can alert on.
		_ = touch(h.paths.unlinked())
		h.announceStatus(false, false)
		fmt.Fprintf(os.Stderr, "wa-archive: logged out (reason %v)\n", e.Reason)
		h.die(exitUnlinked)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "wa-archive: failed to archive %T: %v\n", evt, err)
		h.die(exitFailure)
		return false
	}
	return true
}

func (h *handler) beat(event string) {
	if err := writeHeartbeat(h.paths.heartbeat(), event); err != nil {
		fmt.Fprintf(os.Stderr, "wa-archive: heartbeat: %v\n", err)
	}
}

func (h *handler) commitMessage(e *events.Message, source string) error {
	b, err := h.messageBatch(e, source)
	if err != nil || b == nil {
		return err
	}
	if err := h.arc.Commit(*b); err != nil {
		return err
	}
	h.beat("write")
	if source == "live" {
		for _, m := range b.Messages {
			if autoFetched(m.MediaType) && len(m.MediaRef) > 0 {
				if err := h.paths.queueFetch(m.ChatKey, m.ID); err != nil {
					fmt.Fprintf(os.Stderr, "wa-archive: queue fetch of %s/%s: %v\n", m.ChatKey, m.ID, err)
				}
			}
		}
	}
	return nil
}

// commitBatch commits B, if any.
func (h *handler) commitBatch(b *Batch) error {
	if b == nil {
		return nil
	}
	return h.arc.Commit(*b)
}

// selfKey is the key under which the user's own reactions are filed.
func (h *handler) selfKey() string {
	if h.wa == nil {
		return ""
	}
	return h.wa.OwnID().String()
}

// isSelfChat reports whether KEY is the user's chat with themselves.
func (h *handler) isSelfChat(key string) bool {
	if h.wa == nil {
		return false
	}
	own, lid := h.wa.OwnID(), h.wa.OwnLID()
	return (!own.IsEmpty() && key == own.String()) || (!lid.IsEmpty() && key == lid.String())
}

// receiptKinds maps the receipts other people send for the user's messages to
// what the archive records. Others (inactive, retry, sender, ...) are
// protocol plumbing.
var receiptKinds = map[types.ReceiptType]string{
	types.ReceiptTypeDelivered: "delivered",
	types.ReceiptTypeRead:      "read",
	types.ReceiptTypePlayed:    "played",
}

// receiptBatch records a receipt. From someone else it says they received,
// read or played one of the user's messages. From the user's own account it
// says they read incoming messages on another device: "read" when their read
// receipts are on, "read-self" when they are off.
func (h *handler) receiptBatch(e *events.Receipt) *Batch {
	if len(e.MessageIDs) == 0 {
		return nil
	}
	alt := types.EmptyJID
	if !e.IsGroup && !e.IsFromMe {
		alt = e.SenderAlt
	}
	chatKey, aliases := h.canonical(e.Chat, alt)
	b := &Batch{Aliases: aliases}
	if e.IsFromMe {
		if e.Type != types.ReceiptTypeRead && e.Type != types.ReceiptTypeReadSelf {
			return nil
		}
		if h.isSelfChat(chatKey) {
			return nil
		}
		for _, id := range e.MessageIDs {
			b.Reads = append(b.Reads, ReadMark{ChatKey: chatKey, ID: id})
		}
		return b
	}
	kind, ok := receiptKinds[e.Type]
	if !ok {
		return nil
	}
	readerKey, readerAliases := h.canonical(e.Sender, e.SenderAlt)
	for k, v := range readerAliases {
		b.Aliases[k] = v
	}
	if readerKey == "" {
		return nil
	}
	for _, id := range e.MessageIDs {
		b.Receipts = append(b.Receipts, ReceiptRow{ChatKey: chatKey, MessageID: id, ReaderKey: readerKey,
			Kind: kind, TS: e.Timestamp.Unix()})
	}
	return b
}

// markChatAsReadBatch advances a chat's watermark when the user marks it read
// on another device (an app-state action). Marking a chat unread is not
// recorded: unread state is per message here.
func (h *handler) markChatAsReadBatch(e *events.MarkChatAsRead) *Batch {
	if !e.Action.GetRead() || e.JID.IsEmpty() {
		return nil
	}
	chatKey, aliases := h.canonical(e.JID, types.EmptyJID)
	return &Batch{Aliases: aliases, Watermarks: []Watermark{{ChatKey: chatKey, TS: e.Timestamp.Unix()}}}
}

// reactionRow is the archive row for a reaction by SENDERKEY (the user
// when FROMME) to message TARGET. Its time is the sender's own, in seconds, which
// orders a person's successive reactions however they are delivered.
func (h *handler) reactionRow(chatKey, senderKey string, fromMe bool, target, emoji string, senderMS int64, ts time.Time) Reaction {
	if fromMe {
		if self := h.selfKey(); self != "" {
			senderKey = self
		}
	}
	t := ts.Unix()
	if senderMS > 0 {
		t = senderMS / 1000
	}
	return Reaction{ChatKey: chatKey, TargetID: target, SenderKey: senderKey, FromMe: fromMe, Emoji: emoji, TS: t}
}

func (h *handler) commitChat(jid types.JID, name string, isGroup bool, ts time.Time) error {
	key, aliases := h.canonical(jid, types.EmptyJID)
	return h.arc.Commit(Batch{Aliases: aliases, Chats: []Chat{{Key: key, Name: name, IsGroup: isGroup, UpdatedAt: ts.Unix()}}})
}

// canonical normalizes a JID (and an optional alternate address for the same
// person) to one key: the phone-number JID when the LID store maps the LID to
// one, otherwise the JID itself. It returns any alias it learned.
func (h *handler) canonical(jid, alt types.JID) (string, map[string]string) {
	if jid.IsEmpty() {
		return "", nil
	}
	jid = jid.ToNonAD()
	alt = alt.ToNonAD()
	aliases := map[string]string{}
	if jid.Server == types.HiddenUserServer {
		pn := alt
		if pn.Server != types.DefaultUserServer {
			pn = types.EmptyJID
			if h.wa != nil {
				if found, err := h.wa.PNForLID(h.ctx, jid); err == nil && !found.IsEmpty() {
					pn = found.ToNonAD()
				}
			}
		}
		if !pn.IsEmpty() {
			aliases[jid.String()] = pn.String()
			return pn.String(), aliases
		}
	} else if jid.Server == types.DefaultUserServer && alt.Server == types.HiddenUserServer {
		aliases[alt.String()] = jid.String()
	}
	key, err := h.arc.Canonical(jid.String())
	if err != nil {
		key = jid.String()
	}
	return key, aliases
}

func (h *handler) messageBatch(e *events.Message, source string) (*Batch, error) {
	info := e.Info
	chatAlt := types.EmptyJID
	if !info.IsGroup {
		if info.IsFromMe {
			chatAlt = info.RecipientAlt
		} else {
			chatAlt = info.SenderAlt
		}
	}
	chatKey, aliases := h.canonical(info.Chat, chatAlt)
	senderKey, senderAliases := h.canonical(info.Sender, info.SenderAlt)
	for k, v := range senderAliases {
		aliases[k] = v
	}
	b := &Batch{Aliases: aliases}
	b.Chats = append(b.Chats, Chat{Key: chatKey, IsGroup: info.IsGroup, UpdatedAt: info.Timestamp.Unix()})
	if !info.IsGroup {
		b.Chats[0].Name = h.contactName(chatKey)
		if b.Chats[0].Name == "" && !info.IsFromMe {
			b.Chats[0].Name = info.PushName
		}
	}
	msg := e.Message
	if rm := msg.GetReactionMessage(); rm != nil {
		if rm.GetKey().GetID() == "" {
			return nil, nil
		}
		// A reaction does not move its chat up the list.
		b.Chats[0].UpdatedAt = 0
		b.Reactions = append(b.Reactions, h.reactionRow(chatKey, senderKey, info.IsFromMe,
			rm.GetKey().GetID(), rm.GetText(), rm.GetSenderTimestampMS(), info.Timestamp))
		return b, nil
	}
	if pm := msg.GetProtocolMessage(); pm != nil {
		target := pm.GetKey().GetID()
		switch pm.GetType() {
		case waE2E.ProtocolMessage_MESSAGE_EDIT:
			b.Mutations = append(b.Mutations, Mutation{ChatKey: chatKey, TargetID: target, EventID: info.ID,
				Kind: "edit", TS: info.Timestamp.Unix(), Text: messageText(pm.GetEditedMessage())})
		case waE2E.ProtocolMessage_REVOKE:
			b.Mutations = append(b.Mutations, Mutation{ChatKey: chatKey, TargetID: target, EventID: info.ID,
				Kind: "revoke", TS: info.Timestamp.Unix()})
		default:
			// Other protocol messages (key shares, app state, etc.) carry no content.
			return nil, nil
		}
		return b, nil
	}
	text := messageText(msg)
	mediaType, fileName := mediaInfo(msg)
	if text == "" && mediaType == "" {
		// Store anything else that carries content under its protobuf field
		// name, so an unsupported type is visible rather than silently lost.
		kind := contentKind(msg)
		if kind == "" {
			return nil, nil
		}
		mediaType = kind
	}
	ref, mimetype := mediaRef(msg)
	b.Messages = append(b.Messages, Message{
		ChatKey: chatKey, ID: info.ID, SenderKey: senderKey, SenderName: info.PushName,
		FromMe: info.IsFromMe, TS: info.Timestamp.Unix(), Text: text,
		QuotedID: quotedID(msg), MediaType: mediaType, FileName: fileName, Source: source,
		MediaRef: ref, MediaMime: mimetype,
	})
	return b, nil
}

// handleHistoryNotification downloads one history-sync chunk, commits it with
// its sync_chunks row in a single transaction, and only then sends the
// history receipt and deletes the blob from the server. Any failure returns
// an error before either, so the notification is redelivered.
func (h *handler) handleHistoryNotification(id types.MessageID, notif *waE2E.HistorySyncNotification) error {
	done, err := h.arc.HasChunk(id)
	if err != nil {
		return err
	}
	if !done {
		hs, err := h.cli.DownloadHistorySync(h.ctx, notif, true)
		if err != nil {
			return fmt.Errorf("download history: %w", err)
		}
		b, err := h.historyBatch(id, hs)
		if err != nil {
			return err
		}
		if err := h.arc.Commit(*b); err != nil {
			return err
		}
		h.beat("write")
	}
	if err := h.cli.SendProtocolMessageReceipt(h.ctx, id, types.ReceiptTypeHistorySync); err != nil {
		return fmt.Errorf("history receipt: %w", err)
	}
	if notif.GetDirectPath() != "" {
		if err := h.cli.DeleteMedia(h.ctx, whatsmeow.MediaHistory, notif.GetDirectPath(), notif.GetFileEncSHA256(), notif.GetEncHandle()); err != nil {
			// The chunk is safely committed; a leftover blob only costs server space.
			fmt.Fprintf(os.Stderr, "wa-archive: delete history blob: %v\n", err)
		}
	}
	return nil
}

func (h *handler) historyBatch(id types.MessageID, hs *waHistorySync.HistorySync) (*Batch, error) {
	b := &Batch{Aliases: map[string]string{}}
	count := 0
	for _, conv := range hs.GetConversations() {
		chatJID, err := types.ParseJID(conv.GetID())
		if err != nil {
			continue
		}
		var alt types.JID
		if pn := conv.GetPnJID(); pn != "" {
			alt, _ = types.ParseJID(pn)
		}
		chatKey, aliases := h.canonical(chatJID, alt)
		for k, v := range aliases {
			b.Aliases[k] = v
		}
		name := conv.GetName()
		if name == "" {
			name = conv.GetDisplayName()
		}
		if name == "" && chatJID.Server != types.GroupServer {
			name = h.contactName(chatKey)
		}
		b.Chats = append(b.Chats, Chat{Key: chatKey, Name: name, IsGroup: chatJID.Server == types.GroupServer,
			UpdatedAt: int64(conv.GetConversationTimestamp())})
		// A conversation the phone shows as fully read counts as read up to
		// its newest message here too, so history never leads wsp to send
		// receipts for messages the user has already read.
		if conv.GetUnreadCount() == 0 && !conv.GetMarkedAsUnread() {
			var newest int64
			for _, hm := range conv.GetMessages() {
				newest = max(newest, int64(hm.GetMessage().GetMessageTimestamp()))
			}
			if newest > 0 {
				b.Watermarks = append(b.Watermarks, Watermark{ChatKey: chatKey, TS: newest})
			}
		}
		// The incoming messages this conversation adds, for its unread count.
		var incoming []Message
		keep := func(ms []Message) {
			for _, m := range ms {
				if !m.FromMe && m.ChatKey == chatKey {
					incoming = append(incoming, m)
				}
			}
		}
		for _, hm := range conv.GetMessages() {
			reactions, receipts, aliases := h.webAnnotations(chatJID, chatKey, hm.GetMessage())
			b.Reactions = append(b.Reactions, reactions...)
			b.Receipts = append(b.Receipts, receipts...)
			for k, v := range aliases {
				b.Aliases[k] = v
			}
			if wm := hm.GetMessage(); wm.GetMessageStubType() == waWeb.WebMessageInfo_REVOKE {
				if mb := h.revokedStub(chatJID, wm); mb != nil {
					for k, v := range mb.Aliases {
						b.Aliases[k] = v
					}
					b.Messages = append(b.Messages, mb.Messages...)
					b.Mutations = append(b.Mutations, mb.Mutations...)
					keep(mb.Messages)
				}
				continue
			}
			e, err := h.cli.ParseWebMessage(chatJID, hm.GetMessage())
			if err != nil {
				continue
			}
			mb, err := h.messageBatch(e, "history")
			if err != nil {
				return nil, err
			}
			if mb == nil {
				continue
			}
			for k, v := range mb.Aliases {
				b.Aliases[k] = v
			}
			b.Messages = append(b.Messages, mb.Messages...)
			b.Mutations = append(b.Mutations, mb.Mutations...)
			b.Reactions = append(b.Reactions, mb.Reactions...)
			keep(mb.Messages)
			count += len(mb.Messages)
		}
		unread := int(conv.GetUnreadCount())
		if unread == 0 && conv.GetMarkedAsUnread() {
			// Marked unread with no count: the phone shows the chat as unread,
			// which the newest incoming message stands for here.
			unread = 1
		}
		if r, ok := initialRead(chatKey, incoming, unread); ok {
			b.InitialReads = append(b.InitialReads, r)
		}
	}
	b.Chunk = &Chunk{NotificationID: id, SyncType: hs.GetSyncType().String(),
		ChunkOrder: int(hs.GetChunkOrder()), Progress: int(hs.GetProgress()),
		Conversations: len(hs.GetConversations()), Messages: count, ReceivedAt: time.Now().Unix()}
	return b, nil
}

// initialRead is the read state of a chat whose newest UNREAD incoming
// messages (of INCOMING) are unread on the phone and the rest read: the
// watermark falls just below the oldest unread one, and older messages of
// that same second are marked read by id. Messages are ordered as
// UnreadIncoming orders them (time, then id). Without unread messages, or
// without incoming ones, there is nothing to set (a read conversation gets
// a plain watermark instead).
func initialRead(chatKey string, incoming []Message, unread int) (InitialRead, bool) {
	if unread <= 0 || len(incoming) == 0 {
		return InitialRead{}, false
	}
	sort.Slice(incoming, func(i, j int) bool {
		if incoming[i].TS != incoming[j].TS {
			return incoming[i].TS < incoming[j].TS
		}
		return incoming[i].ID < incoming[j].ID
	})
	first := max(len(incoming)-unread, 0)
	edge := incoming[first].TS
	r := InitialRead{ChatKey: chatKey, WatermarkTS: edge - 1}
	for _, m := range incoming[:first] {
		if m.TS == edge {
			r.ReadIDs = append(r.ReadIDs, m.ID)
		}
	}
	return r, true
}

// webAnnotations returns what a history entry records about message WM
// beyond its content: the reactions to it, and, for the user's own messages, who
// received, read or played it.
func (h *handler) webAnnotations(chatJID types.JID, chatKey string, wm *waWeb.WebMessageInfo) ([]Reaction, []ReceiptRow, map[string]string) {
	target := wm.GetKey().GetID()
	if target == "" {
		return nil, nil, nil
	}
	aliases := map[string]string{}
	person := func(s string) string {
		jid, err := types.ParseJID(s)
		if err != nil || jid.User == "" {
			return ""
		}
		key, a := h.canonical(jid, types.EmptyJID)
		for k, v := range a {
			aliases[k] = v
		}
		return key
	}
	var reactions []Reaction
	for _, r := range wm.GetReactions() {
		k := r.GetKey()
		sender := ""
		switch {
		case k.GetFromMe():
		case k.GetParticipant() != "":
			sender = person(k.GetParticipant())
		case chatJID.Server != types.GroupServer:
			sender = chatKey
		}
		if sender == "" && !k.GetFromMe() {
			continue
		}
		reactions = append(reactions, h.reactionRow(chatKey, sender, k.GetFromMe(), target, r.GetText(),
			r.GetSenderTimestampMS(), time.Unix(int64(wm.GetMessageTimestamp()), 0)))
	}
	var receipts []ReceiptRow
	if wm.GetKey().GetFromMe() {
		for _, ur := range wm.GetUserReceipt() {
			reader := person(ur.GetUserJID())
			if reader == "" {
				continue
			}
			for kind, ts := range map[string]int64{"delivered": ur.GetReceiptTimestamp(),
				"read": ur.GetReadTimestamp(), "played": ur.GetPlayedTimestamp()} {
				if ts > 0 {
					receipts = append(receipts, ReceiptRow{ChatKey: chatKey, MessageID: target, ReaderKey: reader, Kind: kind, TS: ts})
				}
			}
		}
	}
	return reactions, receipts, aliases
}

// contactName returns the best name the contact store has for a direct chat:
// the address-book name, then the business name, then the push name.
func (h *handler) contactName(key string) string {
	jid, err := types.ParseJID(key)
	if err != nil || h.wa == nil {
		return ""
	}
	c, err := h.wa.Contact(h.ctx, jid)
	if err != nil || !c.Found {
		return ""
	}
	for _, name := range []string{c.FullName, c.FirstName, c.BusinessName, c.PushName} {
		if name != "" && !strings.Contains(name, "∙") {
			return name
		}
	}
	return ""
}

// backfillNames names direct chats archived before their contact was known.
func (h *handler) backfillNames() error {
	h.backfillMu.Lock()
	defer h.backfillMu.Unlock()
	keys, err := h.arc.UnnamedDirectChats()
	if err != nil {
		return err
	}
	var chats []Chat
	for _, k := range keys {
		name := h.contactName(k)
		if name == "" {
			if name, err = h.arc.LatestPushName(k); err != nil {
				return err
			}
		}
		if name != "" && !strings.Contains(name, "∙") {
			chats = append(chats, Chat{Key: k, Name: name})
		}
	}
	groups, err := h.groupNames()
	if err != nil {
		return err
	}
	chats = append(chats, groups...)
	if len(chats) == 0 {
		return nil
	}
	return h.arc.Commit(Batch{Chats: chats})
}

// groupNames asks WhatsApp for the subject of every unnamed group. A group
// joined or created after linking gets its name no other way unless it is
// renamed (an unnamed group shows only as an id, which makes it easy to
// confuse with another).
func (h *handler) groupNames() ([]Chat, error) {
	keys, err := h.arc.UnnamedGroups()
	if err != nil || h.wa == nil {
		return nil, err
	}
	var chats []Chat
	for _, k := range keys {
		jid, err := types.ParseJID(k)
		// Broadcast lists are stored as groups but have no group info.
		if err != nil || jid.Server != types.GroupServer {
			continue
		}
		if _, failed := h.noGroupInfo.failedAt(k); failed {
			continue
		}
		info, err := h.wa.GroupInfo(h.ctx, jid)
		if err != nil || info.Name == "" {
			fmt.Fprintf(os.Stderr, "wa-archive: no name for group %s: %v\n", k, err)
			h.noGroupInfo.record(k, time.Now())
			continue
		}
		chats = append(chats, Chat{Key: k, Name: info.Name, IsGroup: true})
	}
	return chats, nil
}

// groupFailures remembers when WhatsApp last refused each group's info. It
// is shared by the backfill and the API's goroutines, hence the lock.
type groupFailures struct {
	mu sync.Mutex
	at map[string]time.Time
}

func (g *groupFailures) record(key string, t time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.at == nil {
		g.at = map[string]time.Time{}
	}
	g.at[key] = t
}

func (g *groupFailures) forget(key string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.at, key)
}

// failedAt returns when KEY's info was last refused, if it was.
func (g *groupFailures) failedAt(key string) (time.Time, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	t, ok := g.at[key]
	return t, ok
}

// revokedStub turns a history entry for a message deleted before linking into
// a placeholder row plus a revoke, so readers can show "[deleted]" in its place.
func (h *handler) revokedStub(chatJID types.JID, wm *waWeb.WebMessageInfo) *Batch {
	key := wm.GetKey()
	if key.GetID() == "" {
		return nil
	}
	chatKey, aliases := h.canonical(chatJID, types.EmptyJID)
	senderKey := ""
	if p := key.GetParticipant(); p != "" {
		if jid, err := types.ParseJID(p); err == nil {
			senderKey, _ = h.canonical(jid, types.EmptyJID)
		}
	} else if !key.GetFromMe() {
		senderKey = chatKey
	}
	ts := int64(wm.GetMessageTimestamp())
	return &Batch{
		Aliases: aliases,
		Messages: []Message{{ChatKey: chatKey, ID: key.GetID(), SenderKey: senderKey, FromMe: key.GetFromMe(),
			TS: ts, MediaType: "deleted", Source: "history"}},
		Mutations: []Mutation{{ChatKey: chatKey, TargetID: key.GetID(), EventID: "history-revoke", Kind: "revoke", TS: ts}},
	}
}

func messageText(m *waE2E.Message) string {
	switch {
	case m == nil:
		return ""
	case m.GetConversation() != "":
		return m.GetConversation()
	case m.GetExtendedTextMessage().GetText() != "":
		return m.GetExtendedTextMessage().GetText()
	case m.GetImageMessage() != nil:
		return m.GetImageMessage().GetCaption()
	case m.GetVideoMessage() != nil:
		return m.GetVideoMessage().GetCaption()
	case m.GetDocumentMessage() != nil:
		return m.GetDocumentMessage().GetCaption()
	case m.GetContactMessage() != nil:
		return "[contact] " + m.GetContactMessage().GetDisplayName()
	case m.GetLocationMessage() != nil:
		l := m.GetLocationMessage()
		return "[location] " + strconv.FormatFloat(l.GetDegreesLatitude(), 'f', 5, 64) + "," +
			strconv.FormatFloat(l.GetDegreesLongitude(), 'f', 5, 64) + " " + l.GetName()
	case m.GetPollCreationMessage() != nil:
		return "[poll] " + m.GetPollCreationMessage().GetName()
	case m.GetGroupInviteMessage() != nil:
		g := m.GetGroupInviteMessage()
		return strings.TrimSpace("[group invite] " + g.GetGroupName() + " " + g.GetCaption())
	}
	return ""
}

func mediaInfo(m *waE2E.Message) (string, string) {
	switch {
	case m.GetImageMessage() != nil:
		return "image", ""
	case m.GetVideoMessage() != nil:
		return "video", ""
	case m.GetAudioMessage() != nil:
		if m.GetAudioMessage().GetPTT() {
			return "voice", ""
		}
		return "audio", ""
	case m.GetDocumentMessage() != nil:
		return "document", m.GetDocumentMessage().GetFileName()
	case m.GetStickerMessage() != nil:
		return "sticker", ""
	}
	return "", ""
}

// nonContentFields are message fields that carry no user content: protocol
// plumbing, and reactions and poll votes, which annotate other messages.
var nonContentFields = map[string]bool{
	"messageContextInfo": true, "senderKeyDistributionMessage": true,
	"protocolMessage": true, "reactionMessage": true, "encReactionMessage": true,
	"pollUpdateMessage": true, "keepInChatMessage": true, "pinInChatMessage": true,
	"encEventResponseMessage": true, "secretEncryptedMessage": true,
}

// contentKind names the first content-bearing field of an otherwise
// unrecognised message, or "" if it has none.
func contentKind(m *waE2E.Message) string {
	if m == nil {
		return ""
	}
	kind := ""
	m.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		if name := fd.JSONName(); !nonContentFields[name] {
			kind = name
			return false
		}
		return true
	})
	return kind
}

func quotedID(m *waE2E.Message) string {
	if ci := m.GetExtendedTextMessage().GetContextInfo(); ci != nil {
		return ci.GetStanzaID()
	}
	return ""
}
