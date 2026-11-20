package main

// The seam between the daemon and whatsmeow's client. Everything the daemon
// sends to WhatsApp, or asks of it outside event handling (uploads,
// downloads, group and contact lookups, privacy settings), goes through
// waClient, so that tests drive the handler and the API with a fake and never
// connect to WhatsApp. liveClient is the only code that calls whatsmeow's
// sending methods (TestNoUnaskedSending checks this).

import (
	"context"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
)

type waClient interface {
	IsConnected() bool
	IsLoggedIn() bool
	// OwnID is the user's phone-number JID without a device part, and OwnLID
	// their LID; either is empty when not logged in.
	OwnID() types.JID
	OwnLID() types.JID
	GenerateMessageID() types.MessageID
	// SendMessage sends MSG under message id ID, generated beforehand so the
	// operation can be journaled before anything goes out.
	SendMessage(ctx context.Context, to types.JID, msg *waE2E.Message, id types.MessageID) (whatsmeow.SendResponse, error)
	// MarkRead sends a receipt of type TYP (read or read-self), always
	// explicit: left to itself, whatsmeow sends "read" (blue ticks) when it
	// cannot fetch the user's privacy settings.
	MarkRead(ctx context.Context, ids []types.MessageID, ts time.Time, chat, sender types.JID, typ types.ReceiptType) error
	SendPresence(ctx context.Context, state types.Presence) error
	SendChatPresence(ctx context.Context, chat types.JID, state types.ChatPresence) error
	Upload(ctx context.Context, data []byte, kind whatsmeow.MediaType) (whatsmeow.UploadResponse, error)
	Download(ctx context.Context, part whatsmeow.DownloadableMessage) ([]byte, error)
	GroupInfo(ctx context.Context, jid types.JID) (*types.GroupInfo, error)
	PNForLID(ctx context.Context, lid types.JID) (types.JID, error)
	LIDForPN(ctx context.Context, pn types.JID) (types.JID, error)
	Contact(ctx context.Context, jid types.JID) (types.ContactInfo, error)
	// ReadReceipts is the user's read-receipts privacy setting: "all", "none",
	// or "unknown" when it cannot be read.
	ReadReceipts(ctx context.Context) string
}

// liveClient is waClient over a connected whatsmeow client.
type liveClient struct {
	cli *whatsmeow.Client
}

func (c liveClient) IsConnected() bool { return c.cli.IsConnected() }
func (c liveClient) IsLoggedIn() bool  { return c.cli.IsLoggedIn() }

func (c liveClient) OwnID() types.JID {
	if c.cli.Store.ID == nil {
		return types.EmptyJID
	}
	return c.cli.Store.ID.ToNonAD()
}

func (c liveClient) OwnLID() types.JID { return c.cli.Store.GetLID().ToNonAD() }

func (c liveClient) GenerateMessageID() types.MessageID { return c.cli.GenerateMessageID() }

func (c liveClient) SendMessage(ctx context.Context, to types.JID, msg *waE2E.Message, id types.MessageID) (whatsmeow.SendResponse, error) {
	return c.cli.SendMessage(ctx, to, msg, whatsmeow.SendRequestExtra{ID: id})
}

func (c liveClient) MarkRead(ctx context.Context, ids []types.MessageID, ts time.Time, chat, sender types.JID, typ types.ReceiptType) error {
	return c.cli.MarkRead(ctx, ids, ts, chat, sender, typ)
}

func (c liveClient) SendPresence(ctx context.Context, state types.Presence) error {
	return c.cli.SendPresence(ctx, state)
}

func (c liveClient) SendChatPresence(ctx context.Context, chat types.JID, state types.ChatPresence) error {
	return c.cli.SendChatPresence(ctx, chat, state, types.ChatPresenceMediaText)
}

func (c liveClient) Upload(ctx context.Context, data []byte, kind whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
	return c.cli.Upload(ctx, data, kind)
}

func (c liveClient) Download(ctx context.Context, part whatsmeow.DownloadableMessage) ([]byte, error) {
	return c.cli.Download(ctx, part)
}

func (c liveClient) GroupInfo(ctx context.Context, jid types.JID) (*types.GroupInfo, error) {
	return c.cli.GetGroupInfo(ctx, jid)
}

func (c liveClient) PNForLID(ctx context.Context, lid types.JID) (types.JID, error) {
	return c.cli.Store.LIDs.GetPNForLID(ctx, lid)
}

func (c liveClient) LIDForPN(ctx context.Context, pn types.JID) (types.JID, error) {
	return c.cli.Store.LIDs.GetLIDForPN(ctx, pn)
}

func (c liveClient) Contact(ctx context.Context, jid types.JID) (types.ContactInfo, error) {
	return c.cli.Store.Contacts.GetContact(ctx, jid)
}

// ReadReceipts reads the setting with a query that changes nothing (or from
// whatsmeow's cache, filled by an earlier query).
func (c liveClient) ReadReceipts(ctx context.Context) string {
	if !c.cli.IsConnected() {
		return "unknown"
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	s, err := c.cli.TryFetchPrivacySettings(ctx, false)
	if err != nil || s == nil {
		return "unknown"
	}
	switch s.ReadReceipts {
	case types.PrivacySettingAll:
		return "all"
	case types.PrivacySettingNone:
		return "none"
	}
	return "unknown"
}
