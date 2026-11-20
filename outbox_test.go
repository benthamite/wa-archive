package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

func TestJobWithInvalidRecipientFailsWithReceipt(t *testing.T) {
	dir := t.TempDir()
	h := &handler{paths: paths{data: dir, state: dir}}
	if err := os.MkdirAll(h.paths.outbox(), 0o700); err != nil {
		t.Fatal(err)
	}
	job := filepath.Join(h.paths.outbox(), "1.json")
	data, _ := json.Marshal(Job{To: "not-a-jid", Text: "hola"})
	if err := os.WriteFile(job, data, 0o600); err != nil {
		t.Fatal(err)
	}
	h.processOutbox()
	if _, err := os.Stat(job); !os.IsNotExist(err) {
		t.Fatal("job was not removed from the outbox")
	}
	var r Receipt
	raw, err := os.ReadFile(filepath.Join(h.paths.outbox(), "failed", "1.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &r); err != nil || r.Status != "failed" || r.Error == "" || r.Text != "hola" {
		t.Fatalf("unexpected receipt %+v (%v)", r, err)
	}
}

func TestTextJobBuildsAConversationMessage(t *testing.T) {
	msg, kind, name, err := (&handler{}).buildMessage(Job{To: "1@s.whatsapp.net", Text: "hola"})
	if err != nil || msg.GetConversation() != "hola" || kind != "" || name != "" {
		t.Fatalf("got %v %q %q %v", msg, kind, name, err)
	}
}

func TestRelativeAttachmentIsRejected(t *testing.T) {
	if _, _, _, err := (&handler{}).buildMessage(Job{Text: "x", File: "a.pdf"}); err == nil {
		t.Fatal("relative attachment path accepted")
	}
}

func TestMediaRefKeepsDocumentsAndSkipsStickers(t *testing.T) {
	doc := &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{Mimetype: proto.String("application/pdf"), DirectPath: proto.String("/x")}}
	ref, mimetype := mediaRef(doc)
	if len(ref) == 0 || mimetype != "application/pdf" {
		t.Fatalf("document ref %v %q", ref, mimetype)
	}
	if ref, _ := mediaRef(&waE2E.Message{StickerMessage: &waE2E.StickerMessage{}}); ref != nil {
		t.Fatal("sticker kept")
	}
}

func TestFetchWithoutReferenceFails(t *testing.T) {
	a, _ := openTest(t)
	dir := t.TempDir()
	h := &handler{arc: a, paths: paths{data: dir, state: dir}}
	if _, err := h.fetch(FetchRequest{Chat: "1@s.whatsapp.net", ID: "nope"}); err == nil {
		t.Fatal("fetch without a reference succeeded")
	}
}

func TestQuotedTextBecomesAnExtendedTextReply(t *testing.T) {
	author := types.NewJID("15550000003", types.DefaultUserServer)
	ci := quoteContext("ABC", author, Message{Text: "Can you send me the details?"})
	msg := withQuote(&waE2E.Message{Conversation: proto.String("Dale")}, ci)
	ext := msg.GetExtendedTextMessage()
	if ext.GetText() != "Dale" || msg.GetConversation() != "" {
		t.Fatalf("text not carried: %v", msg)
	}
	got := ext.GetContextInfo()
	if got.GetStanzaID() != "ABC" || got.GetParticipant() != "15550000003@s.whatsapp.net" ||
		got.GetQuotedMessage().GetConversation() != "Can you send me the details?" {
		t.Fatalf("context %v", got)
	}
}

func TestQuotedDocumentKeepsItsAttachment(t *testing.T) {
	ci := quoteContext("ABC", types.NewJID("1", types.DefaultUserServer), Message{FileName: "contract.pdf"})
	msg := withQuote(&waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{FileName: proto.String("x.pdf")}}, ci)
	if msg.GetDocumentMessage().GetContextInfo().GetQuotedMessage().GetConversation() != "contract.pdf" {
		t.Fatalf("got %v", msg)
	}
}

func TestReplyToUnarchivedMessageFailsBeforeSending(t *testing.T) {
	a, _ := openTest(t)
	h := &handler{arc: a}
	if _, err := h.quote(types.NewJID("1", types.DefaultUserServer), "1@s.whatsapp.net", "nope"); err == nil {
		t.Fatal("quote of a missing message succeeded")
	}
}

func queueJob(t *testing.T, h *handler, name string, job Job) string {
	t.Helper()
	if err := os.MkdirAll(h.paths.outbox(), 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(h.paths.outbox(), name)
	data, _ := json.Marshal(job)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func readReceipt(t *testing.T, h *handler, status, name string) Receipt {
	t.Helper()
	var r Receipt
	raw, err := os.ReadFile(filepath.Join(h.paths.outbox(), status, name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestOutboxSendsUnderTheGeneratedIDAndArchives(t *testing.T) {
	th := newTestHandler(t)
	queueJob(t, th.handler, "1.json", Job{To: alice, Text: "invented text"})
	th.processOutbox()
	r := readReceipt(t, th.handler, "sent", "1.json")
	if r.Status != "sent" || r.MessageID != "3EB0FAKE0001" || r.Error != "" {
		t.Fatalf("receipt %+v", r)
	}
	if len(th.wa.sent) != 1 || th.wa.sent[0].id != "3EB0FAKE0001" || th.wa.sent[0].msg.GetConversation() != "invented text" {
		t.Fatalf("sent %+v", th.wa.sent)
	}
	if count(t, th.db, `SELECT COUNT(*) FROM messages WHERE id = '3EB0FAKE0001' AND from_me = 1 AND source = 'sent'`) != 1 {
		t.Fatal("sent message not archived")
	}
	if len(th.exits) != 0 {
		t.Fatalf("exited %v", th.exits)
	}
}

func TestOutboxCommitFailureAfterSendingIsReportedJournaledAndExits(t *testing.T) {
	th := newTestHandler(t)
	job := queueJob(t, th.handler, "2.json", Job{To: alice, Text: "invented text"})
	failMessageInserts(t, th.db)
	th.processOutbox()
	// The message went out, so the receipt must not say "failed" (a tool
	// would report that nothing was sent), nor plain success.
	r := readReceipt(t, th.handler, "sent", "2.json")
	if r.MessageID != "3EB0FAKE0001" || !strings.Contains(r.Error, "archiving it failed") {
		t.Fatalf("receipt %+v", r)
	}
	if _, err := os.Stat(job); !os.IsNotExist(err) {
		t.Fatal("job left in the outbox would be sent again after the restart")
	}
	o, ok, err := th.arc.GetOp("outbox/2.json")
	if err != nil || !ok || o.State != "sent" || o.MessageID != "3EB0FAKE0001" || o.Error == "" {
		t.Fatalf("journal %+v %v %v", o, ok, err)
	}
	if len(th.exits) != 1 || th.exits[0] != exitFailure {
		t.Fatalf("exits %v, want one exitFailure", th.exits)
	}
}

func TestOutboxSendErrorStillFailsTheJob(t *testing.T) {
	th := newTestHandler(t)
	th.wa.sendErr = whatsmeow.ErrNotLoggedIn
	queueJob(t, th.handler, "3.json", Job{To: alice, Text: "invented text"})
	th.processOutbox()
	if r := readReceipt(t, th.handler, "failed", "3.json"); r.Error == "" || r.MessageID != "" {
		t.Fatalf("receipt %+v", r)
	}
	if len(th.exits) != 0 {
		t.Fatal("a send failure exited the daemon")
	}
}

// A send that may have gone out gets an "uncertain" receipt naming the
// message id it would have, never "failed" (which a tool reports as nothing
// sent, inviting a duplicate).
func TestOutboxUncertainSendWritesAnUncertainReceipt(t *testing.T) {
	th := newTestHandler(t)
	th.wa.sendErr = errors.New("timed out waiting for the server's answer")
	queueJob(t, th.handler, "4.json", Job{To: alice, Text: "invented text"})
	th.processOutbox()
	r := readReceipt(t, th.handler, "uncertain", "4.json")
	if r.Status != "uncertain" || r.MessageID != "3EB0FAKE0001" || !strings.Contains(r.Error, "timed out") {
		t.Fatalf("receipt %+v", r)
	}
	if _, err := os.Stat(filepath.Join(th.paths.outbox(), "failed", "4.json")); !os.IsNotExist(err) {
		t.Fatal("an uncertain send was also reported as failed")
	}
	if len(th.exits) != 0 {
		t.Fatal("an uncertain send exited the daemon")
	}
}
