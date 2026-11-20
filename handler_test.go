package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

func TestUnrecognisedContentIsLabelledNotDropped(t *testing.T) {
	invite := &waE2E.Message{GroupInviteMessage: &waE2E.GroupInviteMessage{GroupName: proto.String("Dinner plans")}}
	if got := messageText(invite); got != "[group invite] Dinner plans" {
		t.Fatalf("messageText = %q", got)
	}
	// A type with no explicit handling is named by its field.
	other := &waE2E.Message{EventMessage: &waE2E.EventMessage{Name: proto.String("x")}}
	if got := contentKind(other); got != "eventMessage" {
		t.Fatalf("contentKind = %q", got)
	}
	// Plumbing-only messages have no content kind.
	plumbing := &waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{Text: proto.String("👍")},
		MessageContextInfo: &waE2E.MessageContextInfo{}}
	if got := contentKind(plumbing); got != "" {
		t.Fatalf("contentKind(reaction) = %q", got)
	}
}

func TestLiveVoiceNoteIsQueuedForDownload(t *testing.T) {
	p := paths{data: t.TempDir()}
	if err := os.MkdirAll(p.fetchDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	if !autoFetched("voice") || !autoFetched("audio") || autoFetched("document") {
		t.Fatal("autoFetched: only voice notes and audio are downloaded on arrival")
	}
	if err := p.queueFetch("15550000003@s.whatsapp.net", "3EB0FAKEFETCH0001"); err != nil {
		t.Fatal(err)
	}
	names, _ := filepath.Glob(filepath.Join(p.fetchDir(), "*.json"))
	if len(names) != 1 {
		t.Fatalf("queued %d requests, want 1", len(names))
	}
	data, _ := os.ReadFile(names[0])
	var r FetchRequest
	if err := json.Unmarshal(data, &r); err != nil || r.ID != "3EB0FAKEFETCH0001" || r.Chat != "15550000003@s.whatsapp.net" {
		t.Fatalf("request = %s (%v)", data, err)
	}
}

// The backfill runs on connect (the event goroutine) and from the ticker
// (the main loop), so it may run twice at once; a group WhatsApp refuses is
// asked about once.
func TestConcurrentBackfillsAskForARefusedGroupOnce(t *testing.T) {
	th := newTestHandler(t)
	mustCommit(t, th.arc, Batch{Chats: []Chat{{Key: group, IsGroup: true}}})
	th.wa.groupErr = errors.New("not a participant")
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := th.backfillNames(); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if th.wa.groupCalls != 1 {
		t.Fatalf("GroupInfo asked %d times, want 1", th.wa.groupCalls)
	}
}
