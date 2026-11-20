package main

// Attachments are downloaded on request, except voice notes and audio, which
// are downloaded as they arrive (and, if WA_TRANSCRIBE names a program,
// transcribed) so their content is always available. The archive keeps each
// attachment's download reference (media_refs); a local tool, the API's
// fetch method, or the daemon itself for live audio, writes a request into
// <data>/fetch/, and the running daemon, which holds the
// only connection, downloads that one file into <data>/media/ and writes a
// receipt to fetch/done/ or fetch/failed/.

import (
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

// FetchRequest names one archived message whose attachment is wanted.
type FetchRequest struct {
	Chat string `json:"chat"` // archive chat key (chats.key)
	ID   string `json:"id"`   // message id
}

// FetchReceipt is written once the request is handled.
type FetchReceipt struct {
	FetchRequest
	Status string `json:"status"` // "done" or "failed"
	Path   string `json:"path,omitempty"`
	Error  string `json:"error,omitempty"`
}

func (p paths) fetchDir() string { return filepath.Join(p.data, "fetch") }
func (p paths) mediaDir() string { return filepath.Join(p.data, "media") }

// mediaRef returns the download reference of MSG's attachment and its MIME
// type, or nil when it has none worth fetching (stickers are skipped).
func mediaRef(msg *waE2E.Message) ([]byte, string) {
	var part proto.Message
	var mimetype string
	switch {
	case msg.GetDocumentMessage() != nil:
		part, mimetype = msg.GetDocumentMessage(), msg.GetDocumentMessage().GetMimetype()
	case msg.GetImageMessage() != nil:
		part, mimetype = msg.GetImageMessage(), msg.GetImageMessage().GetMimetype()
	case msg.GetVideoMessage() != nil:
		part, mimetype = msg.GetVideoMessage(), msg.GetVideoMessage().GetMimetype()
	case msg.GetAudioMessage() != nil:
		part, mimetype = msg.GetAudioMessage(), msg.GetAudioMessage().GetMimetype()
	default:
		return nil, ""
	}
	data, err := proto.Marshal(part)
	if err != nil {
		return nil, ""
	}
	return data, mimetype
}

// processFetches handles every queued request, oldest first.
func (h *handler) processFetches() {
	names, err := filepath.Glob(filepath.Join(h.paths.fetchDir(), "*.json"))
	if err != nil || len(names) == 0 {
		return
	}
	sort.Strings(names)
	for _, name := range names {
		r := FetchReceipt{Status: "failed"}
		data, err := os.ReadFile(name)
		if err == nil {
			err = json.Unmarshal(data, &r.FetchRequest)
		}
		if err == nil {
			r.Path, err = h.fetch(r.FetchRequest)
		}
		if err == nil {
			r.Status = "done"
			// The file is part of what a client shows for the chat.
			key, _ := h.arc.Canonical(r.Chat)
			h.arc.announce(map[string]bool{key: true})
		} else {
			r.Error = err.Error()
		}
		out, _ := json.MarshalIndent(r, "", "  ")
		dest := filepath.Join(h.paths.fetchDir(), r.Status, filepath.Base(name))
		writeReceipt(dest, out)
		_ = os.Remove(name)
	}
}

// autoFetched reports whether an attachment of KIND is downloaded on arrival.
func autoFetched(kind string) bool { return kind == "voice" || kind == "audio" }

// queueFetch writes a fetch request for message ID in CHAT, as an external tool would.
func (p paths) queueFetch(chat, id string) error {
	data, err := json.Marshal(FetchRequest{Chat: chat, ID: id})
	if err != nil {
		return err
	}
	name := filepath.Join(p.fetchDir(), time.Now().UTC().Format("20060102T150405")+"-auto-"+unsafeName.ReplaceAllString(id, "_")+".json")
	tmp := name + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, name)
}

var unsafeName = regexp.MustCompile(`[^\w.\- ]+`)

var errNoMediaRef = errors.New("no attachment reference")

// fetch downloads the attachment of REQ and returns the file's path.
func (h *handler) fetch(req FetchRequest) (string, error) {
	key, err := h.arc.Canonical(req.Chat)
	if err != nil {
		key = req.Chat
	}
	var kind, mimetype, fileName string
	var ref []byte
	err = h.arc.db.QueryRow(`SELECT r.kind, r.mimetype, r.ref, coalesce(m.file_name, '')
		FROM media_refs r LEFT JOIN messages m ON m.chat_key = r.chat_key AND m.id = r.id
		WHERE r.chat_key = ? AND r.id = ?`, key, req.ID).Scan(&kind, &mimetype, &ref, &fileName)
	if err != nil {
		return "", fmt.Errorf("%w for %s/%s (sent before references were kept, or pruned)", errNoMediaRef, key, req.ID)
	}
	var part whatsmeow.DownloadableMessage
	switch kind {
	case "document":
		part = &waE2E.DocumentMessage{}
	case "image":
		part = &waE2E.ImageMessage{}
	case "video":
		part = &waE2E.VideoMessage{}
	case "audio", "voice":
		part = &waE2E.AudioMessage{}
	default:
		return "", fmt.Errorf("cannot fetch a %s", kind)
	}
	if err := proto.Unmarshal(ref, part.(proto.Message)); err != nil {
		return "", err
	}
	data, err := h.wa.Download(h.ctx, part)
	if err != nil {
		return "", fmt.Errorf("download: %w", err)
	}
	if fileName == "" {
		ext := ""
		if exts, _ := mime.ExtensionsByType(mimetype); len(exts) > 0 {
			ext = exts[0]
		}
		fileName = kind + ext
	}
	name := req.ID + "-" + unsafeName.ReplaceAllString(filepath.Base(fileName), "_")
	dir := filepath.Join(h.paths.mediaDir(), unsafeName.ReplaceAllString(key, "_"))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	path := filepath.Join(dir, name)
	if !strings.HasPrefix(path, h.paths.mediaDir()) {
		return "", errors.New("unsafe path")
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return "", err
	}
	if autoFetched(kind) {
		go transcribe(path)
	}
	return path, nil
}

// transcribeSlot runs one transcription at a time; speech recognition is
// CPU-heavy.
var transcribeSlot = make(chan struct{}, 1)

// transcribe runs the program named by WA_TRANSCRIBE on the audio file at
// PATH; by convention it writes the transcript to PATH.txt for readers of the
// archive. Without WA_TRANSCRIBE nothing is transcribed.
func transcribe(path string) {
	script := os.Getenv("WA_TRANSCRIBE")
	if script == "" {
		return
	}
	transcribeSlot <- struct{}{}
	defer func() { <-transcribeSlot }()
	if out, err := exec.Command(script, path).CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "wa-archive: transcribe %s: %v %s\n", path, err, out)
	}
}
