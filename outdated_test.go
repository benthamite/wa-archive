package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow/types/events"
)

func TestClientOutdatedMarksAlertsAndExitsZero(t *testing.T) {
	th := newTestHandler(t)
	// A fake alert command that keeps its arguments and stdin. A real one
	// would notify the user.
	dir := t.TempDir()
	script := filepath.Join(dir, "alert")
	got := filepath.Join(dir, "got")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho \"$@\" > "+got+"\ncat >> "+got+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	th.alert = []string{script, "alert", "wa-archive"}
	if !th.handle(&events.ClientOutdated{}) {
		t.Fatal("handle reported failure")
	}
	if codes := th.exitCodes(); len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("exits %v, want one exit 0 (the service manager must not restart into the rejection)", codes)
	}
	if _, err := os.Stat(th.paths.outdated()); err != nil {
		t.Fatal("no outdated marker")
	}
	alert, err := os.ReadFile(got)
	if err != nil {
		t.Fatal("alert command not run")
	}
	if !strings.HasPrefix(string(alert), "alert wa-archive\n") || !strings.Contains(string(alert), "outdated client") ||
		!strings.Contains(string(alert), "install.sh") {
		t.Fatalf("alert %q", alert)
	}
	var hb map[string]string
	raw, _ := os.ReadFile(th.paths.heartbeat())
	if err := json.Unmarshal(raw, &hb); err != nil || hb["event"] != "client-outdated" {
		t.Fatalf("heartbeat %s (%v)", raw, err)
	}
	if outdatedMessage(th.paths) == "" {
		t.Fatal("run would start despite the marker")
	}
}

func TestClientOutdatedStillExitsWhenTheAlertFails(t *testing.T) {
	th := newTestHandler(t)
	th.alert = []string{filepath.Join(t.TempDir(), "missing")}
	th.handle(&events.ClientOutdated{})
	if codes := th.exitCodes(); len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("exits %v", codes)
	}
	if outdatedMessage(th.paths) == "" {
		t.Fatal("no marker")
	}
}

func TestRunStartsWithoutTheMarker(t *testing.T) {
	if msg := outdatedMessage(paths{state: t.TempDir()}); msg != "" {
		t.Fatalf("message %q without a marker", msg)
	}
}

func TestDefaultAlertComesFromTheEnvironment(t *testing.T) {
	t.Setenv(alertEnv, "")
	if cmd := defaultAlertCommand(); cmd != nil {
		t.Fatalf("alert command %v without %s", cmd, alertEnv)
	}
	t.Setenv(alertEnv, "notify --urgent 'wa-archive'")
	cmd := defaultAlertCommand()
	if len(cmd) != 3 || cmd[0] != "/bin/sh" || cmd[1] != "-c" || cmd[2] != "notify --urgent 'wa-archive'" {
		t.Fatalf("alert command %v", cmd)
	}
}
