package main

// WhatsApp refuses a client whose protocol version it considers outdated
// (events.ClientOutdated). Restarting cannot fix that, only a whatsmeow
// update can, so the daemon leaves a marker, alerts the user at once through
// the command in WA_ARCHIVE_ALERT if set (the remedy is an update), and exits
// 0, which a service manager configured to restart only on failure does not
// restart. While the marker exists `run` exits 0 at once; install.sh removes
// it after a successful build.

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// exitOutdated is the exit status after an outdated-client rejection. It is
// 0 so launchd does not restart into the same rejection.
const exitOutdated = 0

func (p paths) outdated() string { return filepath.Join(p.state, "outdated") }

// alertEnv names the environment variable holding a shell command that
// receives an alert's text on its stdin, e.g. a script that sends a
// notification or an email.
const alertEnv = "WA_ARCHIVE_ALERT"

// defaultAlertCommand is the alert command from WA_ARCHIVE_ALERT, run by
// /bin/sh, or nil when it is unset (the alert then only goes to stderr).
func defaultAlertCommand() []string {
	if cmd := os.Getenv(alertEnv); cmd != "" {
		return []string{"/bin/sh", "-c", cmd}
	}
	return nil
}

// outdatedMessage is what `run` says while the marker exists, or "".
func outdatedMessage(p paths) string {
	if _, err := os.Stat(p.outdated()); err != nil {
		return ""
	}
	return fmt.Sprintf("wa-archive: WhatsApp rejected this build as outdated; not starting until a new build "+
		"is installed (install.sh removes %s)", p.outdated())
}

// clientOutdated handles WhatsApp's rejection of the client as outdated.
func (h *handler) clientOutdated() {
	if err := touch(h.paths.outdated()); err != nil {
		fmt.Fprintf(os.Stderr, "wa-archive: write %s: %v\n", h.paths.outdated(), err)
	}
	h.beat("client-outdated")
	text := strings.Join([]string{
		"WhatsApp rejected wa-archive as an outdated client (" + daemonVersion() + ").",
		"The daemon has stopped and will not restart by itself: WhatsApp is disconnected until it is updated.",
		"Remedy: update whatsmeow (go get go.mau.fi/whatsmeow@main && go mod tidy), rebuild,",
		"and deploy with install.sh, or remove the marker " + h.paths.outdated() + " by hand after deploying.",
	}, "\n") + "\n"
	fmt.Fprint(os.Stderr, text)
	if err := h.sendAlert(text); err != nil {
		fmt.Fprintf(os.Stderr, "wa-archive: alert failed: %v\n", err)
	}
	h.die(exitOutdated)
}

// sendAlert runs the alert command with TEXT on its stdin.
func (h *handler) sendAlert(text string) error {
	if len(h.alert) == 0 {
		return fmt.Errorf("no alert command configured (set %s)", alertEnv)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, h.alert[0], h.alert[1:]...)
	cmd.Stdin = strings.NewReader(text)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%v: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}
