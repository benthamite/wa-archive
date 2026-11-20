#!/bin/sh
# macOS: test, build, sign and restart the wa-archive LaunchAgent. Success
# means the new daemon connected to WhatsApp after the restart; otherwise the
# previous binary is put back and restarted, and the script exits nonzero, so
# an automated updater can rely on its exit status.
#
# The LaunchAgent must already be loaded and run bin/wa-archive from this
# directory (see the README). WA_ARCHIVE_LABEL is its launchd label (default
# "wa-archive").
#
# Building over the running binary in place makes macOS refuse to start it
# (OS_REASON_CODESIGNING), so build to a new file, sign it, and swap it in.
set -eu
cd "$(dirname "$0")"
state="${XDG_STATE_HOME:-$HOME/.local/state}/wa-archive"
heartbeat="$state/heartbeat.json"
label="gui/$(id -u)/${WA_ARCHIVE_LABEL:-wa-archive}"

if ! launchctl print "$label" >/dev/null 2>&1; then
	echo "install.sh: no LaunchAgent $label is loaded; load it first, or set WA_ARCHIVE_LABEL to its label" >&2
	exit 1
fi

go test ./...
go build -o bin/wa-archive.new .
codesign --force -s - bin/wa-archive.new
# A new build is the remedy for WhatsApp rejecting the client as outdated;
# the daemon refuses to start while this marker exists.
rm -f "$state/outdated"
if [ -e bin/wa-archive ]; then
	cp -p bin/wa-archive bin/wa-archive.prev
fi
mv bin/wa-archive.new bin/wa-archive

# connected_after T: whether the heartbeat was written after Unix time T by a
# connected daemon. The daemon writes "connected" on connecting and "write"
# whenever it archives something, which it can only do while connected; the
# offline backlog often replaces "connected" with "write" within a second.
connected_after() {
	[ -f "$heartbeat" ] || return 1
	event=$(plutil -extract event raw -o - "$heartbeat" 2>/dev/null) || return 1
	at=$(plutil -extract at raw -o - "$heartbeat" 2>/dev/null) || return 1
	case "$event" in
	connected | write) ;;
	*) return 1 ;;
	esac
	at_s=$(date -j -u -f '%Y-%m-%dT%H:%M:%SZ' "$at" +%s 2>/dev/null) || return 1
	[ "$at_s" -gt "$1" ]
}

# restart_and_wait: restart the daemon and wait up to 60 s for it to connect.
restart_and_wait() {
	start=$(date +%s)
	launchctl kickstart -k "$label" || true
	i=0
	while [ "$i" -lt 60 ]; do
		sleep 1
		i=$((i + 1))
		if connected_after "$start"; then
			return 0
		fi
		# The first start after the swap is sometimes refused
		# (OS_REASON_CODESIGNING) and launchd then waits before
		# retrying. Start it again whenever it is not running.
		if [ $((i % 5)) -eq 0 ] && ! launchctl print "$label" | grep -q 'state = running'; then
			launchctl kickstart "$label" || true
		fi
	done
	return 1
}

if restart_and_wait; then
	echo "install.sh: wa-archive connected with the new build"
	exit 0
fi
echo "install.sh: the new build did not connect within 60 s; restoring the previous one" >&2
if [ -e bin/wa-archive.prev ]; then
	cp -p bin/wa-archive.prev bin/wa-archive.restore
	mv bin/wa-archive.restore bin/wa-archive
	if restart_and_wait; then
		echo "install.sh: the previous build is running again" >&2
	else
		echo "install.sh: the previous build did not connect either; see: launchctl print $label" >&2
	fi
else
	echo "install.sh: no previous binary to restore" >&2
fi
exit 1
