package checkpoint

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newTestLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, nil)), &buf
}

// TestLoadSkipsCorruptedCheckpointAndLogsIt guards against the silent
// fallback found live in a chaos drill: a corrupted checkpoint file must
// never be swallowed without a trace. Recovery may still proceed using an
// older valid checkpoint, but it must say so in the logs.
func TestLoadSkipsCorruptedCheckpointAndLogsIt(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStorageWithOptions(dir, FileStorageOptions{RetainCompleted: 10})
	if err != nil {
		t.Fatal(err)
	}
	logger, buf := newTestLogger()
	fs.Logger = logger

	good := &CheckpointData{ID: "cp-1", Timestamp: time.Now().UTC(), Status: StatusCompleted}
	if err := fs.Save(good); err != nil {
		t.Fatal(err)
	}
	bad := &CheckpointData{ID: "cp-2", Timestamp: time.Now().UTC().Add(time.Second), Status: StatusCompleted}
	if err := fs.Save(bad); err != nil {
		t.Fatal(err)
	}
	// Corrupt the newer checkpoint after it was legitimately written.
	if err := os.WriteFile(filepath.Join(dir, "checkpoint-cp-2.json"), []byte("not json{{{"), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := fs.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got == nil || got.ID != "cp-1" {
		t.Fatalf("expected fallback to the older valid checkpoint cp-1, got %+v", got)
	}
	if !strings.Contains(buf.String(), "skipping unreadable checkpoint file") || !strings.Contains(buf.String(), "cp-2") {
		t.Fatalf("expected a log entry naming the skipped corrupted file, got: %s", buf.String())
	}
}

// TestLoadAllCorruptedLogsLoudlyWhenPointerExisted guards the worst case: a
// job that HAS checkpointed before, whose entire checkpoint history is now
// unreadable. Falling back to "no checkpoint" silently is exactly the bug —
// it makes an operator-visible incident look identical to a brand-new job.
func TestLoadAllCorruptedLogsLoudlyWhenPointerExisted(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStorageWithOptions(dir, FileStorageOptions{RetainCompleted: 10})
	if err != nil {
		t.Fatal(err)
	}
	logger, buf := newTestLogger()
	fs.Logger = logger

	good := &CheckpointData{ID: "cp-1", Timestamp: time.Now().UTC(), Status: StatusCompleted}
	if err := fs.Save(good); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "checkpoint-cp-1.json"), []byte("not json{{{"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "latest.json"), []byte("not json{{{"), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := fs.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil (no usable checkpoint), got %+v", got)
	}
	if !strings.Contains(buf.String(), "pointer existed but no usable checkpoint was found") {
		t.Fatalf("expected a loud log entry for total checkpoint loss, got: %s", buf.String())
	}
}

// TestLoadFreshDirectoryStaysQuiet guards the other direction: a brand-new
// job that has never checkpointed must not trip the "corruption" logging —
// Load() returning nil, nil here is normal, not an incident.
func TestLoadFreshDirectoryStaysQuiet(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStorageWithOptions(dir, FileStorageOptions{RetainCompleted: 10})
	if err != nil {
		t.Fatal(err)
	}
	logger, buf := newTestLogger()
	fs.Logger = logger

	got, err := fs.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil for a fresh directory, got %+v", got)
	}
	if buf.Len() != 0 {
		t.Fatalf("expected no log output for a brand-new job with no checkpoint history, got: %s", buf.String())
	}
}
