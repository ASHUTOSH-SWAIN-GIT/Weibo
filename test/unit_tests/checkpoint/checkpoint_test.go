package checkpoint_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"mailer/checkpoint"
)

func TestFileStorage_SaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	fs := checkpoint.NewFileStorage(dir)

	data := &checkpoint.CheckpointData{
		ID:        "cp-1",
		Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Operators: map[string][]byte{
			"op-0": []byte("state-0"),
			"op-1": []byte("state-1"),
		},
		Source: map[string][]byte{
			"offset": []byte(`{"offset":42}`),
		},
	}

	if err := fs.Save(data); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := fs.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected checkpoint data, got nil")
	}
	if loaded.ID != "cp-1" {
		t.Errorf("ID: got %q, want %q", loaded.ID, "cp-1")
	}
	if string(loaded.Operators["op-0"]) != "state-0" {
		t.Errorf("op-0: got %q, want %q", loaded.Operators["op-0"], "state-0")
	}
	if string(loaded.Operators["op-1"]) != "state-1" {
		t.Errorf("op-1: got %q, want %q", loaded.Operators["op-1"], "state-1")
	}
	if string(loaded.Source["offset"]) != `{"offset":42}` {
		t.Errorf("source offset: got %q", loaded.Source["offset"])
	}
}

func TestFileStorage_LoadEmpty(t *testing.T) {
	dir := t.TempDir()
	fs := checkpoint.NewFileStorage(dir)

	loaded, err := fs.Load()
	if err != nil {
		t.Fatalf("Load on empty dir: %v", err)
	}
	if loaded != nil {
		t.Errorf("expected nil on empty dir, got %+v", loaded)
	}
}

func TestFileStorage_LoadSpecific(t *testing.T) {
	dir := t.TempDir()
	fs := checkpoint.NewFileStorage(dir)

	data := &checkpoint.CheckpointData{
		ID:        "cp-42",
		Timestamp: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
		Operators: map[string][]byte{},
	}
	if err := fs.Save(data); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := fs.LoadSpecific("cp-42")
	if err != nil {
		t.Fatalf("LoadSpecific: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected checkpoint, got nil")
	}
	if loaded.ID != "cp-42" {
		t.Errorf("ID: got %q, want %q", loaded.ID, "cp-42")
	}
}

func TestFileStorage_LoadSpecificNonexistent(t *testing.T) {
	dir := t.TempDir()
	fs := checkpoint.NewFileStorage(dir)

	loaded, err := fs.LoadSpecific("nonexistent")
	if err != nil {
		t.Fatalf("LoadSpecific nonexistent: %v", err)
	}
	if loaded != nil {
		t.Errorf("expected nil for nonexistent checkpoint, got %+v", loaded)
	}
}

func TestFileStorage_SetsLatest(t *testing.T) {
	dir := t.TempDir()
	fs := checkpoint.NewFileStorage(dir)

	cp1 := &checkpoint.CheckpointData{
		ID:        "cp-1",
		Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Operators: map[string][]byte{},
	}
	cp2 := &checkpoint.CheckpointData{
		ID:        "cp-2",
		Timestamp: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		Operators: map[string][]byte{},
	}

	if err := fs.Save(cp1); err != nil {
		t.Fatalf("Save cp-1: %v", err)
	}
	if err := fs.Save(cp2); err != nil {
		t.Fatalf("Save cp-2: %v", err)
	}

	// Load should return cp-2 (the latest).
	loaded, err := fs.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.ID != "cp-2" {
		t.Errorf("expected latest to be cp-2, got %q", loaded.ID)
	}
}

func TestFileStorage_AtomicWrite(t *testing.T) {
	dir := t.TempDir()
	fs := checkpoint.NewFileStorage(dir)

	data := &checkpoint.CheckpointData{
		ID:        "cp-1",
		Timestamp: time.Now().UTC(),
		Operators: map[string][]byte{},
	}
	if err := fs.Save(data); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Verify no tmp files left behind.
	files, err := filepath.Glob(filepath.Join(dir, "*.tmp"))
	if err != nil {
		t.Fatalf("Glob: %v", err)
	}
	if len(files) > 0 {
		t.Errorf("expected no tmp files, found: %v", files)
	}

	// Verify checkpoint file exists.
	if _, err := os.Stat(filepath.Join(dir, "checkpoint-cp-1.json")); os.IsNotExist(err) {
		t.Error("checkpoint file not found")
	}

	// Verify latest.json exists.
	if _, err := os.Stat(filepath.Join(dir, "latest.json")); os.IsNotExist(err) {
		t.Error("latest.json not found")
	}
}