package checkpoint_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/checkpoint"
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

func TestFileStorage_RetentionPreservesPreparedAndState(t *testing.T) {
	dir := t.TempDir()
	fs, err := checkpoint.NewFileStorageWithOptions(dir, checkpoint.FileStorageOptions{RetainCompleted: 2})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i, status := range []checkpoint.Status{checkpoint.StatusCompleted, checkpoint.StatusPrepared, checkpoint.StatusCompleted, checkpoint.StatusCompleted} {
		id := fmt.Sprintf("cp-%d", i+1)
		if err := os.MkdirAll(fs.StateDir(id), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := fs.Save(&checkpoint.CheckpointData{ID: id, Timestamp: base.Add(time.Duration(i) * time.Second), Status: status}); err != nil {
			t.Fatal(err)
		}
	}
	if data, _ := fs.LoadSpecific("cp-1"); data != nil {
		t.Fatal("oldest completed checkpoint was retained")
	}
	if _, err := os.Stat(fs.StateDir("cp-1")); !os.IsNotExist(err) {
		t.Fatal("oldest completed state directory was retained")
	}
	if data, _ := fs.LoadSpecific("cp-2"); data == nil || data.Status != checkpoint.StatusPrepared {
		t.Fatal("prepared checkpoint was removed")
	}
	if _, err := os.Stat(fs.StateDir("cp-2")); err != nil {
		t.Fatalf("prepared state directory: %v", err)
	}
}

func TestFileStorage_StartupRepairsPointersAndSweepsOrphans(t *testing.T) {
	dir := t.TempDir()
	fs, err := checkpoint.NewFileStorageWithOptions(dir, checkpoint.FileStorageOptions{RetainCompleted: 3})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := fs.Save(&checkpoint.CheckpointData{ID: "done", Timestamp: base, Status: checkpoint.StatusCompleted}); err != nil {
		t.Fatal(err)
	}
	if err := fs.Save(&checkpoint.CheckpointData{ID: "pending", Timestamp: base.Add(time.Second), Status: checkpoint.StatusPrepared}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "latest.json"), []byte("missing"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "latest-completed.json"), []byte("pending"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "checkpoint-torn.json"), []byte("{"), 0o644); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(dir, "checkpoint-orphan.state")
	if err := os.MkdirAll(orphan, 0o755); err != nil {
		t.Fatal(err)
	}

	reopened, err := checkpoint.NewFileStorageWithOptions(dir, checkpoint.FileStorageOptions{RetainCompleted: 3})
	if err != nil {
		t.Fatal(err)
	}
	latest, err := reopened.Load()
	if err != nil || latest == nil || latest.ID != "pending" {
		t.Fatalf("latest repair: data=%+v err=%v", latest, err)
	}
	completed, err := reopened.LoadLatestCompleted()
	if err != nil || completed == nil || completed.ID != "done" {
		t.Fatalf("completed repair: data=%+v err=%v", completed, err)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatal("orphan state directory was not swept")
	}
}

func TestFileStorage_RejectsInvalidRetention(t *testing.T) {
	if _, err := checkpoint.NewFileStorageWithOptions(t.TempDir(), checkpoint.FileStorageOptions{}); err == nil {
		t.Fatal("expected invalid retention error")
	}
}
