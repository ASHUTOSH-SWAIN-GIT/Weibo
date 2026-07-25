package checkpoint_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/checkpoint"
)

func TestFileBlobstore_RoundTripAndGuards(t *testing.T) {
	bs := checkpoint.NewFileBlobstore(t.TempDir())

	if err := bs.Put("savepoints/nightly", strings.NewReader("payload")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if ok, _ := bs.Exists("savepoints/nightly"); !ok {
		t.Fatal("Exists should be true after Put")
	}
	rc, err := bs.Get("savepoints/nightly")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(rc)
	rc.Close()
	if buf.String() != "payload" {
		t.Errorf("Get: got %q", buf.String())
	}

	labels, _ := bs.List("savepoints/")
	if len(labels) != 1 || labels[0] != "savepoints/nightly" {
		t.Errorf("List: %v", labels)
	}

	// Missing key.
	if _, err := bs.Get("savepoints/missing"); err != checkpoint.ErrBlobNotFound {
		t.Errorf("missing Get: got %v, want ErrBlobNotFound", err)
	}
	// Path traversal is rejected.
	if err := bs.Put("../escape", strings.NewReader("x")); err == nil {
		t.Error("expected traversal key to be rejected")
	}

	if err := bs.Delete("savepoints/nightly"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if ok, _ := bs.Exists("savepoints/nightly"); ok {
		t.Error("Exists should be false after Delete")
	}
}

// seedCheckpoint writes a completed checkpoint into a FileStorage with one
// owner's state directory containing a marker file, mimicking what the
// engine's Pebble snapshot produces. Returns the checkpoint ID.
func seedCheckpoint(t *testing.T, fs *checkpoint.FileStorage, id, owner, stateContent string) {
	t.Helper()
	// Write the owner's state dir under the checkpoint state root.
	ownerDir := filepath.Join(fs.StateDir(id), owner)
	if err := os.MkdirAll(ownerDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ownerDir, "000001.sst"), []byte(stateContent), 0o644); err != nil {
		t.Fatal(err)
	}
	data := &checkpoint.CheckpointData{
		ID:        id,
		Timestamp: time.Now().UTC(),
		Operators: map[string][]byte{owner: []byte(`{"state_ref":"` + owner + `"}`)},
		Source:    map[string][]byte{"offset": []byte("42")},
		Status:    checkpoint.StatusCompleted,
		StateDirs: map[string]string{owner: owner},
	}
	if err := fs.Save(data); err != nil {
		t.Fatal(err)
	}
}

func TestArchiveExtract_RoundTripStateDirs(t *testing.T) {
	srcDir := t.TempDir()
	src := checkpoint.NewFileStorage(srcDir)
	seedCheckpoint(t, src, "cp-1", "worker-0", "sst-bytes")

	// Archive to a buffer.
	var buf bytes.Buffer
	if err := checkpoint.ArchiveCheckpoint(src, "cp-1", &buf); err != nil {
		t.Fatalf("ArchiveCheckpoint: %v", err)
	}

	// Extract into a fresh, separate storage.
	dst := checkpoint.NewFileStorage(t.TempDir())
	id, err := checkpoint.ExtractCheckpoint(dst, &buf)
	if err != nil {
		t.Fatalf("ExtractCheckpoint: %v", err)
	}
	if id != "cp-1" {
		t.Fatalf("extracted id: got %q", id)
	}

	// The record round-trips as the latest completed checkpoint.
	data, err := dst.LoadLatestCompleted()
	if err != nil || data == nil {
		t.Fatalf("LoadLatestCompleted: %v (data=%v)", err, data)
	}
	if string(data.Source["offset"]) != "42" {
		t.Errorf("offset not preserved: %q", data.Source["offset"])
	}
	// The state file is materialized at the destination's state root.
	got, err := os.ReadFile(filepath.Join(dst.StateDir("cp-1"), "worker-0", "000001.sst"))
	if err != nil {
		t.Fatalf("state file not extracted: %v", err)
	}
	if string(got) != "sst-bytes" {
		t.Errorf("state content: got %q", got)
	}
}

// The full savepoint round-trip through a blobstore: promote → restore
// into a fresh storage, state and offsets intact.
func TestSavepoint_CreateRestoreThroughBlobstore(t *testing.T) {
	src := checkpoint.NewFileStorage(t.TempDir())
	seedCheckpoint(t, src, "cp-7", "worker-0", "keyed-state")

	bs := checkpoint.NewFileBlobstore(t.TempDir())

	promoted, err := checkpoint.CreateSavepoint(src, bs, "before-upgrade")
	if err != nil {
		t.Fatalf("CreateSavepoint: %v", err)
	}
	if promoted != "cp-7" {
		t.Errorf("promoted id: got %q", promoted)
	}
	if labels, _ := checkpoint.ListSavepoints(bs); len(labels) != 1 || labels[0] != "before-upgrade" {
		t.Errorf("ListSavepoints: %v", labels)
	}

	// Restore into a brand-new storage (simulating a fresh container/run).
	dst := checkpoint.NewFileStorage(t.TempDir())
	id, err := checkpoint.RestoreSavepoint(dst, bs, "before-upgrade")
	if err != nil {
		t.Fatalf("RestoreSavepoint: %v", err)
	}
	if id != "cp-7" {
		t.Errorf("restored id: got %q", id)
	}
	data, _ := dst.LoadLatestCompleted()
	if data == nil || string(data.Source["offset"]) != "42" {
		t.Fatalf("restored checkpoint wrong: %+v", data)
	}
	got, err := os.ReadFile(filepath.Join(dst.StateDir("cp-7"), "worker-0", "000001.sst"))
	if err != nil || string(got) != "keyed-state" {
		t.Fatalf("restored state wrong: %q err=%v", got, err)
	}
}

func TestSavepoint_NoCompletedCheckpoint(t *testing.T) {
	src := checkpoint.NewFileStorage(t.TempDir())
	bs := checkpoint.NewFileBlobstore(t.TempDir())
	if _, err := checkpoint.CreateSavepoint(src, bs, "empty"); err == nil {
		t.Fatal("expected error when there is no completed checkpoint")
	}
}
