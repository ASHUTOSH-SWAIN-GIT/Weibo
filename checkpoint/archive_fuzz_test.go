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

// FuzzExtractCheckpoint feeds arbitrary bytes to the savepoint extractor.
// Invariants: it never panics, it never writes outside the destination
// state root, and any accepted archive restores as a loadable completed
// checkpoint.
func FuzzExtractCheckpoint(f *testing.F) {
	// Seed with a valid archive plus hostile shapes.
	srcDir := f.TempDir()
	src := checkpoint.NewFileStorage(srcDir)
	ownerDir := filepath.Join(src.StateDir("cp-seed"), "worker-0")
	if err := os.MkdirAll(ownerDir, 0o755); err != nil {
		f.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ownerDir, "000001.sst"), []byte("seed"), 0o644); err != nil {
		f.Fatal(err)
	}
	if err := src.Save(&checkpoint.CheckpointData{
		ID:        "cp-seed",
		Timestamp: time.Now().UTC(),
		Operators: map[string][]byte{"worker-0": []byte(`{"state_ref":"worker-0"}`)},
		Source:    map[string][]byte{"offset": []byte("7")},
		Status:    checkpoint.StatusCompleted,
		StateDirs: map[string]string{"worker-0": "worker-0"},
	}); err != nil {
		f.Fatal(err)
	}
	var valid bytes.Buffer
	if err := checkpoint.ArchiveCheckpoint(src, "cp-seed", &valid); err != nil {
		f.Fatal(err)
	}
	f.Add(valid.Bytes())
	f.Add([]byte("not a tar archive"))
	f.Add([]byte("\x00\x01\x02checkpoint.json"))
	f.Add([]byte("state/../../escape"))

	f.Fuzz(func(t *testing.T, raw []byte) {
		parent := t.TempDir()
		dst := checkpoint.NewFileStorage(filepath.Join(parent, "store"))
		id, err := checkpoint.ExtractCheckpoint(dst, bytes.NewReader(raw))
		if err != nil {
			return // corrupt or hostile archives are normal rejections
		}
		// Accepted: the ID must load back as the latest completed
		// checkpoint with its state materialized under the state root.
		data, lerr := dst.LoadLatestCompleted()
		if lerr != nil || data == nil || data.ID != id {
			t.Fatalf("accepted archive %q did not restore as latest completed: %+v err=%v", id, data, lerr)
		}
		// Nothing may escape the destination root: walk the parent and
		// reject any new regular file outside the storage directory.
		_ = filepath.Walk(parent, func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			rel, rerr := filepath.Rel(parent, p)
			if rerr != nil {
				t.Fatalf("walk: %v", rerr)
			}
			if rel == "store" || strings.HasPrefix(rel, "store"+string(filepath.Separator)) {
				return nil
			}
			t.Fatalf("extraction escaped state root: %s", p)
			return nil
		})
	})
}
