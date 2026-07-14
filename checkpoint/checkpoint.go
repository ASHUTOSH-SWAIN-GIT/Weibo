package checkpoint

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Status tracks a checkpoint through the two-phase commit protocol.
//
//	prepared:  offsets + state persisted, sink transaction flushed but
//	           NOT yet committed. The commit decision is logged.
//	completed: sink transaction committed; safe to restore from.
//
// Checkpoints written without an explicit status (legacy, or the
// uncoordinated at-least-once path) are treated as completed.
type Status string

const (
	StatusPrepared  Status = "prepared"
	StatusCompleted Status = "completed"
)

// CheckpointData holds the complete state of a pipeline at a point in time.
// It includes the state of each stateful operator (by index) and the
// source offset (if applicable).  StateDirs maps owner IDs to relative
// paths of native (hard-linked) state directories, used by PebbleBackend
// for O(1)-delta checkpoints.
type CheckpointData struct {
	ID        string            `json:"id"`
	Timestamp time.Time         `json:"timestamp"`
	Operators map[string][]byte `json:"operators"` // operator index -> state bytes
	Source    map[string][]byte `json:"source"`    // source-specific offset data
	Status    Status            `json:"status,omitempty"`
	TxnID     string            `json:"txn_id,omitempty"` // sink transactional id (diagnostics)
	StateDirs map[string]string `json:"state_dirs,omitempty"` // ownerID -> relative path
}

// Completed reports whether this checkpoint is safe to restore from.
func (d *CheckpointData) Completed() bool {
	return d.Status == StatusCompleted || d.Status == ""
}

// Storage is the interface for persisting checkpoint data.
// Implementations can write to local disk, S3, etc.
type Storage interface {
	// Save writes checkpoint data to persistent storage.
	// The implementation must be atomic — a partial write must not
	// corrupt a previous checkpoint — and durable (fsync) before
	// returning, because the coordinator treats a successful Save of a
	// prepared checkpoint as the logged commit decision.
	Save(data *CheckpointData) error

	// Load reads the most recent checkpoint regardless of status.
	// Returns nil with no error if no checkpoint exists.
	Load() (*CheckpointData, error)

	// LoadLatestCompleted reads the most recent checkpoint whose
	// status is completed. Returns nil with no error if none exists.
	LoadLatestCompleted() (*CheckpointData, error)

	// LoadSpecific reads a checkpoint with the given ID.
	LoadSpecific(id string) (*CheckpointData, error)

	// UpdateStatus rewrites the status of an existing checkpoint
	// (prepared → completed promotion).
	// StateDir returns the root directory for native state snapshots
	// (e.g. Pebble hard-links) associated with a checkpoint.
	StateDir(id string) string

	UpdateStatus(id string, status Status) error
}

// FileStorage implements Storage using the local filesystem.
// Each checkpoint is written as a JSON file with the checkpoint ID in the filename.
// Writes are atomic (write to temp file, then rename).
type FileStorage struct {
	dir string
	mu  sync.Mutex
}

// NewFileStorage creates a FileStorage that writes checkpoints to the given directory.
// The directory is created if it doesn't exist.  On startup, call SweepOrphans to
// clean up state directories from failed checkpoints.
func NewFileStorage(dir string) *FileStorage {
	return &FileStorage{dir: dir}
}

// StateDir returns the checkpoint-specific state directory for the
// given checkpoint ID.  All per-owner state directories live under
// this single parent.
func (fs *FileStorage) StateDir(id string) string {
	return filepath.Join(fs.dir, "checkpoint-"+id+".state")
}

// SweepOrphans deletes any <id>.state directories whose matching
// checkpoint JSON does not exist.  Call once at startup before any
// pipeline runs.
func (fs *FileStorage) SweepOrphans() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	entries, err := os.ReadDir(fs.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() || !strings.HasSuffix(name, ".state") {
			continue
		}
		id := strings.TrimSuffix(name, ".state")
		jsonPath := filepath.Join(fs.dir, id+".json")
		if _, err := os.Stat(jsonPath); os.IsNotExist(err) {
			os.RemoveAll(filepath.Join(fs.dir, name))
		}
	}
	return nil
}

// DeleteStateDirs removes the state directories for a checkpoint ID.
// Used during retention/GC.
func (fs *FileStorage) DeleteStateDirs(id string) {
	os.RemoveAll(fs.StateDir(id))
}

// Save writes checkpoint data to a JSON file atomically and durably
// (fsync before rename). Also maintains two pointers: latest.json
// (any status) and latest-completed.json (completed only).
func (fs *FileStorage) Save(data *CheckpointData) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.saveLocked(data)
}

func (fs *FileStorage) saveLocked(data *CheckpointData) error {
	if err := os.MkdirAll(fs.dir, 0755); err != nil {
		return fmt.Errorf("checkpoint: create dir: %w", err)
	}
	if data.Status == "" {
		data.Status = StatusCompleted // legacy / uncoordinated path
	}

	b, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("checkpoint: marshal: %w", err)
	}

	// Sync state dirs before writing the JSON — the JSON is the
	// commit point, and a state dir that is not durable when the
	// JSON lands is lost on crash.
	for _, rel := range data.StateDirs {
		abs := filepath.Join(fs.dir, rel)
		if err := syncDir(abs); err != nil {
			return fmt.Errorf("checkpoint: sync state dir %s: %w", rel, err)
		}
	}

	filePath := filepath.Join(fs.dir, "checkpoint-"+data.ID+".json")
	if err := writeFileSync(filePath, b); err != nil {
		return err
	}

	if err := writeFileSync(filepath.Join(fs.dir, "latest.json"), []byte(data.ID)); err != nil {
		return err
	}
	if data.Status == StatusCompleted {
		if err := writeFileSync(filepath.Join(fs.dir, "latest-completed.json"), []byte(data.ID)); err != nil {
			return err
		}
	}
	return nil
}

// writeFileSync writes bytes to path atomically: temp file → fsync →
// rename. The fsync matters — a prepared checkpoint that survives only
// in the page cache is not a logged commit decision.
func writeFileSync(path string, b []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("checkpoint: open tmp: %w", err)
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("checkpoint: write tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("checkpoint: fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("checkpoint: close tmp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("checkpoint: rename: %w", err)
	}
	return nil
}

// UpdateStatus rewrites an existing checkpoint with a new status
// (prepared → completed promotion after a successful sink commit, or
// during recovery when the transaction marker proves the commit).
func (fs *FileStorage) UpdateStatus(id string, status Status) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	data, err := fs.loadSpecificLocked(id)
	if err != nil {
		return err
	}
	if data == nil {
		return fmt.Errorf("checkpoint: update status: %s not found", id)
	}
	data.Status = status
	return fs.saveLocked(data)
}

// LoadLatestCompleted reads the newest checkpoint that reached
// completed status. Falls back to nil (no error) when none exists.
func (fs *FileStorage) LoadLatestCompleted() (*CheckpointData, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	idBytes, err := os.ReadFile(filepath.Join(fs.dir, "latest-completed.json"))
	if err != nil {
		if os.IsNotExist(err) {
			// Legacy directories predate the pointer: latest.json is
			// completed by definition if its file carries no status.
			data, lerr := fs.loadLatestLocked()
			if lerr != nil || data == nil || !data.Completed() {
				return nil, lerr
			}
			return data, nil
		}
		return nil, fmt.Errorf("checkpoint: read latest-completed: %w", err)
	}
	return fs.loadSpecificLocked(string(idBytes))
}

// Load reads the most recent checkpoint (any status).
// Returns nil with no error if no checkpoint exists.
func (fs *FileStorage) Load() (*CheckpointData, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.loadLatestLocked()
}

func (fs *FileStorage) loadLatestLocked() (*CheckpointData, error) {
	latestPath := filepath.Join(fs.dir, "latest.json")
	idBytes, err := os.ReadFile(latestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("checkpoint: read latest: %w", err)
	}
	return fs.loadSpecificLocked(string(idBytes))
}

// LoadSpecific reads a checkpoint with the given ID.
func (fs *FileStorage) LoadSpecific(id string) (*CheckpointData, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.loadSpecificLocked(id)
}

func (fs *FileStorage) loadSpecificLocked(id string) (*CheckpointData, error) {
	filePath := filepath.Join(fs.dir, "checkpoint-"+id+".json")
	b, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("checkpoint: read %s: %w", id, err)
	}

	var data CheckpointData
	if err := json.Unmarshal(b, &data); err != nil {
		return nil, fmt.Errorf("checkpoint: unmarshal: %w", err)
	}
	return &data, nil
}

// syncDir opens a directory file descriptor and fsyncs it so that
// directory metadata (newly created subdirs) is durable.
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
