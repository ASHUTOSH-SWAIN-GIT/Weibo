package checkpoint

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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
	TxnID     string            `json:"txn_id,omitempty"`     // sink transactional id (diagnostics)
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

	// StateDir returns the root directory for native state snapshots
	// (e.g. Pebble hard-links) associated with a checkpoint.
	StateDir(id string) string

	// UpdateStatus rewrites the status of an existing checkpoint
	// (prepared → completed promotion).
	UpdateStatus(id string, status Status) error
}

// FileStorage implements Storage using the local filesystem.
// Each checkpoint is written as a JSON file with the checkpoint ID in the filename.
// Writes are atomic (write to temp file, then rename).
type FileStorage struct {
	dir             string
	retainCompleted int
	mu              sync.Mutex
	startupErr      error
}

type FileStorageOptions struct{ RetainCompleted int }

const DefaultRetainedCheckpoints = 3

// NewFileStorage creates a FileStorage with default retention. Existing
// directories are repaired and swept automatically before it returns.
func NewFileStorage(dir string) *FileStorage {
	fs, err := NewFileStorageWithOptions(dir, FileStorageOptions{RetainCompleted: DefaultRetainedCheckpoints})
	if err != nil {
		return &FileStorage{dir: dir, retainCompleted: DefaultRetainedCheckpoints, startupErr: err}
	}
	return fs
}

// NewFileStorageWithOptions initializes storage, repairs its latest pointers,
// and removes orphaned native-state directories.
func NewFileStorageWithOptions(dir string, opts FileStorageOptions) (*FileStorage, error) {
	if opts.RetainCompleted < 1 {
		return nil, fmt.Errorf("checkpoint: retain completed must be at least 1")
	}
	fs := &FileStorage{dir: dir, retainCompleted: opts.RetainCompleted}
	fs.mu.Lock()
	err := fs.maintainLocked()
	fs.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return fs, nil
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
	return fs.sweepOrphansLocked()
}

func (fs *FileStorage) sweepOrphansLocked() error {
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
			if err := os.RemoveAll(filepath.Join(fs.dir, name)); err != nil {
				return err
			}
		}
	}
	return syncDir(fs.dir)
}

// DeleteStateDirs removes the state directories for a checkpoint ID.
// Used during retention/GC.
func (fs *FileStorage) DeleteStateDirs(id string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if os.RemoveAll(fs.StateDir(id)) == nil {
		_ = syncDir(fs.dir)
	}
}

// Save writes checkpoint data to a JSON file atomically and durably
// (fsync before rename). Also maintains two pointers: latest.json
// (any status) and latest-completed.json (completed only).
func (fs *FileStorage) Save(data *CheckpointData) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.startupErr != nil {
		return fs.startupErr
	}
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
	// JSON lands is lost on crash. StateDirs values are relative to
	// this checkpoint's StateDir root.
	for _, rel := range data.StateDirs {
		abs := filepath.Join(fs.StateDir(data.ID), rel)
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
	return fs.enforceRetentionLocked()
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
	if err := syncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("checkpoint: fsync parent: %w", err)
	}
	return nil
}

// UpdateStatus rewrites an existing checkpoint with a new status
// (prepared → completed promotion after a successful sink commit, or
// during recovery when the transaction marker proves the commit).
func (fs *FileStorage) UpdateStatus(id string, status Status) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.startupErr != nil {
		return fs.startupErr
	}

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

	if fs.startupErr != nil {
		return nil, fs.startupErr
	}
	return fs.loadPointerLocked("latest-completed.json", true)
}

// Load reads the most recent checkpoint (any status).
// Returns nil with no error if no checkpoint exists.
func (fs *FileStorage) Load() (*CheckpointData, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.startupErr != nil {
		return nil, fs.startupErr
	}
	return fs.loadLatestLocked()
}

func (fs *FileStorage) loadLatestLocked() (*CheckpointData, error) {
	return fs.loadPointerLocked("latest.json", false)
}

func (fs *FileStorage) loadPointerLocked(name string, completedOnly bool) (*CheckpointData, error) {
	idBytes, err := os.ReadFile(filepath.Join(fs.dir, name))
	if err == nil {
		data, loadErr := fs.loadSpecificLocked(strings.TrimSpace(string(idBytes)))
		if loadErr == nil && data != nil && (!completedOnly || data.Completed()) {
			return data, nil
		}
	}
	checkpoints, scanErr := fs.scanLocked()
	if scanErr != nil {
		return nil, scanErr
	}
	for i := len(checkpoints) - 1; i >= 0; i-- {
		if completedOnly && !checkpoints[i].Completed() {
			continue
		}
		if err := writeFileSync(filepath.Join(fs.dir, name), []byte(checkpoints[i].ID)); err != nil {
			return nil, err
		}
		return checkpoints[i], nil
	}
	return nil, nil
}

// LoadSpecific reads a checkpoint with the given ID.
func (fs *FileStorage) LoadSpecific(id string) (*CheckpointData, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.startupErr != nil {
		return nil, fs.startupErr
	}
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

func (fs *FileStorage) maintainLocked() error {
	if _, err := os.Stat(fs.dir); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	if err := fs.sweepOrphansLocked(); err != nil {
		return err
	}
	if _, err := fs.loadPointerLocked("latest.json", false); err != nil {
		return err
	}
	if _, err := fs.loadPointerLocked("latest-completed.json", true); err != nil {
		return err
	}
	return fs.enforceRetentionLocked()
}

func (fs *FileStorage) scanLocked() ([]*CheckpointData, error) {
	entries, err := os.ReadDir(fs.dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []*CheckpointData
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, "checkpoint-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(fs.dir, name))
		if err != nil {
			return nil, err
		}
		var data CheckpointData
		if json.Unmarshal(b, &data) != nil || data.ID == "" {
			continue
		}
		out = append(out, &data)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Timestamp.Equal(out[j].Timestamp) {
			return checkpointSequence(out[i].ID) < checkpointSequence(out[j].ID)
		}
		return out[i].Timestamp.Before(out[j].Timestamp)
	})
	return out, nil
}

func checkpointSequence(id string) int64 {
	parts := strings.Split(id, "-")
	n, _ := strconv.ParseInt(parts[len(parts)-1], 10, 64)
	return n
}

func (fs *FileStorage) enforceRetentionLocked() error {
	if fs.retainCompleted < 1 {
		return nil
	}
	all, err := fs.scanLocked()
	if err != nil {
		return err
	}
	var completed []*CheckpointData
	for _, data := range all {
		if data.Completed() {
			completed = append(completed, data)
		}
	}
	for len(completed) > fs.retainCompleted {
		victim := completed[0]
		completed = completed[1:]
		if err := os.Remove(filepath.Join(fs.dir, "checkpoint-"+victim.ID+".json")); err != nil && !os.IsNotExist(err) {
			return err
		}
		if err := os.RemoveAll(fs.StateDir(victim.ID)); err != nil {
			return err
		}
	}
	if len(all) > 0 {
		return syncDir(fs.dir)
	}
	return nil
}
