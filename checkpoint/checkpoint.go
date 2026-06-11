package checkpoint

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// CheckpointData holds the complete state of a pipeline at a point in time.
// It includes the state of each stateful operator (by index) and the
// source offset (if applicable).
type CheckpointData struct {
	ID        string            `json:"id"`
	Timestamp time.Time         `json:"timestamp"`
	Operators map[string][]byte `json:"operators"` // operator index -> state bytes
	Source    map[string][]byte `json:"source"`    // source-specific offset data
}

// Storage is the interface for persisting checkpoint data.
// Implementations can write to local disk, S3, etc.
type Storage interface {
	// Save writes checkpoint data to persistent storage.
	// The implementation must be atomic — a partial write must not
	// corrupt a previous checkpoint.
	Save(data *CheckpointData) error

	// Load reads the most recent checkpoint from persistent storage.
	// Returns nil with no error if no checkpoint exists.
	Load() (*CheckpointData, error)

	// LoadSpecific reads a checkpoint with the given ID.
	LoadSpecific(id string) (*CheckpointData, error)
}

// FileStorage implements Storage using the local filesystem.
// Each checkpoint is written as a JSON file with the checkpoint ID in the filename.
// Writes are atomic (write to temp file, then rename).
type FileStorage struct {
	dir string
	mu  sync.Mutex
}

// NewFileStorage creates a FileStorage that writes checkpoints to the given directory.
// The directory is created if it doesn't exist.
func NewFileStorage(dir string) *FileStorage {
	return &FileStorage{dir: dir}
}

// Save writes checkpoint data to a JSON file atomically.
func (fs *FileStorage) Save(data *CheckpointData) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if err := os.MkdirAll(fs.dir, 0755); err != nil {
		return fmt.Errorf("checkpoint: create dir: %w", err)
	}

	filePath := filepath.Join(fs.dir, "checkpoint-"+data.ID+".json")
	tmpPath := filePath + ".tmp"

	b, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("checkpoint: marshal: %w", err)
	}

	if err := os.WriteFile(tmpPath, b, 0644); err != nil {
		return fmt.Errorf("checkpoint: write tmp: %w", err)
	}

	if err := os.Rename(tmpPath, filePath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("checkpoint: rename: %w", err)
	}

	// Update the "latest" symlink/reference by writing the latest ID.
	latestPath := filepath.Join(fs.dir, "latest.json")
	latestTmp := latestPath + ".tmp"
	if err := os.WriteFile(latestTmp, []byte(data.ID), 0644); err != nil {
		return fmt.Errorf("checkpoint: write latest: %w", err)
	}
	if err := os.Rename(latestTmp, latestPath); err != nil {
		os.Remove(latestTmp)
		return fmt.Errorf("checkpoint: rename latest: %w", err)
	}

	return nil
}

// Load reads the most recent checkpoint.
// Returns nil with no error if no checkpoint exists.
func (fs *FileStorage) Load() (*CheckpointData, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	latestPath := filepath.Join(fs.dir, "latest.json")
	idBytes, err := os.ReadFile(latestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("checkpoint: read latest: %w", err)
	}
	return fs.LoadSpecific(string(idBytes))
}

// LoadSpecific reads a checkpoint with the given ID.
func (fs *FileStorage) LoadSpecific(id string) (*CheckpointData, error) {
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
