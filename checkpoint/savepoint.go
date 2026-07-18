package checkpoint

import (
	"fmt"
	"io"
	"path"
)

// SavepointPrefix is the blobstore key prefix under which savepoints live.
const SavepointPrefix = "savepoints/"

// SavepointKey returns the blobstore key for a named savepoint.
func SavepointKey(label string) string { return SavepointPrefix + label }

// CreateSavepoint archives the latest completed checkpoint in src and
// stores it in bs under the given label. This is the promotion half of a
// stop-with-savepoint: the job has drained and written its final
// checkpoint, and here that checkpoint is copied into durable, named,
// never-GC'd storage the operator can restart from.
//
// Returns the checkpoint ID that was promoted. Errors if src has no
// completed checkpoint (nothing to promote).
func CreateSavepoint(src Storage, bs Blobstore, label string) (string, error) {
	data, err := src.LoadLatestCompleted()
	if err != nil {
		return "", fmt.Errorf("checkpoint: savepoint load latest: %w", err)
	}
	if data == nil {
		return "", fmt.Errorf("checkpoint: savepoint %q: no completed checkpoint to promote", label)
	}

	// Stream the archive straight into the blobstore without buffering
	// the whole (potentially large) state on the heap.
	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(ArchiveCheckpoint(src, data.ID, pw))
	}()
	if err := bs.Put(SavepointKey(label), pr); err != nil {
		return "", fmt.Errorf("checkpoint: savepoint %q put: %w", label, err)
	}
	return data.ID, nil
}

// RestoreSavepoint extracts the named savepoint from bs into dst, making
// it dst's latest completed checkpoint so the engine resumes from it on
// the next Execute. Returns the restored checkpoint ID.
func RestoreSavepoint(dst Storage, bs Blobstore, label string) (string, error) {
	rc, err := bs.Get(SavepointKey(label))
	if err != nil {
		return "", fmt.Errorf("checkpoint: restore savepoint %q: %w", label, err)
	}
	defer rc.Close()
	id, err := ExtractCheckpoint(dst, rc)
	if err != nil {
		return "", fmt.Errorf("checkpoint: restore savepoint %q: %w", label, err)
	}
	return id, nil
}

// ListSavepoints returns the labels of all savepoints in bs.
func ListSavepoints(bs Blobstore) ([]string, error) {
	keys, err := bs.List(SavepointPrefix)
	if err != nil {
		return nil, err
	}
	labels := make([]string, 0, len(keys))
	for _, k := range keys {
		labels = append(labels, path.Base(k))
	}
	return labels, nil
}
