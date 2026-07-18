package checkpoint

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// ErrBlobNotFound is returned by Blobstore.Get for a missing key.
var ErrBlobNotFound = errors.New("checkpoint: blob not found")

// Blobstore is a minimal object-store abstraction used for savepoints
// (and, later, object-store checkpoints). Keys are '/'-separated paths.
// It exists so the durability mechanic — archive a checkpoint, put it,
// get it, extract it — is written once against an interface: the
// filesystem implementation below serves local Docker today, and an
// S3/GCS adapter drops in for multi-host durability (plan phase P6)
// without touching the archive or savepoint code.
type Blobstore interface {
	// Put stores the contents of r under key, overwriting any existing
	// blob. It reads r to EOF.
	Put(key string, r io.Reader) error
	// Get opens the blob at key. Returns ErrBlobNotFound if absent. The
	// caller closes the reader.
	Get(key string) (io.ReadCloser, error)
	// Exists reports whether a blob is present.
	Exists(key string) (bool, error)
	// List returns keys with the given prefix.
	List(prefix string) ([]string, error)
	// Delete removes a blob; absent keys are not an error.
	Delete(key string) error
}

// FileBlobstore is a Blobstore backed by a directory tree. Each key maps
// to a file under root. In the container model this points at a shared
// volume mounted into every job, so a savepoint written by one job is
// visible to another — the same namespace semantics S3 gives across hosts.
type FileBlobstore struct {
	root string
}

// NewFileBlobstore roots a blobstore at dir (created on first write).
func NewFileBlobstore(dir string) *FileBlobstore {
	return &FileBlobstore{root: dir}
}

// path maps a '/'-separated key to a filesystem path under root, rejecting
// traversal outside the root.
func (b *FileBlobstore) path(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("checkpoint: empty blob key")
	}
	// Reject traversal explicitly rather than silently normalizing it —
	// a ".." segment must never remap a key into a different location.
	if slices.Contains(strings.Split(key, "/"), "..") {
		return "", fmt.Errorf("checkpoint: invalid blob key %q", key)
	}
	clean := filepath.Clean("/" + strings.ReplaceAll(key, "/", string(filepath.Separator)))
	p := filepath.Join(b.root, clean)
	// After Join+Clean, p must remain under root.
	rel, err := filepath.Rel(b.root, p)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("checkpoint: invalid blob key %q", key)
	}
	return p, nil
}

func (b *FileBlobstore) Put(key string, r io.Reader) error {
	p, err := b.path(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	// Atomic: write to a temp file, fsync, rename over the target.
	tmp, err := os.CreateTemp(filepath.Dir(p), ".blob-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename
	if _, err := io.Copy(tmp, r); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, p)
}

func (b *FileBlobstore) Get(key string) (io.ReadCloser, error) {
	p, err := b.path(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrBlobNotFound
	}
	return f, err
}

func (b *FileBlobstore) Exists(key string) (bool, error) {
	p, err := b.path(key)
	if err != nil {
		return false, err
	}
	_, err = os.Stat(p)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func (b *FileBlobstore) List(prefix string) ([]string, error) {
	var keys []string
	err := filepath.WalkDir(b.root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil // empty/absent store lists as nothing
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(b.root, p)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
		return nil
	})
	return keys, err
}

func (b *FileBlobstore) Delete(key string) error {
	p, err := b.path(key)
	if err != nil {
		return err
	}
	err = os.Remove(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
