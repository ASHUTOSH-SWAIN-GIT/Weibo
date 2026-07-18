package checkpoint

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Archive layout inside the tar stream:
//
//	checkpoint.json          the CheckpointData record
//	state/<owner>/...        copied contents of each per-owner state dir
//
// checkpoint.json is written first so extraction knows the checkpoint ID
// (and thus the destination state root) before any state file arrives.
const (
	archiveMetaName  = "checkpoint.json"
	archiveStateRoot = "state"
)

// ArchiveCheckpoint writes a self-contained tar of checkpoint id — its
// JSON record plus a real copy of every per-owner state directory — to w.
// Unlike the live checkpoint (Pebble hard-links on one filesystem), the
// archive is portable: it can be moved to another volume, host, or object
// store and extracted anywhere.
func ArchiveCheckpoint(src Storage, id string, w io.Writer) error {
	data, err := src.LoadSpecific(id)
	if err != nil {
		return fmt.Errorf("checkpoint: archive load %s: %w", id, err)
	}
	if data == nil {
		return fmt.Errorf("checkpoint: archive: no checkpoint %q", id)
	}

	tw := tar.NewWriter(w)

	meta, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if err := writeTarBytes(tw, archiveMetaName, meta); err != nil {
		return err
	}

	// Copy each owner's state directory. StateDirs maps owner → relative
	// path under the state root (in practice the path equals the owner).
	stateRoot := src.StateDir(id)
	for _, rel := range data.StateDirs {
		base := filepath.Join(stateRoot, rel)
		if err := walkIntoTar(tw, stateRoot, base); err != nil {
			return err
		}
	}
	return tw.Close()
}

// ExtractCheckpoint reads an archive produced by ArchiveCheckpoint and
// materializes it into dst as a completed checkpoint: state files are
// written under dst.StateDir(id) and the record is saved so that
// dst.LoadLatestCompleted() returns it. Returns the checkpoint ID.
func ExtractCheckpoint(dst Storage, r io.Reader) (string, error) {
	tr := tar.NewReader(r)
	var data *CheckpointData

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		switch {
		case hdr.Name == archiveMetaName:
			raw, err := io.ReadAll(tr)
			if err != nil {
				return "", err
			}
			data = &CheckpointData{}
			if err := json.Unmarshal(raw, data); err != nil {
				return "", fmt.Errorf("checkpoint: extract meta: %w", err)
			}
		case strings.HasPrefix(hdr.Name, archiveStateRoot+"/"):
			if data == nil {
				return "", fmt.Errorf("checkpoint: extract: state file before %s", archiveMetaName)
			}
			rel := strings.TrimPrefix(hdr.Name, archiveStateRoot+"/")
			dest := filepath.Join(dst.StateDir(data.ID), filepath.FromSlash(rel))
			if err := writeFileFromTar(dest, tr, hdr); err != nil {
				return "", err
			}
		}
	}

	if data == nil {
		return "", fmt.Errorf("checkpoint: extract: archive missing %s", archiveMetaName)
	}
	// A savepoint restores as the canonical completed checkpoint. Save
	// fsyncs the state dirs and writes the latest-completed pointer.
	data.Status = StatusCompleted
	if err := dst.Save(data); err != nil {
		return "", fmt.Errorf("checkpoint: extract save: %w", err)
	}
	return data.ID, nil
}

// walkIntoTar adds every file under base to the tar, named relative to
// stateRoot (so "state/<owner>/..."). Missing dirs are skipped.
func walkIntoTar(tw *tar.Writer, stateRoot, base string) error {
	return filepath.WalkDir(base, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(stateRoot, p)
		if err != nil {
			return err
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer f.Close()
		info, err := f.Stat()
		if err != nil {
			return err
		}
		hdr := &tar.Header{
			Name: archiveStateRoot + "/" + filepath.ToSlash(rel),
			Mode: 0o644,
			Size: info.Size(),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		_, err = io.Copy(tw, f)
		return err
	})
}

func writeTarBytes(tw *tar.Writer, name string, b []byte) error {
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(b))}); err != nil {
		return err
	}
	_, err := tw.Write(b)
	return err
}

func writeFileFromTar(dest string, r io.Reader, hdr *tar.Header) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.CopyN(f, r, hdr.Size); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
