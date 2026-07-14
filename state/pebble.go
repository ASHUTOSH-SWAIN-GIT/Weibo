package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble"
)

const (
	valuePrefix = 0x76 // 'v'
	listPrefix  = 0x6c // 'l'
)

// PebbleBackend implements StateBackend on a CockroachDB Pebble LSM.
// Each owner gets its own Pebble DB (one owner per stateful operator
// instance — per-worker isolation).  The working DB is disposable;
// durability comes from checkpoints (DisableWAL: true).
type PebbleBackend struct {
	db   *pebble.DB
	dir  string
	seqs map[seqKey]uint64
	mu   sync.Mutex

	// life guards the db handle's lifecycle. Straggler goroutines
	// from a force-aborted pipeline can still touch state while (or
	// after) the engine closes backends; operations on a closed
	// backend become no-ops instead of panicking — their results are
	// post-abort garbage that never becomes visible anyway.
	life   sync.RWMutex
	closed bool
}

type seqKey struct {
	namespace string
	key       string
}

// pebbleOptions returns Pebble options tuned for low-latency write path
// and disposable working DB life-cycle (D3).
func pebbleOptions() *pebble.Options {
	return &pebble.Options{
		WALBytesPerSync: 1 << 30, // effectively no sync — fast path
		Logger:          nil,
		EventListener:   nil,
	}
}

var noSync = &pebble.WriteOptions{Sync: false}

// OpenPebble creates a PebbleBackend rooted at dir. The caller owns the
// directory; the backend will create a new Pebble DB or open an existing
// one (e.g. after a checkpoint restore).  Close() when done.
func OpenPebble(dir string) (*PebbleBackend, error) {
	db, err := pebble.Open(dir, pebbleOptions())
	if err != nil {
		return nil, fmt.Errorf("pebble open %s: %w", dir, err)
	}
	return &PebbleBackend{db: db, dir: dir, seqs: make(map[seqKey]uint64)}, nil
}

// Close shuts down the Pebble DB. Idempotent.
func (p *PebbleBackend) Close() error {
	p.life.Lock()
	defer p.life.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	return p.db.Close()
}

// CheckpointTo creates a hard-link-based snapshot of the live Pebble DB
// at dir.  The caller must place dir on the same filesystem as the
// live DB for hard-links to work.
//
// The memtable is flushed first: the write path runs unsynced
// (Sync: false), so recent writes exist only in memory and an
// unflushed WAL — hard-linking without a flush would checkpoint an
// empty/stale view. Flush cost is proportional to data written since
// the previous flush, which is exactly the "changed data" scaling
// native checkpoints promise.
func (p *PebbleBackend) CheckpointTo(dir string) error {
	p.life.RLock()
	defer p.life.RUnlock()
	if p.closed {
		return fmt.Errorf("state/pebble: checkpoint on closed backend")
	}
	if err := p.db.Flush(); err != nil {
		return fmt.Errorf("state/pebble: flush before checkpoint: %w", err)
	}
	return p.db.Checkpoint(dir)
}

// RestoreFrom rebuilds the live DB from a previously saved checkpoint
// directory.  The current DB is closed, the live directory is wiped,
// and the checkpointed data is copied back.
func (p *PebbleBackend) RestoreFrom(dir string) error {
	p.life.Lock()
	defer p.life.Unlock()
	if p.db != nil && !p.closed {
		if err := p.db.Close(); err != nil {
			return fmt.Errorf("state/pebble: close before restore: %w", err)
		}
		p.closed = true
	}
	if err := os.RemoveAll(p.dir); err != nil {
		return fmt.Errorf("state/pebble: wipe live dir: %w", err)
	}
	if err := os.MkdirAll(p.dir, 0755); err != nil {
		return fmt.Errorf("state/pebble: mkdir live dir: %w", err)
	}
	if err := copyDir(dir, p.dir); err != nil {
		return fmt.Errorf("state/pebble: copy checkpoint -> live: %w", err)
	}
	db, err := pebble.Open(p.dir, pebbleOptions())
	if err != nil {
		return fmt.Errorf("state/pebble: reopen after restore: %w", err)
	}
	p.db = db
	p.seqs = make(map[seqKey]uint64)
	p.closed = false
	return nil
}

// ---- StateBackend ----------------------------------------------------------

func (p *PebbleBackend) ValueState(name string) ValueState {
	return &pebbleValueState{backend: p, namespace: name}
}

func (p *PebbleBackend) ListState(name string) ListState {
	return &pebbleListState{backend: p, namespace: name}
}

// ---- ValueState ------------------------------------------------------------

type pebbleValueState struct {
	backend   *PebbleBackend
	namespace string
	key       string
}

func (vs *pebbleValueState) SetKey(k string) { vs.key = k }
func (vs *pebbleValueState) userKey() []byte { return valueUserKey(vs.namespace, vs.key) }

func (vs *pebbleValueState) Get() []byte {
	p := vs.backend
	p.life.RLock()
	defer p.life.RUnlock()
	if p.closed {
		return nil
	}
	val, closer, err := p.db.Get(vs.userKey())
	if err != nil {
		return nil
	}
	defer closer.Close()
	out := make([]byte, len(val))
	copy(out, val)
	return out
}

func (vs *pebbleValueState) Set(value []byte) {
	p := vs.backend
	p.life.RLock()
	defer p.life.RUnlock()
	if p.closed {
		return
	}
	if err := p.db.Set(vs.userKey(), value, noSync); err != nil {
		panic(fmt.Sprintf("state/pebble: Set: %v", err))
	}
}

func (vs *pebbleValueState) Clear() {
	p := vs.backend
	p.life.RLock()
	defer p.life.RUnlock()
	if p.closed {
		return
	}
	if err := p.db.Delete(vs.userKey(), noSync); err != nil {
		panic(fmt.Sprintf("state/pebble: Clear: %v", err))
	}
}

func (vs *pebbleValueState) SnapshotAll() map[string][]byte {
	p := vs.backend
	p.life.RLock()
	defer p.life.RUnlock()
	if p.closed {
		return map[string][]byte{}
	}
	prefix := valuePrefixBytes(vs.namespace)
	iter, _ := p.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	defer iter.Close()

	out := make(map[string][]byte)
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		out[valueUserKeyFromFull(vs.namespace, key)] = cloneBytes(iter.Value())
	}
	return out
}

func (vs *pebbleValueState) RestoreAll(entries map[string][]byte) error {
	p := vs.backend
	p.life.RLock()
	defer p.life.RUnlock()
	if p.closed {
		return fmt.Errorf("state/pebble: restore on closed backend")
	}
	b := p.db.NewBatch()
	defer b.Close()
	b.DeleteRange(prefixStart(vs.namespace), prefixEnd(vs.namespace), noSync)
	for k, v := range entries {
		if err := b.Set(valueUserKey(vs.namespace, k), v, noSync); err != nil {
			return err
		}
	}
	return p.db.Apply(b, noSync)
}

// ---- ListState -------------------------------------------------------------

type pebbleListState struct {
	backend   *PebbleBackend
	namespace string
	key       string
}

func (ls *pebbleListState) SetKey(k string) { ls.key = k }

func (ls *pebbleListState) Append(value []byte) {
	p := ls.backend
	p.life.RLock()
	defer p.life.RUnlock()
	if p.closed {
		return
	}
	p.mu.Lock()
	sk := seqKey{namespace: ls.namespace, key: ls.key}
	seq := p.seqs[sk]
	p.seqs[sk] = seq + 1
	p.mu.Unlock()

	listKey := listEntryKey(ls.namespace, ls.key, seq)
	if err := p.db.Set(listKey, value, noSync); err != nil {
		panic(fmt.Sprintf("state/pebble: ListState.Append: %v", err))
	}
}

func (ls *pebbleListState) GetAll() [][]byte {
	p := ls.backend
	p.life.RLock()
	defer p.life.RUnlock()
	if p.closed {
		return nil
	}
	prefix := listPrefixBytes(ls.namespace, ls.key)
	iter, _ := p.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	defer iter.Close()

	var out [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		out = append(out, cloneBytes(iter.Value()))
	}
	return out
}

func (ls *pebbleListState) Clear() {
	p := ls.backend
	p.life.RLock()
	defer p.life.RUnlock()
	if p.closed {
		return
	}
	start := listPrefixBytes(ls.namespace, ls.key)
	if err := p.db.DeleteRange(start, prefixUpperBound(start), noSync); err != nil {
		panic(fmt.Sprintf("state/pebble: ListState.Clear: %v", err))
	}
}

// ---- Key encoding ----------------------------------------------------------

// valueUserKey builds the Pebble key for a value state entry.
func valueUserKey(namespace, key string) []byte {
	var buf bytes.Buffer
	buf.WriteByte(valuePrefix)
	buf.WriteByte(0x00)
	buf.WriteString(namespace)
	buf.WriteByte(0x00)
	buf.WriteString(key)
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out
}

// valueUserKeyFromFull extracts the user key from a full Pebble key.
func valueUserKeyFromFull(namespace string, fullKey []byte) string {
	expected := valuePrefixBytes(namespace)
	// fullKey format: v\x00<namespace>\x00<userKey>
	// So userKey starts at len(expected)
	if len(fullKey) > len(expected) {
		return string(fullKey[len(expected):])
	}
	return ""
}

// valuePrefixBytes returns a prefix for iterating all value entries
// in a namespace: v\x00<namespace>\x00
func valuePrefixBytes(namespace string) []byte {
	return valueUserKey(namespace, "")
}

// listEntryKey builds the Pebble key for a list entry: l\x00<ns>\x00<key>\x00<seq8>
func listEntryKey(namespace, key string, seq uint64) []byte {
	var buf bytes.Buffer
	buf.WriteByte(listPrefix)
	buf.WriteByte(0x00)
	buf.WriteString(namespace)
	buf.WriteByte(0x00)
	buf.WriteString(key)
	buf.WriteByte(0x00)
	var seqBytes [8]byte
	binary.BigEndian.PutUint64(seqBytes[:], seq)
	buf.Write(seqBytes[:])
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out
}

// listPrefixBytes returns a prefix for iterating all list entries
// for a (namespace, key): l\x00<ns>\x00<key>\x00
func listPrefixBytes(namespace, key string) []byte {
	return listEntryKey(namespace, key, 0)[:len(listEntryKey(namespace, key, 0))-8]
}

// prefixStart returns the start of the namespace range for deletion.
func prefixStart(namespace string) []byte {
	return valueUserKey(namespace, "")
}

// prefixEnd returns the end of the namespace range for deletion.
func prefixEnd(namespace string) []byte {
	start := valueUserKey(namespace, "")
	return prefixUpperBound(start)
}

// prefixUpperBound returns the first key beyond prefix.
func prefixUpperBound(prefix []byte) []byte {
	upper := make([]byte, len(prefix))
	copy(upper, prefix)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] < 0xff {
			upper[i]++
			return upper[:i+1]
		}
	}
	// Overflow: return a key that is longer than prefix by one byte.
	upper = append(prefix, 0xff)
	return upper
}

func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// copyDir copies all files from src to dst recursively.
func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		srcFile, err := os.Open(path)
		if err != nil {
			return err
		}
		defer srcFile.Close()
		dstFile, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode())
		if err != nil {
			return err
		}
		defer dstFile.Close()
		_, err = io.Copy(dstFile, srcFile)
		return err
	})
}
