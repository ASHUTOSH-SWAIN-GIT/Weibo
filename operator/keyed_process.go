package operator

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/state"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

const (
	keyedProcessTimersNS = "_timers"
	keyedProcessWMNS     = "_watermark"
	keyedProcessWMKey    = "wm"
)

// KeyedProcessFn processes one record with access to state scoped to the
// record key. It may emit zero or more records.
type KeyedProcessFn func(*KeyedContext, types.Record) ([]types.Record, error)

// TimerFn is called when an event-time timer registered for a key becomes due.
type TimerFn func(*KeyedContext, time.Time) ([]types.Record, error)

// KeyedProcessOperator is a user-facing keyed state/process operator. It is
// intended to run after KeyBy, where each keyed worker gets an isolated clone.
type KeyedProcessOperator struct {
	Fn      KeyedProcessFn
	OnTimer TimerFn
	Label   string

	backend state.StateBackend
	names   map[string]struct{}

	watermark time.Time

	barrierSnapshot func(checkpointID string, snapshot []byte, err error)
	nativeSnapshot  func(checkpointID string) ([]byte, error)
}

// KeyedProcess creates a keyed process operator with a default in-memory
// backend until the engine injects one.
func KeyedProcess(fn KeyedProcessFn, onTimer TimerFn) *KeyedProcessOperator {
	return &KeyedProcessOperator{
		Fn:      fn,
		OnTimer: onTimer,
		backend: state.NewMemoryBackend(),
		names:   make(map[string]struct{}),
	}
}

func (op *KeyedProcessOperator) Name() string     { return "KeyedProcess" }
func (op *KeyedProcessOperator) GetLabel() string { return op.Label }
func (op *KeyedProcessOperator) DescribeOp() OperatorMeta {
	return OperatorMeta{Type: "KeyedProcess", Label: op.Label}
}

func (op *KeyedProcessOperator) SetStateBackend(b state.StateBackend) { op.backend = b }
func (op *KeyedProcessOperator) Backend() state.StateBackend          { return op.backend }
func (op *KeyedProcessOperator) SetBarrierSnapshot(fn func(checkpointID string, snapshot []byte, err error)) {
	op.barrierSnapshot = fn
}
func (op *KeyedProcessOperator) SetNativeSnapshot(fn func(checkpointID string) ([]byte, error)) {
	op.nativeSnapshot = fn
}

func (op *KeyedProcessOperator) Clone() Operator {
	names := make(map[string]struct{}, len(op.names))
	for name := range op.names {
		names[name] = struct{}{}
	}
	return &KeyedProcessOperator{
		Fn:      op.Fn,
		OnTimer: op.OnTimer,
		Label:   op.Label,
		backend: state.NewMemoryBackend(),
		names:   names,
	}
}

func (op *KeyedProcessOperator) Process(in <-chan types.Record, out chan<- types.Record) {
	defer close(out)
	op.loadWatermark()
	for r := range in {
		switch {
		case r.IsBarrier:
			op.snapshotBarrier(r, out)
		case r.IsWatermark:
			if r.Timestamp.After(op.watermark) {
				op.watermark = r.Timestamp
				op.storeWatermark()
			}
			op.fireTimers(out)
			out <- r
		default:
			ctx := op.contextFor(string(r.Key), r.Timestamp)
			records, err := op.Fn(ctx, r)
			if err != nil {
				panic(&OperatorError{Operator: op.Name(), Label: op.Label, Key: r.Key, Err: err})
			}
			for _, next := range records {
				out <- next
			}
		}
	}
}

func (op *KeyedProcessOperator) snapshotBarrier(r types.Record, out chan<- types.Record) {
	if op.barrierSnapshot != nil {
		var snap []byte
		var err error
		if op.nativeSnapshot != nil {
			snap, err = op.nativeSnapshot(r.CheckpointID)
		} else {
			snap, err = op.Snapshot()
		}
		op.barrierSnapshot(r.CheckpointID, snap, err)
	}
	out <- r
}

func (op *KeyedProcessOperator) contextFor(key string, ts time.Time) *KeyedContext {
	return &KeyedContext{op: op, key: key, timestamp: ts}
}

func (op *KeyedProcessOperator) remember(name string) {
	if op.names == nil {
		op.names = make(map[string]struct{})
	}
	op.names[name] = struct{}{}
}

func (op *KeyedProcessOperator) loadWatermark() {
	vs := op.backend.ValueState(keyedProcessWMNS)
	vs.SetKey(keyedProcessWMKey)
	raw := vs.Get()
	if len(raw) == 8 {
		op.watermark = time.Unix(0, int64(binary.BigEndian.Uint64(raw))).UTC()
	}
}

func (op *KeyedProcessOperator) storeWatermark() {
	vs := op.backend.ValueState(keyedProcessWMNS)
	vs.SetKey(keyedProcessWMKey)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(op.watermark.UnixNano()))
	vs.Set(buf[:])
}

func (op *KeyedProcessOperator) fireTimers(out chan<- types.Record) {
	if op.OnTimer == nil || op.watermark.IsZero() {
		return
	}
	timers := op.backend.ValueState(keyedProcessTimersNS)
	for _, key := range timers.Keys() {
		timers.SetKey(key)
		due, future := splitTimers(timers.Get(), op.watermark)
		if len(due) == 0 {
			continue
		}
		if len(future) == 0 {
			timers.Clear()
		} else {
			timers.Set(marshalTimers(future))
		}
		ctx := op.contextFor(key, op.watermark)
		for _, timerTS := range due {
			records, err := op.OnTimer(ctx, time.Unix(0, timerTS).UTC())
			if err != nil {
				panic(&OperatorError{Operator: op.Name(), Label: op.Label, Key: []byte(key), Err: err})
			}
			for _, r := range records {
				out <- r
			}
		}
	}
}

// KeyedContext exposes state and event-time timers scoped to one key.
type KeyedContext struct {
	op        *KeyedProcessOperator
	key       string
	timestamp time.Time
}

func (c *KeyedContext) Key() []byte                 { return []byte(c.key) }
func (c *KeyedContext) KeyString() string           { return c.key }
func (c *KeyedContext) Timestamp() time.Time        { return c.timestamp }
func (c *KeyedContext) CurrentWatermark() time.Time { return c.op.watermark }

func (c *KeyedContext) ValueState(name string) *KeyedValueState {
	c.op.remember(name)
	vs := c.op.backend.ValueState(name)
	vs.SetKey(c.key)
	return &KeyedValueState{state: vs}
}

func (c *KeyedContext) RegisterEventTimeTimer(ts time.Time) {
	timers := c.op.backend.ValueState(keyedProcessTimersNS)
	timers.SetKey(c.key)
	existing := unmarshalTimers(timers.Get())
	n := ts.UnixNano()
	for _, v := range existing {
		if v == n {
			return
		}
	}
	existing = append(existing, n)
	sort.Slice(existing, func(i, j int) bool { return existing[i] < existing[j] })
	timers.Set(marshalTimers(existing))
}

// KeyedValueState is a convenience wrapper around state.ValueState.
type KeyedValueState struct {
	state state.ValueState
}

func (s *KeyedValueState) Get() []byte  { return s.state.Get() }
func (s *KeyedValueState) Set(v []byte) { s.state.Set(v) }
func (s *KeyedValueState) Clear()       { s.state.Clear() }
func (s *KeyedValueState) GetJSON(v any) error {
	raw := s.state.Get()
	if len(raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw, v)
}
func (s *KeyedValueState) SetJSON(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	s.state.Set(b)
	return nil
}

type keyedProcessSnapshot struct {
	Namespaces map[string]map[string][]byte `json:"namespaces"`
	Timers     map[string][]byte            `json:"timers"`
	Watermark  int64                        `json:"watermark"`
}

func (op *KeyedProcessOperator) Snapshot() ([]byte, error) {
	ns := make(map[string]map[string][]byte, len(op.names))
	for name := range op.names {
		ns[name] = op.backend.ValueState(name).SnapshotAll()
	}
	return json.Marshal(keyedProcessSnapshot{
		Namespaces: ns,
		Timers:     op.backend.ValueState(keyedProcessTimersNS).SnapshotAll(),
		Watermark:  unixNanoOrZero(op.watermark),
	})
}

func (op *KeyedProcessOperator) Restore(data []byte) error {
	var snap keyedProcessSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	for name, entries := range snap.Namespaces {
		op.remember(name)
		if err := op.backend.ValueState(name).RestoreAll(entries); err != nil {
			return fmt.Errorf("restore namespace %q: %w", name, err)
		}
	}
	if err := op.backend.ValueState(keyedProcessTimersNS).RestoreAll(snap.Timers); err != nil {
		return fmt.Errorf("restore timers: %w", err)
	}
	op.watermark = timeFromUnixNanoOrZero(snap.Watermark)
	op.storeWatermark()
	return nil
}

func splitTimers(raw []byte, wm time.Time) (due, future []int64) {
	for _, ts := range unmarshalTimers(raw) {
		if !time.Unix(0, ts).After(wm) {
			due = append(due, ts)
		} else {
			future = append(future, ts)
		}
	}
	return due, future
}

func unmarshalTimers(raw []byte) []int64 {
	if len(raw) == 0 {
		return nil
	}
	var out []int64
	_ = json.Unmarshal(raw, &out)
	return out
}

func marshalTimers(v []int64) []byte {
	b, _ := json.Marshal(v)
	return b
}
