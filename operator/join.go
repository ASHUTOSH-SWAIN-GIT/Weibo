package operator

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/state"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

const (
	joinLeftNS  = "join_left"
	joinRightNS = "join_right"
	joinWMNS    = "join_watermarks"
	joinLeftWM  = "left"
	joinRightWM = "right"
)

// JoinFn combines one left and one right record into the output record.
// The default emits a JSON object with the raw left/right payloads.
type JoinFn func(left, right types.Record) types.Record

// IntervalJoinOperator joins two logical streams multiplexed through one
// channel. Records are assigned to a side by Record.Source. For every left
// record L and right record R with the same key, the operator emits when:
//
//	L.Timestamp - Before <= R.Timestamp <= L.Timestamp + After
//
// Watermarks are also side-aware via Record.Source. The output watermark is the
// minimum of the two side watermarks, so downstream operators never advance past
// the slower input. Buffered state is evicted only when that aligned watermark
// proves no future record can still match it.
type IntervalJoinOperator struct {
	LeftSource  string
	RightSource string
	Before      time.Duration
	After       time.Duration
	Fn          JoinFn
	Label       string

	backend state.StateBackend

	leftWatermark    time.Time
	rightWatermark   time.Time
	emittedWatermark time.Time

	barrierSnapshot func(checkpointID string, snapshot []byte, err error)
	nativeSnapshot  func(checkpointID string) ([]byte, error)
}

// IntervalJoin creates a keyed interval join over two record sources.
func IntervalJoin(leftSource, rightSource string, before, after time.Duration, fn JoinFn) *IntervalJoinOperator {
	if before < 0 {
		before = 0
	}
	if after < 0 {
		after = 0
	}
	return &IntervalJoinOperator{
		LeftSource:  leftSource,
		RightSource: rightSource,
		Before:      before,
		After:       after,
		Fn:          fn,
		backend:     state.NewMemoryBackend(),
	}
}

// JoinWithin creates a symmetric interval join: records match when their event
// times are within d of each other.
func JoinWithin(leftSource, rightSource string, d time.Duration, fn JoinFn) *IntervalJoinOperator {
	return IntervalJoin(leftSource, rightSource, d, d, fn)
}

func (op *IntervalJoinOperator) Name() string     { return "IntervalJoin" }
func (op *IntervalJoinOperator) GetLabel() string { return op.Label }

func (op *IntervalJoinOperator) DescribeOp() OperatorMeta {
	return OperatorMeta{
		Type:  "IntervalJoin",
		Label: op.Label,
		Config: map[string]string{
			"left":   op.LeftSource,
			"right":  op.RightSource,
			"before": op.Before.String(),
			"after":  op.After.String(),
		},
	}
}

func (op *IntervalJoinOperator) SetStateBackend(b state.StateBackend) { op.backend = b }

func (op *IntervalJoinOperator) SetBarrierSnapshot(fn func(checkpointID string, snapshot []byte, err error)) {
	op.barrierSnapshot = fn
}

func (op *IntervalJoinOperator) SetNativeSnapshot(fn func(checkpointID string) ([]byte, error)) {
	op.nativeSnapshot = fn
}

func (op *IntervalJoinOperator) Backend() state.StateBackend { return op.backend }

func (op *IntervalJoinOperator) Clone() Operator {
	return &IntervalJoinOperator{
		LeftSource:  op.LeftSource,
		RightSource: op.RightSource,
		Before:      op.Before,
		After:       op.After,
		Fn:          op.Fn,
		Label:       op.Label,
		backend:     state.NewMemoryBackend(),
	}
}

func (op *IntervalJoinOperator) Process(in <-chan types.Record, out chan<- types.Record) {
	defer close(out)

	op.loadWatermarks()
	left := op.backend.ListState(joinLeftNS)
	right := op.backend.ListState(joinRightNS)

	for r := range in {
		switch {
		case r.IsWatermark:
			op.handleWatermark(r, left, right, out)
		case r.IsBarrier:
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
		default:
			op.handleRecord(r, left, right, out)
		}
	}
}

func (op *IntervalJoinOperator) handleRecord(r types.Record, left, right state.ListState, out chan<- types.Record) {
	side := op.side(r)
	if side == "" {
		return
	}
	key := string(r.Key)
	if side == joinLeftWM {
		op.emitMatches(r, right, false, out)
		left.SetKey(key)
		left.Append(encodeRecord(r))
		return
	}
	op.emitMatches(r, left, true, out)
	right.SetKey(key)
	right.Append(encodeRecord(r))
}

func (op *IntervalJoinOperator) emitMatches(r types.Record, other state.ListState, otherIsLeft bool, out chan<- types.Record) {
	other.SetKey(string(r.Key))
	for _, raw := range other.GetAll() {
		o := decodeRecord(raw)
		var left, right types.Record
		if otherIsLeft {
			left, right = o, r
		} else {
			left, right = r, o
		}
		if op.inRange(left.Timestamp, right.Timestamp) {
			out <- op.join(left, right)
		}
	}
}

func (op *IntervalJoinOperator) inRange(leftTS, rightTS time.Time) bool {
	return !rightTS.Before(leftTS.Add(-op.Before)) && !rightTS.After(leftTS.Add(op.After))
}

func (op *IntervalJoinOperator) join(left, right types.Record) types.Record {
	if op.Fn != nil {
		return op.Fn(left, right)
	}
	val, _ := json.Marshal(map[string]any{
		"left":  jsonValueOrString(left.Value),
		"right": jsonValueOrString(right.Value),
	})
	return types.Record{
		Key:       append([]byte(nil), left.Key...),
		Value:     val,
		Timestamp: maxTime(left.Timestamp, right.Timestamp),
		Headers: map[string][]byte{
			"join_left_source":  []byte(op.LeftSource),
			"join_right_source": []byte(op.RightSource),
		},
	}
}

func jsonValueOrString(v []byte) any {
	if json.Valid(v) {
		return json.RawMessage(v)
	}
	return string(v)
}

func (op *IntervalJoinOperator) side(r types.Record) string {
	switch r.Source {
	case op.LeftSource:
		return joinLeftWM
	case op.RightSource:
		return joinRightWM
	default:
		return ""
	}
}

func (op *IntervalJoinOperator) handleWatermark(r types.Record, left, right state.ListState, out chan<- types.Record) {
	switch r.Source {
	case op.LeftSource:
		if r.Timestamp.After(op.leftWatermark) {
			op.leftWatermark = r.Timestamp
			op.storeWatermark(joinLeftWM, r.Timestamp)
		}
	case op.RightSource:
		if r.Timestamp.After(op.rightWatermark) {
			op.rightWatermark = r.Timestamp
			op.storeWatermark(joinRightWM, r.Timestamp)
		}
	case "":
		if r.Timestamp.After(op.leftWatermark) {
			op.leftWatermark = r.Timestamp
			op.storeWatermark(joinLeftWM, r.Timestamp)
		}
		if r.Timestamp.After(op.rightWatermark) {
			op.rightWatermark = r.Timestamp
			op.storeWatermark(joinRightWM, r.Timestamp)
		}
	default:
		return
	}

	aligned := alignedWatermark(op.leftWatermark, op.rightWatermark)
	if aligned.IsZero() {
		return
	}
	op.evict(left, joinLeftWM, aligned)
	op.evict(right, joinRightWM, aligned)
	if aligned.After(op.emittedWatermark) {
		op.emittedWatermark = aligned
		out <- types.NewWatermark(aligned)
	}
}

func (op *IntervalJoinOperator) evict(ls state.ListState, side string, aligned time.Time) {
	var cutoff time.Time
	if side == joinLeftWM {
		cutoff = aligned.Add(-op.After)
	} else {
		cutoff = aligned.Add(-op.Before)
	}
	for _, key := range ls.Keys() {
		ls.SetKey(key)
		var keep []types.Record
		for _, raw := range ls.GetAll() {
			r := decodeRecord(raw)
			if !r.Timestamp.Before(cutoff) {
				keep = append(keep, r)
			}
		}
		ls.Clear()
		for _, r := range keep {
			ls.Append(encodeRecord(r))
		}
	}
}

func (op *IntervalJoinOperator) loadWatermarks() {
	op.leftWatermark = op.loadWatermark(joinLeftWM)
	op.rightWatermark = op.loadWatermark(joinRightWM)
	op.emittedWatermark = alignedWatermark(op.leftWatermark, op.rightWatermark)
}

func (op *IntervalJoinOperator) loadWatermark(key string) time.Time {
	vs := op.backend.ValueState(joinWMNS)
	vs.SetKey(key)
	raw := vs.Get()
	if len(raw) != 8 {
		return time.Time{}
	}
	return time.Unix(0, int64(binary.BigEndian.Uint64(raw))).UTC()
}

func (op *IntervalJoinOperator) storeWatermark(key string, ts time.Time) {
	vs := op.backend.ValueState(joinWMNS)
	vs.SetKey(key)
	if ts.IsZero() {
		vs.Clear()
		return
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(ts.UnixNano()))
	vs.Set(buf[:])
}

type intervalJoinSnapshot struct {
	LeftWatermark  int64                   `json:"left_watermark"`
	RightWatermark int64                   `json:"right_watermark"`
	Left           map[string][]recordJSON `json:"left"`
	Right          map[string][]recordJSON `json:"right"`
}

func (op *IntervalJoinOperator) Snapshot() ([]byte, error) {
	snap := intervalJoinSnapshot{
		LeftWatermark:  unixNanoOrZero(op.leftWatermark),
		RightWatermark: unixNanoOrZero(op.rightWatermark),
		Left:           snapshotJoinSide(op.backend.ListState(joinLeftNS)),
		Right:          snapshotJoinSide(op.backend.ListState(joinRightNS)),
	}
	return json.Marshal(snap)
}

func snapshotJoinSide(ls state.ListState) map[string][]recordJSON {
	out := map[string][]recordJSON{}
	for _, key := range ls.Keys() {
		ls.SetKey(key)
		for _, raw := range ls.GetAll() {
			out[key] = append(out[key], decodeRecordJSON(raw))
		}
	}
	return out
}

func (op *IntervalJoinOperator) Restore(data []byte) error {
	var snap intervalJoinSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	op.leftWatermark = timeFromUnixNanoOrZero(snap.LeftWatermark)
	op.rightWatermark = timeFromUnixNanoOrZero(snap.RightWatermark)
	op.emittedWatermark = alignedWatermark(op.leftWatermark, op.rightWatermark)
	op.storeWatermark(joinLeftWM, op.leftWatermark)
	op.storeWatermark(joinRightWM, op.rightWatermark)
	restoreJoinSide(op.backend.ListState(joinLeftNS), snap.Left)
	restoreJoinSide(op.backend.ListState(joinRightNS), snap.Right)
	return nil
}

func restoreJoinSide(ls state.ListState, entries map[string][]recordJSON) {
	for _, key := range ls.Keys() {
		ls.SetKey(key)
		ls.Clear()
	}
	for key, recs := range entries {
		ls.SetKey(key)
		for _, r := range recs {
			ls.Append(encodeRecordJSON(r))
		}
	}
}

func alignedWatermark(a, b time.Time) time.Time {
	if a.IsZero() || b.IsZero() {
		return time.Time{}
	}
	if a.Before(b) {
		return a
	}
	return b
}

func unixNanoOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func timeFromUnixNanoOrZero(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func (op *IntervalJoinOperator) Validate() error {
	if op.LeftSource == "" || op.RightSource == "" {
		return fmt.Errorf("join: left and right sources are required")
	}
	if op.LeftSource == op.RightSource {
		return fmt.Errorf("join: left and right sources must differ")
	}
	return nil
}
