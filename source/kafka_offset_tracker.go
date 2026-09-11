package source

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/segmentio/kafka-go"
)

// offsetTracker records per-partition consumption progress for a KafkaSource
// and produces / restores the checkpoint offset map.
//
// The meaning of a stored offset is unchanged: it is the NEXT offset to read
// for a partition (lastConsumedOffset + 1). reader.Stats() surfaces only a
// single partition in consumer-group mode, so this map — not Stats — is the
// source of truth for checkpoints across every partition.
type offsetTracker struct {
	// mu guards consumed and restored.
	mu sync.Mutex

	// consumed maps partition -> next offset to read, advanced as each
	// message is consumed. It is what CheckpointOffset snapshots.
	consumed map[int]int64

	// restored maps partition -> seek target populated from a checkpoint by
	// restore. Readers seek to these on startup.
	restored  map[int]int64
	progress  map[topicPartition]PartitionProgress
	committed map[int]int64
}

type topicPartition struct {
	topic     string
	partition int
}

// PartitionProgress is one Kafka partition's live consumption position.
type PartitionProgress struct {
	Topic            string `json:"topic"`
	Partition        int    `json:"partition"`
	CurrentOffset    int64  `json:"currentOffset"`
	CheckpointOffset int64  `json:"checkpointOffset,omitempty"`
	HighWatermark    int64  `json:"highWatermark,omitempty"`
	Lag              int64  `json:"lag"`
}

// newOffsetTracker returns an offsetTracker with initialised maps.
func newOffsetTracker() *offsetTracker {
	return &offsetTracker{
		consumed:  make(map[int]int64),
		restored:  make(map[int]int64),
		progress:  make(map[topicPartition]PartitionProgress),
		committed: make(map[int]int64),
	}
}

// track records progress past a consumed message so a snapshot reports the
// next offset to read for every partition — not just the single partition
// reader.Stats() happens to surface in consumer-group mode.
func (t *offsetTracker) track(msg kafka.Message) {
	t.mu.Lock()
	t.consumed[msg.Partition] = msg.Offset + 1
	key := topicPartition{topic: msg.Topic, partition: msg.Partition}
	p := t.progress[key]
	p.Topic, p.Partition = msg.Topic, msg.Partition
	p.CurrentOffset = msg.Offset + 1
	p.CheckpointOffset = max(t.restored[msg.Partition], t.committed[msg.Partition])
	p.HighWatermark = msg.HighWaterMark
	p.Lag = max(0, p.HighWatermark-p.CurrentOffset)
	t.progress[key] = p
	t.mu.Unlock()
}

func (t *offsetTracker) markCommitted(msgs ...kafka.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, msg := range msgs {
		next := msg.Offset + 1
		t.committed[msg.Partition] = next
		for key, p := range t.progress {
			if key.partition == msg.Partition && (msg.Topic == "" || key.topic == msg.Topic) {
				p.CheckpointOffset = next
				t.progress[key] = p
			}
		}
	}
}

func (t *offsetTracker) operationalState() []PartitionProgress {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]PartitionProgress, 0, len(t.progress))
	for _, p := range t.progress {
		out = append(out, p)
	}
	slices.SortFunc(out, func(a, b PartitionProgress) int {
		if c := strings.Compare(a.Topic, b.Topic); c != 0 {
			return c
		}
		return cmp.Compare(a.Partition, b.Partition)
	})
	return out
}

// snapshot returns the current position as JSON bytes:
// {"<partition>": <nextOffsetToRead>} for every partition consumed so far.
// Keyed by partition only, matching restore/CommitOffsets and the
// barrier-aligned offset map.
func (t *offsetTracker) snapshot() ([]byte, error) {
	t.mu.Lock()
	offsets := make(map[string]int64, len(t.consumed))
	for part, next := range t.consumed {
		offsets[strconv.Itoa(part)] = next
	}
	t.mu.Unlock()
	return json.Marshal(offsets)
}

// restore loads per-partition offsets from a checkpoint. Each partition's
// offset becomes both a seek target (restored) and a seed in consumed, so a
// partition that receives no new message this run still carries its restored
// position into the next checkpoint — otherwise a quiet partition would be
// dropped and a later restart would not resume it.
func (t *offsetTracker) restore(data []byte) error {
	var offsets map[string]int64
	if err := json.Unmarshal(data, &offsets); err != nil {
		return fmt.Errorf("restore offset: unmarshal: %w", err)
	}
	t.mu.Lock()
	for partStr, off := range offsets {
		var partInt int
		fmt.Sscanf(partStr, "%d", &partInt)
		t.restored[partInt] = off
		t.consumed[partInt] = off
	}
	t.mu.Unlock()
	return nil
}

// hasRestored reports whether any seek targets were loaded from a checkpoint.
func (t *offsetTracker) hasRestored() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.restored) > 0
}

// restoredOffset returns the seek target for a partition, if one was restored.
func (t *offsetTracker) restoredOffset(part int) (int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	off, ok := t.restored[part]
	return off, ok
}
