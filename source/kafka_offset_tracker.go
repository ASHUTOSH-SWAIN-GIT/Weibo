package source

import (
	"cmp"
	"slices"
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

	// consumed maps topic+partition -> next offset to read, advanced as each
	// message is consumed. It is what CheckpointOffset snapshots.
	consumed map[topicPartition]int64

	// restored maps partition -> seek target populated from a checkpoint by
	// restore. Readers seek to these on startup.
	restored  map[topicPartition]int64
	progress  map[topicPartition]PartitionProgress
	committed map[topicPartition]int64
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
		consumed:  make(map[topicPartition]int64),
		restored:  make(map[topicPartition]int64),
		progress:  make(map[topicPartition]PartitionProgress),
		committed: make(map[topicPartition]int64),
	}
}

// track records progress past a consumed message so a snapshot reports the
// next offset to read for every partition — not just the single partition
// reader.Stats() happens to surface in consumer-group mode.
func (t *offsetTracker) track(msg kafka.Message) {
	t.mu.Lock()
	key := topicPartition{topic: msg.Topic, partition: msg.Partition}
	t.consumed[key] = msg.Offset + 1
	p := t.progress[key]
	p.Topic, p.Partition = msg.Topic, msg.Partition
	p.CurrentOffset = msg.Offset + 1
	p.CheckpointOffset = max(t.restored[key], t.committed[key])
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
		key := topicPartition{topic: msg.Topic, partition: msg.Partition}
		t.committed[key] = next
		if p, ok := t.progress[key]; ok {
			p.CheckpointOffset = next
			t.progress[key] = p
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

// snapshot returns every current topic+partition position in the shared
// versioned checkpoint format.
func (t *offsetTracker) snapshot() ([]byte, error) {
	t.mu.Lock()
	positions := make([]Position, 0, len(t.consumed))
	for key, next := range t.consumed {
		positions = append(positions, Position{Source: key.topic, Partition: key.partition, Offset: next})
	}
	t.mu.Unlock()
	return EncodePositions(positions)
}

// restore loads per-partition offsets from a checkpoint. Each partition's
// offset becomes both a seek target (restored) and a seed in consumed, so a
// partition that receives no new message this run still carries its restored
// position into the next checkpoint — otherwise a quiet partition would be
// dropped and a later restart would not resume it.
func (t *offsetTracker) restore(data []byte, legacySource string) error {
	positions, err := DecodePositions(data, legacySource)
	if err != nil {
		return err
	}
	t.mu.Lock()
	for _, position := range positions {
		key := topicPartition{topic: position.Source, partition: position.Partition}
		t.restored[key] = position.Offset
		t.consumed[key] = position.Offset
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
func (t *offsetTracker) restoredOffset(topic string, part int) (int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	off, ok := t.restored[topicPartition{topic: topic, partition: part}]
	return off, ok
}

func (t *offsetTracker) restoredPositions() []Position {
	t.mu.Lock()
	defer t.mu.Unlock()
	positions := make([]Position, 0, len(t.restored))
	for key, offset := range t.restored {
		positions = append(positions, Position{Source: key.topic, Partition: key.partition, Offset: offset})
	}
	return positions
}
