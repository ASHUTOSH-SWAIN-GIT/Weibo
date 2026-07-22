package source

import (
	"encoding/json"
	"testing"

	"github.com/segmentio/kafka-go"
)

// TestCheckpointOffset_AllPartitions verifies that CheckpointOffset reports
// the next offset to read for EVERY consumed partition — not just one, which
// was the reader.Stats() bug that silently dropped multi-partition progress
// from checkpoints in consumer-group mode.
func TestCheckpointOffset_AllPartitions(t *testing.T) {
	k := NewKafkaSource(
		KafkaBrokers("localhost:9092"),
		KafkaTopic("orders"),
		KafkaGroupID("g"),
	)

	// A single consumer-group reader consumes interleaved partitions.
	k.trackOffset(kafka.Message{Partition: 0, Offset: 10})
	k.trackOffset(kafka.Message{Partition: 1, Offset: 20})
	k.trackOffset(kafka.Message{Partition: 2, Offset: 5})
	k.trackOffset(kafka.Message{Partition: 0, Offset: 11}) // later offset wins

	data, err := k.CheckpointOffset()
	if err != nil {
		t.Fatalf("CheckpointOffset: %v", err)
	}
	var got map[string]int64
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	want := map[string]int64{"0": 12, "1": 21, "2": 6} // nextOffset = lastOffset+1
	if len(got) != len(want) {
		t.Fatalf("partition count: got %d (%v), want %d", len(got), got, len(want))
	}
	for p, w := range want {
		if got[p] != w {
			t.Errorf("partition %s: got offset %d, want %d", p, got[p], w)
		}
	}
}

// TestCheckpointOffset_RoundTripsThroughRestore checks the checkpoint offset
// format is exactly what RestoreOffset consumes.
func TestCheckpointOffset_RoundTripsThroughRestore(t *testing.T) {
	k := NewKafkaSource(KafkaBrokers("localhost:9092"), KafkaTopic("t"), KafkaGroupID("g"))
	k.trackOffset(kafka.Message{Partition: 3, Offset: 99})

	data, err := k.CheckpointOffset()
	if err != nil {
		t.Fatalf("CheckpointOffset: %v", err)
	}
	k2 := NewKafkaSource(KafkaBrokers("localhost:9092"), KafkaTopic("t"), KafkaGroupID("g"))
	if err := k2.RestoreOffset(data); err != nil {
		t.Fatalf("RestoreOffset: %v", err)
	}
	if k2.restoredOffsets[3] != 100 {
		t.Errorf("restored offset partition 3: got %d, want 100", k2.restoredOffsets[3])
	}
}
