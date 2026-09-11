package source

import (
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
	k.offsets.track(kafka.Message{Topic: "orders", Partition: 0, Offset: 10})
	k.offsets.track(kafka.Message{Topic: "orders", Partition: 1, Offset: 20})
	k.offsets.track(kafka.Message{Topic: "orders", Partition: 2, Offset: 5})
	k.offsets.track(kafka.Message{Topic: "orders", Partition: 0, Offset: 11}) // later offset wins

	data, err := k.CheckpointOffset()
	if err != nil {
		t.Fatalf("CheckpointOffset: %v", err)
	}
	got := decodePositionMap(t, data, "")

	want := map[string]int64{"orders/0": 12, "orders/1": 21, "orders/2": 6}
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
	k.offsets.track(kafka.Message{Topic: "t", Partition: 3, Offset: 99})

	data, err := k.CheckpointOffset()
	if err != nil {
		t.Fatalf("CheckpointOffset: %v", err)
	}
	k2 := NewKafkaSource(KafkaBrokers("localhost:9092"), KafkaTopic("t"), KafkaGroupID("g"))
	if err := k2.RestoreOffset(data); err != nil {
		t.Fatalf("RestoreOffset: %v", err)
	}
	if off, ok := k2.offsets.restoredOffset("t", 3); !ok || off != 100 {
		t.Errorf("restored offset partition 3: got %d (ok=%v), want 100", off, ok)
	}

	// A partition that receives no new message this run must still appear in
	// the next checkpoint at its restored position (only partition 5 gets a
	// new message; partition 3 stays quiet but must be retained).
	k2.offsets.track(kafka.Message{Topic: "t", Partition: 5, Offset: 7})
	data2, err := k2.CheckpointOffset()
	if err != nil {
		t.Fatalf("CheckpointOffset: %v", err)
	}
	got := decodePositionMap(t, data2, "")
	if got["t/3"] != 100 {
		t.Errorf("quiet restored partition 3 dropped from checkpoint: got %v", got)
	}
	if got["t/5"] != 8 {
		t.Errorf("partition 5: got %d, want 8", got["t/5"])
	}
}

func TestCheckpointOffset_DistinguishesTopicsWithSamePartition(t *testing.T) {
	k := NewKafkaSource(KafkaBrokers("localhost:9092"), KafkaTopics("orders", "payments"), KafkaGroupID("g"))
	k.offsets.track(kafka.Message{Topic: "orders", Partition: 0, Offset: 4})
	k.offsets.track(kafka.Message{Topic: "payments", Partition: 0, Offset: 8})
	data, err := k.CheckpointOffset()
	if err != nil {
		t.Fatal(err)
	}
	got := decodePositionMap(t, data, "")
	if got["orders/0"] != 5 || got["payments/0"] != 9 || len(got) != 2 {
		t.Fatalf("topic-aware positions = %v", got)
	}
}

func TestRestoreOffset_LegacySingleTopicOnly(t *testing.T) {
	single := NewKafkaSource(KafkaBrokers("localhost:9092"), KafkaTopic("orders"), KafkaGroupID("g"))
	if err := single.RestoreOffset([]byte(`{"0":12}`)); err != nil {
		t.Fatalf("single-topic legacy restore: %v", err)
	}
	if off, ok := single.offsets.restoredOffset("orders", 0); !ok || off != 12 {
		t.Fatalf("legacy restored offset = %d, %v", off, ok)
	}
	multi := NewKafkaSource(KafkaBrokers("localhost:9092"), KafkaTopics("orders", "payments"), KafkaGroupID("g"))
	if err := multi.RestoreOffset([]byte(`{"0":12}`)); err == nil {
		t.Fatal("ambiguous multi-topic legacy restore unexpectedly succeeded")
	}
}
