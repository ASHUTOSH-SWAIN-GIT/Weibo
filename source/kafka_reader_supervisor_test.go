package source

import "testing"

func TestReaderSupervisor_BuildsSerialReader(t *testing.T) {
	s := newSerialReaderSupervisor(kafkaSourceConfig{
		brokers: []string{"localhost:9092"},
		topic:   "t",
		groupID: "g",
	})
	defer s.closeAll()

	if len(s.readers) != 1 {
		t.Fatalf("readers: got %d, want 1", len(s.readers))
	}
	if len(s.partitionIDs) != 1 || s.partitionIDs[0] != -1 {
		t.Errorf("partitionIDs: got %v, want [-1]", s.partitionIDs)
	}
	if s.primary() == nil {
		t.Error("primary: got nil reader")
	}
}

func TestBuildPartitionReader_PinsPartition(t *testing.T) {
	// buildPartitionReader must not dial; it only constructs a lazy reader.
	r := buildPartitionReader(kafkaSourceConfig{
		brokers: []string{"localhost:9092"},
		topic:   "t",
	}, 3)
	defer r.Close()
	if got := r.Config().Partition; got != 3 {
		t.Errorf("partition: got %d, want 3", got)
	}
}
