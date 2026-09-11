package sink

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestInspectMarkerPartition(t *testing.T) {
	tests := []struct {
		name        string
		partition   kgo.FetchPartition
		boundary    int64
		wantFound   bool
		wantReached bool
	}{
		{name: "marker present", partition: kgo.FetchPartition{Records: []*kgo.Record{{Offset: 2, Key: []byte("txn"), Value: []byte("cp")}}}, boundary: 5, wantFound: true, wantReached: true},
		{name: "more batches remain", partition: kgo.FetchPartition{Records: []*kgo.Record{{Offset: 2}}}, boundary: 5},
		{name: "visible record reaches boundary", partition: kgo.FetchPartition{Records: []*kgo.Record{{Offset: 4}}}, boundary: 5, wantReached: true},
		{name: "aborted tail drained", partition: kgo.FetchPartition{LastStableOffset: 8}, boundary: 8, wantReached: true},
		{name: "stable boundary not reached", partition: kgo.FetchPartition{LastStableOffset: 7}, boundary: 8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			found, reached := inspectMarkerPartition(tt.partition, tt.boundary, "txn", "cp")
			if found != tt.wantFound || reached != tt.wantReached {
				t.Fatalf("got found=%v reached=%v", found, reached)
			}
		})
	}
}

func TestAllMarkerPartitionsDone(t *testing.T) {
	boundaries := map[int32]int64{0: 4, 1: 8}
	if allMarkerPartitionsDone(boundaries, map[int32]int64{0: 4, 1: 7}) {
		t.Fatal("unfinished partition reported complete")
	}
	if !allMarkerPartitionsDone(boundaries, map[int32]int64{0: 4, 1: 8}) {
		t.Fatal("all partitions reported unfinished")
	}
}
