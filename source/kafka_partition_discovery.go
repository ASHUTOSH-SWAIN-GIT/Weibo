package source

import (
	"fmt"

	"github.com/segmentio/kafka-go"
)

// partitionDiscovery queries Kafka metadata to enumerate the partitions of a
// single topic. It only discovers partition IDs — it never creates or runs
// readers; that is the ReaderSupervisor's job.
type partitionDiscovery struct {
	brokers []string
	topic   string
}

// discover dials the first broker and returns the topic's partition IDs in
// broker-reported order. The returned error is surfaced by the caller so the
// existing fail-fast (panic at construction) behaviour is preserved.
func (d partitionDiscovery) discover() ([]int, error) {
	conn, err := kafka.Dial("tcp", d.brokers[0])
	if err != nil {
		return nil, fmt.Errorf("cannot dial broker for partitions: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(d.topic)
	if err != nil {
		return nil, fmt.Errorf("cannot read partitions for %s: %w", d.topic, err)
	}

	ids := make([]int, 0, len(partitions))
	for _, p := range partitions {
		ids = append(ids, p.ID)
	}
	return ids, nil
}
