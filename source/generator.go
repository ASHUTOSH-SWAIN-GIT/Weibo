package source

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// GeneratorSource produces a fixed slice of records and then closes.
// Useful for testing and examples.
type GeneratorSource struct {
	records []types.Record
	emitted atomic.Int64 // records sent so far, for OperationalState
}

// NewGeneratorSource creates a source that emits the given records in order.
func NewGeneratorSource(records []types.Record) *GeneratorSource {
	return &GeneratorSource{records: records}
}

// FromSlices is a convenience function that creates records from string key-value pairs.
// Each record gets an incrementing offset and the current timestamp.
func FromSlices(keys []string, values []string) *GeneratorSource {
	if len(keys) != len(values) {
		panic(fmt.Sprintf("source.FromSlices: keys and values length mismatch (%d != %d)", len(keys), len(values)))
	}
	records := make([]types.Record, len(keys))
	for i := range keys {
		records[i] = types.Record{
			Key:       []byte(keys[i]),
			Value:     []byte(values[i]),
			Offset:    int64(i),
			Timestamp: time.Now().UTC(),
		}
	}
	return NewGeneratorSource(records)
}

// Run emits all records into the output channel and then returns.
// The channel owner (StreamExecutionEnv) is responsible for closing it.
func (s *GeneratorSource) Run(ctx context.Context, out chan<- types.Record) error {
	for _, record := range s.records {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- record:
			s.emitted.Add(1)
		}
	}
	return nil
}

// Describe returns metadata for the dashboard.
func (s *GeneratorSource) Describe() SourceInfo {
	return SourceInfo{
		Type: "Generator",
		Props: map[string]string{
			"records": fmt.Sprintf("%d", len(s.records)),
		},
	}
}

// generatorPosition is the OperationalState shape for GeneratorSource: how
// far through the fixed record slice this run has emitted.
type generatorPosition struct {
	Emitted int64 `json:"emitted"`
	Total   int   `json:"total"`
}

// OperationalState reports emit progress through the fixed record slice —
// the closest a synthetic in-memory source has to a Kafka-style position.
func (s *GeneratorSource) OperationalState() any {
	return generatorPosition{Emitted: s.emitted.Load(), Total: len(s.records)}
}
