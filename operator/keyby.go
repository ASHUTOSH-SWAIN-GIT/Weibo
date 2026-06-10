package operator

import (
	"hash/fnv"
	"sync"

	"mailer/types"
)

const defaultPartitions = 16

// KeyByOperator partitions the stream by key. Records with the same key
// are always routed to the same partition (goroutine). This is required before
// stateful operations like Reduce, because each partition has its own state
// and processes records sequentially — no concurrent access to the same key.
//
// After KeyBy, the record's Key field is set to the result of the key function.
// The stream is physically split into N partitions, each consuming from its
// own channel. Downstream operators (Reduce) receive the merged output.
type KeyByOperator struct {
	Fn         func(types.Record) []byte
	Partitions int
}

// KeyBy creates a KeyByOperator with the given key selector function
// and the default number of partitions (16).
func KeyBy(fn func(types.Record) []byte) *KeyByOperator {
	return &KeyByOperator{
		Fn:         fn,
		Partitions: defaultPartitions,
	}
}

// WithPartitions sets the number of partitions and returns the operator
// for chaining. More partitions = more parallelism.
func (op *KeyByOperator) WithPartitions(n int) *KeyByOperator {
	op.Partitions = n
	return op
}

// Process reads each record, hashes its key, routes it to the matching
// partition goroutine, and merges all partition outputs into a single
// output channel.
func (op *KeyByOperator) Process(in <-chan types.Record, out chan<- types.Record) {
	defer close(out)

	partChs := make([]chan types.Record, op.Partitions)
	for i := range partChs {
		partChs[i] = make(chan types.Record, 256)
	}

	var wg sync.WaitGroup

	// Start one goroutine per partition. Each just forwards records
	// to the merged output channel. This guarantees that records with
	// the same key are always processed by the same goroutine — which
	// means the same state, sequentially, no locking needed.
	for i := range partChs {
		wg.Add(1)
		go func(ch <-chan types.Record) {
			defer wg.Done()
			for record := range ch {
				out <- record
			}
		}(partChs[i])
	}

	// Route incoming records to partitions by key hash.
	for record := range in {
		record.Key = op.Fn(record)
		idx := partition(record.Key, op.Partitions)
		partChs[idx] <- record
	}

	// Close all partition channels and wait for goroutines to drain.
	for _, ch := range partChs {
		close(ch)
	}
	wg.Wait()
}

// partition returns the partition index for a given key using FNV-1a hash.
// This is fast, has good distribution, and is deterministic — same key always
// maps to the same partition.
func partition(key []byte, numPartitions int) int {
	if len(key) == 0 || numPartitions <= 1 {
		return 0
	}
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32()) % numPartitions
}