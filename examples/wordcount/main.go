package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"

	"mailer"
	"mailer/operator"
	"mailer/sink"
	"mailer/source"
	"mailer/types"
)

func main() {
	env := mailer.NewEnv()

	sentences := []string{
		"hello world",
		"hello mailer",
		"world of stream processing",
		"hello stream processing",
	}

	keys := make([]string, len(sentences))
	for i := range sentences {
		keys[i] = "sentence"
	}

	src := source.FromSlices(keys, sentences)

	// Pipeline:
	//   Source → FlatMap(split into words) → KeyBy(word) → Reduce(count) → Map(format) → Sink(stdout)
	env.
		FromSource(src).
		FlatMap(func(r types.Record) []types.Record {
			words := strings.Fields(string(r.Value))
			records := make([]types.Record, 0, len(words))
			for _, word := range words {
				records = append(records, types.NewRecord([]byte(word), []byte(word)))
			}
			return records
		}).
		KeyBy(func(r types.Record) []byte { return r.Value }).
		Reduce(operator.ReduceFn(countWords)).
		Map(func(r types.Record) types.Record {
			count := binary.BigEndian.Uint64(r.Value)
			fmt.Printf("%s: %d\n", string(r.Key), count)
			return r
		}).
		ToSink(sink.NewBlackholeSink())

	if err := env.Execute(context.Background()); err != nil {
		fmt.Printf("pipeline error: %v\n", err)
	}
}

// countWords counts how many times each word (key) appears.
// accum is nil on first call. Each call increments the count
// stored as an 8-byte big-endian uint64.
func countWords(accum []byte, curr types.Record) []byte {
	count := uint64(0)
	if accum != nil {
		count = binary.BigEndian.Uint64(accum)
	}
	count++

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, count)
	return buf
}
