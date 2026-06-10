package main

import (
	"context"
	"fmt"
	"strings"

	"mailer"
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

	env.
		FromSource(src).
		FlatMap(func(r types.Record) []types.Record {
			words := strings.Fields(string(r.Value))
			records := make([]types.Record, 0, len(words))
			for _, word := range words {
				records = append(records, types.Record{
					Key:   []byte(word),
					Value: []byte(word),
				})
			}
			return records
		}).
		Filter(func(r types.Record) bool {
			return len(r.Value) > 3
		}).
		Map(func(r types.Record) types.Record {
			fmt.Printf("word: %s\n", string(r.Key))
			return r
		}).
		ToSink(sink.NewStdoutSink())

	if err := env.Execute(context.Background()); err != nil {
		fmt.Printf("pipeline error: %v\n", err)
	}
}