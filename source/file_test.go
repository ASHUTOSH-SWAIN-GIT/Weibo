package source

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

func TestFileSource_ReadsLinesAndCheckpoints(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input.ndjson")
	if err := os.WriteFile(path, []byte("{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	src, err := NewFileSourceE(FilePath(path), FileSourceName("orders"), FileDeserialize(JSONLineDeserializer))
	if err != nil {
		t.Fatal(err)
	}
	out := make(chan types.Record, 4)
	if err := src.Run(context.Background(), out); err != nil {
		t.Fatal(err)
	}
	close(out)

	var got []types.Record
	for r := range out {
		got = append(got, r)
	}
	if len(got) != 3 {
		t.Fatalf("records: got %d, want 3", len(got))
	}
	if got[0].Source != "orders" || got[0].Offset != 0 || string(got[0].Key) != "0" {
		t.Fatalf("record metadata not set: %+v", got[0])
	}
	if got[0].Parsed == nil {
		t.Fatal("expected JSON parsed payload")
	}

	cp, err := src.CheckpointOffset()
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := NewFileSourceE(FilePath(path), FileSourceName("orders"))
	if err != nil {
		t.Fatal(err)
	}
	if err := resumed.RestoreOffset(cp); err != nil {
		t.Fatal(err)
	}
	out = make(chan types.Record, 1)
	if err := resumed.Run(context.Background(), out); err != nil {
		t.Fatal(err)
	}
	close(out)
	for r := range out {
		t.Fatalf("expected no records after restoring EOF checkpoint, got %+v", r)
	}
}

func TestFileSource_RestoreMiddle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input.txt")
	if err := os.WriteFile(path, []byte("a\nb\nc\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cp, err := EncodePositions([]Position{{Source: path, Partition: 0, Offset: 1}})
	if err != nil {
		t.Fatal(err)
	}
	src := NewFileSource(FilePath(path))
	if err := src.RestoreOffset(cp); err != nil {
		t.Fatal(err)
	}
	out := make(chan types.Record, 4)
	if err := src.Run(context.Background(), out); err != nil {
		t.Fatal(err)
	}
	close(out)
	var values []string
	for r := range out {
		values = append(values, string(r.Value))
	}
	if len(values) != 2 || values[0] != "b" || values[1] != "c" {
		t.Fatalf("resume values: got %v, want [b c]", values)
	}
}
