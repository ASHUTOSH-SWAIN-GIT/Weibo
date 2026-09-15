package sink

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

func TestFileSink_WritesAndAppendsLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.txt")
	s, err := NewFileSinkE(FileSinkPath(path))
	if err != nil {
		t.Fatal(err)
	}
	in := make(chan types.Record, 2)
	in <- types.NewRecord(nil, []byte("a"))
	in <- types.NewRecord(nil, []byte("b"))
	close(in)
	if err := s.Write(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if got := mustReadFile(t, path); got != "a\nb\n" {
		t.Fatalf("first write: got %q", got)
	}

	s, err = NewFileSinkE(FileSinkPath(path), FileSinkAppend())
	if err != nil {
		t.Fatal(err)
	}
	in = make(chan types.Record, 1)
	in <- types.NewRecord(nil, []byte("c"))
	close(in)
	if err := s.Write(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if got := mustReadFile(t, path); got != "a\nb\nc\n" {
		t.Fatalf("append write: got %q", got)
	}
}

func TestFileSink_JSONSerializer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.ndjson")
	s := NewFileSink(FileSinkPath(path), FileSinkSerialize(NewJSONSerializer()))
	in := make(chan types.Record, 1)
	in <- types.Record{Parsed: map[string]any{"n": float64(1)}}
	close(in)
	if err := s.Write(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if got := mustReadFile(t, path); got != "{\"n\":1}\n" {
		t.Fatalf("json write: got %q", got)
	}
}

func mustReadFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
