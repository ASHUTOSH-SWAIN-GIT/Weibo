package backend

import (
	"archive/tar"
	"bytes"
	"io"
	"testing"
)

func TestFirstString(t *testing.T) {
	if got := firstString(nil); got != "" {
		t.Errorf("nil slice: got %q, want empty", got)
	}
	if got := firstString([]string{"a", "b"}); got != "a" {
		t.Errorf("got %q, want a", got)
	}
}

func TestJoinReason(t *testing.T) {
	cases := []struct{ a, b, want string }{
		{"", "", ""},
		{"a", "", "a"},
		{"", "b", "b"},
		{"a", "b", "a; b"},
	}
	for _, c := range cases {
		if got := joinReason(c.a, c.b); got != c.want {
			t.Errorf("joinReason(%q, %q) = %q, want %q", c.a, c.b, got, c.want)
		}
	}
}

func TestShortID(t *testing.T) {
	if got := shortID("short"); got != "short" {
		t.Errorf("short id unchanged: got %q", got)
	}
	if got := shortID("0123456789abcdef"); got != "0123456789ab" {
		t.Errorf("long id truncated to 12: got %q", got)
	}
}

func TestMin64Max64(t *testing.T) {
	if got := min64(5); got != 5 {
		t.Errorf("min64 single arg: got %d", got)
	}
	if got := min64(5, 3, 9, -1); got != -1 {
		t.Errorf("min64: got %d, want -1", got)
	}
	if got := max64(5, 9); got != 9 {
		t.Errorf("max64: got %d, want 9", got)
	}
	if got := max64(9, 5); got != 9 {
		t.Errorf("max64 (reversed args): got %d, want 9", got)
	}
}

func TestCPUToMilliAndMemoryToBytes(t *testing.T) {
	milli, err := cpuToMilli("1500m")
	if err != nil || milli != 1500 {
		t.Fatalf("cpuToMilli(1500m) = %d, %v", milli, err)
	}
	if _, err := cpuToMilli("not-a-quantity"); err == nil {
		t.Error("expected an error for an invalid CPU quantity")
	}

	bytesVal, err := memoryToBytes("512Mi")
	if err != nil || bytesVal != 512*1024*1024 {
		t.Fatalf("memoryToBytes(512Mi) = %d, %v", bytesVal, err)
	}
	if _, err := memoryToBytes("not-a-quantity"); err == nil {
		t.Error("expected an error for an invalid memory quantity")
	}
}

func TestTarFile(t *testing.T) {
	content := []byte("hello world")
	buf, err := tarFile("workflow.yaml", content)
	if err != nil {
		t.Fatalf("tarFile: %v", err)
	}

	tr := tar.NewReader(buf)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatalf("reading tar header: %v", err)
	}
	if hdr.Name != "workflow.yaml" || hdr.Size != int64(len(content)) {
		t.Fatalf("header = %+v, want name=workflow.yaml size=%d", hdr, len(content))
	}
	got, err := io.ReadAll(tr)
	if err != nil {
		t.Fatalf("reading tar content: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("content = %q, want %q", got, content)
	}
	if _, err := tr.Next(); err != io.EOF {
		t.Fatalf("expected exactly one entry, got next err = %v", err)
	}
}
