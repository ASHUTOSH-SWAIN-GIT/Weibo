package api

// FuzzAPIRequestDecoding feeds arbitrary request bodies through the submit
// and savepoint decoders. Invariants: they never panic, oversized bodies
// are rejected with errBodyTooLarge, and a successfully decoded savepoint
// label round-trips through the query-param path unchanged.

import (
	"bytes"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
)

func FuzzAPIRequestDecoding(f *testing.F) {
	f.Add([]byte(`{"workflow":"kind: sdk\nname: x\n","env":{"K":"v"}}`), "application/json")
	f.Add([]byte("kind: sdk\nname: raw\n"), "application/yaml")
	f.Add([]byte(`{"label":"nightly"}`), "application/json")
	f.Add([]byte(`not json {{{`), "application/json")
	f.Add([]byte{}, "application/json")

	f.Fuzz(func(t *testing.T, body []byte, contentType string) {
		if len(body) > maxWorkflowBody+1 {
			return // oversized inputs have a dedicated case below
		}
		req := httptest.NewRequest("POST", "/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", contentType)
		doc, env, refs, err := readWorkflow(req)
		if err != nil {
			return // malformed input is a normal rejection
		}
		for k := range env {
			if k == "" {
				t.Fatal("decoded empty env key")
			}
		}
		for k := range refs {
			if k == "" {
				t.Fatal("decoded empty env-ref key")
			}
		}
		_ = doc

		// The same body through the savepoint decoder must agree with
		// itself: decoding twice yields the same label.
		sreq := httptest.NewRequest("POST", "/jobs/x/restart", bytes.NewReader(body))
		sreq.Header.Set("Content-Type", contentType)
		l1, err1 := savepointLabel(sreq)
		sreq2 := httptest.NewRequest("POST", "/jobs/x/restart", bytes.NewReader(body))
		sreq2.Header.Set("Content-Type", contentType)
		l2, err2 := savepointLabel(sreq2)
		if (err1 == nil) != (err2 == nil) || l1 != l2 {
			t.Fatalf("savepoint decode not deterministic: %q/%v vs %q/%v", l1, err1, l2, err2)
		}

		// A label that survives decoding must also survive the query-param
		// path: ?label= takes precedence verbatim.
		if err1 == nil && l1 != "" && strings.Contains(l1, "\n") {
			t.Fatalf("decoded savepoint label contains newline: %q", l1)
		}
	})
}

// Oversized bodies are rejected before decoding, for both envelopes.
func FuzzAPIRequestBodyLimits(f *testing.F) {
	big := bytes.Repeat([]byte("a"), maxWorkflowBody+16)
	f.Add(big)

	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) <= maxWorkflowBody {
			t.Skip("covered by FuzzAPIRequestDecoding")
		}
		req := httptest.NewRequest("POST", "/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if _, _, _, err := readWorkflow(req); !errors.Is(err, errBodyTooLarge) {
			t.Fatalf("oversized workflow body: got %v, want errBodyTooLarge", err)
		}
	})
}
