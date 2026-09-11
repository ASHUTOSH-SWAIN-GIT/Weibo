package jobagent

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProbeSemantics(t *testing.T) {
	a := &Agent{st: State{Phase: PhaseStarting}}
	h := a.Handler()

	for _, tc := range []struct {
		path string
		want int
	}{{"/livez", http.StatusOK}, {"/readyz", http.StatusServiceUnavailable}} {
		r := httptest.NewRequest(http.MethodGet, tc.path, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Fatalf("%s status = %d, want %d", tc.path, w.Code, tc.want)
		}
	}

	a.mu.Lock()
	a.st.Phase = PhaseRunning
	a.mu.Unlock()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("ready status = %d, want 200", w.Code)
	}
}
