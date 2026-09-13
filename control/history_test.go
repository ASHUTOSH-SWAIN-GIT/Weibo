package control_test

import (
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control"
)

func mkSample(i int) control.Sample {
	return control.Sample{
		At:         time.Now().UTC().Add(time.Duration(i) * time.Second),
		Phase:      "running",
		RecordsIn:  int64(i * 10),
		RecordsOut: int64(i * 8),
	}
}

func TestHistoryRingOverflow(t *testing.T) {
	h := control.NewHistory(3)
	for i := 0; i < 5; i++ {
		h.Add("j", mkSample(i))
	}
	got := h.Series("j", 0)
	if len(got) != 3 {
		t.Fatalf("len=%d, want 3", len(got))
	}
	if got[0].RecordsIn != 20 || got[2].RecordsIn != 40 {
		t.Fatalf("wrong survivors: %+v", got)
	}
}

func TestHistoryDownsampleKeepsEndpoints(t *testing.T) {
	h := control.NewHistory(0) // default cap
	for i := 0; i < 10; i++ {
		h.Add("j", mkSample(i))
	}
	got := h.Series("j", 4)
	if len(got) != 4 {
		t.Fatalf("len=%d, want 4", len(got))
	}
	if got[0].RecordsIn != 0 || got[3].RecordsIn != 90 {
		t.Fatalf("endpoints not preserved: %+v", got)
	}
	for i := 1; i < len(got); i++ {
		if !got[i].At.After(got[i-1].At) {
			t.Fatalf("not oldest-first: %+v", got)
		}
	}
}

func TestHistoryDrop(t *testing.T) {
	h := control.NewHistory(10)
	h.Add("a", mkSample(0))
	h.Add("b", mkSample(0))
	h.Drop("a")
	if len(h.Series("a", 0)) != 0 {
		t.Fatal("dropped job should have no series")
	}
	if len(h.Series("b", 0)) != 1 {
		t.Fatal("other jobs must be untouched")
	}
	if ids := h.JobsWithHistory(); len(ids) != 1 || ids[0] != "b" {
		t.Fatalf("JobsWithHistory=%v", ids)
	}
}
