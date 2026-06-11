package watermark_test

import (
	"testing"
	"time"

	"mailer/watermark"
)

func TestBoundedOutOfOrderness_OnRecord_UpdatesMaxTimestamp(t *testing.T) {
	g := watermark.NewBoundedOutOfOrderness(5 * time.Second)

	g.OnRecord(time.Unix(10, 0))
	g.OnRecord(time.Unix(8, 0))
	g.OnRecord(time.Unix(15, 0))

	wm := g.GetWatermark()
	want := time.Unix(10, 0) // 15 - 5 = 10
	if !wm.Equal(want) {
		t.Errorf("GetWatermark(): got %v, want %v", wm, want)
	}
}

func TestBoundedOutOfOrderness_CurrentWatermark_MatchesGetWatermark(t *testing.T) {
	g := watermark.NewBoundedOutOfOrderness(3 * time.Second)
	g.OnRecord(time.Unix(100, 0))

	cw := g.CurrentWatermark()
	gw := g.GetWatermark()
	if !cw.Equal(gw) {
		t.Errorf("CurrentWatermark()=%v != GetWatermark()=%v", cw, gw)
	}
}

func TestBoundedOutOfOrderness_NoRecords_ReturnsZero(t *testing.T) {
	g := watermark.NewBoundedOutOfOrderness(5 * time.Second)
	wm := g.GetWatermark()
	if !wm.IsZero() {
		t.Errorf("expected zero time with no records, got %v", wm)
	}
	cw := g.CurrentWatermark()
	if !cw.IsZero() {
		t.Errorf("expected zero CurrentWatermark with no records, got %v", cw)
	}
}

func TestBoundedOutOfOrderness_AllowedLatenessZero(t *testing.T) {
	g := watermark.NewBoundedOutOfOrderness(0)
	g.OnRecord(time.Unix(50, 0))
	wm := g.GetWatermark()
	if !wm.Equal(time.Unix(50, 0)) {
		t.Errorf("with zero lateness: got %v, want %v", wm, time.Unix(50, 0))
	}
}

func TestBoundedOutOfOrderness_RecordsArriveInOrder(t *testing.T) {
	g := watermark.NewBoundedOutOfOrderness(2 * time.Second)
	for _, ts := range []int64{1, 5, 10, 20} {
		g.OnRecord(time.Unix(ts, 0))
	}
	wm := g.GetWatermark()
	want := time.Unix(18, 0) // 20 - 2
	if !wm.Equal(want) {
		t.Errorf("got %v, want %v", wm, want)
	}
}

func TestBoundedOutOfOrderness_RecordsArriveOutOfOrder(t *testing.T) {
	g := watermark.NewBoundedOutOfOrderness(5 * time.Second)
	g.OnRecord(time.Unix(10, 0))
	g.OnRecord(time.Unix(3, 0)) // late but within bound
	g.OnRecord(time.Unix(8, 0)) // also within bound
	wm := g.GetWatermark()
	want := time.Unix(5, 0) // 10 - 5
	if !wm.Equal(want) {
		t.Errorf("got %v, want %v", wm, want)
	}
}

func TestBoundedOutOfOrderness_InterfaceCompliance(t *testing.T) {
	var _ watermark.WatermarkGenerator = watermark.NewBoundedOutOfOrderness(0)
}
