package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

type errorSource struct{ err error }

func (s errorSource) Run(context.Context, chan<- types.Record) error { return s.err }

type cancelSource struct{}

func (cancelSource) Run(ctx context.Context, _ chan<- types.Record) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestSourceStagePropagatesFatalSourceError(t *testing.T) {
	want := errors.New("broker unavailable")
	out := make(chan types.Record)
	err := (&SourceStage{Source: errorSource{err: want}, DrainTimeout: time.Second}).Run(
		context.Background(), context.Background(), nil, out,
	)
	if !errors.Is(err, want) || !strings.Contains(err.Error(), "source:") {
		t.Fatalf("Run error = %v, want wrapped fatal source error", err)
	}
}

func TestSourceStageTreatsRequestedCancellationAsCleanStop(t *testing.T) {
	runCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err := (&SourceStage{Source: cancelSource{}, DrainTimeout: time.Second}).Run(
		runCtx, context.Background(), nil, make(chan types.Record),
	)
	if err != nil {
		t.Fatalf("Run error = %v, want clean cancellation", err)
	}
}
