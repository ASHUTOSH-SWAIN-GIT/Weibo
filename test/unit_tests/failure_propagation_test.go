package weibo_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/source"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

type fatalSource struct{ err error }

func (s fatalSource) Run(context.Context, chan<- types.Record) error { return s.err }

type failingRecordDLQ struct{ err error }

func (d failingRecordDLQ) Write(context.Context, types.Record) error { return d.err }

func TestExecutePropagatesFatalSourceError(t *testing.T) {
	want := errors.New("source connection lost")
	env := weibo.NewEnv()
	env.FromSource(fatalSource{err: want}).ToSink(sink.NewBlackholeSink())

	err := env.Execute(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("Execute error = %v, want fatal source error", err)
	}
}

func TestExecutePropagatesProcessFailure(t *testing.T) {
	env := weibo.NewEnv()
	env.FromSource(source.NewSliceSource([]types.Record{{Key: []byte("bad")}})).
		Process(func(types.Record) (types.Record, error) {
			return types.Record{}, errors.New("invalid event")
		}, operator.WithProcessFailurePolicy(operator.ProcFailureFail)).
		ToSink(sink.NewBlackholeSink())

	err := env.Execute(context.Background())
	if err == nil || !strings.Contains(err.Error(), "invalid event") {
		t.Fatalf("Execute error = %v, want Process failure", err)
	}
}

func TestExecutePropagatesProcessDLQFailure(t *testing.T) {
	env := weibo.NewEnv()
	env.FromSource(source.NewSliceSource([]types.Record{{Key: []byte("bad")}})).
		Process(func(types.Record) (types.Record, error) {
			return types.Record{}, errors.New("invalid event")
		},
			operator.WithProcessFailurePolicy(operator.ProcFailureDLQ),
			operator.WithProcessDLQ(failingRecordDLQ{err: errors.New("dlq unavailable")}),
		).
		ToSink(sink.NewBlackholeSink())

	err := env.Execute(context.Background())
	if err == nil || !strings.Contains(err.Error(), "dlq unavailable") {
		t.Fatalf("Execute error = %v, want DLQ failure", err)
	}
}
