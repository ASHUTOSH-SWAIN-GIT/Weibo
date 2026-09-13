package operator

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

type failingRecordSink struct {
	err error
}

func (s failingRecordSink) Write(context.Context, types.Record) error {
	return s.err
}

func TestProcessOperatorFailureReturnsStructuredError(t *testing.T) {
	op := NewProcess(
		func(types.Record) (types.Record, error) { return types.Record{}, errors.New("invalid payload") },
		WithProcessLabel("validate"),
		WithProcessFailurePolicy(ProcFailureFail),
	)
	_, err := op.ProcessOneE(context.Background(), types.Record{Key: []byte("k1")})
	if err == nil {
		t.Fatal("expected error")
	}
	var opErr *OperatorError
	if !errors.As(err, &opErr) {
		t.Fatalf("expected OperatorError, got %T %v", err, err)
	}
	if opErr.Label != "validate" || string(opErr.Key) != "k1" || !strings.Contains(err.Error(), "invalid payload") {
		t.Fatalf("operator context not preserved: %+v", opErr)
	}
}

func TestProcessOperatorDLQErrorsReturnError(t *testing.T) {
	op := NewProcess(
		func(types.Record) (types.Record, error) { return types.Record{}, errors.New("invalid payload") },
		WithProcessLabel("validate"),
		WithProcessFailurePolicy(ProcFailureDLQ),
	)
	if _, err := op.ProcessOneE(context.Background(), types.Record{Key: []byte("k1")}); err == nil || !strings.Contains(err.Error(), "DLQ is nil") {
		t.Fatalf("expected nil DLQ error, got %v", err)
	}

	op = NewProcess(
		func(types.Record) (types.Record, error) { return types.Record{}, errors.New("invalid payload") },
		WithProcessLabel("validate"),
		WithProcessFailurePolicy(ProcFailureDLQ),
		WithProcessDLQ(failingRecordSink{err: errors.New("dlq unavailable")}),
	)
	if _, err := op.ProcessOneE(context.Background(), types.Record{Key: []byte("k1")}); err == nil || !strings.Contains(err.Error(), "DLQ write failed") {
		t.Fatalf("expected DLQ write error, got %v", err)
	}
}
