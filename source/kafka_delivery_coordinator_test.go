package source

import (
	"context"
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// captureSink records everything written to it, standing in for a DLQ.
type captureSink struct{ records []types.Record }

func (s *captureSink) Write(_ context.Context, r types.Record) error {
	s.records = append(s.records, r)
	return nil
}

func TestDeliveryCoordinator_NoDeserializerPassesThrough(t *testing.T) {
	d := &deliveryCoordinator{}
	rec := d.toRecord(kafka.Message{Value: []byte("hi")})
	if rec == nil {
		t.Fatal("toRecord: got nil, want record")
	}
	if string(rec.Value) != "hi" || rec.Parsed != nil {
		t.Errorf("unexpected record: value=%q parsed=%v", rec.Value, rec.Parsed)
	}
}

func TestDeliveryCoordinator_DeserializeSuccessSetsParsed(t *testing.T) {
	d := &deliveryCoordinator{
		deserializer: DeserializerFunc(func(data []byte, _ map[string][]byte) (any, error) {
			return string(data) + "!", nil
		}),
	}
	rec := d.toRecord(kafka.Message{Value: []byte("ok")})
	if rec == nil || rec.Parsed != "ok!" {
		t.Fatalf("toRecord: got %+v, want Parsed=ok!", rec)
	}
}

func TestDeliveryCoordinator_FailurePolicies(t *testing.T) {
	deser := DeserializerFunc(func(_ []byte, _ map[string][]byte) (any, error) {
		return nil, errors.New("boom")
	})

	// Drop: record discarded, no DLQ.
	drop := &deliveryCoordinator{deserializer: deser, deserFailPolicy: DeserFailureDrop}
	if rec := drop.toRecord(kafka.Message{Value: []byte("x")}); rec != nil {
		t.Errorf("Drop: got %+v, want nil", rec)
	}

	// Fail: record discarded (current behaviour drops; caller handles error).
	fail := &deliveryCoordinator{deserializer: deser, deserFailPolicy: DeserFailureFail}
	if rec := fail.toRecord(kafka.Message{Value: []byte("x")}); rec != nil {
		t.Errorf("Fail: got %+v, want nil", rec)
	}

	// DLQ: record discarded from stream but forwarded to the DLQ sink with
	// the error attached as a header.
	sink := &captureSink{}
	dlq := &deliveryCoordinator{deserializer: deser, deserFailPolicy: DeserFailureDLQ, deserDLQ: sink}
	if rec := dlq.toRecord(kafka.Message{Value: []byte("x")}); rec != nil {
		t.Errorf("DLQ: got %+v, want nil", rec)
	}
	if len(sink.records) != 1 {
		t.Fatalf("DLQ sink: got %d records, want 1", len(sink.records))
	}
	if string(sink.records[0].Headers["_deser_error"]) != "boom" {
		t.Errorf("DLQ header: got %q, want boom", sink.records[0].Headers["_deser_error"])
	}
}

func TestDeliveryCoordinator_EmitRespectsContextCancel(t *testing.T) {
	d := &deliveryCoordinator{}
	out := make(chan types.Record) // unbuffered, no reader -> would block
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := d.emit(ctx, out, types.Record{})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("emit on cancelled ctx: got %v, want context.Canceled", err)
	}
}

func TestDeliveryCoordinator_EmitDeliversRecord(t *testing.T) {
	d := &deliveryCoordinator{}
	out := make(chan types.Record, 1)
	if err := d.emit(context.Background(), out, types.Record{Value: []byte("v")}); err != nil {
		t.Fatalf("emit: %v", err)
	}
	if got := <-out; string(got.Value) != "v" {
		t.Errorf("emitted value: got %q, want v", got.Value)
	}
}
