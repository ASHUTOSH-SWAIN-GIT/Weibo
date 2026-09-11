package source

import (
	"bytes"
	"testing"
)

func TestPositionsRoundTripDeterministically(t *testing.T) {
	positions := []Position{
		{Source: "payments", Partition: 0, Offset: 9},
		{Source: "orders", Partition: 1, Offset: 4},
		{Source: "orders", Partition: 0, Offset: 7},
	}
	data, err := EncodePositions(positions)
	if err != nil {
		t.Fatal(err)
	}
	wantJSON := `{"version":2,"positions":[{"source":"orders","partition":0,"offset":7},{"source":"orders","partition":1,"offset":4},{"source":"payments","partition":0,"offset":9}]}`
	if !bytes.Equal(data, []byte(wantJSON)) {
		t.Fatalf("encoded positions = %s", data)
	}
	got, err := DecodePositions(data, "")
	if err != nil || len(got) != 3 {
		t.Fatalf("decoded positions = %+v, err=%v", got, err)
	}
}

func TestDecodePositionsValidation(t *testing.T) {
	tests := []struct {
		name, data, legacySource string
	}{
		{"unsupported version", `{"version":3,"positions":[]}`, ""},
		{"duplicate", `{"version":2,"positions":[{"source":"t","partition":0,"offset":1},{"source":"t","partition":0,"offset":2}]}`, ""},
		{"missing source", `{"version":2,"positions":[{"partition":0,"offset":1}]}`, ""},
		{"ambiguous legacy", `{"0":1}`, ""},
		{"bad legacy partition", `{"x":1}`, "orders"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodePositions([]byte(test.data), test.legacySource); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestDecodePositionsLegacySingleSource(t *testing.T) {
	got, err := DecodePositions([]byte(`{"0":12,"2":7}`), "orders")
	if err != nil {
		t.Fatal(err)
	}
	positions := map[PositionKey]int64{}
	for _, position := range got {
		positions[PositionKey{Source: position.Source, Partition: position.Partition}] = position.Offset
	}
	if positions[PositionKey{Source: "orders", Partition: 0}] != 12 ||
		positions[PositionKey{Source: "orders", Partition: 2}] != 7 {
		t.Fatalf("legacy positions = %+v", got)
	}
}
