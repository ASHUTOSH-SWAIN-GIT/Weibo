// Package record provides a structured JSON view of a types.Record so
// declarative, field-level operations (map/filter/project by field
// name) can read and modify record contents safely.
//
// A record's JSON payload is decoded into a JSONRecord and cached on
// types.Record.Parsed. Built-in operators call DecodeJSON to obtain the
// structured view, read/modify fields by dotted path, and EncodeJSON to
// serialize the result back into Record.Value.
//
// # Numeric behavior
//
// Numbers are decoded as json.Number, not float64. This is deliberate:
// float64 cannot represent every int64 exactly (integers beyond 2^53
// lose precision) and it turns "1" into "1" that re-encodes as "1"
// but "amount: 100" round-trips through float as "100" only by luck —
// any arithmetic in between risks "100.00000001". json.Number keeps the
// exact source literal until a caller explicitly converts it:
//
//	v, _ := GetField(rec, "payment.amount")   // v is json.Number
//	n := v.(json.Number)
//	cents, _ := n.Int64()                     // exact
//	// or n.Float64(), or string(n)
//
// EncodeJSON writes a json.Number back as its literal (no quotes, no
// float rounding), so decode → modify → encode preserves numeric
// fidelity. Values you SetField should likewise be json.Number (or
// plain int/float when precision is not a concern).
package record

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// JSONRecord is a decoded JSON object: field name → value. Nested
// objects are themselves map[string]any (as produced by the JSON
// decoder); numbers are json.Number. It is stored on
// types.Record.Parsed.
type JSONRecord map[string]any

// DecodeJSON returns the structured JSON view of a record. If
// r.Parsed already holds a decoded object (JSONRecord or map[string]any,
// e.g. set by a source deserializer or a prior operator) it is reused;
// otherwise r.Value is decoded with numbers as json.Number.
//
// An empty Value yields an empty JSONRecord (so field-building
// operators can start from scratch). Non-empty Value that is not a JSON
// object (a string, array, number, or malformed) returns an error.
//
// Callers that want the result cached should assign it to r.Parsed:
//
//	jr, err := record.DecodeJSON(r)
//	r.Parsed = jr
func DecodeJSON(r types.Record) (JSONRecord, error) {
	switch p := r.Parsed.(type) {
	case JSONRecord:
		return p, nil
	case map[string]any:
		return JSONRecord(p), nil
	}
	return decodeJSONBytes(r.Value)
}

// decodeJSONBytes decodes raw JSON object bytes into a JSONRecord with
// numbers as json.Number. Empty input yields an empty object.
func decodeJSONBytes(data []byte) (JSONRecord, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return JSONRecord{}, nil
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var jr JSONRecord
	if err := dec.Decode(&jr); err != nil {
		return nil, fmt.Errorf("record: decode json object: %w", err)
	}
	return jr, nil
}

// DeserializeJSON matches source.Deserializer's function shape: it
// decodes a record's JSON value into a JSONRecord (numbers as
// json.Number, consistent with DecodeJSON). Wire it into a Kafka source
// with:
//
//	source.KafkaDeserialize(source.DeserializerFunc(record.DeserializeJSON))
func DeserializeJSON(data []byte, _ map[string][]byte) (any, error) {
	return decodeJSONBytes(data)
}

// EncodeJSON serializes a JSONRecord back to JSON bytes. json.Number
// values are written as their exact literal.
func EncodeJSON(data JSONRecord) ([]byte, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("record: encode json: %w", err)
	}
	return b, nil
}
