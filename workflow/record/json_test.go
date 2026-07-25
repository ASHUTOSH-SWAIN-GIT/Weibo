package record_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow/record"
)

func decode(t *testing.T, jsonStr string) record.JSONRecord {
	t.Helper()
	jr, err := record.DecodeJSON(types.Record{Value: []byte(jsonStr)})
	if err != nil {
		t.Fatalf("DecodeJSON(%s): %v", jsonStr, err)
	}
	return jr
}

// The headline numeric guarantee: integers keep exact precision (no
// float64 rounding) because numbers decode as json.Number.
func TestDecode_NumbersAreJSONNumber(t *testing.T) {
	jr := decode(t, `{"payment":{"amount":1234567890123456789},"qty":3,"rate":1.5}`)

	amount, ok := record.GetField(jr, "payment.amount")
	if !ok {
		t.Fatal("payment.amount missing")
	}
	n, isNum := amount.(json.Number)
	if !isNum {
		t.Fatalf("payment.amount should be json.Number, got %T", amount)
	}
	// A float64 round-trip would corrupt this 19-digit integer.
	if got, _ := n.Int64(); got != 1234567890123456789 {
		t.Errorf("amount lost precision: got %d", got)
	}
	if string(n) != "1234567890123456789" {
		t.Errorf("amount literal changed: %s", n)
	}

	if v, _ := record.GetField(jr, "rate"); v.(json.Number).String() != "1.5" {
		t.Errorf("rate: got %v", v)
	}
}

func TestGetField_NestedPaths(t *testing.T) {
	jr := decode(t, `{
		"customer": {"id": "c-1", "name": "Ada"},
		"address": {"country": "UK"},
		"tags": ["a", "b"]
	}`)

	cases := []struct {
		path string
		want any
		ok   bool
	}{
		{"customer.id", "c-1", true},
		{"customer.name", "Ada", true},
		{"address.country", "UK", true},
		{"customer.missing", nil, false}, // absent leaf
		{"customer.id.deep", nil, false}, // traverse through a string
		{"nope.here", nil, false},        // absent intermediate
		{"tags.0", nil, false},           // arrays aren't path-indexable
	}
	for _, c := range cases {
		got, ok := record.GetField(jr, c.path)
		if ok != c.ok {
			t.Errorf("GetField(%q): ok=%v, want %v", c.path, ok, c.ok)
			continue
		}
		if c.ok && got != c.want {
			t.Errorf("GetField(%q): got %v, want %v", c.path, got, c.want)
		}
	}
}

func TestGetField_PresentNull(t *testing.T) {
	jr := decode(t, `{"middle_name": null}`)
	v, ok := record.GetField(jr, "middle_name")
	if !ok {
		t.Fatal("present null should return ok=true")
	}
	if v != nil {
		t.Errorf("present null should return nil value, got %v", v)
	}
}

func TestSetField_NestedCreateAndOverwrite(t *testing.T) {
	jr := decode(t, `{"customer":{"id":"c-1"}}`)

	// Overwrite an existing leaf.
	if err := record.SetField(jr, "customer.id", "c-2"); err != nil {
		t.Fatal(err)
	}
	// Create a new nested branch.
	if err := record.SetField(jr, "address.country", "US"); err != nil {
		t.Fatal(err)
	}
	// Add a sibling to an existing object.
	if err := record.SetField(jr, "customer.tier", "gold"); err != nil {
		t.Fatal(err)
	}

	if v, _ := record.GetField(jr, "customer.id"); v != "c-2" {
		t.Errorf("overwrite failed: %v", v)
	}
	if v, _ := record.GetField(jr, "address.country"); v != "US" {
		t.Errorf("created branch failed: %v", v)
	}
	if v, _ := record.GetField(jr, "customer.tier"); v != "gold" {
		t.Errorf("sibling add failed: %v", v)
	}
}

func TestSetField_ConflictOnNonObject(t *testing.T) {
	jr := decode(t, `{"customer":"c-1"}`) // customer is a string, not an object
	err := record.SetField(jr, "customer.id", "x")
	if err == nil {
		t.Fatal("expected error setting through a non-object intermediate")
	}
}

func TestDeleteField(t *testing.T) {
	jr := decode(t, `{"customer":{"id":"c-1","secret":"s"}}`)

	if err := record.DeleteField(jr, "customer.secret"); err != nil {
		t.Fatal(err)
	}
	if _, ok := record.GetField(jr, "customer.secret"); ok {
		t.Error("secret should be gone")
	}
	if _, ok := record.GetField(jr, "customer.id"); !ok {
		t.Error("sibling should remain")
	}

	// Idempotent: deleting a missing field (or missing parent) is nil.
	if err := record.DeleteField(jr, "customer.secret"); err != nil {
		t.Errorf("delete missing leaf should be nil: %v", err)
	}
	if err := record.DeleteField(jr, "nope.here"); err != nil {
		t.Errorf("delete missing parent should be nil: %v", err)
	}

	// Conflict: traverse through a non-object.
	if err := record.DeleteField(jr, "customer.id.deep"); err == nil {
		t.Error("expected error deleting through a non-object")
	}
}

func TestBadPaths(t *testing.T) {
	jr := decode(t, `{"a":1}`)
	if _, ok := record.GetField(jr, ""); ok {
		t.Error("empty path should not resolve")
	}
	if err := record.SetField(jr, "a..b", "x"); err == nil {
		t.Error("empty segment should error")
	}
	if err := record.DeleteField(jr, ".a"); err == nil {
		t.Error("leading dot should error")
	}
}

func TestEncode_RoundTripPreservesNumbers(t *testing.T) {
	jr := decode(t, `{"payment":{"amount":1234567890123456789},"qty":3}`)

	record.SetField(jr, "payment.currency", "GBP")
	record.DeleteField(jr, "qty")

	out, err := record.EncodeJSON(jr)
	if err != nil {
		t.Fatal(err)
	}
	// The big integer must survive as a bare integer literal, unquoted
	// and un-floated.
	got := decode(t, string(out))
	amount, _ := record.GetField(got, "payment.amount")
	if amount.(json.Number).String() != "1234567890123456789" {
		t.Errorf("amount corrupted through encode: %v", amount)
	}
	if v, _ := record.GetField(got, "payment.currency"); v != "GBP" {
		t.Errorf("added field lost: %v", v)
	}
	if _, ok := record.GetField(got, "qty"); ok {
		t.Error("deleted field reappeared")
	}
}

func TestDecode_ReusesParsed(t *testing.T) {
	pre := record.JSONRecord{"a": "cached"}
	r := types.Record{Value: []byte(`{"a":"from-bytes"}`), Parsed: pre}
	jr, err := record.DecodeJSON(r)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := record.GetField(jr, "a"); v != "cached" {
		t.Errorf("expected cached Parsed to be reused, got %v", v)
	}

	// A plain map[string]any in Parsed is also reused.
	r2 := types.Record{Parsed: map[string]any{"a": "plainmap"}}
	jr2, _ := record.DecodeJSON(r2)
	if v, _ := record.GetField(jr2, "a"); v != "plainmap" {
		t.Errorf("expected map[string]any Parsed reuse, got %v", v)
	}
}

func TestDecode_EmptyAndInvalid(t *testing.T) {
	empty, err := record.DecodeJSON(types.Record{Value: nil})
	if err != nil || len(empty) != 0 {
		t.Errorf("empty value should decode to empty object, got %v err=%v", empty, err)
	}
	if _, err := record.DecodeJSON(types.Record{Value: []byte(`"a string"`)}); err == nil {
		t.Error("a non-object JSON value should error")
	}
	if _, err := record.DecodeJSON(types.Record{Value: []byte(`{bad`)}); err == nil {
		t.Error("malformed JSON should error")
	}
}

// End-to-end: what a built-in field operator would do — decode, read a
// nested field, write a derived field, drop a field, re-encode.
func TestBuiltinOperatorFlow(t *testing.T) {
	in := types.Record{Value: []byte(`{"customer":{"id":"c-1"},"payment":{"amount":250},"pii":"x"}`)}

	jr, err := record.DecodeJSON(in)
	if err != nil {
		t.Fatal(err)
	}
	amount, _ := record.GetField(jr, "payment.amount")
	if amount.(json.Number).String() != "250" {
		t.Fatalf("read failed: %v", amount)
	}
	if err := record.SetField(jr, "flags.highValue", amount.(json.Number).String() == "250"); err != nil {
		t.Fatal(err)
	}
	if err := record.DeleteField(jr, "pii"); err != nil {
		t.Fatal(err)
	}

	out, err := record.EncodeJSON(jr)
	if err != nil {
		t.Fatal(err)
	}
	var want map[string]any
	json.Unmarshal([]byte(`{"customer":{"id":"c-1"},"payment":{"amount":250},"flags":{"highValue":true}}`), &want)
	var got map[string]any
	json.Unmarshal(out, &got)
	// Compare via generic unmarshal (float64) — structure equality only.
	if !reflect.DeepEqual(got, want) {
		t.Errorf("operator flow mismatch:\n got: %v\nwant: %v", got, want)
	}
}
