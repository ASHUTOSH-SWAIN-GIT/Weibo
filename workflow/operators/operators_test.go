package operators_test

import (
	"encoding/json"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/operators"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/record"
)

func rec(jsonStr string) types.Record {
	return types.Record{Key: []byte("k"), Value: []byte(jsonStr)}
}

// getStr reads a field as a string via the record model (helper for
// hand-written equivalents).
func getField(r types.Record, path string) (any, bool) {
	jr, err := record.DecodeJSON(r)
	if err != nil {
		return nil, false
	}
	return record.GetField(jr, path)
}

// ---- Filter: every operator, builtin vs hand-written SDK equivalent ----

func TestFilter_EqualsMatchesSDK(t *testing.T) {
	builtin := operators.BuildFilter(operators.FilterConfig{
		Field: "status", Operator: "equals", Value: "completed",
	})
	// Equivalent hand-written SDK predicate.
	sdk := func(r types.Record) bool {
		v, ok := getField(r, "status")
		return ok && v == "completed"
	}

	recs := []types.Record{
		rec(`{"status":"completed"}`),
		rec(`{"status":"pending"}`),
		rec(`{"other":"x"}`),
		rec(`not json`),
	}
	for _, r := range recs {
		if builtin(r) != sdk(r) {
			t.Errorf("filter equals diverged from SDK on %s: builtin=%v sdk=%v",
				r.Value, builtin(r), sdk(r))
		}
	}
}

func TestFilter_AllOperators(t *testing.T) {
	r := rec(`{"amount":100,"status":"completed","tags":["a","b"],"note":"hello world"}`)

	cases := []struct {
		field, op string
		value     any
		want      bool
	}{
		{"status", "equals", "completed", true},
		{"status", "equals", "pending", false},
		{"status", "not_equals", "pending", true},
		{"amount", "equals", 100, true},
		{"amount", "greater_than", 50, true},
		{"amount", "greater_than", 100, false},
		{"amount", "greater_than_or_equal", 100, true},
		{"amount", "less_than", 200, true},
		{"amount", "less_than_or_equal", 100, true},
		{"amount", "less_than", 50, false},
		{"note", "contains", "world", true},
		{"note", "contains", "planet", false},
		{"tags", "contains", "b", true},
		{"tags", "contains", "z", false},
		{"status", "exists", nil, true},
		{"missing", "exists", nil, false},
		{"missing", "not_exists", nil, true},
		{"status", "not_exists", nil, false},
		// missing field: every value comparison is false
		{"missing", "equals", "x", false},
		{"missing", "greater_than", 0, false},
		// number vs quoted string is not equal (predictable typing)
		{"amount", "equals", "100", false},
	}
	for _, c := range cases {
		f := operators.BuildFilter(operators.FilterConfig{Field: c.field, Operator: c.op, Value: c.value})
		if got := f(r); got != c.want {
			t.Errorf("%s %s %v: got %v, want %v", c.field, c.op, c.value, got, c.want)
		}
	}
}

// The success condition against the numeric edge: a YAML config (int)
// and a JSON config (float64) for the same threshold behave identically.
func TestFilter_YAMLIntAndJSONFloatParity(t *testing.T) {
	r := rec(`{"amount":100}`)
	yamlCfg := operators.BuildFilter(operators.FilterConfig{Field: "amount", Operator: "greater_than_or_equal", Value: 100})   // YAML → int
	jsonCfg := operators.BuildFilter(operators.FilterConfig{Field: "amount", Operator: "greater_than_or_equal", Value: 100.0}) // JSON → float64
	if yamlCfg(r) != jsonCfg(r) || !yamlCfg(r) {
		t.Errorf("YAML-int and JSON-float configs diverged: yaml=%v json=%v", yamlCfg(r), jsonCfg(r))
	}
}

// Large integers keep exact precision because record fields are
// json.Number (not float64).
func TestFilter_LargeIntPrecision(t *testing.T) {
	r := rec(`{"id":9007199254740993}`) // 2^53 + 1, not representable as float64
	// equals the exact int → true; equals the float64-rounded neighbor → false
	eq := operators.BuildFilter(operators.FilterConfig{Field: "id", Operator: "equals", Value: int64(9007199254740993)})
	if !eq(r) {
		t.Error("large integer equality lost precision")
	}
}

// ---- Field transforms: builtin vs hand-written SDK equivalent ----

// normalize compares two records by their decoded JSON structure
// (order- and formatting-independent).
func sameJSON(t *testing.T, a, b types.Record) bool {
	t.Helper()
	var ma, mb map[string]any
	if err := json.Unmarshal(a.Value, &ma); err != nil {
		t.Fatalf("unmarshal a (%s): %v", a.Value, err)
	}
	if err := json.Unmarshal(b.Value, &mb); err != nil {
		t.Fatalf("unmarshal b (%s): %v", b.Value, err)
	}
	return jsonEqual(ma, mb)
}

func jsonEqual(a, b any) bool {
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	// Re-decode to normalize map ordering.
	var am, bm any
	json.Unmarshal(ab, &am)
	json.Unmarshal(bb, &bm)
	x, _ := json.Marshal(am)
	y, _ := json.Marshal(bm)
	return string(x) == string(y)
}

func TestSelect_MatchesSDK(t *testing.T) {
	in := rec(`{"customer_id":"c-1","amount":250,"secret":"s","nested":{"keep":1,"drop":2}}`)

	builtin := operators.BuildSelect(operators.SelectConfig{Fields: []string{"customer_id", "amount", "nested.keep"}})

	// Hand-written SDK map that keeps the same fields.
	sdk := func(r types.Record) types.Record {
		jr, _ := record.DecodeJSON(r)
		out := record.JSONRecord{}
		for _, f := range []string{"customer_id", "amount", "nested.keep"} {
			if v, ok := record.GetField(jr, f); ok {
				record.SetField(out, f, v)
			}
		}
		b, _ := record.EncodeJSON(out)
		nr := r
		nr.Value = b
		return nr
	}

	if !sameJSON(t, builtin(in), sdk(in)) {
		t.Errorf("select diverged from SDK:\nbuiltin=%s\nsdk=%s", builtin(in).Value, sdk(in).Value)
	}
	// And it actually dropped the unlisted fields.
	got := builtin(in)
	if _, ok := getField(got, "secret"); ok {
		t.Error("select should have dropped 'secret'")
	}
	if _, ok := getField(got, "nested.drop"); ok {
		t.Error("select should have dropped 'nested.drop'")
	}
	if _, ok := getField(got, "nested.keep"); !ok {
		t.Error("select should have kept 'nested.keep'")
	}
}

func TestRename_MatchesSDK(t *testing.T) {
	in := rec(`{"cust_id":"c-1","amt":250}`)
	builtin := operators.BuildRename(operators.RenameConfig{Renames: []operators.FieldRename{
		{From: "cust_id", To: "customer_id"},
		{From: "amt", To: "payment.amount"},
	}})

	got := builtin(in)
	if v, ok := getField(got, "customer_id"); !ok || v != "c-1" {
		t.Errorf("rename to customer_id failed: %v", v)
	}
	if v, ok := getField(got, "payment.amount"); !ok || v.(json.Number).String() != "250" {
		t.Errorf("rename to nested payment.amount failed: %v", v)
	}
	if _, ok := getField(got, "cust_id"); ok {
		t.Error("original cust_id should be gone")
	}
	if _, ok := getField(got, "amt"); ok {
		t.Error("original amt should be gone")
	}
}

func TestSet_MatchesSDK(t *testing.T) {
	in := rec(`{"id":"o-1"}`)
	builtin := operators.BuildSet(operators.SetConfig{Sets: []operators.FieldSet{
		{Field: "source", Value: "web"},
		{Field: "flags.processed", Value: true},
	}})

	got := builtin(in)
	if v, ok := getField(got, "source"); !ok || v != "web" {
		t.Errorf("set source failed: %v", v)
	}
	if v, ok := getField(got, "flags.processed"); !ok || v != true {
		t.Errorf("set nested flag failed: %v", v)
	}
	if v, ok := getField(got, "id"); !ok || v != "o-1" {
		t.Errorf("existing field should be preserved: %v", v)
	}
}

func TestMapOperators_PassThroughOnNonJSON(t *testing.T) {
	in := types.Record{Value: []byte("not json")}
	sel := operators.BuildSelect(operators.SelectConfig{Fields: []string{"a"}})
	if got := sel(in); string(got.Value) != "not json" {
		t.Errorf("non-JSON record should pass through unchanged, got %s", got.Value)
	}
}

func TestConfigValidation(t *testing.T) {
	if err := (operators.FilterConfig{Operator: "equals", Value: "x"}).Validate(); err == nil {
		t.Error("filter without field should fail validation")
	}
	if err := (operators.FilterConfig{Field: "a", Operator: "bogus", Value: "x"}).Validate(); err == nil {
		t.Error("filter with unsupported operator should fail validation")
	}
	if err := (operators.FilterConfig{Field: "a", Operator: "equals"}).Validate(); err == nil {
		t.Error("equals without a value should fail validation")
	}
	if err := (operators.FilterConfig{Field: "a", Operator: "exists"}).Validate(); err != nil {
		t.Errorf("exists needs no value: %v", err)
	}
	if err := (operators.SelectConfig{}).Validate(); err == nil {
		t.Error("select with no fields should fail")
	}
}
