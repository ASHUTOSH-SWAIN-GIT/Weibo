// Package operators provides built-in stateless operators for
// declarative workflows. Each builder turns a typed config into an
// ordinary Weibo function — a func(types.Record) bool for filters, or
// a func(types.Record) types.Record for field transforms — so a YAML
// operator behaves exactly like an equivalent hand-written SDK function.
//
// The operators work on the JSON record model (workflow/record):
// records are decoded to a JSONRecord, fields are addressed by dotted
// path, and numbers are json.Number (exact, no float rounding).
package operators

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// CompareOp is a filter comparison operator.
type CompareOp string

const (
	OpEquals             CompareOp = "equals"
	OpNotEquals          CompareOp = "not_equals"
	OpGreaterThan        CompareOp = "greater_than"
	OpGreaterThanOrEqual CompareOp = "greater_than_or_equal"
	OpLessThan           CompareOp = "less_than"
	OpLessThanOrEqual    CompareOp = "less_than_or_equal"
	OpContains           CompareOp = "contains"
	OpExists             CompareOp = "exists"
	OpNotExists          CompareOp = "not_exists"
)

var compareOps = map[CompareOp]bool{
	OpEquals: true, OpNotEquals: true,
	OpGreaterThan: true, OpGreaterThanOrEqual: true,
	OpLessThan: true, OpLessThanOrEqual: true,
	OpContains: true, OpExists: true, OpNotExists: true,
}

// Valid reports whether op is a supported comparison operator.
func (op CompareOp) Valid() bool { return compareOps[op] }

// NeedsValue reports whether op compares against a configured value
// (everything except exists/not_exists).
func (op CompareOp) NeedsValue() bool { return op != OpExists && op != OpNotExists }

// Compare evaluates op against a field value. present says whether the
// field existed at all (a missing field is not equal to / greater than
// anything, but exists/not_exists still work). target is the configured
// comparison value.
//
// Coercion makes YAML and JSON configs behave identically: record
// numbers are json.Number and config numbers arrive as int (YAML) or
// float64 (JSON); both are normalized before comparing. Numbers only
// compare with numbers, strings with strings, bools with bools — a
// number never equals a quoted string, so behavior is predictable.
func Compare(op CompareOp, fieldVal any, present bool, target any) (bool, error) {
	switch op {
	case OpExists:
		return present, nil
	case OpNotExists:
		return !present, nil
	}
	if !op.Valid() {
		return false, fmt.Errorf("operators: unsupported comparison operator %q", op)
	}
	if !present {
		return false, nil // missing field: every value comparison is false
	}
	switch op {
	case OpEquals:
		return valuesEqual(fieldVal, target), nil
	case OpNotEquals:
		return !valuesEqual(fieldVal, target), nil
	case OpGreaterThan:
		return orderedCmp(fieldVal, target, func(c int) bool { return c > 0 }), nil
	case OpGreaterThanOrEqual:
		return orderedCmp(fieldVal, target, func(c int) bool { return c >= 0 }), nil
	case OpLessThan:
		return orderedCmp(fieldVal, target, func(c int) bool { return c < 0 }), nil
	case OpLessThanOrEqual:
		return orderedCmp(fieldVal, target, func(c int) bool { return c <= 0 }), nil
	case OpContains:
		return contains(fieldVal, target), nil
	}
	return false, fmt.Errorf("operators: unsupported comparison operator %q", op)
}

// valuesEqual compares for equality with type awareness.
func valuesEqual(a, b any) bool {
	an, aNum := asNumber(a)
	bn, bNum := asNumber(b)
	if aNum && bNum {
		return numbersEqual(an, bn)
	}
	if aNum != bNum {
		return false // number vs non-number
	}
	if ab, ok := a.(bool); ok {
		bb, ok2 := b.(bool)
		return ok2 && ab == bb
	}
	if as, ok := a.(string); ok {
		bs, ok2 := b.(string)
		return ok2 && as == bs
	}
	return false
}

// orderedCmp compares a and b for ordering and applies pred to the sign
// (-1/0/1). Not-comparable pairs (e.g. a number vs a non-numeric
// string) yield false.
func orderedCmp(a, b any, pred func(int) bool) bool {
	if af, aok := asFloat(a); aok {
		if bf, bok := asFloat(b); bok {
			switch {
			case af < bf:
				return pred(-1)
			case af > bf:
				return pred(1)
			default:
				return pred(0)
			}
		}
		return false
	}
	// Fall back to lexicographic ordering for two strings (useful for
	// ISO date/time strings).
	if as, aok := a.(string); aok {
		if bs, bok := b.(string); bok {
			return pred(strings.Compare(as, bs))
		}
	}
	return false
}

// contains supports substring (string field) and membership (array field).
func contains(field, target any) bool {
	switch f := field.(type) {
	case string:
		return strings.Contains(f, looseString(target))
	case []any:
		for _, e := range f {
			if valuesEqual(e, target) {
				return true
			}
		}
	}
	return false
}

// ---- numeric coercion ------------------------------------------------------

// asNumber coerces actual numeric types (not strings) to json.Number,
// so numeric comparison never accidentally treats a string like "100"
// as the number 100.
func asNumber(v any) (json.Number, bool) {
	switch n := v.(type) {
	case json.Number:
		return n, true
	case int:
		return json.Number(strconv.Itoa(n)), true
	case int64:
		return json.Number(strconv.FormatInt(n, 10)), true
	case float64:
		return json.Number(strconv.FormatFloat(n, 'g', -1, 64)), true
	default:
		return "", false
	}
}

func asFloat(v any) (float64, bool) {
	n, ok := asNumber(v)
	if !ok {
		return 0, false
	}
	f, err := n.Float64()
	return f, err == nil
}

// numbersEqual compares two json.Numbers exactly when both are integers
// (no float rounding for large ints), else by float value.
func numbersEqual(a, b json.Number) bool {
	if ai, err := a.Int64(); err == nil {
		if bi, err2 := b.Int64(); err2 == nil {
			return ai == bi
		}
	}
	af, aerr := a.Float64()
	bf, berr := b.Float64()
	return aerr == nil && berr == nil && af == bf
}

// looseString renders a target as a string for substring matching.
func looseString(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case json.Number:
		return string(s)
	case nil:
		return ""
	default:
		return fmt.Sprint(v)
	}
}
