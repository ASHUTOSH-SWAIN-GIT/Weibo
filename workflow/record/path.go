package record

import (
	"fmt"
	"strings"
)

// Paths are dot-separated field names, e.g. "customer.id" or
// "payment.amount". Each segment indexes one level of nested object.
// Field names containing "." are not addressable (out of scope).

// splitPath splits a dotted path into segments, rejecting an empty path
// or empty segments (e.g. "a..b", ".a", "a.").
func splitPath(path string) ([]string, error) {
	if path == "" {
		return nil, fmt.Errorf("record: empty field path")
	}
	segs := strings.Split(path, ".")
	for _, s := range segs {
		if s == "" {
			return nil, fmt.Errorf("record: invalid field path %q (empty segment)", path)
		}
	}
	return segs, nil
}

// asObject coerces a value to a mutable object map. Nested objects
// decode as map[string]any; the top-level is a JSONRecord — both are
// the same underlying type but distinct to Go's type system.
func asObject(v any) (map[string]any, bool) {
	switch m := v.(type) {
	case JSONRecord:
		return map[string]any(m), true
	case map[string]any:
		return m, true
	default:
		return nil, false
	}
}

// GetField returns the value at a dotted path and whether it was
// present. A JSON null that is present returns (nil, true); a missing
// field or a path that traverses a non-object returns (nil, false).
// Numbers are returned as json.Number.
func GetField(data JSONRecord, path string) (any, bool) {
	segs, err := splitPath(path)
	if err != nil {
		return nil, false
	}
	cur := map[string]any(data)
	for i, s := range segs {
		v, ok := cur[s]
		if !ok {
			return nil, false
		}
		if i == len(segs)-1 {
			return v, true
		}
		cur, ok = asObject(v)
		if !ok {
			return nil, false // intermediate is not an object
		}
	}
	return nil, false
}

// SetField sets the value at a dotted path, creating intermediate
// objects as needed. It returns an error only if an existing
// intermediate value is not an object (a structural conflict) or the
// path is malformed. The mutation is applied in place (maps are
// references), so the caller's JSONRecord is updated.
func SetField(data JSONRecord, path string, value any) error {
	segs, err := splitPath(path)
	if err != nil {
		return err
	}
	cur := map[string]any(data)
	for i, s := range segs {
		if i == len(segs)-1 {
			cur[s] = value
			return nil
		}
		next, ok := cur[s]
		if !ok {
			m := map[string]any{}
			cur[s] = m
			cur = m
			continue
		}
		obj, ok := asObject(next)
		if !ok {
			return fmt.Errorf("record: cannot set %q: %q is not an object", path, strings.Join(segs[:i+1], "."))
		}
		cur = obj
	}
	return nil
}

// DeleteField removes the value at a dotted path. Deleting a field that
// (or whose parent) does not exist is a no-op and returns nil
// (idempotent). It returns an error only if an existing intermediate
// value is not an object, or the path is malformed.
func DeleteField(data JSONRecord, path string) error {
	segs, err := splitPath(path)
	if err != nil {
		return err
	}
	cur := map[string]any(data)
	for i, s := range segs {
		if i == len(segs)-1 {
			delete(cur, s)
			return nil
		}
		next, ok := cur[s]
		if !ok {
			return nil // parent missing: nothing to delete
		}
		obj, ok := asObject(next)
		if !ok {
			return fmt.Errorf("record: cannot delete %q: %q is not an object", path, strings.Join(segs[:i+1], "."))
		}
		cur = obj
	}
	return nil
}
