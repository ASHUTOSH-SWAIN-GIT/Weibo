package record_test

import (
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow/record"
)

// FuzzRecordFieldPaths feeds arbitrary documents and paths through the
// field operators. Invariants: nothing panics, and a successful Set is
// always observable through Get.
func FuzzRecordFieldPaths(f *testing.F) {
	f.Add(`{"customer":{"id":"c1"},"amount":10}`, "customer.id")
	f.Add(`{"a":1}`, "")
	f.Add(`{"a":1}`, "a..b")
	f.Add(`{"customer":"c-1"}`, "customer.id")
	f.Add(`{}`, "x.y.z")

	f.Fuzz(func(t *testing.T, doc, path string) {
		jr, err := record.DecodeJSON(types.Record{Value: []byte(doc)})
		if err != nil {
			return // malformed input is a normal rejection
		}
		_, _ = record.GetField(jr, path)
		if err := record.SetField(jr, path, "v"); err != nil {
			return // structural conflict or malformed path: rejected
		}
		got, ok := record.GetField(jr, path)
		if !ok || got != "v" {
			t.Fatalf("Set(%q) succeeded but Get returned (%v, %v)", path, got, ok)
		}
		if err := record.DeleteField(jr, path); err != nil {
			t.Fatalf("Delete(%q) after successful Set: %v", path, err)
		}
		if _, ok := record.GetField(jr, path); ok {
			t.Fatalf("Get(%q) after Delete still resolves", path)
		}
		if _, err := record.EncodeJSON(jr); err != nil {
			t.Fatalf("EncodeJSON after fuzz ops: %v", err)
		}
	})
}
