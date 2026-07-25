package state_test

import (
	"fmt"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/state"
)

// Each backend gets its own test run via this table.
func backends(t *testing.T) []struct {
	Name    string
	Factory state.BackendFactory
} {
	return []struct {
		Name    string
		Factory state.BackendFactory
	}{
		{"Memory", state.InMemory()},
		{"Pebble", state.Pebble(t.TempDir())},
	}
}

func newBackend(t *testing.T, factory state.BackendFactory, owner string) state.StateBackend {
	t.Helper()
	b, err := factory(owner)
	if err != nil {
		t.Fatalf("factory(%q): %v", owner, err)
	}
	return b
}

func closeBackend(t *testing.T, b state.StateBackend) {
	t.Helper()
	if c, ok := b.(interface{ Close() error }); ok {
		if err := c.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
}

// ---- ValueState ------------------------------------------------------------

func TestConformance_Value_GetUnsetKey(t *testing.T) {
	for _, bk := range backends(t) {
		t.Run(bk.Name, func(t *testing.T) {
			b := newBackend(t, bk.Factory, "test")
			defer closeBackend(t, b)
			vs := b.ValueState("ns")
			vs.SetKey("k")
			if v := vs.Get(); v != nil {
				t.Errorf("expected nil, got %q", v)
			}
		})
	}
}

func TestConformance_Value_SetGet(t *testing.T) {
	for _, bk := range backends(t) {
		t.Run(bk.Name, func(t *testing.T) {
			b := newBackend(t, bk.Factory, "test")
			defer closeBackend(t, b)
			vs := b.ValueState("ns")
			vs.SetKey("k1")
			vs.Set([]byte("hello"))
			if v := vs.Get(); string(v) != "hello" {
				t.Errorf("expected 'hello', got %q", v)
			}
		})
	}
}

func TestConformance_Value_Overwrite(t *testing.T) {
	for _, bk := range backends(t) {
		t.Run(bk.Name, func(t *testing.T) {
			b := newBackend(t, bk.Factory, "test")
			defer closeBackend(t, b)
			vs := b.ValueState("ns")
			vs.SetKey("k")
			vs.Set([]byte("first"))
			vs.Set([]byte("second"))
			if v := vs.Get(); string(v) != "second" {
				t.Errorf("expected 'second', got %q", v)
			}
		})
	}
}

func TestConformance_Value_Clear(t *testing.T) {
	for _, bk := range backends(t) {
		t.Run(bk.Name, func(t *testing.T) {
			b := newBackend(t, bk.Factory, "test")
			defer closeBackend(t, b)
			vs := b.ValueState("ns")
			vs.SetKey("k")
			vs.Set([]byte("v"))
			vs.Clear()
			if v := vs.Get(); v != nil {
				t.Errorf("expected nil after Clear, got %q", v)
			}
		})
	}
}

func TestConformance_Value_MultipleKeys(t *testing.T) {
	for _, bk := range backends(t) {
		t.Run(bk.Name, func(t *testing.T) {
			b := newBackend(t, bk.Factory, "test")
			defer closeBackend(t, b)
			vs := b.ValueState("ns")
			vs.SetKey("alice")
			vs.Set([]byte("100"))
			vs.SetKey("bob")
			vs.Set([]byte("200"))

			vs.SetKey("alice")
			if v := vs.Get(); string(v) != "100" {
				t.Errorf("alice: got %q", v)
			}
			vs.SetKey("bob")
			if v := vs.Get(); string(v) != "200" {
				t.Errorf("bob: got %q", v)
			}
		})
	}
}

func TestConformance_Value_MultipleNamespaces(t *testing.T) {
	for _, bk := range backends(t) {
		t.Run(bk.Name, func(t *testing.T) {
			b := newBackend(t, bk.Factory, "test")
			defer closeBackend(t, b)
			a := b.ValueState("a")
			bk := b.ValueState("b")
			a.SetKey("k")
			a.Set([]byte("from-a"))
			bk.SetKey("k")
			bk.Set([]byte("from-b"))

			if v := a.Get(); string(v) != "from-a" {
				t.Errorf("namespace a: got %q", v)
			}
			if v := bk.Get(); string(v) != "from-b" {
				t.Errorf("namespace b: got %q", v)
			}
		})
	}
}

func TestConformance_Value_SnapshotAll(t *testing.T) {
	for _, bk := range backends(t) {
		t.Run(bk.Name, func(t *testing.T) {
			b := newBackend(t, bk.Factory, "test")
			defer closeBackend(t, b)
			vs := b.ValueState("reduce")
			vs.SetKey("alice")
			vs.Set([]byte("100"))
			vs.SetKey("bob")
			vs.Set([]byte("200"))

			snap := vs.SnapshotAll()
			if len(snap) != 2 {
				t.Fatalf("expected 2 entries, got %d", len(snap))
			}
			if string(snap["alice"]) != "100" {
				t.Errorf("alice: got %q", snap["alice"])
			}
			if string(snap["bob"]) != "200" {
				t.Errorf("bob: got %q", snap["bob"])
			}
		})
	}
}

func TestConformance_Value_RestoreAll(t *testing.T) {
	for _, bk := range backends(t) {
		t.Run(bk.Name, func(t *testing.T) {
			b := newBackend(t, bk.Factory, "test")
			defer closeBackend(t, b)
			vs := b.ValueState("reduce")
			vs.SetKey("old")
			vs.Set([]byte("should-be-gone"))

			if err := vs.RestoreAll(map[string][]byte{
				"alice": []byte("400"),
				"carol": []byte("300"),
			}); err != nil {
				t.Fatalf("RestoreAll: %v", err)
			}

			vs.SetKey("alice")
			if v := vs.Get(); string(v) != "400" {
				t.Errorf("alice: got %q", v)
			}
			vs.SetKey("old")
			if v := vs.Get(); v != nil {
				t.Errorf("old key should be gone: got %q", v)
			}
			vs.SetKey("carol")
			if v := vs.Get(); string(v) != "300" {
				t.Errorf("carol: got %q", v)
			}
		})
	}
}

func TestConformance_Value_SnapshotRestoreRoundtrip(t *testing.T) {
	for _, bk := range backends(t) {
		t.Run(bk.Name, func(t *testing.T) {
			b := newBackend(t, bk.Factory, "test")
			defer closeBackend(t, b)
			vs := b.ValueState("reduce")
			for i := 0; i < 50; i++ {
				vs.SetKey(fmt.Sprintf("key-%d", i))
				vs.Set([]byte(fmt.Sprintf("val-%d", i)))
			}

			snap := vs.SnapshotAll()
			if len(snap) != 50 {
				t.Fatalf("snapshot: expected 50, got %d", len(snap))
			}

			if err := vs.RestoreAll(snap); err != nil {
				t.Fatalf("RestoreAll: %v", err)
			}

			for i := 0; i < 50; i++ {
				vs.SetKey(fmt.Sprintf("key-%d", i))
				want := fmt.Sprintf("val-%d", i)
				if v := vs.Get(); string(v) != want {
					t.Errorf("key-%d: got %q, want %q", i, v, want)
				}
			}
		})
	}
}

// ---- ListState ------------------------------------------------------------

func TestConformance_List_AppendGetAll(t *testing.T) {
	for _, bk := range backends(t) {
		t.Run(bk.Name, func(t *testing.T) {
			b := newBackend(t, bk.Factory, "test")
			defer closeBackend(t, b)
			ls := b.ListState("events")
			ls.SetKey("k")
			ls.Append([]byte("a"))
			ls.Append([]byte("b"))
			ls.Append([]byte("c"))

			all := ls.GetAll()
			if len(all) != 3 {
				t.Fatalf("expected 3 entries, got %d", len(all))
			}
			for i, want := range []string{"a", "b", "c"} {
				if string(all[i]) != want {
					t.Errorf("entry %d: got %q, want %q", i, all[i], want)
				}
			}
		})
	}
}

func TestConformance_List_GetAllEmpty(t *testing.T) {
	for _, bk := range backends(t) {
		t.Run(bk.Name, func(t *testing.T) {
			b := newBackend(t, bk.Factory, "test")
			defer closeBackend(t, b)
			ls := b.ListState("events")
			ls.SetKey("k")
			if all := ls.GetAll(); len(all) != 0 {
				t.Errorf("expected empty, got %v", all)
			}
		})
	}
}

func TestConformance_List_Clear(t *testing.T) {
	for _, bk := range backends(t) {
		t.Run(bk.Name, func(t *testing.T) {
			b := newBackend(t, bk.Factory, "test")
			defer closeBackend(t, b)
			ls := b.ListState("events")
			ls.SetKey("k")
			ls.Append([]byte("a"))
			ls.Append([]byte("b"))
			ls.Clear()
			if all := ls.GetAll(); len(all) != 0 {
				t.Errorf("expected empty after Clear, got %v", all)
			}
		})
	}
}

func TestConformance_List_MultipleKeys(t *testing.T) {
	for _, bk := range backends(t) {
		t.Run(bk.Name, func(t *testing.T) {
			b := newBackend(t, bk.Factory, "test")
			defer closeBackend(t, b)
			ls := b.ListState("events")
			ls.SetKey("k1")
			ls.Append([]byte("a1"))
			ls.SetKey("k2")
			ls.Append([]byte("b1"))
			ls.SetKey("k1")
			ls.Append([]byte("a2"))

			ls.SetKey("k1")
			if all := ls.GetAll(); len(all) != 2 {
				t.Fatalf("k1: expected 2, got %d", len(all))
			}
			ls.SetKey("k2")
			if all := ls.GetAll(); len(all) != 1 {
				t.Fatalf("k2: expected 1, got %d", len(all))
			}
		})
	}
}

func TestConformance_List_Keys(t *testing.T) {
	for _, bk := range backends(t) {
		t.Run(bk.Name, func(t *testing.T) {
			b := newBackend(t, bk.Factory, "test")
			defer closeBackend(t, b)
			ls := b.ListState("windows")

			if len(ls.Keys()) != 0 {
				t.Fatalf("empty namespace should have no keys, got %v", ls.Keys())
			}

			// Keys that contain the delimiter bytes windows use, plus a
			// NUL, to prove positional extraction is robust.
			want := map[string]bool{
				"alice/2026/2027": true,
				"bob/x/y":         true,
				"has\x00nul/1/2":  true,
			}
			for k := range want {
				ls.SetKey(k)
				ls.Append([]byte("r1"))
				ls.Append([]byte("r2"))
			}

			got := ls.Keys()
			if len(got) != len(want) {
				t.Fatalf("expected %d keys, got %d: %v", len(want), len(got), got)
			}
			for _, k := range got {
				if !want[k] {
					t.Errorf("unexpected key %q", k)
				}
			}

			// A cleared key disappears from Keys(); others remain.
			ls.SetKey("bob/x/y")
			ls.Clear()
			got = ls.Keys()
			if len(got) != 2 {
				t.Fatalf("after clear expected 2 keys, got %d: %v", len(got), got)
			}
			for _, k := range got {
				if k == "bob/x/y" {
					t.Errorf("cleared key still present")
				}
			}
		})
	}
}

// ---- Owner isolation -------------------------------------------------------

func TestConformance_OwnerIsolation(t *testing.T) {
	for _, bk := range backends(t) {
		t.Run(bk.Name, func(t *testing.T) {
			f := bk.Factory
			b1 := newBackend(t, f, "owner-1")
			defer closeBackend(t, b1)
			b2 := newBackend(t, f, "owner-2")
			defer closeBackend(t, b2)

			vs1 := b1.ValueState("reduce")
			vs1.SetKey("k")
			vs1.Set([]byte("from-1"))

			vs2 := b2.ValueState("reduce")
			vs2.SetKey("k")
			vs2.Set([]byte("from-2"))

			if v := vs1.Get(); string(v) != "from-1" {
				t.Errorf("owner-1: got %q", v)
			}
			if v := vs2.Get(); string(v) != "from-2" {
				t.Errorf("owner-2: got %q", v)
			}
		})
	}
}
