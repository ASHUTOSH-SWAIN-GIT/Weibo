package state_test

import (
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/state"
)

func TestPebbleBackend_ValueState_Basic(t *testing.T) {
	dir := t.TempDir()
	factory := state.Pebble(dir)
	backend, err := factory("test-owner")
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	defer backend.(interface{ Close() error }).Close()

	vs := backend.ValueState("test-ns")
	vs.SetKey("key1")
	if val := vs.Get(); val != nil {
		t.Errorf("expected nil for unset key, got %q", val)
	}
	vs.Set([]byte("hello"))
	if val := vs.Get(); string(val) != "hello" {
		t.Errorf("expected 'hello', got %q", val)
	}
	vs.Set([]byte("world"))
	if val := vs.Get(); string(val) != "world" {
		t.Errorf("expected 'world', got %q", val)
	}
	vs.Clear()
	if val := vs.Get(); val != nil {
		t.Errorf("expected nil after Clear, got %q", val)
	}
}

func TestPebbleBackend_ValueState_SnapshotRestore(t *testing.T) {
	dir := t.TempDir()
	factory := state.Pebble(dir)
	backend, err := factory("test-owner")
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	defer backend.(interface{ Close() error }).Close()

	vs := backend.ValueState("reduce")
	vs.SetKey("alice")
	vs.Set([]byte("100"))
	vs.SetKey("bob")
	vs.Set([]byte("200"))

	snap := vs.SnapshotAll()
	if len(snap) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(snap))
	}
	if string(snap["alice"]) != "100" || string(snap["bob"]) != "200" {
		t.Errorf("snapshot mismatch: %v", snap)
	}

	if err := vs.RestoreAll(map[string][]byte{"alice": []byte("400"), "carol": []byte("300")}); err != nil {
		t.Fatalf("RestoreAll: %v", err)
	}
	vs.SetKey("alice")
	if val := vs.Get(); string(val) != "400" {
		t.Errorf("after restore alice: got %q, want '400'", val)
	}
	vs.SetKey("bob")
	if val := vs.Get(); val != nil {
		t.Errorf("after restore bob should be gone, got %q", val)
	}
	vs.SetKey("carol")
	if val := vs.Get(); string(val) != "300" {
		t.Errorf("after restore carol: got %q, want '300'", val)
	}
}

func TestPebbleBackend_ListState_Basic(t *testing.T) {
	dir := t.TempDir()
	factory := state.Pebble(dir)
	backend, err := factory("test-owner")
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	defer backend.(interface{ Close() error }).Close()

	ls := backend.ListState("events")
	ls.SetKey("key1")
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

	ls.Clear()
	if len(ls.GetAll()) != 0 {
		t.Errorf("expected empty after Clear, got %v", ls.GetAll())
	}
}

func TestPebbleBackend_Isolation(t *testing.T) {
	dir := t.TempDir()
	factory := state.Pebble(dir)

	b1, _ := factory("owner-1")
	defer b1.(interface{ Close() error }).Close()
	b2, _ := factory("owner-2")
	defer b2.(interface{ Close() error }).Close()

	vs1 := b1.ValueState("reduce")
	vs1.SetKey("x")
	vs1.Set([]byte("from-owner1"))

	vs2 := b2.ValueState("reduce")
	vs2.SetKey("x")
	vs2.Set([]byte("from-owner2"))

	if val := vs1.Get(); string(val) != "from-owner1" {
		t.Errorf("owner1: got %q", val)
	}
	if val := vs2.Get(); string(val) != "from-owner2" {
		t.Errorf("owner2: got %q", val)
	}
}
