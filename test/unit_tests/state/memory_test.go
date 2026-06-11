package state_test

import (
	"testing"

	"mailer/state"
)

func TestMemoryBackend_ValueState_GetSet(t *testing.T) {
	mb := state.NewMemoryBackend()
	vs := mb.ValueState("test")

	vs.SetKey("key1")
	vs.Set([]byte("value1"))

	got := vs.Get()
	if string(got) != "value1" {
		t.Errorf("Get: got %q, want %q", got, "value1")
	}
}

func TestMemoryBackend_ValueState_GetNil(t *testing.T) {
	mb := state.NewMemoryBackend()
	vs := mb.ValueState("test")

	vs.SetKey("nonexistent")
	got := vs.Get()
	if got != nil {
		t.Errorf("Get on unset key: got %v, want nil", got)
	}
}

func TestMemoryBackend_ValueState_Overwrite(t *testing.T) {
	mb := state.NewMemoryBackend()
	vs := mb.ValueState("test")

	vs.SetKey("key1")
	vs.Set([]byte("first"))
	vs.Set([]byte("second"))

	got := vs.Get()
	if string(got) != "second" {
		t.Errorf("Get after overwrite: got %q, want %q", got, "second")
	}
}

func TestMemoryBackend_ValueState_Clear(t *testing.T) {
	mb := state.NewMemoryBackend()
	vs := mb.ValueState("test")

	vs.SetKey("key1")
	vs.Set([]byte("value"))
	vs.Clear()

	got := vs.Get()
	if got != nil {
		t.Errorf("Get after Clear: got %v, want nil", got)
	}
}

func TestMemoryBackend_ValueState_MultipleKeys(t *testing.T) {
	mb := state.NewMemoryBackend()
	vs := mb.ValueState("test")

	vs.SetKey("key1")
	vs.Set([]byte("val1"))
	vs.SetKey("key2")
	vs.Set([]byte("val2"))

	vs.SetKey("key1")
	if string(vs.Get()) != "val1" {
		t.Errorf("key1: got %q, want %q", vs.Get(), "val1")
	}
	vs.SetKey("key2")
	if string(vs.Get()) != "val2" {
		t.Errorf("key2: got %q, want %q", vs.Get(), "val2")
	}
}

func TestMemoryBackend_ValueState_MultipleNames(t *testing.T) {
	mb := state.NewMemoryBackend()
	vs1 := mb.ValueState("namespace1")
	vs2 := mb.ValueState("namespace2")

	vs1.SetKey("key")
	vs1.Set([]byte("from-vs1"))

	vs2.SetKey("key")
	vs2.Set([]byte("from-vs2"))

	if string(vs1.Get()) != "from-vs1" {
		t.Errorf("vs1: got %q, want %q", vs1.Get(), "from-vs1")
	}
	if string(vs2.Get()) != "from-vs2" {
		t.Errorf("vs2: got %q, want %q", vs2.Get(), "from-vs2")
	}
}

func TestMemoryBackend_ListState_AppendAndGetAll(t *testing.T) {
	mb := state.NewMemoryBackend()
	ls := mb.ListState("test")

	ls.SetKey("key1")
	ls.Append([]byte("a"))
	ls.Append([]byte("b"))
	ls.Append([]byte("c"))

	all := ls.GetAll()
	if len(all) != 3 {
		t.Fatalf("expected 3 items, got %d", len(all))
	}
	if string(all[0]) != "a" || string(all[1]) != "b" || string(all[2]) != "c" {
		t.Errorf("items: got %v, want [a b c]", all)
	}
}

func TestMemoryBackend_ListState_GetAllEmpty(t *testing.T) {
	mb := state.NewMemoryBackend()
	ls := mb.ListState("test")

	ls.SetKey("nonexistent")
	all := ls.GetAll()
	if all != nil {
		t.Errorf("GetAll on empty: got %v, want nil", all)
	}
}

func TestMemoryBackend_ListState_Clear(t *testing.T) {
	mb := state.NewMemoryBackend()
	ls := mb.ListState("test")

	ls.SetKey("key1")
	ls.Append([]byte("a"))
	ls.Append([]byte("b"))
	ls.Clear()

	all := ls.GetAll()
	if all != nil {
		t.Errorf("GetAll after Clear: got %v, want nil", all)
	}
}

func TestMemoryBackend_ListState_MultipleKeys(t *testing.T) {
	mb := state.NewMemoryBackend()
	ls := mb.ListState("test")

	ls.SetKey("key1")
	ls.Append([]byte("1"))
	ls.SetKey("key2")
	ls.Append([]byte("2"))

	ls.SetKey("key1")
	if len(ls.GetAll()) != 1 || string(ls.GetAll()[0]) != "1" {
		t.Errorf("key1: got %v", ls.GetAll())
	}
	ls.SetKey("key2")
	if len(ls.GetAll()) != 1 || string(ls.GetAll()[0]) != "2" {
		t.Errorf("key2: got %v", ls.GetAll())
	}
}

func TestMemoryBackend_InterfaceCompliance(t *testing.T) {
	var _ state.StateBackend = state.NewMemoryBackend()
}
