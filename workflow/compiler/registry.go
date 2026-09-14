package compiler

import (
	"fmt"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// FunctionRegistry resolves ref-based map/flatMap/process workflow operators
// into ordinary Go functions. A nil or empty registry keeps the compiler fully
// declarative and rejects refs.
type FunctionRegistry struct {
	Maps      map[string]func(types.Record) types.Record
	FlatMaps  map[string]func(types.Record) []types.Record
	Processes map[string]func(types.Record) (types.Record, error)
}

// RegisterMap registers a 1:1 transform ref.
func (r *FunctionRegistry) RegisterMap(name string, fn func(types.Record) types.Record) {
	if r.Maps == nil {
		r.Maps = make(map[string]func(types.Record) types.Record)
	}
	r.Maps[name] = fn
}

// RegisterFlatMap registers a 1:N transform ref.
func (r *FunctionRegistry) RegisterFlatMap(name string, fn func(types.Record) []types.Record) {
	if r.FlatMaps == nil {
		r.FlatMaps = make(map[string]func(types.Record) []types.Record)
	}
	r.FlatMaps[name] = fn
}

// RegisterProcess registers an error-aware 1:1 transform ref.
func (r *FunctionRegistry) RegisterProcess(name string, fn func(types.Record) (types.Record, error)) {
	if r.Processes == nil {
		r.Processes = make(map[string]func(types.Record) (types.Record, error))
	}
	r.Processes[name] = fn
}

func (r *FunctionRegistry) mapFn(name string) (func(types.Record) types.Record, error) {
	if r == nil || r.Maps == nil || r.Maps[name] == nil {
		return nil, fmt.Errorf("function registry has no map ref %q", name)
	}
	return r.Maps[name], nil
}

func (r *FunctionRegistry) flatMapFn(name string) (func(types.Record) []types.Record, error) {
	if r == nil || r.FlatMaps == nil || r.FlatMaps[name] == nil {
		return nil, fmt.Errorf("function registry has no flatMap ref %q", name)
	}
	return r.FlatMaps[name], nil
}

func (r *FunctionRegistry) processFn(name string) (func(types.Record) (types.Record, error), error) {
	if r == nil || r.Processes == nil || r.Processes[name] == nil {
		return nil, fmt.Errorf("function registry has no process ref %q", name)
	}
	return r.Processes[name], nil
}
