package compiler

import (
	"encoding/json"
	"fmt"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/source"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/window"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/operators"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/record"
)

// applyOperators applies each declarative operator to the stream in
// order, returning the stream after the last one. Only the built-in
// declarative operators are compilable; ref-based map/flatMap/process
// require a function registry and return an error.
func applyOperators(env *mailer.StreamExecutionEnv, src source.Source, ops []workflow.Operator) (*mailer.Stream, error) {
	stream := env.FromSource(src)
	for i, op := range ops {
		var err error
		stream, err = applyOperator(stream, op)
		if err != nil {
			return nil, fmt.Errorf("compiler: pipeline[%d] %q: %w", i, op.ID, err)
		}
	}
	return stream, nil
}

func applyOperator(stream *mailer.Stream, op workflow.Operator) (*mailer.Stream, error) {
	switch {
	case op.Filter != nil:
		fn := operators.BuildFilter(operators.FilterConfig{
			Field: op.Filter.Field, Operator: op.Filter.Operator, Value: op.Filter.Value,
		})
		return stream.Filter(fn, op.ID), nil

	case op.SelectFields != nil:
		fn := operators.BuildSelect(operators.SelectConfig{Fields: op.SelectFields.Fields})
		return stream.Map(fn, op.ID), nil

	case op.RenameFields != nil:
		renames := make([]operators.FieldRename, len(op.RenameFields.Renames))
		for i, r := range op.RenameFields.Renames {
			renames[i] = operators.FieldRename{From: r.From, To: r.To}
		}
		fn := operators.BuildRename(operators.RenameConfig{Renames: renames})
		return stream.Map(fn, op.ID), nil

	case op.SetFields != nil:
		sets := make([]operators.FieldSet, len(op.SetFields.Sets))
		for i, s := range op.SetFields.Sets {
			sets[i] = operators.FieldSet{Field: s.Field, Value: s.Value}
		}
		fn := operators.BuildSet(operators.SetConfig{Sets: sets})
		return stream.Map(fn, op.ID), nil

	case op.KeyBy != nil:
		return stream.KeyBy(buildKeySelector(op.KeyBy.Field), op.ID).
			WithPartitions(partitionsOrDefault(op.KeyBy.Partitions)), nil

	case op.Reduce != nil:
		fn, err := buildReducer(op.Reduce)
		if err != nil {
			return nil, err
		}
		return stream.Reduce(fn, op.ID), nil

	case op.Window != nil:
		assigner, err := buildWindowAssigner(op.Window)
		if err != nil {
			return nil, err
		}
		if op.Window.IdleTimeout > 0 {
			return stream.WindowWithIdleTimeout(assigner, op.Window.IdleTimeout.Std(), op.ID), nil
		}
		return stream.Window(assigner, op.ID), nil

	case op.Map != nil, op.FlatMap != nil, op.Process != nil:
		return nil, fmt.Errorf("ref-based operators (map/flatMap/process) require a function registry, which the declarative compiler does not provide")

	default:
		return nil, fmt.Errorf("operator %q has no recognized config block", op.Type)
	}
}

func partitionsOrDefault(n int) int {
	if n <= 0 {
		return 16
	}
	return n
}

// buildKeySelector extracts a record field as the partition key.
func buildKeySelector(field string) func(types.Record) []byte {
	return func(r types.Record) []byte {
		jr, err := record.DecodeJSON(r)
		if err != nil {
			return nil
		}
		v, ok := record.GetField(jr, field)
		if !ok {
			return nil
		}
		return []byte(fmt.Sprint(v))
	}
}

// buildReducer builds a built-in keyed aggregation. Both count and sum
// emit a JSON accumulator ({"count": n} / {"sum": x}) so downstream JSON
// operators and sinks can consume the result.
func buildReducer(cfg *workflow.ReduceConfig) (func([]byte, types.Record) []byte, error) {
	switch cfg.Function {
	case "count":
		return func(accum []byte, _ types.Record) []byte {
			n := readAgg(accum, "count")
			return writeAgg("count", n+1)
		}, nil
	case "sum":
		if cfg.Field == "" {
			return nil, fmt.Errorf("reduce sum requires a field")
		}
		field := cfg.Field
		return func(accum []byte, r types.Record) []byte {
			s := readAgg(accum, "sum")
			jr, err := record.DecodeJSON(r)
			if err == nil {
				if v, ok := record.GetField(jr, field); ok {
					s += toFloat(v)
				}
			}
			return writeAgg("sum", s)
		}, nil
	default:
		return nil, fmt.Errorf("unsupported reduce function %q (count or sum)", cfg.Function)
	}
}

func readAgg(accum []byte, key string) float64 {
	if len(accum) == 0 {
		return 0
	}
	var m map[string]json.Number
	if json.Unmarshal(accum, &m) != nil {
		return 0
	}
	f, _ := m[key].Float64()
	return f
}

func writeAgg(key string, val float64) []byte {
	// Emit integers without a fractional part for clean output.
	var num json.Number
	if val == float64(int64(val)) {
		num = json.Number(fmt.Sprintf("%d", int64(val)))
	} else {
		num = json.Number(fmt.Sprintf("%g", val))
	}
	b, _ := json.Marshal(map[string]json.Number{key: num})
	return b
}

func toFloat(v any) float64 {
	switch n := v.(type) {
	case json.Number:
		f, _ := n.Float64()
		return f
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	default:
		return 0
	}
}

func buildWindowAssigner(cfg *workflow.WindowConfig) (window.WindowAssigner, error) {
	switch cfg.Type {
	case "tumbling":
		if cfg.Size <= 0 {
			return nil, fmt.Errorf("tumbling window requires a positive size")
		}
		w := window.NewTumbling(cfg.Size.Std())
		if cfg.Offset > 0 {
			w = w.WithOffset(cfg.Offset.Std())
		}
		return w, nil
	case "sliding":
		if cfg.Size <= 0 || cfg.Slide <= 0 {
			return nil, fmt.Errorf("sliding window requires positive size and slide")
		}
		w := window.NewSliding(cfg.Size.Std(), cfg.Slide.Std())
		if cfg.Offset > 0 {
			w = w.WithOffset(cfg.Offset.Std())
		}
		return w, nil
	case "session":
		if cfg.Gap <= 0 {
			return nil, fmt.Errorf("session window requires a positive gap")
		}
		return window.NewSession(cfg.Gap.Std()), nil
	default:
		return nil, fmt.Errorf("unsupported window type %q (tumbling, sliding, session)", cfg.Type)
	}
}
