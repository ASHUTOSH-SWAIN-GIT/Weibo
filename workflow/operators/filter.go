package operators

import (
	"fmt"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow/record"
)

// FilterConfig configures a built-in filter: keep records where the
// value at Field satisfies Operator against Value.
//
//   - id: completed-orders
//     type: filter
//     config:
//     field: status
//     operator: equals
//     value: completed
type FilterConfig struct {
	Field    string `yaml:"field" json:"field"`
	Operator string `yaml:"operator" json:"operator"`
	Value    any    `yaml:"value,omitempty" json:"value,omitempty"`
}

// Validate checks the config independently of any record.
func (cfg FilterConfig) Validate() error {
	if cfg.Field == "" {
		return fmt.Errorf("filter: field is required")
	}
	op := CompareOp(cfg.Operator)
	if !op.Valid() {
		return fmt.Errorf("filter: unsupported operator %q", cfg.Operator)
	}
	if op.NeedsValue() && cfg.Value == nil {
		return fmt.Errorf("filter: operator %q requires a value", cfg.Operator)
	}
	return nil
}

// BuildFilter returns an ordinary Weibo predicate. A record whose JSON
// cannot be decoded is dropped (returns false).
func BuildFilter(cfg FilterConfig) func(types.Record) bool {
	op := CompareOp(cfg.Operator)
	field := cfg.Field
	target := cfg.Value
	return func(r types.Record) bool {
		jr, err := record.DecodeJSON(r)
		if err != nil {
			return false
		}
		val, present := record.GetField(jr, field)
		keep, err := Compare(op, val, present, target)
		if err != nil {
			return false
		}
		return keep
	}
}
