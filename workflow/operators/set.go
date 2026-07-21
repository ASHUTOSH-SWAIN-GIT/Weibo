package operators

import (
	"fmt"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/record"
)

// FieldSet assigns a constant value to a dotted field path.
type FieldSet struct {
	Field string `yaml:"field" json:"field"`
	Value any    `yaml:"value" json:"value"`
}

// SetConfig configures a built-in set_fields: assign each field in order
// (creating nested objects as needed).
//
//   - id: tag
//     type: setFields
//     config:
//     sets:
//   - { field: source, value: web }
//   - { field: flags.processed, value: true }
type SetConfig struct {
	Sets []FieldSet `yaml:"sets" json:"sets"`
}

// Validate checks the config independently of any record.
func (cfg SetConfig) Validate() error {
	if len(cfg.Sets) == 0 {
		return fmt.Errorf("set_fields: at least one set is required")
	}
	for i, s := range cfg.Sets {
		if s.Field == "" {
			return fmt.Errorf("set_fields: sets[%d] field is required", i)
		}
	}
	return nil
}

// BuildSet returns an ordinary Mailer map function that assigns each
// configured field.
func BuildSet(cfg SetConfig) func(types.Record) types.Record {
	sets := append([]FieldSet(nil), cfg.Sets...)
	return func(r types.Record) types.Record {
		return applyMap(r, func(in record.JSONRecord) (record.JSONRecord, error) {
			for _, s := range sets {
				_ = record.SetField(in, s.Field, s.Value)
			}
			return in, nil
		})
	}
}
