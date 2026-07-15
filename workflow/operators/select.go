package operators

import (
	"fmt"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/record"
)

// SelectConfig configures a built-in select_fields (projection): keep
// only the listed fields, dropping everything else. Fields are dotted
// paths, so nested fields can be projected too.
//
//	- id: output-fields
//	  type: select_fields
//	  config:
//	    fields:
//	      - customer_id
//	      - amount
type SelectConfig struct {
	Fields []string `yaml:"fields" json:"fields"`
}

// Validate checks the config independently of any record.
func (cfg SelectConfig) Validate() error {
	if len(cfg.Fields) == 0 {
		return fmt.Errorf("select_fields: at least one field is required")
	}
	for _, f := range cfg.Fields {
		if f == "" {
			return fmt.Errorf("select_fields: field names must not be empty")
		}
	}
	return nil
}

// BuildSelect returns an ordinary Mailer map function that projects each
// record to the configured fields. A field absent from the input is
// simply omitted from the output.
func BuildSelect(cfg SelectConfig) func(types.Record) types.Record {
	fields := append([]string(nil), cfg.Fields...)
	return func(r types.Record) types.Record {
		return applyMap(r, func(in record.JSONRecord) (record.JSONRecord, error) {
			out := record.JSONRecord{}
			for _, f := range fields {
				if v, ok := record.GetField(in, f); ok {
					_ = record.SetField(out, f, v)
				}
			}
			return out, nil
		})
	}
}

// applyMap decodes a record's JSON, applies transform, and re-encodes
// the result into a new record (Value and Parsed updated, all other
// fields preserved). A record whose JSON cannot be decoded or encoded
// is passed through unchanged — a map operator must not drop records.
func applyMap(r types.Record, transform func(in record.JSONRecord) (record.JSONRecord, error)) types.Record {
	in, err := record.DecodeJSON(r)
	if err != nil {
		return r
	}
	out, err := transform(in)
	if err != nil {
		return r
	}
	b, err := record.EncodeJSON(out)
	if err != nil {
		return r
	}
	nr := r
	nr.Value = b
	nr.Parsed = out
	return nr
}
