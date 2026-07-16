package operators

import (
	"fmt"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/record"
)

// FieldRename moves a field from one dotted path to another.
type FieldRename struct {
	From string `yaml:"from" json:"from"`
	To   string `yaml:"to" json:"to"`
}

// RenameConfig configures a built-in rename: apply each rename in order.
//
//   - id: normalize
//     type: rename_fields
//     config:
//     renames:
//   - { from: cust_id, to: customer_id }
//   - { from: amt, to: payment.amount }
type RenameConfig struct {
	Renames []FieldRename `yaml:"renames" json:"renames"`
}

// Validate checks the config independently of any record.
func (cfg RenameConfig) Validate() error {
	if len(cfg.Renames) == 0 {
		return fmt.Errorf("rename_fields: at least one rename is required")
	}
	for i, rn := range cfg.Renames {
		if rn.From == "" || rn.To == "" {
			return fmt.Errorf("rename_fields: renames[%d] needs both from and to", i)
		}
	}
	return nil
}

// BuildRename returns an ordinary Mailer map function that renames
// fields in order. A rename whose source field is absent is skipped.
func BuildRename(cfg RenameConfig) func(types.Record) types.Record {
	renames := append([]FieldRename(nil), cfg.Renames...)
	return func(r types.Record) types.Record {
		return applyMap(r, func(in record.JSONRecord) (record.JSONRecord, error) {
			for _, rn := range renames {
				v, ok := record.GetField(in, rn.From)
				if !ok {
					continue
				}
				if err := record.SetField(in, rn.To, v); err != nil {
					continue // structural conflict at destination: leave source intact
				}
				_ = record.DeleteField(in, rn.From)
			}
			return in, nil
		})
	}
}
