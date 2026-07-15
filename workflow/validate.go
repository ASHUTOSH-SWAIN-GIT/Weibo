package workflow

import (
	"fmt"
	"os"
	"regexp"
	"strings"
)

// ValidationError is a single problem found in a workflow, located by a
// dotted document path (e.g. "env.checkpointing.interval" or
// `pipeline[2] "totals"`).
type ValidationError struct {
	Path string
	Msg  string
}

func (e *ValidationError) Error() string {
	if e.Path == "" {
		return e.Msg
	}
	return e.Path + ": " + e.Msg
}

// ValidationErrors is the aggregate result of Validate: every problem
// found, reported together.
type ValidationErrors []*ValidationError

func (v ValidationErrors) Error() string {
	var b strings.Builder
	b.WriteString("Workflow validation failed:\n")
	for _, e := range v {
		b.WriteString("  - ")
		b.WriteString(e.Error())
		b.WriteByte('\n')
	}
	return strings.TrimRight(b.String(), "\n")
}

// Validate checks a parsed workflow for structural, configuration,
// pipeline-ordering, and delivery-guarantee problems, returning every
// problem found as a ValidationErrors (or nil if the workflow is
// valid). It performs no network I/O and opens no Kafka, Postgres, or
// Pebble connection — the only side effect is creating configured
// state/checkpoint directories to verify they can be created. An
// invalid workflow therefore never reaches the runtime.
func Validate(wf *Workflow) error {
	v := &validator{}
	v.structural(wf)
	v.configuration(wf)
	v.pipeline(wf)
	v.deliveryGuarantee(wf)
	if len(v.errs) == 0 {
		return nil
	}
	return v.errs
}

var (
	supportedVersions      = map[string]bool{"": true, "1": true}
	supportedSourceTypes   = map[string]bool{"kafka": true, "slice": true, "generator": true}
	supportedSinkTypes     = map[string]bool{"kafka": true, "txnKafka": true, "postgres": true, "stdout": true, "blackhole": true}
	supportedOperatorTypes = map[string]bool{
		"filter": true, "selectFields": true, "renameFields": true, "setFields": true,
		"keyBy": true, "reduce": true, "window": true,
		"map": true, "flatMap": true, "process": true,
	}
	supportedWindowTypes   = map[string]bool{"tumbling": true, "sliding": true, "session": true}

	nameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)
)

type validator struct {
	errs ValidationErrors
}

func (v *validator) add(path, msg string) {
	v.errs = append(v.errs, &ValidationError{Path: path, Msg: msg})
}

func (v *validator) addf(path, format string, args ...any) {
	v.add(path, fmt.Sprintf(format, args...))
}

// opPath returns the document path for operator i, including its id.
func opPath(i int, op Operator) string {
	if op.ID != "" {
		return fmt.Sprintf("pipeline[%d] %q", i, op.ID)
	}
	return fmt.Sprintf("pipeline[%d]", i)
}

// configBlocks returns the names of the operator's set config blocks.
func (o Operator) configBlocks() []string {
	var b []string
	if o.Filter != nil {
		b = append(b, "filter")
	}
	if o.SelectFields != nil {
		b = append(b, "selectFields")
	}
	if o.RenameFields != nil {
		b = append(b, "renameFields")
	}
	if o.SetFields != nil {
		b = append(b, "setFields")
	}
	if o.KeyBy != nil {
		b = append(b, "keyBy")
	}
	if o.Reduce != nil {
		b = append(b, "reduce")
	}
	if o.Window != nil {
		b = append(b, "window")
	}
	if o.Map != nil {
		b = append(b, "map")
	}
	if o.FlatMap != nil {
		b = append(b, "flatMap")
	}
	if o.Process != nil {
		b = append(b, "process")
	}
	return b
}

// ---- Structural ------------------------------------------------------------

func (v *validator) structural(wf *Workflow) {
	if !supportedVersions[wf.Version] {
		v.addf("version", "unsupported workflow version %q (supported: \"1\")", wf.Version)
	}

	if wf.Name == "" {
		v.add("name", "workflow name is required")
	} else if !nameRe.MatchString(wf.Name) {
		v.addf("name", "invalid workflow name %q (letters, digits, and _.- only)", wf.Name)
	}

	if wf.Source.Type == "" {
		v.add("source", "source is required")
	} else if !supportedSourceTypes[wf.Source.Type] {
		v.addf("source.type", "unsupported source type %q", wf.Source.Type)
	}

	if wf.Sink.Type == "" {
		v.add("sink", "sink is required")
	} else if !supportedSinkTypes[wf.Sink.Type] {
		v.addf("sink.type", "unsupported sink type %q", wf.Sink.Type)
	}

	ids := make(map[string]int)
	for i, op := range wf.Pipeline {
		path := opPath(i, op)

		if op.ID == "" {
			v.add(path, "operator id is required")
		} else if prev, dup := ids[op.ID]; dup {
			v.addf(path, "duplicate operator id %q (also used by pipeline[%d])", op.ID, prev)
		} else {
			ids[op.ID] = i
		}

		if op.Type == "" {
			v.add(path, "operator type is required")
			continue
		}
		if !supportedOperatorTypes[op.Type] {
			v.addf(path, "unsupported operator type %q", op.Type)
			continue
		}

		// The typed config block must match the operator type exactly.
		blocks := op.configBlocks()
		switch {
		case len(blocks) == 0:
			v.addf(path, "operator type %q requires a %q config block", op.Type, op.Type)
		case len(blocks) > 1:
			v.addf(path, "operator has multiple config blocks %v (expected only %q)", blocks, op.Type)
		case blocks[0] != op.Type:
			v.addf(path, "operator type %q does not match config block %q", op.Type, blocks[0])
		}
	}
}

// ---- Configuration ---------------------------------------------------------

func (v *validator) configuration(wf *Workflow) {
	v.envConfig(wf.Env)
	v.sourceConfig(wf.Source)
	v.sinkConfig("sink", wf.Sink)

	for i, op := range wf.Pipeline {
		path := opPath(i, op)
		switch {
		case op.Filter != nil:
			if op.Filter.Field == "" {
				v.add(path+".filter.field", "a field is required")
			}
			if op.Filter.Operator == "" {
				v.add(path+".filter.operator", "an operator is required")
			}
		case op.SelectFields != nil:
			if len(op.SelectFields.Fields) == 0 {
				v.add(path+".selectFields.fields", "at least one field is required")
			}
		case op.RenameFields != nil:
			if len(op.RenameFields.Renames) == 0 {
				v.add(path+".renameFields.renames", "at least one rename is required")
			}
		case op.SetFields != nil:
			if len(op.SetFields.Sets) == 0 {
				v.add(path+".setFields.sets", "at least one set is required")
			}
		case op.KeyBy != nil:
			if op.KeyBy.Field == "" {
				v.add(path+".keyBy.field", "a key field is required")
			}
			if op.KeyBy.Partitions < 0 {
				v.add(path+".keyBy.partitions", "partitions must be greater than zero")
			}
		case op.Reduce != nil:
			switch op.Reduce.Function {
			case "count":
			case "sum":
				if op.Reduce.Field == "" {
					v.add(path+".reduce.field", "sum requires a field")
				}
			case "":
				v.add(path+".reduce.function", "a function is required (count or sum)")
			default:
				v.addf(path+".reduce.function", "unsupported reduce function %q (count or sum)", op.Reduce.Function)
			}
		case op.Window != nil:
			v.windowConfig(path+".window", op.Window)
		case op.Process != nil:
			if op.Process.Ref == "" {
				v.add(path+".process.ref", "a function ref is required")
			}
		case op.Map != nil:
			if op.Map.Ref == "" {
				v.add(path+".map.ref", "a function ref is required")
			}
		case op.FlatMap != nil:
			if op.FlatMap.Ref == "" {
				v.add(path+".flatMap.ref", "a function ref is required")
			}
		}
	}
}


func (v *validator) envConfig(env *EnvSpec) {
	if env == nil {
		return
	}
	if env.BufferSize < 0 {
		v.add("env.bufferSize", "buffer size must not be negative")
	}
	if env.Checkpointing != nil {
		if env.Checkpointing.Interval <= 0 {
			v.add("env.checkpointing.interval", "checkpoint interval must be greater than zero")
		}
		// Dir is optional: the compiler derives a job-specific
		// directory when it is omitted. If set, it must be creatable.
		if env.Checkpointing.Dir != "" {
			v.ensureDir("env.checkpointing.dir", env.Checkpointing.Dir)
		}
	}
	if env.State != nil {
		switch env.State.Backend {
		case "memory":
		case "pebble":
			if env.State.Dir != "" {
				v.ensureDir("env.state.dir", env.State.Dir)
			}
		case "":
			v.add("env.state.backend", "a backend is required (memory or pebble)")
		default:
			v.addf("env.state.backend", "unsupported state backend %q (memory or pebble)", env.State.Backend)
		}
	}
}

func (v *validator) sourceConfig(src SourceSpec) {
	if src.Type != "kafka" {
		return // slice/generator carry inline data; nothing external to check
	}
	if src.Kafka == nil {
		v.add("source.kafka", "kafka source configuration is required")
		return
	}
	if len(src.Kafka.Brokers) == 0 {
		v.add("source.kafka.brokers", "at least one broker is required")
	}
	if src.Kafka.Topic == "" && len(src.Kafka.Topics) == 0 {
		v.add("source.kafka.topic", "a topic (or topics) is required")
	}
	if src.Kafka.Parallel && src.Kafka.GroupID != "" {
		v.add("source.kafka", "parallel and groupID are mutually exclusive")
	}
	if src.Kafka.Watermark != nil && src.Kafka.Watermark.MaxOutOfOrderness <= 0 {
		v.add("source.kafka.watermark.maxOutOfOrderness", "must be greater than zero")
	}
}

func (v *validator) sinkConfig(path string, snk SinkSpec) {
	switch snk.Type {
	case "kafka":
		if snk.Kafka == nil {
			v.add(path+".kafka", "kafka sink configuration is required")
			return
		}
		if len(snk.Kafka.Brokers) == 0 {
			v.add(path+".kafka.brokers", "at least one broker is required")
		}
		if snk.Kafka.Topic == "" {
			v.add(path+".kafka.topic", "a topic is required")
		}
	case "txnKafka":
		if snk.TxnKafka == nil {
			v.add(path+".txnKafka", "transactional kafka sink configuration is required")
			return
		}
		if len(snk.TxnKafka.Brokers) == 0 {
			v.add(path+".txnKafka.brokers", "at least one broker is required")
		}
		if snk.TxnKafka.Topic == "" {
			v.add(path+".txnKafka.topic", "a topic is required")
		}
		if snk.TxnKafka.TransactionalID == "" {
			v.add(path+".txnKafka.transactionalID", "a transactional id is required for transactional Kafka")
		}
	case "postgres":
		if snk.Postgres == nil {
			v.add(path+".postgres", "postgres sink configuration is required")
			return
		}
		if snk.Postgres.DSN == "" {
			v.add(path+".postgres.dsn", "a dsn is required")
		}
		if snk.Postgres.Table == "" {
			v.add(path+".postgres.table", "a table is required")
		}
		if len(snk.Postgres.Mapping) == 0 {
			v.add(path+".postgres.mapping", "a non-empty field→column mapping is required")
		}
	case "stdout", "blackhole":
		// no configuration
	}
}

func (v *validator) windowConfig(path string, w *WindowConfig) {
	switch w.Type {
	case "":
		v.add(path+".type", "a window type is required (tumbling, sliding, or session)")
	case "tumbling":
		if w.Size <= 0 {
			v.add(path+".size", "window size must be greater than zero")
		}
	case "sliding":
		if w.Size <= 0 {
			v.add(path+".size", "window size must be greater than zero")
		}
		if w.Slide <= 0 {
			v.add(path+".slide", "window slide must be greater than zero")
		}
		if w.Size > 0 && w.Slide > 0 && w.Slide > w.Size {
			v.add(path+".slide", "window slide must be less than or equal to size")
		}
	case "session":
		if w.Gap <= 0 {
			v.add(path+".gap", "session gap must be greater than zero")
		}
	default:
		if !supportedWindowTypes[w.Type] {
			v.addf(path+".type", "unsupported window type %q", w.Type)
		}
	}
}

// ---- Pipeline ordering -----------------------------------------------------
//
// Note: "only stateless operators can use parallelism" is guaranteed by
// the schema — the parallelism field exists only on the stateless
// operator configs (map/filter/flatMap/process). A keyed or windowed
// operator structurally cannot carry it, so there is no runtime check.

func (v *validator) pipeline(wf *Workflow) {
	sawKeyBy := false      // a keyBy has appeared at all
	sawReduceInGroup := false // a reduce has appeared since the last keyBy

	for i, op := range wf.Pipeline {
		path := opPath(i, op)
		switch op.Type {
		case "keyBy":
			sawKeyBy = true
			sawReduceInGroup = false
		case "reduce":
			if !sawKeyBy {
				v.add(path, "reduce requires a keyBy before it")
			}
			sawReduceInGroup = true
		case "window":
			if !sawKeyBy {
				v.add(path, "window requires a keyBy before it")
			}
			if sawReduceInGroup {
				v.add(path, "window must appear before the aggregation that consumes it")
			}
		}
	}
}

// ---- Delivery guarantee ----------------------------------------------------

func (v *validator) deliveryGuarantee(wf *Workflow) {
	srcEO := wf.Source.Type == "kafka" && wf.Source.Kafka != nil && wf.Source.Kafka.ExactlyOnce
	sinkTxn := wf.Sink.Type == "txnKafka"
	ckptOn := wf.Env != nil && wf.Env.Checkpointing != nil && wf.Env.Checkpointing.Interval > 0

	// Exactly-once is requested if either end asks for it. A partial
	// configuration is a silent correctness bug, so require the full set.
	if !srcEO && !sinkTxn {
		return
	}

	if !srcEO {
		v.add("source", "exactly-once requires a Kafka source with exactlyOnce: true")
	}
	if !sinkTxn {
		v.add("sink", "exactly-once requires a txnKafka (transactional) sink")
	}
	if !ckptOn {
		v.add("env.checkpointing", "exactly-once requires checkpointing to be enabled with a positive interval")
	}
	// The stable transactional id is validated in sinkConfig (always
	// required for a txnKafka sink); no need to repeat it here.
}

// ensureDir verifies dir exists or can be created. It never opens a
// database — creating the directory is the runtime's prerequisite, not
// an external connection.
func (v *validator) ensureDir(path, dir string) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		v.addf(path, "directory %q cannot be created: %v", dir, err)
	}
}
