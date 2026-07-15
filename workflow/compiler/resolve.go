package compiler

import (
	"strings"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow"
)

// resolveConnections returns a copy of the workflow with connection and
// secret strings (broker lists, DSNs, SASL credentials) resolved by the
// resolver. The original spec is not mutated. Only values containing a
// ${...} reference are passed to the resolver, so literal "$" in
// passwords is left untouched.
func resolveConnections(wf *workflow.Workflow, r ConnectionResolver) (*workflow.Workflow, error) {
	out := *wf

	if out.Source.Kafka != nil {
		k := *out.Source.Kafka
		brokers, err := resolveSlice(r, k.Brokers)
		if err != nil {
			return nil, err
		}
		k.Brokers = brokers
		if k.SASL != nil {
			sasl, err := resolveSASL(r, k.SASL)
			if err != nil {
				return nil, err
			}
			k.SASL = sasl
		}
		out.Source.Kafka = &k
	}

	switch out.Sink.Type {
	case "kafka":
		if out.Sink.Kafka != nil {
			k := *out.Sink.Kafka
			brokers, err := resolveSlice(r, k.Brokers)
			if err != nil {
				return nil, err
			}
			k.Brokers = brokers
			if k.SASL != nil {
				sasl, err := resolveSASL(r, k.SASL)
				if err != nil {
					return nil, err
				}
				k.SASL = sasl
			}
			out.Sink.Kafka = &k
		}
	case "txnKafka", "transactional_kafka":
		if out.Sink.TxnKafka != nil {
			k := *out.Sink.TxnKafka
			brokers, err := resolveSlice(r, k.Brokers)
			if err != nil {
				return nil, err
			}
			k.Brokers = brokers
			out.Sink.TxnKafka = &k
		}
	case "postgres":
		if out.Sink.Postgres != nil {
			p := *out.Sink.Postgres
			dsn, err := resolveOne(r, p.DSN)
			if err != nil {
				return nil, err
			}
			p.DSN = dsn
			out.Sink.Postgres = &p
		}
	}

	return &out, nil
}

func resolveOne(r ConnectionResolver, value string) (string, error) {
	if !strings.Contains(value, "${") {
		return value, nil
	}
	return r.Resolve(value)
}

func resolveSlice(r ConnectionResolver, values []string) ([]string, error) {
	if len(values) == 0 {
		return values, nil
	}
	out := make([]string, len(values))
	for i, v := range values {
		rv, err := resolveOne(r, v)
		if err != nil {
			return nil, err
		}
		out[i] = rv
	}
	return out, nil
}

func resolveSASL(r ConnectionResolver, s *workflow.SASLSpec) (*workflow.SASLSpec, error) {
	c := *s
	u, err := resolveOne(r, c.Username)
	if err != nil {
		return nil, err
	}
	p, err := resolveOne(r, c.Password)
	if err != nil {
		return nil, err
	}
	c.Username, c.Password = u, p
	return &c, nil
}
