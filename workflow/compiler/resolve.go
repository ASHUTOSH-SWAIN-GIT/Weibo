package compiler

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow/secrets"
)

var secretRefRe = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// resolveSecrets returns a copy of the workflow with sensitive secret
// references resolved by the resolver. The original spec is not mutated.
// Only ${NAME} placeholders are expanded, so literal "$" in passwords is
// left untouched.
func resolveSecrets(wf *workflow.Workflow, r secrets.SecretResolver) (*workflow.Workflow, error) {
	out := *wf

	if out.Source.Kafka != nil {
		k := *out.Source.Kafka
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
			if k.SASL != nil {
				sasl, err := resolveSASL(r, k.SASL)
				if err != nil {
					return nil, err
				}
				k.SASL = sasl
			}
			out.Sink.TxnKafka = &k
		}
	case "postgres":
		if out.Sink.Postgres != nil {
			p := *out.Sink.Postgres
			dsn, err := resolveOne(r, "dsn", p.DSN)
			if err != nil {
				return nil, err
			}
			p.DSN = dsn
			out.Sink.Postgres = &p
		}
	}

	return &out, nil
}

func resolveOne(r secrets.SecretResolver, field, value string) (string, error) {
	if !strings.Contains(value, "${") {
		return value, nil
	}
	var firstErr error
	out := secretRefRe.ReplaceAllStringFunc(value, func(match string) string {
		if firstErr != nil {
			return ""
		}
		name := secretRefRe.FindStringSubmatch(match)[1]
		resolved, err := r.Resolve(name)
		if err != nil {
			firstErr = fmt.Errorf("%s: secret %q could not be resolved", field, name)
			return ""
		}
		return resolved
	})
	if firstErr != nil {
		return "", firstErr
	}
	if strings.Contains(out, "${") {
		return "", fmt.Errorf("%s: invalid secret reference syntax", field)
	}
	return out, nil
}

func resolveSASL(r secrets.SecretResolver, s *workflow.SASLSpec) (*workflow.SASLSpec, error) {
	c := *s
	u, err := resolveOne(r, "username", c.Username)
	if err != nil {
		return nil, err
	}
	p, err := resolveOne(r, "password", c.Password)
	if err != nil {
		return nil, err
	}
	c.Username, c.Password = u, p
	return &c, nil
}
