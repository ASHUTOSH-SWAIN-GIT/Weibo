package sink

import (
	"testing"

	wlog "github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/log"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/trace"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

func TestSinkObservabilityOptions(t *testing.T) {
	logger := wlog.Discard()
	tr := trace.Noop()

	pg, err := NewPostgresSinkE(
		PostgresDSN("postgres://example/db"),
		PostgresMapper(func(types.Record) (string, []string, []any) { return "", nil, nil }),
		PostgresLogger(logger),
		PostgresTracer(tr),
	)
	if err != nil {
		t.Fatal(err)
	}
	if pg.cfg.logger != logger || pg.cfg.tracer == nil {
		t.Error("postgres observability options not applied")
	}
	if pg.log() == nil || pg.tracing() == nil {
		t.Error("postgres log/tracing helpers must never be nil")
	}

	k, err := NewKafkaSinkE(
		KafkaSinkBrokers("localhost:9092"),
		KafkaSinkTopic("t"),
		KafkaLogger(logger),
		KafkaTracer(tr),
	)
	if err != nil {
		t.Fatal(err)
	}
	if k.cfg.logger != logger || k.cfg.tracer == nil {
		t.Error("kafka observability options not applied")
	}
	if k.log() == nil || k.tracing() == nil {
		t.Error("kafka log/tracing helpers must never be nil")
	}
}
