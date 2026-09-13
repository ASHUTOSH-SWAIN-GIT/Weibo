package log_test

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	wlog "github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/log"
)

func TestSecretRedacted(t *testing.T) {
	for _, format := range []string{"text", "json", "bogus-falls-back-to-text"} {
		var buf bytes.Buffer
		l := slog.New(slog.NewTextHandler(&buf, nil))
		if format == "json" {
			l = slog.New(slog.NewJSONHandler(&buf, nil))
		}
		l.Info("launch", "dsn", wlog.Secret("postgres://user:s3cr3t@db/x"))
		if strings.Contains(buf.String(), "s3cr3t") {
			t.Errorf("%s: secret value leaked: %q", format, buf.String())
		}
		if !strings.Contains(buf.String(), "[REDACTED]") {
			t.Errorf("%s: redaction marker missing: %q", format, buf.String())
		}
	}
	if got := wlog.Secret("x").String(); got != "[REDACTED]" {
		t.Errorf("String: got %q", got)
	}
}

func TestEnvKeysExcludesValues(t *testing.T) {
	keys := wlog.EnvKeys(map[string]string{"API_KEY": "s3cr3t", "DB": "postgres://x"})
	if len(keys) != 2 || keys[0] != "API_KEY" || keys[1] != "DB" {
		t.Fatalf("keys=%v", keys)
	}
}

func TestParseLevel(t *testing.T) {
	for in, want := range map[string]slog.Level{"": slog.LevelInfo, "debug": slog.LevelDebug, "WARN": slog.LevelWarn, "error": slog.LevelError} {
		got, err := wlog.ParseLevel(in)
		if err != nil || got != want {
			t.Errorf("ParseLevel(%q)=%v,%v", in, got, err)
		}
	}
	if _, err := wlog.ParseLevel("verbose"); err == nil {
		t.Error("expected error for unknown level")
	}
	if wlog.Discard() == nil {
		t.Error("Discard must return a logger")
	}
}
