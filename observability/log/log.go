// Package log is Weibo's structured-logging foundation: slog loggers
// plus central secret redaction. It depends only on the standard library,
// so the engine can use it without dragging observability clients into
// every `go get`.
//
// Redaction rule (enforced by tests, relied on by every component): secret
// VALUES never enter logs. Use Secret for values, EnvKeys for maps.
package log

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strings"
)

// New builds a logger writing to stderr: "json" for containers and log
// aggregators, "text" for local development. Unknown formats fall back to
// text so a typo never silences the process.
func New(level slog.Level, format string) *slog.Logger {
	var h slog.Handler
	opts := &slog.HandlerOptions{Level: level}
	if strings.EqualFold(format, "json") {
		h = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		h = slog.NewTextHandler(os.Stderr, opts)
	}
	return slog.New(h)
}

// Discard returns a logger that drops everything. Used as the default
// inside libraries and tests so constructing a pipeline never spews.
func Discard() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// ParseLevel parses "debug|info|warn|error" (case-insensitive).
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("log: unknown level %q (want debug|info|warn|error)", s)
	}
}

// Secret is a credential value. It renders as [REDACTED] in both text and
// JSON output — pass secrets through it whenever they border a log call.
type Secret string

// LogValue implements slog.LogValuer, the central redaction point.
func (s Secret) LogValue() slog.Value { return slog.StringValue("[REDACTED]") }

// String keeps accidental %v/%s formatting from leaking the value either.
func (s Secret) String() string { return "[REDACTED]" }

// EnvKeys returns the sorted key names of an environment map for logging.
// Names are safe (they are also persisted as secret references); values
// are never included.
func EnvKeys(env map[string]string) []string {
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
