package trace

import (
	"math"
	"os"
	"strconv"
	"strings"
)

// withEnvDefaults fills unset options from the standard OpenTelemetry
// environment: OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_EXPORTER_OTLP_INSECURE,
// OTEL_EXPORTER_OTLP_HEADERS, OTEL_SERVICE_NAME.
func withEnvDefaults(opts Options) Options {
	if opts.Endpoint == "" {
		opts.Endpoint = strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	}
	if !opts.Insecure {
		switch strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_INSECURE"))) {
		case "1", "true":
			opts.Insecure = true
		}
	}
	if len(opts.Headers) == 0 {
		opts.Headers = parseHeaders(os.Getenv("OTEL_EXPORTER_OTLP_HEADERS"))
	}
	if opts.ServiceName == "" {
		opts.ServiceName = strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME"))
	}
	return opts
}

// parseHeaders decodes "k1=v1,k2=v2" header maps.
func parseHeaders(s string) map[string]string {
	var out map[string]string
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		k, v, ok := strings.Cut(part, "=")
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if !ok || k == "" {
			continue
		}
		if out == nil {
			out = map[string]string{}
		}
		out[k] = v
	}
	return out
}

// ParseSampleRatio parses OTEL_TRACES_SAMPLER_ARG-style ratios for tests
// and callers that accept a raw string.
func ParseSampleRatio(s string) float64 {
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil || math.IsNaN(f) || f <= 0 || f > 1 {
		return 1
	}
	return f
}
