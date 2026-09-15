package sink

import (
	"bufio"
	"context"
	"fmt"
	"os"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

type fileSinkConfig struct {
	path       string
	appendMode bool
	serializer Serializer
}

// FileSinkOption configures FileSink.
type FileSinkOption func(*fileSinkConfig)

// FileSinkPath sets the destination file. Required.
func FileSinkPath(path string) FileSinkOption {
	return func(c *fileSinkConfig) { c.path = path }
}

// FileSinkAppend appends to an existing file instead of truncating it.
func FileSinkAppend() FileSinkOption {
	return func(c *fileSinkConfig) { c.appendMode = true }
}

// FileSinkSerialize sets the record serializer. Defaults to raw Record.Value.
func FileSinkSerialize(s Serializer) FileSinkOption {
	return func(c *fileSinkConfig) { c.serializer = s }
}

// FileSink writes one serialized record per line to a local file.
//
// Delivery is at-least-once with checkpointing: records may be replayed after a
// restart unless downstream consumers deduplicate them. It is intended for
// durable local exports, test fixtures, and file-based handoff.
type FileSink struct {
	cfg fileSinkConfig
}

// NewFileSink creates a file sink. FileSinkPath is required; if missing it
// panics. Prefer NewFileSinkE for user-provided config.
func NewFileSink(opts ...FileSinkOption) *FileSink {
	s, err := NewFileSinkE(opts...)
	if err != nil {
		panic(fmt.Sprintf("weibo/sink: %v", err))
	}
	return s
}

// NewFileSinkE creates a file sink without panicking.
func NewFileSinkE(opts ...FileSinkOption) (*FileSink, error) {
	cfg := fileSinkConfig{serializer: SerializerFunc(func(r types.Record) ([]byte, error) {
		return r.Value, nil
	})}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.path == "" {
		return nil, fmt.Errorf("FileSink requires FileSinkPath(...)")
	}
	if cfg.serializer == nil {
		return nil, fmt.Errorf("FileSink requires a non-nil serializer")
	}
	return &FileSink{cfg: cfg}, nil
}

// Write drains records to the configured file, flushing before return.
func (s *FileSink) Write(ctx context.Context, in <-chan types.Record) error {
	flag := os.O_CREATE | os.O_WRONLY
	if s.cfg.appendMode {
		flag |= os.O_APPEND
	} else {
		flag |= os.O_TRUNC
	}
	f, err := os.OpenFile(s.cfg.path, flag, 0o644)
	if err != nil {
		return fmt.Errorf("file sink: open %s: %w", s.cfg.path, err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for {
		select {
		case <-ctx.Done():
			if err := w.Flush(); err != nil {
				return fmt.Errorf("file sink: flush %s: %w", s.cfg.path, err)
			}
			return ctx.Err()
		case r, ok := <-in:
			if !ok {
				if err := w.Flush(); err != nil {
					return fmt.Errorf("file sink: flush %s: %w", s.cfg.path, err)
				}
				return nil
			}
			b, err := s.cfg.serializer.Serialize(r)
			if err != nil {
				return fmt.Errorf("file sink: serialize: %w", err)
			}
			if _, err := w.Write(b); err != nil {
				return fmt.Errorf("file sink: write %s: %w", s.cfg.path, err)
			}
			if err := w.WriteByte('\n'); err != nil {
				return fmt.Errorf("file sink: write %s: %w", s.cfg.path, err)
			}
		}
	}
}

// SinkCapabilities declares FileSink optional contracts.
func (s *FileSink) SinkCapabilities() Capabilities {
	return Capabilities{Describe: true}
}

// Describe returns dashboard metadata.
func (s *FileSink) Describe() SinkInfo {
	mode := "truncate"
	if s.cfg.appendMode {
		mode = "append"
	}
	return SinkInfo{
		Type: "File",
		Props: map[string]string{
			"path": s.cfg.path,
			"mode": mode,
		},
	}
}
