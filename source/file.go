package source

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

const defaultFileScannerBuffer = 1024 * 1024

type fileSourceConfig struct {
	path       string
	sourceName string
	maxLine    int
	deser      Deserializer
}

// FileSourceOption configures FileSource.
type FileSourceOption func(*fileSourceConfig)

// FilePath sets the file to read. Required.
func FilePath(path string) FileSourceOption {
	return func(c *fileSourceConfig) { c.path = path }
}

// FileSourceName sets Record.Source and checkpoint source identity.
// Empty defaults to the path.
func FileSourceName(name string) FileSourceOption {
	return func(c *fileSourceConfig) { c.sourceName = name }
}

// FileMaxLineBytes raises the scanner limit for large records.
func FileMaxLineBytes(n int) FileSourceOption {
	return func(c *fileSourceConfig) { c.maxLine = n }
}

// FileDeserialize parses each line into Record.Parsed.
func FileDeserialize(d Deserializer) FileSourceOption {
	return func(c *fileSourceConfig) { c.deser = d }
}

// FileSource reads one record per line from a local file.
//
// It is finite, ordered, and checkpoint-aware: offsets are zero-based line
// numbers, and CheckpointOffset records the next line to emit. Delivery after
// restore is at-least-once unless downstream sinks participate in coordinated
// checkpoints.
type FileSource struct {
	cfg      fileSourceConfig
	nextLine atomic.Int64
}

// NewFileSource creates a file source. FilePath is required; if missing it
// panics. Prefer NewFileSourceE when compiling user-provided configs.
func NewFileSource(opts ...FileSourceOption) *FileSource {
	src, err := NewFileSourceE(opts...)
	if err != nil {
		panic(fmt.Sprintf("weibo/source: %v", err))
	}
	return src
}

// NewFileSourceE creates a file source without panicking.
func NewFileSourceE(opts ...FileSourceOption) (*FileSource, error) {
	cfg := fileSourceConfig{maxLine: defaultFileScannerBuffer}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.path == "" {
		return nil, fmt.Errorf("FileSource requires FilePath(...)")
	}
	if cfg.sourceName == "" {
		cfg.sourceName = cfg.path
	}
	if cfg.maxLine <= 0 {
		cfg.maxLine = defaultFileScannerBuffer
	}
	return &FileSource{cfg: cfg}, nil
}

// Run emits one record per line, starting at the restored checkpoint offset.
func (s *FileSource) Run(ctx context.Context, out chan<- types.Record) error {
	f, err := os.Open(s.cfg.path)
	if err != nil {
		return fmt.Errorf("file source: open %s: %w", s.cfg.path, err)
	}
	defer f.Close()

	start := s.nextLine.Load()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), s.cfg.maxLine)

	var line int64
	for sc.Scan() {
		if line < start {
			line++
			continue
		}
		value := append([]byte(nil), sc.Bytes()...)
		record := types.Record{
			Key:       []byte(strconv.FormatInt(line, 10)),
			Value:     value,
			Source:    s.cfg.sourceName,
			Partition: 0,
			Offset:    line,
		}
		if s.cfg.deser != nil {
			parsed, err := s.cfg.deser.Deserialize(value, nil)
			if err != nil {
				return fmt.Errorf("file source: deserialize %s line %d: %w", s.cfg.path, line+1, err)
			}
			record.Parsed = parsed
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- record:
			s.nextLine.Store(line + 1)
		}
		line++
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("file source: read %s: %w", s.cfg.path, err)
	}
	return nil
}

// CheckpointOffset returns the next line number to emit.
func (s *FileSource) CheckpointOffset() ([]byte, error) {
	return EncodePositions([]Position{{
		Source:    s.cfg.sourceName,
		Partition: 0,
		Offset:    s.nextLine.Load(),
	}})
}

// RestoreOffset seeks the source to the checkpointed line number.
func (s *FileSource) RestoreOffset(data []byte) error {
	positions, err := DecodePositions(data, s.cfg.sourceName)
	if err != nil {
		return err
	}
	if len(positions) != 1 {
		return fmt.Errorf("file source: expected one checkpoint position, got %d", len(positions))
	}
	p := positions[0]
	if p.Source != s.cfg.sourceName || p.Partition != 0 {
		return fmt.Errorf("file source: checkpoint belongs to %s/%d, want %s/0", p.Source, p.Partition, s.cfg.sourceName)
	}
	s.nextLine.Store(p.Offset)
	return nil
}

// CheckpointPosition maps emitted records back to the file line offset.
func (s *FileSource) CheckpointPosition(record types.Record) PositionKey {
	return PositionKey{Source: record.Source, Partition: record.Partition}
}

// SourceCapabilities declares the optional contracts FileSource supports.
func (s *FileSource) SourceCapabilities() Capabilities {
	return Capabilities{
		CheckpointOffsets:     true,
		PositionedCheckpoints: true,
		Describe:              true,
	}
}

// Describe returns dashboard metadata.
func (s *FileSource) Describe() SourceInfo {
	return SourceInfo{
		Type: "File",
		Props: map[string]string{
			"path":   s.cfg.path,
			"source": s.cfg.sourceName,
		},
	}
}

// JSONLineDeserializer decodes each line as JSON into any.
var JSONLineDeserializer = DeserializerFunc(func(data []byte, _ map[string][]byte) (any, error) {
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}
	return v, nil
})
