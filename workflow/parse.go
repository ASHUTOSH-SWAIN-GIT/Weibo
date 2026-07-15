package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// ParseYAML decodes a workflow document from YAML. Decoding is strict:
// unknown fields are rejected, so typos in keys are caught immediately.
// This is structural parsing only — it does not validate that the
// document describes a runnable pipeline (that is a later phase).
func ParseYAML(data []byte) (*Workflow, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var wf Workflow
	if err := dec.Decode(&wf); err != nil {
		return nil, fmt.Errorf("workflow: parse yaml: %w", err)
	}
	return &wf, nil
}

// ParseJSON decodes a workflow document from JSON. Decoding is strict:
// unknown fields are rejected. Structural parsing only.
func ParseJSON(data []byte) (*Workflow, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var wf Workflow
	if err := dec.Decode(&wf); err != nil {
		return nil, fmt.Errorf("workflow: parse json: %w", err)
	}
	return &wf, nil
}

// Load reads and parses a workflow document from a file, choosing the
// decoder by extension: .yaml/.yml → YAML, .json → JSON.
func Load(path string) (*Workflow, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("workflow: read %s: %w", path, err)
	}
	switch ext := strings.ToLower(filepath.Ext(path)); ext {
	case ".yaml", ".yml":
		return ParseYAML(data)
	case ".json":
		return ParseJSON(data)
	default:
		return nil, fmt.Errorf("workflow: unsupported extension %q for %s (want .yaml, .yml, or .json)", ext, path)
	}
}
