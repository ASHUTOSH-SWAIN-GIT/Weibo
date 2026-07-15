package workflow

import (
	"encoding/json"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that (de)serializes as a Go duration
// string ("30s", "5m", "500ms") in YAML and JSON. The zero value is
// omitted by omitempty (it is an integer kind underneath), so optional
// duration fields default cleanly.
type Duration time.Duration

// Std returns the underlying time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

func (d Duration) String() string {
	if d == 0 {
		return "0s"
	}
	return time.Duration(d).String()
}

func parseDuration(s string) (Duration, error) {
	if s == "" {
		return 0, nil
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q (want e.g. \"30s\", \"5m\", \"500ms\"): %w", s, err)
	}
	return Duration(v), nil
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return fmt.Errorf("duration must be a string: %w", err)
	}
	p, err := parseDuration(s)
	if err != nil {
		return err
	}
	*d = p
	return nil
}

// MarshalYAML renders the duration as its string form.
func (d Duration) MarshalYAML() (any, error) { return d.String(), nil }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a JSON string: %w", err)
	}
	p, err := parseDuration(s)
	if err != nil {
		return err
	}
	*d = p
	return nil
}

// MarshalJSON renders the duration as its string form.
func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }
