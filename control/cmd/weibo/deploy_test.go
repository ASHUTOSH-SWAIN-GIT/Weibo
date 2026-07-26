package main

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestFullImageRef(t *testing.T) {
	cases := []struct {
		registry, image, want string
	}{
		{"", "orders:1.0", "orders:1.0"},                               // no registry -> unchanged
		{"docker.io/ashu", "orders:1.0", "docker.io/ashu/orders:1.0"},  // bare -> prefixed
		{"docker.io/ashu/", "orders:1.0", "docker.io/ashu/orders:1.0"}, // trailing slash trimmed
		{"docker.io/ashu", "myrepo/orders:1.0", "myrepo/orders:1.0"},   // already qualified -> verbatim
		{"123.dkr.ecr.ap-south-1.amazonaws.com", "job:v2", "123.dkr.ecr.ap-south-1.amazonaws.com/job:v2"},
	}
	for _, c := range cases {
		if got := fullImageRef(c.registry, c.image); got != c.want {
			t.Errorf("fullImageRef(%q,%q)=%q, want %q", c.registry, c.image, got, c.want)
		}
	}
}

func TestRewriteManifestImage(t *testing.T) {
	doc := []byte("kind: sdk\nname: orders\nimage: orders:1.0\nenv:\n  LOG_LEVEL: info\n")
	out, err := rewriteManifestImage(doc, "docker.io/ashu/orders:1.0")
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	// The controller re-parses this exact struct, so assert those fields.
	var m struct {
		Kind, Name, Image string
		Env               map[string]string
	}
	if err := yaml.Unmarshal(out, &m); err != nil {
		t.Fatalf("reparse: %v", err)
	}
	if m.Image != "docker.io/ashu/orders:1.0" {
		t.Errorf("image = %q, want the rewritten ref", m.Image)
	}
	if m.Kind != "sdk" || m.Name != "orders" || m.Env["LOG_LEVEL"] != "info" {
		t.Errorf("other fields not preserved: %+v", m)
	}
}

func TestRewriteManifestImage_NoImage(t *testing.T) {
	if _, err := rewriteManifestImage([]byte("kind: sdk\nname: x\n"), "r/x:1"); err == nil {
		t.Error("expected error when manifest has no image field")
	}
	if _, err := rewriteManifestImage([]byte("just a scalar"), "r/x:1"); err == nil {
		t.Error("expected error when manifest is not a mapping")
	}
}

// guards against an accidental double-prefix regression in the deploy flow.
func TestFullImageRef_NoDoublePrefix(t *testing.T) {
	reg := "docker.io/ashu"
	once := fullImageRef(reg, "orders:1.0")
	twice := fullImageRef(reg, once) // once already has "/", so stays put
	if twice != once || strings.Count(twice, reg) != 1 {
		t.Errorf("double prefix: %q", twice)
	}
}
