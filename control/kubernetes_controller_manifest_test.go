package control

import (
	"os"
	"testing"

	"gopkg.in/yaml.v3"
)

type manifestDoc struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec map[string]any `yaml:"spec"`
}

func TestKubernetesControllerManifestIncludesProductionBasics(t *testing.T) {
	data, err := os.ReadFile("kubernetes-controller.yaml")
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var docs []manifestDoc
	for _, raw := range splitYAMLDocs(data) {
		var doc manifestDoc
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("parse manifest document: %v", err)
		}
		if doc.Kind != "" {
			docs = append(docs, doc)
		}
	}

	kinds := map[string]bool{}
	for _, doc := range docs {
		kinds[doc.Kind+"/"+doc.Metadata.Name] = true
	}
	for _, want := range []string{
		"ServiceAccount/weibo-controller",
		"Role/weibo-controller",
		"RoleBinding/weibo-controller",
		"ConfigMap/weibo-controller-config",
		"PersistentVolumeClaim/weibo-controller-data",
		"Deployment/weibo-controller",
		"Service/weibo-controller",
		"PodDisruptionBudget/weibo-controller",
	} {
		if !kinds[want] {
			t.Fatalf("manifest missing %s; got %v", want, kinds)
		}
	}

	deploy := findManifestDoc(t, docs, "Deployment", "weibo-controller")
	spec := asMap(t, deploy.Spec, "deployment spec")
	if got := spec["replicas"]; got != 1 {
		t.Fatalf("deployment replicas=%v, want 1 for SQLite PVC safety", got)
	}
	strategy := asMap(t, spec["strategy"], "deployment strategy")
	if got := strategy["type"]; got != "Recreate" {
		t.Fatalf("deployment strategy=%v, want Recreate for ReadWriteOnce SQLite PVC", got)
	}

	pod := asMap(t, asMap(t, spec["template"], "pod template")["spec"], "pod spec")
	if got := pod["serviceAccountName"]; got != "weibo-controller" {
		t.Fatalf("serviceAccountName=%v, want weibo-controller", got)
	}
	containers := asSlice(t, pod["containers"], "containers")
	if len(containers) != 1 {
		t.Fatalf("containers=%d, want 1", len(containers))
	}
	container := asMap(t, containers[0], "controller container")
	assertProbe(t, container, "livenessProbe", "/livez")
	assertProbe(t, container, "readinessProbe", "/readyz")
	assertArgContains(t, container, "-backend=kubernetes")
	assertArgContains(t, container, "-db=/var/lib/weibo/control.db")

	mounts := asSlice(t, container["volumeMounts"], "volumeMounts")
	if !hasNamedMount(mounts, "data", "/var/lib/weibo") {
		t.Fatalf("controller data PVC is not mounted at /var/lib/weibo: %v", mounts)
	}

	pdb := findManifestDoc(t, docs, "PodDisruptionBudget", "weibo-controller")
	pdbSpec := asMap(t, pdb.Spec, "pdb spec")
	if got := pdbSpec["minAvailable"]; got != 1 {
		t.Fatalf("pdb minAvailable=%v, want 1", got)
	}
}

func splitYAMLDocs(data []byte) [][]byte {
	var docs [][]byte
	start := 0
	for i := 0; i < len(data); i++ {
		if i == 0 || data[i-1] == '\n' {
			if i+3 <= len(data) && string(data[i:i+3]) == "---" {
				if i > start {
					docs = append(docs, data[start:i])
				}
				start = i + 3
			}
		}
	}
	if start < len(data) {
		docs = append(docs, data[start:])
	}
	return docs
}

func findManifestDoc(t *testing.T, docs []manifestDoc, kind, name string) manifestDoc {
	t.Helper()
	for _, doc := range docs {
		if doc.Kind == kind && doc.Metadata.Name == name {
			return doc
		}
	}
	t.Fatalf("missing %s/%s", kind, name)
	return manifestDoc{}
}

func asMap(t *testing.T, v any, name string) map[string]any {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("%s: got %T, want map", name, v)
	}
	return m
}

func asSlice(t *testing.T, v any, name string) []any {
	t.Helper()
	s, ok := v.([]any)
	if !ok {
		t.Fatalf("%s: got %T, want slice", name, v)
	}
	return s
}

func assertProbe(t *testing.T, container map[string]any, field, path string) {
	t.Helper()
	probe := asMap(t, container[field], field)
	httpGet := asMap(t, probe["httpGet"], field+".httpGet")
	if got := httpGet["path"]; got != path {
		t.Fatalf("%s path=%v, want %s", field, got, path)
	}
}

func assertArgContains(t *testing.T, container map[string]any, want string) {
	t.Helper()
	for _, arg := range asSlice(t, container["args"], "args") {
		if arg == want {
			return
		}
	}
	t.Fatalf("args missing %q: %v", want, container["args"])
}

func hasNamedMount(mounts []any, name, path string) bool {
	for _, raw := range mounts {
		mount, ok := raw.(map[string]any)
		if ok && mount["name"] == name && mount["mountPath"] == path {
			return true
		}
	}
	return false
}
