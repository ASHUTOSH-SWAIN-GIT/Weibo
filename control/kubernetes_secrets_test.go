//go:build kubernetes

package control_test

import (
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/store"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestKubernetesSecretProvider(t *testing.T) {
	cs := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "weibo"},
		Data:       map[string][]byte{"api-key": []byte("secret-value")},
	})
	provider := control.NewKubernetesSecretProviderClient(cs, "weibo")
	got, err := provider.ResolveSecret(store.SecretRef{Provider: "kubernetes", Name: "orders", Key: "api-key"})
	if err != nil {
		t.Fatal(err)
	}
	if got != "secret-value" {
		t.Fatalf("resolved secret = %q", got)
	}
	if _, err := provider.ResolveSecret(store.SecretRef{Provider: "kubernetes", Name: "orders", Key: "missing"}); err == nil {
		t.Fatal("expected missing key error")
	}
}
