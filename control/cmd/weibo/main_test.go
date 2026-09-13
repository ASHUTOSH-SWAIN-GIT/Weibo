package main

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestParseKeyValues(t *testing.T) {
	got, err := parseKeyValues("workload=weibo,disk=ssd")
	if err != nil {
		t.Fatal(err)
	}
	if got["workload"] != "weibo" || got["disk"] != "ssd" {
		t.Fatalf("parseKeyValues=%v", got)
	}
	if _, err := parseKeyValues("workload"); err == nil {
		t.Fatal("expected malformed selector error")
	}
}

func TestParseTolerations(t *testing.T) {
	got, err := parseTolerations("dedicated=weibo:NoSchedule,gpu:PreferNoSchedule")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("len=%d, want 2", len(got))
	}
	if got[0].Key != "dedicated" || got[0].Operator != corev1.TolerationOpEqual ||
		got[0].Value != "weibo" || got[0].Effect != corev1.TaintEffectNoSchedule {
		t.Fatalf("first toleration=%+v", got[0])
	}
	if got[1].Key != "gpu" || got[1].Operator != corev1.TolerationOpExists ||
		got[1].Effect != corev1.TaintEffectPreferNoSchedule {
		t.Fatalf("second toleration=%+v", got[1])
	}
	if _, err := parseTolerations("dedicated=weibo:BadEffect"); err == nil {
		t.Fatal("expected bad effect error")
	}
}

func TestAuthAndBindGuards(t *testing.T) {
	if !isPublicBind(":9000") || !isPublicBind("0.0.0.0:9000") || !isPublicBind("[::]:9000") {
		t.Fatal("expected wildcard listen addresses to be public")
	}
	if isPublicBind("127.0.0.1:9000") || isPublicBind("localhost:9000") || isPublicBind("[::1]:9000") {
		t.Fatal("expected loopback listen addresses to be non-public")
	}
	if authConfigured("", "") {
		t.Fatal("empty auth should not be configured")
	}
	if !authConfigured("plain", "") || !authConfigured("", "abcd") {
		t.Fatal("token or hash should configure auth")
	}
}
