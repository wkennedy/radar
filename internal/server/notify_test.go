package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNotifier_ShouldNotify(t *testing.T) {
	cases := map[string]bool{
		"done": true, "error": true,
		"noise": false, "cancelled": false, "streaming": false,
	}
	for status, want := range cases {
		if got := shouldNotify(&DiagnosisRecord{Status: status}); got != want {
			t.Errorf("shouldNotify(%q) = %v, want %v", status, got, want)
		}
	}
	if shouldNotify(nil) {
		t.Error("shouldNotify(nil) should be false")
	}
}

func TestNotifier_DeepLinkAndPayload(t *testing.T) {
	n := newNotifier("http://hook", "https://radar.example.com/", 9280)
	rec := &DiagnosisRecord{
		ID: "1", Kind: "Deployment", Namespace: "payments", Name: "api",
		Status: "done", RootCause: "OOMKilled", ValidityScore: 0.9, Trigger: "auto",
	}

	if link := n.deepLink(rec); link != "https://radar.example.com/workload/Deployment/payments/api" {
		t.Errorf("deepLink = %q", link)
	}

	p := n.buildPayload(rec)
	text, _ := p["text"].(string)
	for _, want := range []string{"OOMKilled", "auto", "confidence 90%", "radar.example.com/workload/Deployment/payments/api"} {
		if !strings.Contains(text, want) {
			t.Errorf("notification text missing %q:\n%s", want, text)
		}
	}
	d, _ := p["diagnosis"].(map[string]any)
	if d["status"] != "done" || d["kind"] != "Deployment" {
		t.Errorf("structured diagnosis payload wrong: %+v", d)
	}
}

func TestNotifier_DefaultBaseURLFromPort(t *testing.T) {
	n := newNotifier("http://hook", "", 9281)
	rec := &DiagnosisRecord{Kind: "Pod", Namespace: "ns", Name: "p", Status: "done"}
	if got := n.deepLink(rec); got != "http://localhost:9281/workload/Pod/ns/p" {
		t.Errorf("default base url deepLink = %q", got)
	}
}

func TestNotifier_SendPostsToWebhook(t *testing.T) {
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	n := newNotifier(srv.URL, "http://localhost:9280", 9280)
	n.send(&DiagnosisRecord{ID: "1", Kind: "Deployment", Namespace: "ns", Name: "web", Status: "done", RootCause: "boom"})

	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("webhook body not JSON: %v", err)
	}
	if got["text"] == nil || got["diagnosis"] == nil {
		t.Errorf("webhook payload missing fields: %+v", got)
	}
}

func TestNotifier_DisabledIsNoop(t *testing.T) {
	n := newNotifier("", "", 9280)
	if n.enabled() {
		t.Error("empty webhook should be disabled")
	}
	n.notify(&DiagnosisRecord{Status: "done"}) // must be a safe no-op
}
