package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/skyhook-io/radar/internal/opensre"
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
	n := newNotifier("http://hook", "https://radar.example.com/", 9280, nil, false)
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
	n := newNotifier("http://hook", "", 9281, nil, false)
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

	n := newNotifier(srv.URL, "http://localhost:9280", 9280, nil, false)
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
	n := newNotifier("", "", 9280, nil, false)
	if n.enabled() {
		t.Error("empty webhook should be disabled")
	}
	n.notify(&DiagnosisRecord{Status: "done"}) // must be a safe no-op
}

func TestNotifier_OpenSREPublishGating(t *testing.T) {
	// opensre-notify on but client unconfigured → not enabled.
	n := newNotifier("", "", 9280, opensre.NewClient("", ""), true)
	if n.enabled() || n.opensrePublishEnabled() {
		t.Error("opensre-notify with unconfigured client should be disabled")
	}
	// configured client + flag on → enabled even without a webhook.
	n = newNotifier("", "", 9280, opensre.NewClient("http://opensre:8080", "tok"), true)
	if !n.enabled() || !n.opensrePublishEnabled() {
		t.Error("opensre-notify with configured client should be enabled")
	}
	// flag off → disabled regardless of client.
	n = newNotifier("", "", 9280, opensre.NewClient("http://opensre:8080", "tok"), false)
	if n.opensrePublishEnabled() {
		t.Error("opensre-notify off should not publish")
	}
}

func TestNotifier_PublishViaOpenSRE(t *testing.T) {
	var got opensre.PublishRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/publish" {
			t.Errorf("expected /publish, got %s", r.URL.Path)
		}
		if r.Header.Get("X-API-Key") != "tok" {
			t.Errorf("missing/wrong X-API-Key: %q", r.Header.Get("X-API-Key"))
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		_ = json.NewEncoder(w).Encode(opensre.PublishResult{Published: true, Channel: "telegram"})
	}))
	defer srv.Close()

	n := newNotifier("", "https://radar.example.com", 9280, opensre.NewClient(srv.URL, "tok"), true)
	n.publishViaOpenSRE(&DiagnosisRecord{
		ID: "1", Kind: "Deployment", Namespace: "payments", Name: "api",
		Status: "done", RootCause: "OOMKilled", Report: "## RCA", ValidityScore: 0.9, Trigger: "auto",
	})

	if got.Channel != "telegram" || got.RootCause != "OOMKilled" || got.Trigger != "auto" {
		t.Errorf("unexpected publish request: %+v", got)
	}
	if got.ResourceURL != "https://radar.example.com/workload/Deployment/payments/api" {
		t.Errorf("deep link not threaded: %q", got.ResourceURL)
	}
	if got.ValidityScore == nil || *got.ValidityScore != 0.9 {
		t.Errorf("validity score not threaded: %v", got.ValidityScore)
	}
}

func TestNotifier_PublishSkipsNoiseAndError(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	n := newNotifier("", "", 9280, opensre.NewClient(srv.URL, "tok"), true)
	n.publishViaOpenSRE(&DiagnosisRecord{Status: "done", IsNoise: true, Name: "x"})
	n.publishViaOpenSRE(&DiagnosisRecord{Status: "error", Name: "y"})
	if called {
		t.Error("noise/error diagnoses must not be published")
	}
}
