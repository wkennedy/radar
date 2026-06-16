package opensre

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestIsConfigured(t *testing.T) {
	if NewClient("", "").IsConfigured() {
		t.Error("empty client should not be configured")
	}
	if NewClient("http://x", "").IsConfigured() {
		t.Error("missing token should not be configured")
	}
	if !NewClient("http://x", "tok").IsConfigured() {
		t.Error("url+token should be configured")
	}
	var nilClient *Client
	if nilClient.IsConfigured() {
		t.Error("nil client should not be configured")
	}
}

func TestInvestigateStream_SendsContractAndParsesSSE(t *testing.T) {
	var gotAPIKey, gotAccept string
	var gotBody InvestigateRequest

	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/investigate/stream" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		gotAPIKey = r.Header.Get("X-API-Key")
		gotAccept = r.Header.Get("Accept")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)

		w.Header().Set("Content-Type", "text/event-stream")
		fl, _ := w.(http.Flusher)
		fmt.Fprint(w, "event: events\ndata: {\"event\":\"on_tool_start\",\"name\":\"investigation_agent\"}\n\n")
		fmt.Fprint(w, ": heartbeat\n\n")
		fmt.Fprint(w, "event: end\ndata: {}\n\n")
		fl.Flush()
	}))
	defer mock.Close()

	client := NewClient(mock.URL, "secret-key")
	envelope := map[string]any{"source": "radar", "subject": map[string]any{"name": "api"}}
	ch, err := client.InvestigateStream(context.Background(), InvestigateRequest{
		RawAlert:     envelope,
		AlertName:    "CrashLoopBackOff: payments/api",
		PipelineName: "radar",
		Severity:     "high",
	})
	if err != nil {
		t.Fatalf("InvestigateStream: %v", err)
	}

	var events []StreamEvent
	for ev := range ch {
		events = append(events, ev)
	}

	// Request carried the verified contract.
	if gotAPIKey != "secret-key" {
		t.Errorf("X-API-Key = %q, want secret-key", gotAPIKey)
	}
	if gotAccept != "text/event-stream" {
		t.Errorf("Accept = %q, want text/event-stream", gotAccept)
	}
	if gotBody.PipelineName != "radar" || gotBody.Severity != "high" {
		t.Errorf("body metadata = %+v", gotBody)
	}
	if gotBody.RawAlert == nil {
		t.Error("raw_alert must be present in the request body")
	}

	// Heartbeat comment is dropped; the two real frames survive.
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2: %+v", len(events), events)
	}
	if events[0].Event != "events" {
		t.Errorf("event[0].Event = %q, want events", events[0].Event)
	}
	if events[1].Event != "end" {
		t.Errorf("event[1].Event = %q, want end", events[1].Event)
	}
}

func TestInvestigateStream_NonOKStatusErrors(t *testing.T) {
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "Forbidden", http.StatusForbidden)
	}))
	defer mock.Close()

	_, err := NewClient(mock.URL, "k").InvestigateStream(context.Background(), InvestigateRequest{})
	if err == nil {
		t.Fatal("expected an error on non-200 response")
	}
}

func TestInvestigateStream_NotConfiguredErrors(t *testing.T) {
	_, err := NewClient("", "").InvestigateStream(context.Background(), InvestigateRequest{})
	if err == nil {
		t.Fatal("expected an error when not configured")
	}
}
