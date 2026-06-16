package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
)

// mockOpenSRE returns an httptest server that emits a short OpenSRE-style SSE
// investigation stream and then ends.
func mockOpenSRE(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/investigate/stream" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fl, _ := w.(http.Flusher)
		fmt.Fprint(w, "event: events\ndata: {\"event\":\"on_chain_start\",\"name\":\"investigation_agent\"}\n\n")
		fmt.Fprint(w, "event: end\ndata: {}\n\n")
		fl.Flush()
	}))
}

// Pre-flight failures are delivered as SSE "event: error" frames (HTTP 200) so
// the browser's EventSource can read the reason, not a dataless connection error.
func TestHandleDiagnoseStream_NotConfigured(t *testing.T) {
	srv := New(Config{DevMode: true}) // no OpenSRE URL/token
	req := httptest.NewRequest(http.MethodGet, "/api/diagnose/stream?kind=Pod&namespace=default&name=x", nil)
	rec := httptest.NewRecorder()

	srv.handleDiagnoseStream(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (error delivered as SSE frame)", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "event: error") || !strings.Contains(body, "not configured") {
		t.Errorf("expected an SSE error frame about configuration:\n%s", body)
	}
}

func TestHandleDiagnoseStream_MissingParams(t *testing.T) {
	mock := mockOpenSRE(t)
	defer mock.Close()
	srv := New(Config{DevMode: true, OpenSREURL: mock.URL, OpenSREToken: "k"})

	req := httptest.NewRequest(http.MethodGet, "/api/diagnose/stream?kind=Pod&namespace=default", nil) // no name
	rec := httptest.NewRecorder()

	srv.handleDiagnoseStream(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (error delivered as SSE frame)", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "event: error") || !strings.Contains(body, "kind and name are required") {
		t.Errorf("expected an SSE error frame about missing params:\n%s", body)
	}
}

// An OpenSRE rejection (e.g. 403) must surface as a readable error frame with the
// upstream detail, not a generic dropped connection.
func TestHandleDiagnoseStream_UpstreamErrorSurfacesAsFrame(t *testing.T) {
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "Forbidden", http.StatusForbidden)
	}))
	defer mock.Close()
	srv := New(Config{DevMode: true, OpenSREURL: mock.URL, OpenSREToken: "k"})

	req := httptest.NewRequest(http.MethodGet, "/api/diagnose/stream?kind=Deployment&namespace=broken&name=stuck-app", nil)
	rec := httptest.NewRecorder()

	srv.handleDiagnoseStream(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "event: error") || !strings.Contains(body, "403") {
		t.Errorf("expected an SSE error frame carrying the upstream 403:\n%s", body)
	}
}

func TestHandleDiagnoseStream_RelaysOpenSREStream(t *testing.T) {
	mock := mockOpenSRE(t)
	defer mock.Close()
	srv := New(Config{DevMode: true, OpenSREURL: mock.URL, OpenSREToken: "k"})

	// Cluster is marked connected and the cache populated by TestMain.
	req := httptest.NewRequest(http.MethodGet, "/api/diagnose/stream?kind=Deployment&namespace=broken&name=stuck-app", nil)
	rec := httptest.NewRecorder()

	srv.handleDiagnoseStream(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "event: events") {
		t.Errorf("relayed body missing OpenSRE frame:\n%s", body)
	}
	if !strings.Contains(body, "investigation_agent") {
		t.Errorf("relayed body missing OpenSRE data payload:\n%s", body)
	}
	if !strings.Contains(body, "event: done") {
		t.Errorf("relayed body missing terminal done frame:\n%s", body)
	}
}

func TestBuildAlertEnvelope(t *testing.T) {
	envelope, alertName, severity := buildAlertEnvelope("Pod", "payments", "api-7c9f", "user-initiated")

	if envelope["source"] != "radar" {
		t.Errorf("source = %v, want radar", envelope["source"])
	}
	if envelope["contractVersion"] != contractVersion {
		t.Errorf("contractVersion = %v, want %s", envelope["contractVersion"], contractVersion)
	}
	subject, ok := envelope["subject"].(map[string]any)
	if !ok || subject["kind"] != "Pod" || subject["namespace"] != "payments" || subject["name"] != "api-7c9f" {
		t.Errorf("subject = %v", envelope["subject"])
	}
	// No Warning events in the fake cluster for this object → default severity.
	if severity != "warning" {
		t.Errorf("severity = %q, want warning (no warning events)", severity)
	}
	if !strings.Contains(alertName, "payments/api-7c9f") {
		t.Errorf("alertName = %q, want it to reference payments/api-7c9f", alertName)
	}
}

// mockOpenSREWithResult emits a final publish_findings frame so the persisted
// record captures the structured result.
func mockOpenSREWithResult(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/investigate/stream" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fl, _ := w.(http.Flusher)
		fmt.Fprint(w, "event: events\ndata: {\"event\":\"on_chain_start\",\"name\":\"investigation_agent\"}\n\n")
		fmt.Fprint(w, "event: events\ndata: {\"event\":\"on_chain_end\",\"name\":\"publish_findings\",\"data\":{\"output\":{\"report\":\"## RCA\\nOOMed\",\"root_cause\":\"OOMKilled\",\"validity_score\":0.9,\"evidence_entries\":[{\"source\":\"radar\",\"tool\":\"get_pod_logs\",\"summary\":\"saw OOM\"}]}}}\n\n")
		fmt.Fprint(w, "event: end\ndata: {}\n\n")
		fl.Flush()
	}))
}

func TestHandleDiagnoseStream_PersistsAndServesHistory(t *testing.T) {
	mock := mockOpenSREWithResult(t)
	defer mock.Close()
	srv := New(Config{DevMode: true, OpenSREURL: mock.URL, OpenSREToken: "k"})

	req := httptest.NewRequest(http.MethodGet, "/api/diagnose/stream?kind=Deployment&namespace=broken&name=stuck-app", nil)
	srv.handleDiagnoseStream(httptest.NewRecorder(), req)

	// A record was persisted with the parsed result.
	recs := srv.diagnoses.list("Deployment", "broken", "stuck-app")
	if len(recs) != 1 {
		t.Fatalf("stored diagnoses = %d, want 1", len(recs))
	}
	got := recs[0]
	if got.Status != "done" || got.RootCause != "OOMKilled" || got.ValidityScore != 0.9 {
		t.Errorf("persisted record wrong: %+v", got)
	}
	if len(got.Evidence) != 1 || got.Evidence[0].Tool != "get_pod_logs" {
		t.Errorf("evidence not persisted: %+v", got.Evidence)
	}

	// List endpoint returns it (kind spelled plural to exercise kindMatches).
	listRec := httptest.NewRecorder()
	srv.handleListDiagnoses(listRec, httptest.NewRequest(http.MethodGet,
		"/api/diagnoses?kind=deployments&namespace=broken&name=stuck-app", nil))
	var listed []DiagnosisRecord
	if err := json.Unmarshal(listRec.Body.Bytes(), &listed); err != nil {
		t.Fatalf("list response not JSON: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != got.ID {
		t.Fatalf("list endpoint returned %d records, want the stored one", len(listed))
	}

	// Get endpoint returns the record by id.
	getReq := httptest.NewRequest(http.MethodGet, "/api/diagnoses/"+got.ID, nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", got.ID)
	getReq = getReq.WithContext(context.WithValue(getReq.Context(), chi.RouteCtxKey, rctx))
	getRec := httptest.NewRecorder()
	srv.handleGetDiagnosis(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status = %d, want 200", getRec.Code)
	}
	var one DiagnosisRecord
	if err := json.Unmarshal(getRec.Body.Bytes(), &one); err != nil || one.ID != got.ID {
		t.Fatalf("get response wrong: %v / %+v", err, one)
	}

	// Unknown id → 404.
	missReq := httptest.NewRequest(http.MethodGet, "/api/diagnoses/nope", nil)
	mrctx := chi.NewRouteContext()
	mrctx.URLParams.Add("id", "nope")
	missReq = missReq.WithContext(context.WithValue(missReq.Context(), chi.RouteCtxKey, mrctx))
	missRec := httptest.NewRecorder()
	srv.handleGetDiagnosis(missRec, missReq)
	if missRec.Code != http.StatusNotFound {
		t.Errorf("unknown id status = %d, want 404", missRec.Code)
	}
}
