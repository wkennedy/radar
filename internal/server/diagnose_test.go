package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/skyhook-io/radar/internal/opensre"
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

func TestBuildScopedEnvelope(t *testing.T) {
	env, alert, _, recKind, recName := buildScopedEnvelope("cluster", "", "", "")
	if env["scope"] != "cluster" {
		t.Errorf("scope = %v, want cluster", env["scope"])
	}
	if _, ok := env["issues"]; !ok {
		t.Error("cluster envelope should carry an issues list")
	}
	_ = recName // = kube context name; empty in the test harness, set in production
	if recKind != "Cluster" {
		t.Errorf("cluster rec kind = %q, want Cluster", recKind)
	}
	if !strings.Contains(alert, "Cluster health") {
		t.Errorf("alert = %q", alert)
	}

	envN, alertN, _, rk, rn := buildScopedEnvelope("namespace", "", "broken", "")
	if envN["scope"] != "namespace" || envN["namespace"] != "broken" {
		t.Errorf("namespace envelope = %v", envN)
	}
	if rk != "Namespace" || rn != "broken" {
		t.Errorf("namespace rec kind/name = %q/%q", rk, rn)
	}
	if !strings.Contains(alertN, "Namespace broken") {
		t.Errorf("alertN = %q", alertN)
	}
}

func TestHandleDiagnoseStream_ClusterScope(t *testing.T) {
	mock := mockOpenSREWithResult(t)
	defer mock.Close()
	srv := New(Config{DevMode: true, OpenSREURL: mock.URL, OpenSREToken: "k"})

	req := httptest.NewRequest(http.MethodGet, "/api/diagnose/stream?scope=cluster", nil)
	srv.handleDiagnoseStream(httptest.NewRecorder(), req)

	recs := srv.diagnoses.list("Cluster", "", "")
	if len(recs) != 1 {
		t.Fatalf("cluster-scope diagnoses = %d, want 1", len(recs))
	}
	if recs[0].Status != "done" {
		t.Errorf("status = %q, want done", recs[0].Status)
	}
}

func TestHandleDiagnoseStream_ScopeValidation(t *testing.T) {
	mock := mockOpenSREWithResult(t)
	defer mock.Close()
	srv := New(Config{DevMode: true, OpenSREURL: mock.URL, OpenSREToken: "k"})

	// scope=namespace without a namespace → error frame.
	rec := httptest.NewRecorder()
	srv.handleDiagnoseStream(rec, httptest.NewRequest(http.MethodGet, "/api/diagnose/stream?scope=namespace", nil))
	if !strings.Contains(rec.Body.String(), "event: error") || !strings.Contains(rec.Body.String(), "namespace is required") {
		t.Errorf("expected namespace-required error frame:\n%s", rec.Body.String())
	}

	// invalid scope → error frame.
	rec = httptest.NewRecorder()
	srv.handleDiagnoseStream(rec, httptest.NewRequest(http.MethodGet, "/api/diagnose/stream?scope=bogus", nil))
	if !strings.Contains(rec.Body.String(), "invalid scope") {
		t.Errorf("expected invalid-scope error frame:\n%s", rec.Body.String())
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

func TestHandleDiagnoseStream_FiresNotification(t *testing.T) {
	mock := mockOpenSREWithResult(t)
	defer mock.Close()

	got := make(chan map[string]any, 1)
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		select {
		case got <- body:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer sink.Close()

	srv := New(Config{
		DevMode: true, OpenSREURL: mock.URL, OpenSREToken: "k",
		NotifyWebhook: sink.URL, RadarBaseURL: "http://localhost:9280",
	})
	req := httptest.NewRequest(http.MethodGet, "/api/diagnose/stream?kind=Deployment&namespace=broken&name=stuck-app", nil)
	srv.handleDiagnoseStream(httptest.NewRecorder(), req)

	select {
	case body := <-got:
		text, _ := body["text"].(string)
		if !strings.Contains(text, "stuck-app") || !strings.Contains(text, "/workload/Deployment/broken/stuck-app") {
			t.Errorf("notification missing resource/deep link: %q", text)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("expected a notification webhook call after diagnosis completion")
	}
}

func TestHandleDiagnoseChat(t *testing.T) {
	var gotBody opensre.ChatRequest
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"reply":"It OOMed because the memory limit was 32Mi."}`)
	}))
	defer mock.Close()

	srv := New(Config{DevMode: true, OpenSREURL: mock.URL, OpenSREToken: "k"})
	srv.diagnoses.put(&DiagnosisRecord{
		ID: "d1", Kind: "Deployment", Namespace: "ns", Name: "web",
		RootCause: "OOMKilled", Report: "## RCA", Status: "done",
	})

	body := `{"id":"d1","message":"why did it oom?","history":[{"role":"user","content":"q"},{"role":"assistant","content":"a"}]}`
	rec := httptest.NewRecorder()
	srv.handleDiagnoseChat(rec, httptest.NewRequest(http.MethodPost, "/api/diagnose/chat", strings.NewReader(body)))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || resp["reply"] == "" {
		t.Fatalf("bad reply response: %v / %v", err, resp)
	}
	// The stored record grounded the request, and the thread was forwarded.
	if gotBody.Context.RootCause != "OOMKilled" || gotBody.Context.Report != "## RCA" {
		t.Errorf("context not grounded from record: %+v", gotBody.Context)
	}
	if len(gotBody.History) != 2 || gotBody.Message != "why did it oom?" {
		t.Errorf("message/history not forwarded: msg=%q history=%d", gotBody.Message, len(gotBody.History))
	}
}

func TestHandleDiagnoseChat_Errors(t *testing.T) {
	srv := New(Config{DevMode: true, OpenSREURL: "http://127.0.0.1:9099", OpenSREToken: "k"})
	srv.diagnoses.put(&DiagnosisRecord{ID: "d1", Kind: "Deployment", Namespace: "ns", Name: "web", Status: "done"})

	// Empty message → 400.
	rec := httptest.NewRecorder()
	srv.handleDiagnoseChat(rec, httptest.NewRequest(http.MethodPost, "/api/diagnose/chat", strings.NewReader(`{"id":"d1","message":"  "}`)))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("empty message status = %d, want 400", rec.Code)
	}

	// Unknown diagnosis id → 404.
	rec = httptest.NewRecorder()
	srv.handleDiagnoseChat(rec, httptest.NewRequest(http.MethodPost, "/api/diagnose/chat", strings.NewReader(`{"id":"nope","message":"hi"}`)))
	if rec.Code != http.StatusNotFound {
		t.Errorf("unknown id status = %d, want 404", rec.Code)
	}
}

func TestHandleDiagnoseRemediation(t *testing.T) {
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/remediation" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"actions":[{"type":"restart","kind":"Deployment","namespace":"ns","name":"web","risk":"low"},{"type":"scale","kind":"Deployment","namespace":"ns","name":"web","replicas":3}]}`)
	}))
	defer mock.Close()

	srv := New(Config{DevMode: true, OpenSREURL: mock.URL, OpenSREToken: "k", OpenSRERemediation: true})
	srv.diagnoses.put(&DiagnosisRecord{ID: "d1", Kind: "Deployment", Namespace: "ns", Name: "web", RootCause: "wedged", Report: "## RCA", Status: "done"})

	rec := httptest.NewRecorder()
	srv.handleDiagnoseRemediation(rec, httptest.NewRequest(http.MethodPost, "/api/diagnose/remediation", strings.NewReader(`{"id":"d1"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp struct {
		Actions []opensre.RemediationAction `json:"actions"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("bad response: %v", err)
	}
	if len(resp.Actions) != 2 || resp.Actions[0].Type != "restart" || resp.Actions[1].Type != "scale" {
		t.Fatalf("actions = %+v", resp.Actions)
	}
	if resp.Actions[1].Replicas == nil || *resp.Actions[1].Replicas != 3 {
		t.Errorf("scale replicas not parsed: %+v", resp.Actions[1])
	}
}

func TestHandleDiagnoseRemediation_DisabledByDefault(t *testing.T) {
	srv := New(Config{DevMode: true, OpenSREURL: "http://x", OpenSREToken: "k"}) // OpenSRERemediation false
	srv.diagnoses.put(&DiagnosisRecord{ID: "d1", Kind: "Deployment", Namespace: "ns", Name: "web", Status: "done"})

	rec := httptest.NewRecorder()
	srv.handleDiagnoseRemediation(rec, httptest.NewRequest(http.MethodPost, "/api/diagnose/remediation", strings.NewReader(`{"id":"d1"}`)))
	if rec.Code != http.StatusNotFound {
		t.Errorf("disabled remediation status = %d, want 404", rec.Code)
	}
}
