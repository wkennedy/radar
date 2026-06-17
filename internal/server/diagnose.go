package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/skyhook-io/radar/internal/issues"
	"github.com/skyhook-io/radar/internal/k8s"
	"github.com/skyhook-io/radar/internal/opensre"
)

// diagSeq disambiguates record ids generated within the same nanosecond.
var diagSeq uint64

func newDiagnosisID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), atomic.AddUint64(&diagSeq, 1))
}

// contractVersion is the OpenSRE↔Radar interface contract version stamped into
// the alert envelope. Keep in sync with integration/INTERFACE_CONTRACT.md.
const contractVersion = "0.1.0"

// maxEnvelopeEvents bounds the events embedded in the alert envelope. The
// envelope is a small starting pointer — OpenSRE fetches deeper context on
// demand via Radar's read-only MCP tools (Direction A).
const maxEnvelopeEvents = 10

// diagnoseEvent is a trimmed, redaction-safe event for the alert envelope.
type diagnoseEvent struct {
	Type          string `json:"type"`
	Reason        string `json:"reason"`
	Message       string `json:"message"`
	Count         int32  `json:"count"`
	LastTimestamp string `json:"lastTimestamp,omitempty"`
}

// handleDiagnoseStream triggers an OpenSRE investigation for a single resource
// and relays OpenSRE's SSE stream back to the browser. It is a GET (so the
// browser can use EventSource) with kind/namespace/name query params.
//
// SSE headers are sent up front so EVERY failure (misconfig, disconnect, bad
// params, OpenSRE rejecting the request) surfaces as an "event: error" frame the
// browser can actually read — a non-200 response would only give EventSource a
// dataless error, hiding the reason behind a generic "connection failed".
func (s *Server) handleDiagnoseStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		// No way to stream — the only failure that can't be an SSE error frame.
		s.writeError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	sendError := func(detail string) {
		payload, _ := json.Marshal(map[string]string{"detail": detail})
		fmt.Fprintf(w, "event: error\ndata: %s\n\n", payload)
		flusher.Flush()
	}

	if !s.opensreClient.IsConfigured() {
		sendError("OpenSRE is not configured (set --opensre-url and --opensre-token).")
		return
	}
	if !k8s.IsConnected() {
		sendError("Not connected to a cluster.")
		return
	}

	scope := strings.TrimSpace(r.URL.Query().Get("scope"))
	if scope == "" {
		scope = "resource"
	}
	kind := strings.TrimSpace(r.URL.Query().Get("kind"))
	namespace := strings.TrimSpace(r.URL.Query().Get("namespace"))
	name := strings.TrimSpace(r.URL.Query().Get("name"))
	switch scope {
	case "resource":
		if kind == "" || name == "" {
			sendError("kind and name are required.")
			return
		}
	case "namespace":
		if namespace == "" {
			sendError("namespace is required for scope=namespace.")
			return
		}
	case "cluster":
		// no resource identity needed
	default:
		sendError("invalid scope (use resource, namespace, or cluster).")
		return
	}

	envelope, alertName, severity, recKind, recName := buildScopedEnvelope(scope, kind, namespace, name)
	stream, err := s.opensreClient.InvestigateStream(r.Context(), opensre.InvestigateRequest{
		RawAlert:     envelope,
		AlertName:    alertName,
		PipelineName: "radar",
		Severity:     severity,
	})
	if err != nil {
		sendError(fmt.Sprintf("OpenSRE investigation failed to start: %v", err))
		return
	}

	// Accumulate the structured result as frames stream by, then persist a
	// record at the terminal state so it shows up in the resource's history.
	rec := &DiagnosisRecord{
		ID:        newDiagnosisID(),
		Kind:      recKind,
		Namespace: namespace,
		Name:      recName,
		Context:   k8s.GetContextName(),
		CreatedAt: time.Now(),
		Trigger:   "manual",
		Status:    "streaming",
	}
	persisted := false
	persist := func() {
		if persisted {
			return
		}
		persisted = true
		s.diagnoses.put(rec)
	}

	for {
		select {
		case <-r.Context().Done():
			// Client closed the panel mid-flight. Keep a partial only if it
			// already produced something useful — avoids littering history with
			// empty cancelled runs.
			if rec.Status == "streaming" {
				rec.Status = "cancelled"
			}
			if rec.Report != "" || rec.RootCause != "" || rec.Error != "" {
				persist()
			}
			return
		case ev, open := <-stream:
			if !open {
				if rec.Status == "streaming" {
					rec.Status = "done"
					if rec.IsNoise {
						rec.Status = "noise"
					}
				}
				persist()
				s.notify.notify(rec)
				fmt.Fprint(w, "event: done\ndata: {}\n\n")
				flusher.Flush()
				return
			}
			applyOpenSREFrame(rec, ev)
			data := ev.Data
			if len(data) == 0 {
				data = []byte("{}")
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Event, data)
			flusher.Flush()
		}
	}
}

// applyOpenSREFrame folds one relayed OpenSRE SSE frame into the persisted
// record: error frames set the error/status; the final publish_findings
// "events" frame (carrying data.output) yields the report, root cause, validity
// score, noise flag, remediation steps, and evidence trail.
func applyOpenSREFrame(rec *DiagnosisRecord, ev opensre.StreamEvent) {
	switch ev.Event {
	case "error":
		var p struct {
			Detail string `json:"detail"`
		}
		_ = json.Unmarshal(ev.Data, &p)
		rec.Status = "error"
		if p.Detail != "" {
			rec.Error = p.Detail
		}
		return
	case "events":
		// no-op; handled below
	default:
		return
	}

	var frame struct {
		Data struct {
			Output map[string]any `json:"output"`
		} `json:"data"`
	}
	if err := json.Unmarshal(ev.Data, &frame); err != nil || frame.Data.Output == nil {
		return
	}
	out := frame.Data.Output
	if v, ok := out["report"].(string); ok && strings.TrimSpace(v) != "" {
		rec.Report = v
	}
	if v, ok := out["root_cause"].(string); ok && v != "" {
		rec.RootCause = v
	}
	if v, ok := out["validity_score"].(float64); ok {
		rec.ValidityScore = v
	}
	if v, ok := out["is_noise"].(bool); ok {
		rec.IsNoise = v
	}
	if v, ok := out["remediation_steps"].([]any); ok {
		rec.Remediation = toStringSlice(v)
	}
	if v, ok := out["evidence_entries"].([]any); ok {
		rec.Evidence = toEvidence(v)
	}
}

func toStringSlice(items []any) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		if s, ok := it.(string); ok && strings.TrimSpace(s) != "" {
			out = append(out, s)
		}
	}
	return out
}

// toEvidence extracts a concise, best-effort evidence trail from OpenSRE's
// evidence_entries (whose exact shape varies): tool + source + a one-line
// summary. Entries with no identifying field are skipped; capped to bound size.
func toEvidence(items []any) []DiagnosisEvidence {
	const maxEvidence = 50
	out := make([]DiagnosisEvidence, 0, len(items))
	for _, it := range items {
		m, ok := it.(map[string]any)
		if !ok {
			continue
		}
		ev := DiagnosisEvidence{
			Tool:    firstString(m, "tool", "tool_name", "name"),
			Source:  firstString(m, "source", "integration"),
			Summary: firstString(m, "summary", "title", "description", "evidence_type"),
		}
		if ev.Tool == "" && ev.Source == "" && ev.Summary == "" {
			continue
		}
		out = append(out, ev)
		if len(out) >= maxEvidence {
			break
		}
	}
	return out
}

func firstString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k].(string); ok && strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// handleListDiagnoses returns stored diagnoses for a resource, newest first.
// GET /api/diagnoses?kind=&namespace=&name=
func (s *Server) handleListDiagnoses(w http.ResponseWriter, r *http.Request) {
	kind := strings.TrimSpace(r.URL.Query().Get("kind"))
	namespace := strings.TrimSpace(r.URL.Query().Get("namespace"))
	name := strings.TrimSpace(r.URL.Query().Get("name"))
	// NOTE: when auth is enabled, list reads should be scoped to the user's
	// namespace access (mirror parseNamespacesForUser). Deferred — v1 targets the
	// no-auth/local case; records are keyed to a resource the user is viewing.
	s.writeJSON(w, s.diagnoses.list(kind, namespace, name))
}

// runHeadlessInvestigation runs an OpenSRE investigation without a browser
// client (used by auto-diagnosis), accumulating the result into a persisted
// record. reason/message, when set, seed the envelope's symptom from the
// detecting issue. Returns the stored record.
func (s *Server) runHeadlessInvestigation(ctx context.Context, kind, namespace, name, reason, message string) *DiagnosisRecord {
	rec := &DiagnosisRecord{
		ID:        newDiagnosisID(),
		Kind:      kind,
		Namespace: namespace,
		Name:      name,
		Context:   k8s.GetContextName(),
		CreatedAt: time.Now(),
		Trigger:   "auto",
		Status:    "streaming",
	}

	envelope, alertName, severity := buildAlertEnvelope(kind, namespace, name, "radar-auto")
	if reason != "" {
		if sym, ok := envelope["symptom"].(map[string]any); ok {
			sym["reason"] = reason
			if message != "" {
				sym["message"] = message
			}
		}
	}

	stream, err := s.opensreClient.InvestigateStream(ctx, opensre.InvestigateRequest{
		RawAlert:     envelope,
		AlertName:    alertName,
		PipelineName: "radar",
		Severity:     severity,
	})
	if err != nil {
		rec.Status = "error"
		rec.Error = err.Error()
		s.diagnoses.put(rec)
		s.notify.notify(rec)
		return rec
	}

	for ev := range stream {
		applyOpenSREFrame(rec, ev)
	}
	if rec.Status == "streaming" {
		rec.Status = "done"
		if rec.IsNoise {
			rec.Status = "noise"
		}
	}
	s.diagnoses.put(rec)
	s.notify.notify(rec)
	return rec
}

// handleGetDiagnosis returns one stored diagnosis by id.
// GET /api/diagnoses/{id}
func (s *Server) handleGetDiagnosis(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	rec, ok := s.diagnoses.get(id)
	if !ok {
		s.writeError(w, http.StatusNotFound, "diagnosis not found")
		return
	}
	s.writeJSON(w, rec)
}

// handleDiagnoseChat answers a follow-up question about a stored diagnosis.
// POST /api/diagnose/chat {id, message, history}. Stateless: the browser holds
// the thread and replays prior turns; the server grounds the answer in the
// stored record's root cause + report. Outside the 60s middleware group because
// a local LLM reply can take longer.
func (s *Server) handleDiagnoseChat(w http.ResponseWriter, r *http.Request) {
	if !s.opensreClient.IsConfigured() {
		s.writeError(w, http.StatusNotFound, "OpenSRE is not configured")
		return
	}
	var body struct {
		ID      string             `json:"id"`
		Message string             `json:"message"`
		History []opensre.ChatTurn `json:"history"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if strings.TrimSpace(body.Message) == "" {
		s.writeError(w, http.StatusBadRequest, "message is required")
		return
	}
	rec, ok := s.diagnoses.get(body.ID)
	if !ok {
		s.writeError(w, http.StatusNotFound, "diagnosis not found")
		return
	}

	reply, err := s.opensreClient.Chat(r.Context(), opensre.ChatRequest{
		Message: body.Message,
		Context: opensre.ChatContext{RootCause: rec.RootCause, Report: rec.Report},
		History: body.History,
	})
	if err != nil {
		s.writeError(w, http.StatusBadGateway, fmt.Sprintf("OpenSRE chat failed: %v", err))
		return
	}
	s.writeJSON(w, map[string]string{"reply": reply})
}

// handleDiagnoseRemediation proposes typed, safe fixes for a stored diagnosis.
// POST /api/diagnose/remediation {id}. Gated on --opensre-remediation. Returns
// the proposed actions; it does NOT execute anything — the browser applies them
// via Radar's existing RBAC-enforced workload endpoints, with confirmation.
func (s *Server) handleDiagnoseRemediation(w http.ResponseWriter, r *http.Request) {
	if !s.remediationEnabled {
		s.writeError(w, http.StatusNotFound, "remediation suggestions are disabled (set --opensre-remediation)")
		return
	}
	if !s.opensreClient.IsConfigured() {
		s.writeError(w, http.StatusNotFound, "OpenSRE is not configured")
		return
	}
	var body struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	rec, ok := s.diagnoses.get(body.ID)
	if !ok {
		s.writeError(w, http.StatusNotFound, "diagnosis not found")
		return
	}

	actions, err := s.opensreClient.Remediate(r.Context(), opensre.RemediationRequest{
		RootCause: rec.RootCause,
		Report:    rec.Report,
		Subject:   opensre.RemediationSubject{Kind: rec.Kind, Namespace: rec.Namespace, Name: rec.Name},
	})
	if err != nil {
		s.writeError(w, http.StatusBadGateway, fmt.Sprintf("OpenSRE remediation failed: %v", err))
		return
	}
	s.writeJSON(w, map[string]any{"actions": actions})
}

// handleDiagnoseFeedback records a thumbs rating on a stored diagnosis and
// forwards it to OpenSRE's eval dataset (best-effort, async).
// POST /api/diagnose/feedback {id, verdict, note}.
func (s *Server) handleDiagnoseFeedback(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID      string `json:"id"`
		Verdict string `json:"verdict"`
		Note    string `json:"note"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if body.Verdict != "up" && body.Verdict != "down" {
		s.writeError(w, http.StatusBadRequest, "verdict must be 'up' or 'down'")
		return
	}
	rec, ok := s.diagnoses.get(body.ID)
	if !ok {
		s.writeError(w, http.StatusNotFound, "diagnosis not found")
		return
	}

	rec.Feedback = &DiagnosisFeedback{Verdict: body.Verdict, Note: body.Note, At: time.Now()}
	s.diagnoses.put(rec)

	// Forward to OpenSRE's eval dataset out of band — never block or fail the UI.
	if s.opensreClient.IsConfigured() {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if err := s.opensreClient.Feedback(ctx, opensre.FeedbackRequest{
				InvestigationID: rec.ID,
				Verdict:         body.Verdict,
				Note:            body.Note,
				RootCause:       rec.RootCause,
				Kind:            rec.Kind,
				Namespace:       rec.Namespace,
				Name:            rec.Name,
			}); err != nil {
				log.Printf("[feedback] forward to OpenSRE failed: %v", err)
			}
		}()
	}
	s.writeJSON(w, map[string]bool{"ok": true})
}

// buildAlertEnvelope assembles the small Radar alert envelope (the OpenSRE
// raw_alert payload) from the resource identity plus its recent events. Owner
// chains, topology, logs, etc. are intentionally omitted — OpenSRE pulls those
// on demand via Radar's MCP tools. Returns the envelope, a human alert name,
// and a severity derived from the events.
func buildAlertEnvelope(kind, namespace, name, detectedBy string) (map[string]any, string, string) {
	events := recentEventsFor(kind, namespace, name, maxEnvelopeEvents)

	severity := "warning"
	reason := ""
	message := ""
	for _, e := range events {
		if strings.EqualFold(e.Type, "Warning") {
			severity = "high"
			if reason == "" {
				reason = e.Reason
				message = e.Message
			}
		}
	}

	trimmed := make([]diagnoseEvent, 0, len(events))
	for _, e := range events {
		ts := ""
		if !e.LastTimestamp.IsZero() {
			ts = e.LastTimestamp.UTC().Format("2006-01-02T15:04:05Z")
		}
		trimmed = append(trimmed, diagnoseEvent{
			Type:          e.Type,
			Reason:        e.Reason,
			Message:       e.Message,
			Count:         e.Count,
			LastTimestamp: ts,
		})
	}

	symptom := map[string]any{"detectedBy": detectedBy}
	if reason != "" {
		symptom["reason"] = reason
		symptom["message"] = message
	}

	envelope := map[string]any{
		"source":          "radar",
		"contractVersion": contractVersion,
		"cluster": map[string]any{
			"context": k8s.GetContextName(),
		},
		"subject": map[string]any{
			"kind":      kind,
			"namespace": namespace,
			"name":      name,
		},
		"symptom":      symptom,
		"recentEvents": trimmed,
	}

	label := reason
	if label == "" {
		label = kind
	}
	target := name
	if namespace != "" {
		target = namespace + "/" + name
	}
	alertName := fmt.Sprintf("%s: %s", label, target)
	return envelope, alertName, severity
}

// buildScopedEnvelope dispatches envelope construction by scope. Resource scope
// uses the single-resource envelope; namespace/cluster scope seed from Radar's
// detected issues so OpenSRE can investigate "what's wrong here" broadly. Returns
// the envelope, alert name, severity, and the kind/name to record under.
func buildScopedEnvelope(scope, kind, namespace, name string) (
	env map[string]any, alertName, severity, recKind, recName string,
) {
	switch scope {
	case "cluster":
		env, alertName, severity = scopedIssuesEnvelope("cluster", "")
		return env, alertName, severity, "Cluster", k8s.GetContextName()
	case "namespace":
		env, alertName, severity = scopedIssuesEnvelope("namespace", namespace)
		return env, alertName, severity, "Namespace", namespace
	default:
		env, alertName, severity = buildAlertEnvelope(kind, namespace, name, "user-initiated")
		return env, alertName, severity, kind, name
	}
}

// scopedIssuesEnvelope builds an alert envelope seeded from Radar's current
// issues for a namespace or the whole cluster (bounded). OpenSRE uses this as a
// starting picture and can drill in further via Radar's MCP tools (Direction A).
func scopedIssuesEnvelope(scope, namespace string) (map[string]any, string, string) {
	const maxIssues = 20
	var list []issues.Issue
	if provider := issues.NewCacheProvider(); provider != nil {
		f := issues.Filters{
			Severities: []issues.Severity{issues.SeverityCritical, issues.SeverityWarning},
			Grouped:    true,
			Limit:      maxIssues,
		}
		if scope == "namespace" && namespace != "" {
			f.Namespaces = []string{namespace}
		}
		list, _ = issues.ComposeWithStats(provider, f)
	}

	hasCritical := false
	items := make([]map[string]any, 0, len(list))
	for _, iss := range list {
		if iss.Severity == issues.SeverityCritical {
			hasCritical = true
		}
		items = append(items, map[string]any{
			"severity":  string(iss.Severity),
			"category":  string(iss.Category),
			"kind":      iss.Kind,
			"namespace": iss.Namespace,
			"name":      iss.Name,
			"message":   iss.Message,
		})
	}
	severity := "warning"
	if hasCritical {
		severity = "high"
	}

	scopeLabel := "the cluster"
	if scope == "namespace" {
		scopeLabel = "namespace " + namespace
	}
	envelope := map[string]any{
		"source":          "radar",
		"contractVersion": contractVersion,
		"scope":           scope,
		"cluster":         map[string]any{"context": k8s.GetContextName()},
		"symptom": map[string]any{
			"detectedBy": "user-initiated",
			"summary":    fmt.Sprintf("%d issue(s) detected in %s", len(items), scopeLabel),
		},
		"issues": items,
	}
	if scope == "namespace" && namespace != "" {
		envelope["namespace"] = namespace
	}

	alertName := fmt.Sprintf("Cluster health: %d issue(s)", len(items))
	if scope == "namespace" {
		alertName = fmt.Sprintf("Namespace %s: %d issue(s)", namespace, len(items))
	}
	return envelope, alertName, severity
}

// recentEventsFor returns up to limit events involving (kind, name) in the
// namespace, newest first. Kind-agnostic: it filters the shared event cache by
// InvolvedObject, so it works for any resource kind without a type switch.
func recentEventsFor(kind, namespace, name string, limit int) []*corev1.Event {
	cache := k8s.GetResourceCache()
	if cache == nil || cache.Events() == nil {
		return nil
	}
	all, err := cache.Events().Events(namespace).List(labels.Everything())
	if err != nil {
		return nil
	}
	var matched []*corev1.Event
	for _, e := range all {
		if strings.EqualFold(e.InvolvedObject.Kind, kind) && e.InvolvedObject.Name == name {
			matched = append(matched, e)
		}
	}
	sort.Slice(matched, func(i, j int) bool {
		return matched[i].LastTimestamp.After(matched[j].LastTimestamp.Time)
	})
	if len(matched) > limit {
		matched = matched[:limit]
	}
	return matched
}
