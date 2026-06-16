package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/skyhook-io/radar/internal/k8s"
	"github.com/skyhook-io/radar/internal/opensre"
)

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

	kind := strings.TrimSpace(r.URL.Query().Get("kind"))
	namespace := strings.TrimSpace(r.URL.Query().Get("namespace"))
	name := strings.TrimSpace(r.URL.Query().Get("name"))
	if kind == "" || name == "" {
		sendError("kind and name are required.")
		return
	}

	envelope, alertName, severity := buildAlertEnvelope(kind, namespace, name)
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

	for {
		select {
		case <-r.Context().Done():
			return
		case ev, open := <-stream:
			if !open {
				fmt.Fprint(w, "event: done\ndata: {}\n\n")
				flusher.Flush()
				return
			}
			data := ev.Data
			if len(data) == 0 {
				data = []byte("{}")
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Event, data)
			flusher.Flush()
		}
	}
}

// buildAlertEnvelope assembles the small Radar alert envelope (the OpenSRE
// raw_alert payload) from the resource identity plus its recent events. Owner
// chains, topology, logs, etc. are intentionally omitted — OpenSRE pulls those
// on demand via Radar's MCP tools. Returns the envelope, a human alert name,
// and a severity derived from the events.
func buildAlertEnvelope(kind, namespace, name string) (map[string]any, string, string) {
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

	symptom := map[string]any{"detectedBy": "user-initiated"}
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
