package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// notifier posts a compact, Slack-incoming-webhook-compatible message when a
// diagnosis completes, with a deep link back to the resource in Radar. It fires
// for terminal done/error records (skips noise/cancelled/streaming) and is
// best-effort: failures are logged, never block the request. Generic enough for
// any JSON webhook sink (the `diagnosis` field carries the structured result;
// Slack ignores it and renders `text`).
type notifier struct {
	webhookURL string
	baseURL    string // Radar external base URL for deep links
	client     *http.Client
}

func newNotifier(webhookURL, baseURL string, port int) *notifier {
	base := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if base == "" {
		base = fmt.Sprintf("http://localhost:%d", port)
	}
	return &notifier{
		webhookURL: strings.TrimSpace(webhookURL),
		baseURL:    base,
		client:     &http.Client{Timeout: 10 * time.Second},
	}
}

func (n *notifier) enabled() bool { return n != nil && n.webhookURL != "" }

func (n *notifier) deepLink(rec *DiagnosisRecord) string {
	return fmt.Sprintf("%s/workload/%s/%s/%s",
		n.baseURL,
		url.PathEscape(rec.Kind),
		url.PathEscape(rec.Namespace),
		url.PathEscape(rec.Name),
	)
}

// shouldNotify keeps notifications to actionable terminal states.
func shouldNotify(rec *DiagnosisRecord) bool {
	if rec == nil {
		return false
	}
	return rec.Status == "done" || rec.Status == "error"
}

// notify fires asynchronously; safe to call on any persisted record.
func (n *notifier) notify(rec *DiagnosisRecord) {
	if !n.enabled() || !shouldNotify(rec) {
		return
	}
	go n.send(rec)
}

func (n *notifier) buildPayload(rec *DiagnosisRecord) map[string]any {
	link := n.deepLink(rec)
	trigger := rec.Trigger
	if trigger == "" {
		trigger = "manual"
	}
	target := fmt.Sprintf("%s %s/%s", rec.Kind, rec.Namespace, rec.Name)

	var text string
	if rec.Status == "error" {
		text = fmt.Sprintf(":warning: *AI diagnosis failed* (%s) for <%s|%s>\n%s", trigger, link, target, rec.Error)
	} else {
		conf := ""
		if rec.ValidityScore > 0 {
			conf = fmt.Sprintf(" · confidence %d%%", int(rec.ValidityScore*100+0.5))
		}
		rc := rec.RootCause
		if rc == "" {
			rc = "(no root cause identified)"
		}
		text = fmt.Sprintf(":mag: *AI diagnosis* (%s%s) for <%s|%s>\n*Root cause:* %s", trigger, conf, link, target, rc)
	}

	return map[string]any{
		"text": text,
		"diagnosis": map[string]any{
			"id":            rec.ID,
			"kind":          rec.Kind,
			"namespace":     rec.Namespace,
			"name":          rec.Name,
			"status":        rec.Status,
			"trigger":       trigger,
			"rootCause":     rec.RootCause,
			"validityScore": rec.ValidityScore,
			"url":           link,
		},
	}
}

func (n *notifier) send(rec *DiagnosisRecord) {
	body, err := json.Marshal(n.buildPayload(rec))
	if err != nil {
		log.Printf("[notify] marshal failed: %v", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.webhookURL, bytes.NewReader(body))
	if err != nil {
		log.Printf("[notify] build request failed: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.client.Do(req)
	if err != nil {
		log.Printf("[notify] post to webhook failed: %v", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		log.Printf("[notify] webhook returned %d for diagnosis %s", resp.StatusCode, rec.ID)
	}
}
