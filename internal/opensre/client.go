// Package opensre is a thin client for triggering OpenSRE investigations from
// Radar (the "Diagnose with AI" action). Radar POSTs an alert envelope to
// OpenSRE's /investigate/stream endpoint and relays the Server-Sent Events
// stream back to the browser.
//
// Contract (verified against OpenSRE app/remote/server.py):
//   - POST {baseURL}/investigate/stream
//   - Auth header: X-API-Key: <token>   (NOT Authorization: Bearer)
//   - Body: {"raw_alert": <envelope>, "alert_name", "pipeline_name", "severity"}
//   - Response: text/event-stream with frames "event: events|end|error".
package opensre

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// streamTimeout bounds a whole investigation. Investigations are long-running
// (multi-step LLM + tool calls), so this is generous relative to ordinary API
// calls. The browser can also disconnect early via request-context cancel.
const streamTimeout = 10 * time.Minute

// Client talks to a single OpenSRE service. The zero value is unusable; build
// with NewClient. A nil *Client is a valid "not configured" sentinel — methods
// guard with IsConfigured.
type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

// NewClient builds an OpenSRE client. baseURL/token come from --opensre-url /
// --opensre-token (or their env vars). When either is empty the client reports
// IsConfigured()==false and the diagnose feature stays hidden/disabled.
func NewClient(baseURL, token string) *Client {
	return &Client{
		baseURL:    strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		token:      strings.TrimSpace(token),
		httpClient: &http.Client{Timeout: streamTimeout},
	}
}

// IsConfigured reports whether OpenSRE is wired up (URL + token present).
func (c *Client) IsConfigured() bool {
	return c != nil && c.baseURL != "" && c.token != ""
}

// InvestigateRequest is the OpenSRE /investigate(/stream) request body.
type InvestigateRequest struct {
	RawAlert     any    `json:"raw_alert"`
	AlertName    string `json:"alert_name,omitempty"`
	PipelineName string `json:"pipeline_name,omitempty"`
	Severity     string `json:"severity,omitempty"`
}

// StreamEvent is one Server-Sent Event from OpenSRE: the SSE "event:" name and
// its raw "data:" payload (JSON), passed through to the browser untouched.
type StreamEvent struct {
	Event string
	Data  []byte
}

// ChatTurn is one prior message in a follow-up conversation.
type ChatTurn struct {
	Role    string `json:"role"` // user | assistant
	Content string `json:"content"`
}

// ChatContext grounds a follow-up in a completed investigation.
type ChatContext struct {
	AlertName string `json:"alert_name,omitempty"`
	RootCause string `json:"root_cause,omitempty"`
	ProblemMD string `json:"problem_md,omitempty"`
	Report    string `json:"report,omitempty"`
}

// ChatRequest is the OpenSRE /chat request body.
type ChatRequest struct {
	Message string      `json:"message"`
	Context ChatContext `json:"context"`
	History []ChatTurn  `json:"history,omitempty"`
}

// Chat asks OpenSRE a follow-up question grounded in a completed investigation.
// Stateless: the caller supplies the RCA context + prior turns. Returns the
// assistant's reply.
func (c *Client) Chat(ctx context.Context, reqBody ChatRequest) (string, error) {
	if !c.IsConfigured() {
		return "", fmt.Errorf("OpenSRE is not configured")
	}
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshal chat request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/chat", bytes.NewReader(payload))
	if err != nil {
		return "", fmt.Errorf("build chat request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("call OpenSRE: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return "", fmt.Errorf("OpenSRE returned %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	var out struct {
		Reply string `json:"reply"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("decode chat response: %w", err)
	}
	return out.Reply, nil
}

// RemediationAction is one typed, safe, reversible fix OpenSRE proposes. Only
// restart/scale are supported in v1 (the allowlist is enforced OpenSRE-side).
type RemediationAction struct {
	Type        string `json:"type"` // restart | scale
	Kind        string `json:"kind"`
	Namespace   string `json:"namespace"`
	Name        string `json:"name"`
	Replicas    *int   `json:"replicas,omitempty"`
	Description string `json:"description"`
	Risk        string `json:"risk"`
}

// RemediationSubject is the workload a remediation plan targets.
type RemediationSubject struct {
	Kind      string `json:"kind"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// RemediationRequest is the OpenSRE /remediation request body.
type RemediationRequest struct {
	RootCause string             `json:"root_cause"`
	Report    string             `json:"report"`
	Subject   RemediationSubject `json:"subject"`
}

// Remediate asks OpenSRE for typed remediation actions grounded in a completed
// investigation. Returns the proposed actions (possibly empty).
func (c *Client) Remediate(ctx context.Context, reqBody RemediationRequest) ([]RemediationAction, error) {
	if !c.IsConfigured() {
		return nil, fmt.Errorf("OpenSRE is not configured")
	}
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal remediation request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/remediation", bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build remediation request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call OpenSRE: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("OpenSRE returned %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	var out struct {
		Actions []RemediationAction `json:"actions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode remediation response: %w", err)
	}
	return out.Actions, nil
}

// InvestigateStream POSTs the request to OpenSRE /investigate/stream and returns
// a channel of parsed SSE frames. The channel closes when the stream ends, the
// context is cancelled, or an error occurs (errors are surfaced via the return
// value before the channel is created, or logged as a synthetic error frame).
//
// The caller owns relaying frames to its own client and must drain the channel
// (or cancel ctx) to release the underlying connection.
func (c *Client) InvestigateStream(ctx context.Context, reqBody InvestigateRequest) (<-chan StreamEvent, error) {
	if !c.IsConfigured() {
		return nil, fmt.Errorf("OpenSRE is not configured")
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal investigate request: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, c.baseURL+"/investigate/stream", bytes.NewReader(payload),
	)
	if err != nil {
		return nil, fmt.Errorf("build investigate request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("X-API-Key", c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call OpenSRE: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("OpenSRE returned %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	}

	out := make(chan StreamEvent, 16)
	go func() {
		defer close(out)
		defer resp.Body.Close()
		parseSSE(ctx, resp.Body, out)
	}()
	return out, nil
}

// parseSSE reads a text/event-stream body and emits one StreamEvent per frame.
// A frame ends at a blank line; "event:" sets the name (default "message"),
// "data:" lines accumulate (joined with newlines, per the SSE spec).
func parseSSE(ctx context.Context, body io.Reader, out chan<- StreamEvent) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var event string
	var data []string

	emit := func() {
		if event == "" && len(data) == 0 {
			return
		}
		name := event
		if name == "" {
			name = "message"
		}
		frame := StreamEvent{Event: name, Data: []byte(strings.Join(data, "\n"))}
		event = ""
		data = data[:0]
		select {
		case out <- frame:
		case <-ctx.Done():
		}
	}

	for scanner.Scan() {
		if ctx.Err() != nil {
			return
		}
		line := scanner.Text()
		switch {
		case line == "":
			emit()
		case strings.HasPrefix(line, ":"):
			// SSE comment / heartbeat — ignore.
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	emit() // flush a trailing frame with no terminating blank line
}
