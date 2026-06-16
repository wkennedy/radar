package server

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// DiagnosisEvidence is one item in a diagnosis's evidence trail — typically a
// tool the OpenSRE agent consulted (Radar MCP tools are tagged source "radar").
type DiagnosisEvidence struct {
	Tool    string `json:"tool,omitempty"`
	Source  string `json:"source,omitempty"`
	Summary string `json:"summary,omitempty"`
}

// DiagnosisRecord is a persisted OpenSRE investigation result for one resource.
type DiagnosisRecord struct {
	ID            string              `json:"id"`
	Kind          string              `json:"kind"`
	Namespace     string              `json:"namespace"`
	Name          string              `json:"name"`
	Context       string              `json:"context,omitempty"`
	CreatedAt     time.Time           `json:"createdAt"`
	Trigger       string              `json:"trigger,omitempty"` // manual | auto
	Status        string              `json:"status"`            // done | noise | error | cancelled
	RootCause     string              `json:"rootCause,omitempty"`
	Report        string              `json:"report,omitempty"`
	ValidityScore float64             `json:"validityScore,omitempty"`
	IsNoise       bool                `json:"isNoise,omitempty"`
	Remediation   []string            `json:"remediation,omitempty"`
	Evidence      []DiagnosisEvidence `json:"evidence,omitempty"`
	Error         string              `json:"error,omitempty"`
}

// diagnoseStore is an in-memory, bounded store of diagnosis records — the v1
// persistence backing the "Past diagnoses" history. SQLite durability across
// restarts is a planned follow-up; see
// integration/proposals/03-reports-in-ui.md. Records are evicted oldest-first
// past the cap.
type diagnoseStore struct {
	mu    sync.RWMutex
	byID  map[string]*DiagnosisRecord
	order []string // insertion order, oldest first
	max   int
}

func newDiagnoseStore(max int) *diagnoseStore {
	if max <= 0 {
		max = 500
	}
	return &diagnoseStore{byID: make(map[string]*DiagnosisRecord), max: max}
}

func (d *diagnoseStore) put(rec *DiagnosisRecord) {
	if rec == nil || rec.ID == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.byID[rec.ID]; !exists {
		d.order = append(d.order, rec.ID)
	}
	d.byID[rec.ID] = rec
	for len(d.order) > d.max {
		oldest := d.order[0]
		d.order = d.order[1:]
		delete(d.byID, oldest)
	}
}

func (d *diagnoseStore) get(id string) (*DiagnosisRecord, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	rec, ok := d.byID[id]
	return rec, ok
}

// list returns records for a resource, newest first. Empty filters match
// anything. Kind matching tolerates singular/plural + case differences so the
// history query (which may use a different kind spelling than the launch path)
// still resolves — see kindMatches.
func (d *diagnoseStore) list(kind, namespace, name string) []*DiagnosisRecord {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]*DiagnosisRecord, 0, len(d.order))
	for _, id := range d.order {
		rec := d.byID[id]
		if kind != "" && !kindMatches(rec.Kind, kind) {
			continue
		}
		if namespace != "" && rec.Namespace != namespace {
			continue
		}
		if name != "" && rec.Name != name {
			continue
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out
}

// kindMatches compares two Kubernetes kind spellings tolerantly: case-
// insensitive, and singular/plural off by a trailing "s" (Deployment ==
// deployments). Good enough for the workload kinds this surface targets; it does
// not handle irregular plurals (ingress/ingresses).
func kindMatches(a, b string) bool {
	la, lb := strings.ToLower(a), strings.ToLower(b)
	if la == lb {
		return true
	}
	return strings.TrimSuffix(la, "s") == strings.TrimSuffix(lb, "s")
}
