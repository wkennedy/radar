package server

import (
	"testing"
	"time"

	"github.com/skyhook-io/radar/internal/opensre"
)

func TestDiagnoseStore_PutGetListEvict(t *testing.T) {
	store := newDiagnoseStore(2)

	mk := func(id, name string, ageSec int) *DiagnosisRecord {
		return &DiagnosisRecord{
			ID: id, Kind: "Deployment", Namespace: "payments", Name: name,
			CreatedAt: time.Now().Add(-time.Duration(ageSec) * time.Second), Status: "done",
		}
	}
	store.put(mk("a", "api", 30))
	store.put(mk("b", "api", 20))
	store.put(mk("c", "api", 10)) // exceeds cap of 2 → "a" evicted

	if _, ok := store.get("a"); ok {
		t.Error("oldest record should have been evicted")
	}
	if _, ok := store.get("c"); !ok {
		t.Error("newest record should be present")
	}

	list := store.list("Deployment", "payments", "api")
	if len(list) != 2 {
		t.Fatalf("list len = %d, want 2", len(list))
	}
	// Newest first.
	if list[0].ID != "c" || list[1].ID != "b" {
		t.Errorf("list order = %s,%s; want c,b", list[0].ID, list[1].ID)
	}
}

func TestDiagnoseStore_ListFilters(t *testing.T) {
	store := newDiagnoseStore(10)
	store.put(&DiagnosisRecord{ID: "1", Kind: "Deployment", Namespace: "a", Name: "x", CreatedAt: time.Now()})
	store.put(&DiagnosisRecord{ID: "2", Kind: "Deployment", Namespace: "a", Name: "y", CreatedAt: time.Now()})

	// kindMatches tolerates plural/case: "deployments" matches stored "Deployment".
	if got := store.list("deployments", "a", "x"); len(got) != 1 || got[0].ID != "1" {
		t.Errorf("plural/case kind filter failed: %+v", got)
	}
	if got := store.list("Deployment", "a", "y"); len(got) != 1 || got[0].ID != "2" {
		t.Errorf("name filter failed: %+v", got)
	}
	if got := store.list("Deployment", "other", "x"); len(got) != 0 {
		t.Errorf("namespace filter should exclude: %+v", got)
	}
}

func TestKindMatches(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"Deployment", "deployments", true},
		{"deployments", "Deployment", true},
		{"Pod", "pods", true},
		{"Deployment", "Deployment", true},
		{"Deployment", "StatefulSet", false},
	}
	for _, c := range cases {
		if got := kindMatches(c.a, c.b); got != c.want {
			t.Errorf("kindMatches(%q,%q) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

func TestApplyOpenSREFrame_ParsesOutput(t *testing.T) {
	rec := &DiagnosisRecord{Status: "streaming"}
	applyOpenSREFrame(rec, opensre.StreamEvent{
		Event: "events",
		Data: []byte(`{"event":"on_chain_end","name":"publish_findings","data":{"output":{
			"report":"## RCA","root_cause":"OOMKilled","validity_score":0.9,"is_noise":false,
			"remediation_steps":["raise memory limit"],
			"evidence_entries":[{"source":"radar","tool":"get_pod_logs","summary":"saw OOM"}]}}}`),
	})

	if rec.Report != "## RCA" || rec.RootCause != "OOMKilled" {
		t.Errorf("report/root_cause not parsed: %+v", rec)
	}
	if rec.ValidityScore != 0.9 || rec.IsNoise {
		t.Errorf("validity/noise not parsed: %+v", rec)
	}
	if len(rec.Remediation) != 1 || rec.Remediation[0] != "raise memory limit" {
		t.Errorf("remediation not parsed: %+v", rec.Remediation)
	}
	if len(rec.Evidence) != 1 || rec.Evidence[0].Source != "radar" || rec.Evidence[0].Tool != "get_pod_logs" {
		t.Errorf("evidence not parsed: %+v", rec.Evidence)
	}
}

func TestApplyOpenSREFrame_ErrorFrame(t *testing.T) {
	rec := &DiagnosisRecord{Status: "streaming"}
	applyOpenSREFrame(rec, opensre.StreamEvent{Event: "error", Data: []byte(`{"detail":"OpenSRE returned 403"}`)})
	if rec.Status != "error" || rec.Error != "OpenSRE returned 403" {
		t.Errorf("error frame not applied: %+v", rec)
	}
}
