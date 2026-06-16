package server

import (
	"testing"
	"time"

	"github.com/skyhook-io/radar/internal/issues"
)

func issue(id, kind, ns, name string) issues.Issue {
	return issues.Issue{ID: id, Kind: kind, Namespace: ns, Name: name}
}

func TestAutoDiagnoser_Cooldown(t *testing.T) {
	srv := New(Config{DevMode: true})
	ad := newAutoDiagnoser(srv, AutoDiagnoseConfig{Cooldown: time.Hour, MaxPerHour: 100})

	a := issue("a", "Deployment", "ns", "web")
	if !ad.shouldFire(a) {
		t.Fatal("first occurrence should fire")
	}
	ad.markFired(a)
	if ad.shouldFire(a) {
		t.Error("same issue within cooldown must be suppressed")
	}
}

func TestAutoDiagnoser_RateCap(t *testing.T) {
	srv := New(Config{DevMode: true})
	ad := newAutoDiagnoser(srv, AutoDiagnoseConfig{Cooldown: time.Nanosecond, MaxPerHour: 2})

	ad.markFired(issue("x1", "Deployment", "ns", "a"))
	ad.markFired(issue("x2", "Deployment", "ns", "b"))
	// Two launched this hour; the cap blocks a third regardless of cooldown.
	if ad.shouldFire(issue("x3", "Deployment", "ns", "c")) {
		t.Error("hourly rate cap must block the third investigation")
	}
}

func TestAutoDiagnoser_SkipsRecentlyDiagnosedResource(t *testing.T) {
	srv := New(Config{DevMode: true})
	ad := newAutoDiagnoser(srv, AutoDiagnoseConfig{Cooldown: time.Hour, MaxPerHour: 100})

	// A manual (or prior) diagnosis exists for this resource within cooldown.
	srv.diagnoses.put(&DiagnosisRecord{
		ID: "rec1", Kind: "Deployment", Namespace: "ns", Name: "web", CreatedAt: time.Now(),
	})
	if ad.shouldFire(issue("new-issue-id", "Deployment", "ns", "web")) {
		t.Error("should skip: resource was diagnosed within cooldown")
	}

	// A stale diagnosis (older than cooldown) does not block.
	ad2 := newAutoDiagnoser(srv, AutoDiagnoseConfig{Cooldown: time.Minute, MaxPerHour: 100})
	srv.diagnoses.put(&DiagnosisRecord{
		ID: "rec2", Kind: "StatefulSet", Namespace: "ns", Name: "db",
		CreatedAt: time.Now().Add(-2 * time.Hour),
	})
	if !ad2.shouldFire(issue("issue-db", "StatefulSet", "ns", "db")) {
		t.Error("a diagnosis older than cooldown should not block")
	}
}

func TestAutoDiagnoseConfig_Defaults(t *testing.T) {
	c := AutoDiagnoseConfig{}.withDefaults()
	if c.Interval <= 0 || c.Cooldown <= 0 || c.MaxPerHour <= 0 {
		t.Errorf("defaults not applied: %+v", c)
	}
}
