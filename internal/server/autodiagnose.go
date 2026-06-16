package server

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/skyhook-io/radar/internal/issues"
	"github.com/skyhook-io/radar/internal/k8s"
)

// AutoDiagnoseConfig configures proactive auto-diagnosis. Disabled by default;
// when enabled it is intentionally conservative (critical severity only, owner-
// grouped, cooldown + hourly cap) to bound LLM cost.
type AutoDiagnoseConfig struct {
	Enabled    bool
	Interval   time.Duration // poll cadence
	Cooldown   time.Duration // per-issue + per-resource refire suppression
	MaxPerHour int           // hard cap on auto-investigations launched per rolling hour
}

func (c AutoDiagnoseConfig) withDefaults() AutoDiagnoseConfig {
	if c.Interval <= 0 {
		c.Interval = 60 * time.Second
	}
	if c.Cooldown <= 0 {
		c.Cooldown = 30 * time.Minute
	}
	if c.MaxPerHour <= 0 {
		c.MaxPerHour = 10
	}
	return c
}

// autoDiagnoser polls Radar's critical-issue list and fires OpenSRE
// investigations for new ones. Issues are grouped to the owning workload (one
// investigation per Deployment, not per crashlooping pod); each is suppressed
// for a cooldown window and the whole feature is bounded by a per-hour cap and a
// single in-flight investigation at a time.
type autoDiagnoser struct {
	srv *Server
	cfg AutoDiagnoseConfig

	mu         sync.Mutex
	lastFired  map[string]time.Time // issue ID -> last fire time (cooldown)
	fires      []time.Time          // launch timestamps within the rolling hour (rate cap)
	rateLogged bool                 // throttle the "rate cap reached" log line

	sem    chan struct{} // concurrency limit: one investigation at a time
	cancel context.CancelFunc
}

func newAutoDiagnoser(srv *Server, cfg AutoDiagnoseConfig) *autoDiagnoser {
	return &autoDiagnoser{
		srv:       srv,
		cfg:       cfg.withDefaults(),
		lastFired: make(map[string]time.Time),
		sem:       make(chan struct{}, 1),
	}
}

func (a *autoDiagnoser) start() {
	var ctx context.Context
	ctx, a.cancel = context.WithCancel(context.Background())
	go a.run(ctx)
	log.Printf("[autodiagnose] enabled: interval=%s cooldown=%s maxPerHour=%d",
		a.cfg.Interval, a.cfg.Cooldown, a.cfg.MaxPerHour)
}

func (a *autoDiagnoser) stop() {
	if a.cancel != nil {
		a.cancel()
	}
}

func (a *autoDiagnoser) run(ctx context.Context) {
	ticker := time.NewTicker(a.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.tick(ctx)
		}
	}
}

func (a *autoDiagnoser) tick(ctx context.Context) {
	if !k8s.IsConnected() {
		return
	}
	provider := issues.NewCacheProvider()
	if provider == nil {
		return
	}
	critical, _ := issues.ComposeWithStats(provider, issues.Filters{
		Severities: []issues.Severity{issues.SeverityCritical},
		Grouped:    true, // collapse pod fan-out to one row per owning workload
		Limit:      -1,
	})
	for _, iss := range critical {
		if iss.Name == "" {
			continue
		}
		if !a.shouldFire(iss) {
			continue
		}
		// Claim the single worker slot; if an investigation is already running,
		// stop this tick — remaining issues are re-evaluated next tick.
		select {
		case a.sem <- struct{}{}:
		default:
			return
		}
		a.markFired(iss)
		go func(iss issues.Issue) {
			defer func() { <-a.sem }()
			a.fire(ctx, iss)
		}(iss)
	}
}

// shouldFire applies the rate cap, per-issue cooldown, and a "recently
// diagnosed this resource" check (which also covers manual diagnoses).
func (a *autoDiagnoser) shouldFire(iss issues.Issue) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	a.pruneFires(now)
	if len(a.fires) >= a.cfg.MaxPerHour {
		if !a.rateLogged {
			log.Printf("[autodiagnose] hourly cap (%d) reached; skipping further auto-investigations", a.cfg.MaxPerHour)
			a.rateLogged = true
		}
		return false
	}
	if t, ok := a.lastFired[iss.ID]; ok && now.Sub(t) < a.cfg.Cooldown {
		return false
	}
	if recs := a.srv.diagnoses.list(iss.Kind, iss.Namespace, iss.Name); len(recs) > 0 {
		if now.Sub(recs[0].CreatedAt) < a.cfg.Cooldown {
			return false
		}
	}
	return true
}

func (a *autoDiagnoser) markFired(iss issues.Issue) {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	a.lastFired[iss.ID] = now
	a.fires = append(a.fires, now)
	a.rateLogged = false
}

// pruneFires drops fire timestamps older than one hour. Caller holds a.mu.
func (a *autoDiagnoser) pruneFires(now time.Time) {
	cutoff := now.Add(-time.Hour)
	i := 0
	for i < len(a.fires) && a.fires[i].Before(cutoff) {
		i++
	}
	if i > 0 {
		a.fires = a.fires[i:]
	}
}

func (a *autoDiagnoser) fire(ctx context.Context, iss issues.Issue) {
	reason := iss.Reason
	if reason == "" {
		reason = string(iss.Category)
	}
	log.Printf("[autodiagnose] investigating %s %s/%s (category=%s)", iss.Kind, iss.Namespace, iss.Name, iss.Category)
	rec := a.srv.runHeadlessInvestigation(ctx, iss.Kind, iss.Namespace, iss.Name, reason, iss.Message)
	if rec != nil {
		log.Printf("[autodiagnose] %s/%s -> %s", iss.Namespace, iss.Name, rec.Status)
	}
}
