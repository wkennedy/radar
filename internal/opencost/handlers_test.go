package opencost

import (
	"context"
	"testing"
)

func TestWorkloadCostFor_DegradesGracefully(t *testing.T) {
	// No Prometheus client configured in this test binary → cost is unavailable.
	if _, ok := WorkloadCostFor(context.Background(), "payments", "api"); ok {
		t.Error("expected ok=false when no Prometheus/OpenCost client is configured")
	}
	// Empty identity is always unavailable.
	if _, ok := WorkloadCostFor(context.Background(), "", ""); ok {
		t.Error("expected ok=false for empty namespace/name")
	}
}
