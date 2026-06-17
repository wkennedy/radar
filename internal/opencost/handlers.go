package opencost

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/skyhook-io/radar/internal/k8s"
	prometheuspkg "github.com/skyhook-io/radar/internal/prometheus"
	pkgopencost "github.com/skyhook-io/radar/pkg/opencost"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// WorkloadCostFor returns the cost breakdown for a single workload by name in a
// namespace, or ok=false when cost data is unavailable (no Prometheus/OpenCost,
// not connected, or no match). Best-effort — callers degrade gracefully.
func WorkloadCostFor(ctx context.Context, namespace, name string) (pkgopencost.WorkloadCost, bool) {
	if namespace == "" || name == "" {
		return pkgopencost.WorkloadCost{}, false
	}
	client := prometheuspkg.GetClient()
	if client == nil {
		return pkgopencost.WorkloadCost{}, false
	}
	if _, _, err := client.EnsureConnected(ctx); err != nil {
		return pkgopencost.WorkloadCost{}, false
	}
	resp := pkgopencost.ComputeWorkloadsFromProm(ctx, client.Prom(), namespace, buildPodOwnerLookup(namespace))
	if resp == nil || !resp.Available {
		return pkgopencost.WorkloadCost{}, false
	}
	for _, w := range resp.Workloads {
		if w.Name == name {
			return w, true
		}
	}
	return pkgopencost.WorkloadCost{}, false
}

// RegisterRoutes registers OpenCost routes on the given router.
func RegisterRoutes(r chi.Router) {
	r.Get("/opencost/summary", handleSummary)
	r.Get("/opencost/workloads", handleWorkloads)
	r.Get("/opencost/trend", handleTrend)
	r.Get("/opencost/nodes", handleNodes)
}

// handleSummary returns namespace-level cost summary from OpenCost Prometheus metrics.
func handleSummary(w http.ResponseWriter, r *http.Request) {
	client := prometheuspkg.GetClient()
	if client == nil {
		writeJSON(w, http.StatusOK, pkgopencost.CostSummary{Available: false, Reason: pkgopencost.ReasonNoPrometheus})
		return
	}
	if _, _, err := client.EnsureConnected(r.Context()); err != nil {
		log.Printf("[opencost] EnsureConnected failed (summary): %v", err)
		writeJSON(w, http.StatusOK, pkgopencost.CostSummary{Available: false, Reason: pkgopencost.ReasonNoPrometheus})
		return
	}
	writeJSON(w, http.StatusOK, pkgopencost.ComputeCostSummaryFromProm(
		r.Context(), client.Prom(), pkgopencost.SummaryOptions{}))
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[opencost] Failed to encode JSON response: %v", err)
	}
}

// handleWorkloads returns workload-level cost breakdown for a namespace.
func handleWorkloads(w http.ResponseWriter, r *http.Request) {
	ns := r.URL.Query().Get("namespace")
	if ns == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "namespace parameter is required"})
		return
	}

	client := prometheuspkg.GetClient()
	if client == nil {
		writeJSON(w, http.StatusOK, pkgopencost.WorkloadCostResponse{Namespace: ns, Reason: pkgopencost.ReasonNoPrometheus})
		return
	}
	if _, _, err := client.EnsureConnected(r.Context()); err != nil {
		log.Printf("[opencost] EnsureConnected failed (workloads): %v", err)
		writeJSON(w, http.StatusOK, pkgopencost.WorkloadCostResponse{Namespace: ns, Reason: pkgopencost.ReasonNoPrometheus})
		return
	}

	writeJSON(w, http.StatusOK, pkgopencost.ComputeWorkloadsFromProm(
		r.Context(), client.Prom(), ns, buildPodOwnerLookup(ns)))
}

// buildPodOwnerLookup snapshots radar's pod informer for `ns` so
// pkg/opencost.ComputeWorkloadsFromProm can resolve pod→workload without
// depending on client-go.
func buildPodOwnerLookup(ns string) pkgopencost.PodOwnerLookup {
	rc := k8s.GetResourceCache()
	if rc == nil || rc.Pods() == nil {
		return nil
	}
	pods, err := rc.Pods().Pods(ns).List(labels.Everything())
	if err != nil || len(pods) == 0 {
		return nil
	}
	owners := make(map[string]pkgopencost.WorkloadOwner, len(pods))
	for _, p := range pods {
		owners[p.Name] = resolvePodOwner(p.OwnerReferences)
	}
	return func(podName string) (pkgopencost.WorkloadOwner, bool) {
		o, ok := owners[podName]
		return o, ok
	}
}

// resolvePodOwner walks owner references to find the top-level workload.
// Pods owned by a ReplicaSet are mapped back to the parent Deployment by
// stripping the RS hash suffix.
func resolvePodOwner(refs []metav1.OwnerReference) pkgopencost.WorkloadOwner {
	if len(refs) == 0 {
		return pkgopencost.WorkloadOwner{Kind: "standalone"}
	}
	owner := refs[0]
	if owner.Kind == "ReplicaSet" {
		if deployName := stripReplicaSetSuffix(owner.Name); deployName != owner.Name {
			return pkgopencost.WorkloadOwner{Name: deployName, Kind: "Deployment"}
		}
	}
	return pkgopencost.WorkloadOwner{Name: owner.Name, Kind: owner.Kind}
}

func stripReplicaSetSuffix(name string) string {
	if idx := strings.LastIndex(name, "-"); idx > 0 {
		return name[:idx]
	}
	return name
}

// handleTrend returns cost trend data over time as a stacked series per namespace.
func handleTrend(w http.ResponseWriter, r *http.Request) {
	client := prometheuspkg.GetClient()
	if client == nil {
		writeJSON(w, http.StatusOK, pkgopencost.CostTrendResponse{Available: false, Reason: pkgopencost.ReasonNoPrometheus})
		return
	}
	if _, _, err := client.EnsureConnected(r.Context()); err != nil {
		log.Printf("[opencost] EnsureConnected failed (trend): %v", err)
		writeJSON(w, http.StatusOK, pkgopencost.CostTrendResponse{Available: false, Reason: pkgopencost.ReasonNoPrometheus})
		return
	}
	writeJSON(w, http.StatusOK, pkgopencost.ComputeCostTrendFromProm(
		r.Context(), client.Prom(), pkgopencost.TrendPromOptions{Range: r.URL.Query().Get("range")}))
}

// handleNodes returns per-node cost breakdown.
func handleNodes(w http.ResponseWriter, r *http.Request) {
	client := prometheuspkg.GetClient()
	if client == nil {
		writeJSON(w, http.StatusOK, pkgopencost.NodeCostResponse{Available: false, Reason: pkgopencost.ReasonNoPrometheus})
		return
	}
	if _, _, err := client.EnsureConnected(r.Context()); err != nil {
		log.Printf("[opencost] EnsureConnected failed (nodes): %v", err)
		writeJSON(w, http.StatusOK, pkgopencost.NodeCostResponse{Available: false, Reason: pkgopencost.ReasonNoPrometheus})
		return
	}
	writeJSON(w, http.StatusOK, pkgopencost.ComputeNodeCosts(r.Context(), client.Prom()))
}
