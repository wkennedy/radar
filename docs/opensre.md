# Diagnose with AI (OpenSRE integration)

Radar can hand a misbehaving workload — or a whole namespace or cluster — to
[OpenSRE](https://github.com/skyhook-io/opensre), an AI root-cause analysis
engine, and stream the investigation back into the UI as a report. OpenSRE in
turn reads live cluster context **through Radar's own MCP read tools**, so the
two form a loop: Radar sees the cluster, OpenSRE reasons about it.

The integration is **off by default** and **read-only** unless you explicitly
opt into remediation. Point Radar at an OpenSRE instance and a "Diagnose with AI"
action appears on workloads and on the dashboard.

## Quick reference

| Flag (env)                                                   | What it does                                                                   |
|--------------------------------------------------------------|--------------------------------------------------------------------------------|
| `--opensre-url` (`RADAR_OPENSRE_URL`)                        | OpenSRE service URL, e.g. `http://opensre:8080`. Empty = integration disabled. |
| `--opensre-token` (`RADAR_OPENSRE_TOKEN`)                    | OpenSRE API key, sent as `X-API-Key`.                                          |
| `--opensre-autodiagnose` (`RADAR_OPENSRE_AUTODIAGNOSE=true`) | Auto-investigate new **critical** cluster issues.                              |
| `--opensre-autodiagnose-interval`                            | Poll cadence (default `60s`).                                                  |
| `--opensre-autodiagnose-cooldown`                            | Don't re-diagnose the same issue/resource within this window (default `30m`).  |
| `--opensre-autodiagnose-max-per-hour`                        | Hard cap on auto-investigations per rolling hour (default `10`).               |
| `--opensre-remediation` (`RADAR_OPENSRE_REMEDIATION=true`)   | Offer **restart/scale** fixes on diagnoses, applied with confirmation.         |
| `--notify-webhook` (`RADAR_NOTIFY_WEBHOOK`)                  | POST a summary to a Slack-compatible webhook when a diagnosis completes.       |
| `--radar-base-url` (`RADAR_BASE_URL`)                        | External base URL used for deep links in notifications.                        |

## Setup

### 1. Run OpenSRE with its remote server reachable

OpenSRE serves the investigation API (`POST /investigate/stream`) plus the
follow-up endpoints (`/chat`, `/remediation`, `/feedback`) on its remote server.
Set an API key (`OPENSRE_API_KEY`) and point it at your LLM provider (a local
LM Studio / Ollama endpoint works well for sensitive clusters). See OpenSRE's
own `docs/radar.mdx` for the reverse direction (giving OpenSRE Radar context).

### 2. Point Radar at it

```bash
kubectl-radar \
  --opensre-url   http://opensre:8080 \
  --opensre-token "$OPENSRE_API_KEY"
```

`/api/capabilities` now reports `openSREEnabled: true`, and the UI shows the
diagnose actions. Nothing else is needed for manual, on-demand diagnosis.

## Using it

**Diagnose a workload.** Open a Deployment / StatefulSet / DaemonSet and click
**Diagnose with AI**. A panel streams OpenSRE's progress, then renders the
root-cause report (markdown), a validity score, and the evidence trail. OpenSRE
pulls pod logs, events, topology, and audit findings live from Radar as it works.

**Diagnose a namespace or the whole cluster.** The dashboard has a **Diagnose
cluster with AI** button; namespace scope seeds the investigation from Radar's
current issues for that scope. Useful for "something's wrong, I don't know where."

**Ask follow-up questions.** Each completed diagnosis has a chat box. Questions
are answered by the same LLM, grounded in that report (`POST /chat`).

**Rate it.** 👍 / 👎 on a diagnosis is stored on the record and forwarded to
OpenSRE, which appends it to an evaluation dataset.

**Review past diagnoses.** Completed investigations are listed per resource and
served from `GET /api/diagnoses`. (v1 store is in-memory; it resets on restart.)

## Proactive auto-diagnosis (optional)

```bash
kubectl-radar --opensre-url http://opensre:8080 --opensre-token "$KEY" \
  --opensre-autodiagnose
```

Radar polls for new **critical** issues and fires one owner-grouped investigation
each, with a cooldown, an hourly cap, and single-flight de-duplication so a flare
of related events doesn't stampede OpenSRE. Auto-diagnoses are tagged
`trigger: auto` in the history. Conservative by design — tune the cooldown / cap
to your tolerance.

## Notifications (optional)

```bash
kubectl-radar ... --notify-webhook https://hooks.slack.com/services/... \
  --radar-base-url https://radar.example.com
```

When a diagnosis completes (manual or auto), Radar POSTs a summary with a deep
link back to the resource. The payload is Slack-incoming-webhook compatible.

## Remediation (optional, off by default)

```bash
kubectl-radar ... --opensre-remediation
```

Completed diagnoses then show a **Suggested fixes** section with OpenSRE-proposed
**restart** / **scale** actions. Each is applied only on explicit confirmation,
through Radar's existing workload-mutation endpoints — which run under the
caller's Kubernetes RBAC. Without the flag, the remediation endpoint returns
`404` and no fixes are offered. Rollback / patch / apply / delete are not
included. See the [security model](#security) before enabling.

## Security

- **Read-only by default.** Only `--opensre-remediation` adds (constrained)
  writes; everything else is read + reason.
- **Secrets are never exposed.** Radar's MCP layer never emits Secret
  `.data`/`.stringData` and redacts credential-shaped env values; OpenSRE masks
  tool output again before prompting the LLM. Verified — full model, data-flow,
  and the least-privilege RBAC the OpenSRE identity needs are in the integration
  repo's `SECURITY.md` and `deploy/opensre-readonly-rbac.yaml`.
- **Keep Radar network-isolated.** The reverse direction (OpenSRE → Radar) uses
  trusted proxy headers; expose Radar only inside the cluster.
- **Mind the LLM provider.** Redacted cluster metadata and log/event text leave
  for whatever model OpenSRE is configured against. Use a self-hosted model for
  sensitive clusters.
