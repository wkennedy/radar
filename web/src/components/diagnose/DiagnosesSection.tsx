// "Past diagnoses" — a per-resource history of OpenSRE investigations, rendered
// in the workload overview. Clicking a row re-opens it read-only via the shared
// diagnose launcher. Hidden when OpenSRE isn't configured or there's no history.
import { Sparkles, CheckCircle2, AlertTriangle, Loader2, MinusCircle } from 'lucide-react'
import { usePastDiagnoses, type DiagnosisRecord } from '../../api/client'
import { useCanDiagnoseWithAI } from '../../contexts/CapabilitiesContext'

function StatusIcon({ status }: { status: string }) {
  switch (status) {
    case 'error':
      return <AlertTriangle className="w-3.5 h-3.5 text-red-400 shrink-0" />
    case 'noise':
      return <MinusCircle className="w-3.5 h-3.5 text-amber-400 shrink-0" />
    case 'cancelled':
      return <MinusCircle className="w-3.5 h-3.5 text-theme-text-tertiary shrink-0" />
    default:
      return <CheckCircle2 className="w-3.5 h-3.5 text-green-400 shrink-0" />
  }
}

function summaryLine(rec: DiagnosisRecord): string {
  if (rec.rootCause) return rec.rootCause
  if (rec.error) return rec.error
  if (rec.status === 'cancelled') return 'Cancelled before completion'
  return 'No root cause identified'
}

export function DiagnosesSection({
  kind,
  namespace,
  name,
  onOpen,
}: {
  kind: string
  namespace: string
  name: string
  onOpen: (record: DiagnosisRecord) => void
}) {
  const enabled = useCanDiagnoseWithAI()
  const { data, isLoading } = usePastDiagnoses(kind, namespace, name, enabled)

  if (!enabled) return null
  const records = data ?? []
  if (!isLoading && records.length === 0) return null

  return (
    <section className="rounded-lg border border-theme-border bg-theme-surface p-4">
      <div className="flex items-center gap-2 mb-3">
        <Sparkles className="w-4 h-4 text-theme-text-secondary" />
        <h3 className="text-sm font-semibold text-theme-text-primary">AI Diagnoses</h3>
        {records.length > 0 && (
          <span className="text-xs text-theme-text-tertiary">({records.length})</span>
        )}
      </div>

      {isLoading ? (
        <div className="flex items-center gap-2 text-xs text-theme-text-tertiary">
          <Loader2 className="w-3.5 h-3.5 animate-spin" /> Loading…
        </div>
      ) : (
        <ul className="space-y-1">
          {records.map((rec) => (
            <li key={rec.id}>
              <button
                onClick={() => onOpen(rec)}
                className="flex w-full items-start gap-2 rounded-md p-2 text-left hover:bg-theme-hover"
              >
                <StatusIcon status={rec.status} />
                <span className="min-w-0 flex-1">
                  <span className="block truncate text-sm text-theme-text-primary">{summaryLine(rec)}</span>
                  <span className="flex items-center gap-1.5 text-xs text-theme-text-tertiary">
                    {new Date(rec.createdAt).toLocaleString()}
                    {rec.trigger === 'auto' && (
                      <span className="rounded bg-theme-elevated px-1 py-0.5 text-[10px] uppercase tracking-wide text-theme-text-secondary">
                        auto
                      </span>
                    )}
                  </span>
                </span>
              </button>
            </li>
          ))}
        </ul>
      )}
    </section>
  )
}
