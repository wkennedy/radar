// "Diagnose with AI" — triggers an OpenSRE investigation for a resource and
// streams the result into a modal. Self-contained in web/ (own portal, no new
// k8s-ui public surface). The button lives in k8s-ui's ResourceActionsBar and
// calls back into useDiagnoseLauncher() here via the actionsBarProps seam.
import { useState, useEffect, useCallback } from 'react'
import { createPortal } from 'react-dom'
import { Sparkles, X, Loader2, AlertTriangle, CheckCircle2 } from 'lucide-react'
import { createDiagnoseStream } from '../../api/client'
import { useCanDiagnoseWithAI } from '../../contexts/CapabilitiesContext'
import { Markdown } from '../ui/Markdown'

export interface DiagnoseTarget {
  kind: string
  namespace: string
  name: string
}

type DiagnoseStatus = 'streaming' | 'done' | 'error'

interface DiagnoseState {
  status: DiagnoseStatus
  steps: string[]
  report: string
  rootCause: string
  error: string | null
}

const INITIAL: DiagnoseState = { status: 'streaming', steps: [], report: '', rootCause: '', error: null }

// Friendly label for one OpenSRE progress frame, or null to skip it.
function stepLabel(frame: any): string | null {
  const kind = String(frame?.event || '')
  const node = String(frame?.name || '').replace(/_/g, ' ')
  switch (kind) {
    case 'on_chain_start':
      return node ? `Running ${node}…` : null
    case 'on_tool_start': {
      const tool = String(frame?.data?.name || frame?.name || '').replace(/_/g, ' ')
      return tool ? `Querying ${tool}…` : null
    }
    default:
      return null
  }
}

// useDiagnoseStream opens an EventSource for the given target and accumulates
// progress + the final report. Re-runs when the target identity changes; a null
// target is inert. We deliberately close on the first connection error instead
// of letting EventSource auto-reconnect — a reconnect would re-POST and start a
// whole new (costly) investigation.
function useDiagnoseStream(target: DiagnoseTarget | null): DiagnoseState {
  const [state, setState] = useState<DiagnoseState>(INITIAL)

  useEffect(() => {
    if (!target) {
      setState(INITIAL)
      return
    }
    setState(INITIAL)
    const es = createDiagnoseStream(target.kind, target.namespace, target.name)
    let done = false
    const finish = (patch: Partial<DiagnoseState>) => {
      done = true
      setState((s) => ({ ...s, ...patch }))
      es.close()
    }

    es.addEventListener('events', (e: MessageEvent) => {
      let frame: any
      try {
        frame = JSON.parse(e.data)
      } catch {
        return
      }
      const output = frame?.data?.output
      if (output && typeof output.report === 'string' && output.report.trim()) {
        setState((s) => ({
          ...s,
          report: output.report,
          rootCause: typeof output.root_cause === 'string' ? output.root_cause : s.rootCause,
        }))
      }
      const label = stepLabel(frame)
      if (label) {
        setState((s) => (s.steps[s.steps.length - 1] === label ? s : { ...s, steps: [...s.steps, label] }))
      }
    })

    const complete = () => {
      if (!done) finish({ status: 'done' })
    }
    es.addEventListener('done', complete)
    es.addEventListener('end', complete)

    es.addEventListener('error', (e: MessageEvent) => {
      // A server-sent `event: error` carries data; a native EventSource
      // connection error does not. Distinguish so we surface OpenSRE's detail
      // when present, and otherwise report a transport failure (no reconnect).
      if (e?.data) {
        let detail = 'Investigation failed.'
        try {
          const parsed = JSON.parse(e.data)
          if (parsed?.detail) detail = String(parsed.detail)
        } catch {
          /* keep default */
        }
        finish({ status: 'error', error: detail })
      } else if (!done) {
        finish({ status: 'error', error: 'Connection to the investigation stream failed.' })
      }
    })

    return () => {
      done = true
      es.close()
    }
  }, [target?.kind, target?.namespace, target?.name])

  return state
}

function DiagnosePanel({ target, onClose }: { target: DiagnoseTarget | null; onClose: () => void }) {
  const state = useDiagnoseStream(target)
  const streaming = state.status === 'streaming'

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !streaming) onClose()
    }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [streaming, onClose])

  if (!target) return null

  return createPortal(
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div
        className="absolute inset-0 bg-black/60 backdrop-blur-sm"
        onClick={streaming ? undefined : onClose}
      />
      <div className="relative bg-theme-surface border border-theme-border rounded-lg shadow-2xl mx-4 w-full max-w-3xl max-h-[90vh] flex flex-col">
        <div className="flex items-center justify-between p-4 border-b border-theme-border shrink-0">
          <div className="flex items-center gap-2">
            <Sparkles className="w-4 h-4 text-theme-text-secondary" />
            <h3 className="text-base font-semibold text-theme-text-primary">
              AI Diagnosis — {target.kind}/{target.name}
            </h3>
          </div>
          <button
            onClick={onClose}
            disabled={streaming}
            className="text-theme-text-secondary hover:text-theme-text-primary disabled:opacity-40"
            title={streaming ? 'Investigation in progress…' : 'Close'}
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="flex-1 min-h-0 overflow-y-auto p-4 space-y-4">
          {state.error && (
            <div className="flex items-start gap-2 text-sm text-red-400">
              <AlertTriangle className="w-4 h-4 mt-0.5 shrink-0" />
              <span>{state.error}</span>
            </div>
          )}

          {state.rootCause && (
            <div className="rounded-md border border-theme-border bg-theme-elevated p-3">
              <div className="text-xs font-medium uppercase tracking-wide text-theme-text-tertiary mb-1">
                Root cause
              </div>
              <div className="text-sm text-theme-text-primary">{state.rootCause}</div>
            </div>
          )}

          {state.report ? (
            <Markdown>{state.report}</Markdown>
          ) : (
            !state.error && (
              <div className="space-y-2">
                <div className="flex items-center gap-2 text-sm text-theme-text-secondary">
                  <Loader2 className="w-4 h-4 animate-spin" />
                  Investigating with OpenSRE…
                </div>
                {state.steps.length > 0 && (
                  <ul className="text-xs text-theme-text-tertiary space-y-1 pl-6 list-disc">
                    {state.steps.slice(-10).map((s, i) => (
                      <li key={`${i}-${s}`}>{s}</li>
                    ))}
                  </ul>
                )}
              </div>
            )
          )}
        </div>

        <div className="flex items-center justify-between gap-2 p-4 border-t border-theme-border shrink-0">
          <div className="text-xs text-theme-text-tertiary">
            {streaming ? (
              <span className="flex items-center gap-1.5">
                <Loader2 className="w-3.5 h-3.5 animate-spin" /> Streaming…
              </span>
            ) : state.status === 'done' ? (
              <span className="flex items-center gap-1.5 text-theme-text-secondary">
                <CheckCircle2 className="w-3.5 h-3.5" /> Investigation complete
              </span>
            ) : null}
          </div>
          <button
            onClick={onClose}
            disabled={streaming}
            className="px-3 py-1.5 text-xs font-medium rounded-lg border border-theme-border text-theme-text-primary hover:bg-theme-hover disabled:opacity-40"
          >
            Close
          </button>
        </div>
      </div>
    </div>,
    document.body,
  )
}

export interface DiagnoseLauncher {
  /** Wire into ResourceActionsBar's `onDiagnose` prop. */
  onDiagnose: (params: DiagnoseTarget) => void
  /** Wire into ResourceActionsBar's `canDiagnoseWithAI` prop. */
  canDiagnoseWithAI: boolean
  /** Wire into ResourceActionsBar's `isDiagnosing` prop. */
  isDiagnosing: boolean
  /** Render this in the component tree (e.g. alongside other dialogs). */
  panel: React.ReactNode
}

// useDiagnoseLauncher owns the panel state. A host renders `panel` and spreads
// onDiagnose/canDiagnoseWithAI/isDiagnosing into the action bar's props.
export function useDiagnoseLauncher(): DiagnoseLauncher {
  const canDiagnoseWithAI = useCanDiagnoseWithAI()
  const [target, setTarget] = useState<DiagnoseTarget | null>(null)
  const onDiagnose = useCallback((params: DiagnoseTarget) => setTarget(params), [])
  const onClose = useCallback(() => setTarget(null), [])

  return {
    onDiagnose,
    canDiagnoseWithAI,
    isDiagnosing: target !== null,
    panel: <DiagnosePanel target={target} onClose={onClose} />,
  }
}
