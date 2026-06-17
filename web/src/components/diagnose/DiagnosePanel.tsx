// "Diagnose with AI" — triggers an OpenSRE investigation for a resource and
// streams the result into a modal, OR re-opens a stored diagnosis (read-only).
// Self-contained in web/ (own portal, no new k8s-ui public surface). The button
// lives in k8s-ui's ResourceActionsBar and calls back into useDiagnoseLauncher()
// here via the actionsBarProps seam; the history list (DiagnosesSection) opens
// stored records through the same launcher.
import { useState, useEffect, useCallback } from 'react'
import { createPortal } from 'react-dom'
import { useQueryClient } from '@tanstack/react-query'
import { Sparkles, X, Loader2, AlertTriangle, CheckCircle2, ChevronRight, ChevronDown, MessageSquare } from 'lucide-react'
import {
  createDiagnoseStream,
  sendDiagnoseChat,
  type DiagnosisRecord,
  type DiagnosisEvidence,
  type DiagnoseChatTurn,
} from '../../api/client'
import { useCanDiagnoseWithAI } from '../../contexts/CapabilitiesContext'
import { Markdown } from '../ui/Markdown'

export interface DiagnoseTarget {
  kind: string
  namespace: string
  name: string
}

type DiagnoseStatus = 'streaming' | 'done' | 'noise' | 'error'

interface DiagnoseState {
  status: DiagnoseStatus
  steps: string[]
  report: string
  rootCause: string
  validityScore?: number
  isNoise?: boolean
  evidence: DiagnosisEvidence[]
  error: string | null
}

const INITIAL: DiagnoseState = {
  status: 'streaming',
  steps: [],
  report: '',
  rootCause: '',
  evidence: [],
  error: null,
}

function firstStr(o: Record<string, unknown>, ...keys: string[]): string | undefined {
  for (const k of keys) {
    const v = o[k]
    if (typeof v === 'string' && v.trim()) return v
  }
  return undefined
}

// Mirror of the backend toEvidence(): a concise, best-effort trail from
// OpenSRE's evidence_entries (whose exact shape varies).
function mapEvidence(items: unknown): DiagnosisEvidence[] {
  if (!Array.isArray(items)) return []
  const out: DiagnosisEvidence[] = []
  for (const it of items) {
    if (!it || typeof it !== 'object') continue
    const m = it as Record<string, unknown>
    const tool = firstStr(m, 'tool', 'tool_name', 'name')
    const source = firstStr(m, 'source', 'integration')
    const summary = firstStr(m, 'summary', 'title', 'description', 'evidence_type')
    if (!tool && !source && !summary) continue
    out.push({ tool, source, summary })
    if (out.length >= 50) break
  }
  return out
}

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
// progress + the final structured result. A null target is inert. We close on
// the first connection error rather than letting EventSource auto-reconnect — a
// reconnect would re-POST and start a whole new (costly) investigation.
function useDiagnoseStream(target: DiagnoseTarget | null): DiagnoseState {
  const [state, setState] = useState<DiagnoseState>(INITIAL)
  const queryClient = useQueryClient()

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
      // Refresh the resource's history so the just-finished run appears.
      queryClient.invalidateQueries({ queryKey: ['diagnoses', target.kind, target.namespace, target.name] })
    }

    es.addEventListener('events', (e: MessageEvent) => {
      let frame: any
      try {
        frame = JSON.parse(e.data)
      } catch {
        return
      }
      const output = frame?.data?.output
      if (output && typeof output === 'object') {
        setState((s) => ({
          ...s,
          report: typeof output.report === 'string' && output.report.trim() ? output.report : s.report,
          rootCause: typeof output.root_cause === 'string' && output.root_cause ? output.root_cause : s.rootCause,
          validityScore: typeof output.validity_score === 'number' ? output.validity_score : s.validityScore,
          isNoise: typeof output.is_noise === 'boolean' ? output.is_noise : s.isNoise,
          evidence: Array.isArray(output.evidence_entries) ? mapEvidence(output.evidence_entries) : s.evidence,
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
      // connection error does not. Surface OpenSRE's detail when present,
      // otherwise report a transport failure (no reconnect).
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
  }, [target?.kind, target?.namespace, target?.name, queryClient])

  return state
}

// A normalized view model so live streams and stored records render identically.
interface DiagnoseVM {
  titleKind: string
  titleName: string
  streaming: boolean
  status: string
  error: string | null
  rootCause: string
  report: string
  steps: string[]
  validityScore?: number
  isNoise?: boolean
  evidence: DiagnosisEvidence[]
}

function vmFromLive(state: DiagnoseState, target: DiagnoseTarget): DiagnoseVM {
  return {
    titleKind: target.kind,
    titleName: target.name,
    streaming: state.status === 'streaming',
    status: state.status,
    error: state.error,
    rootCause: state.rootCause,
    report: state.report,
    steps: state.steps,
    validityScore: state.validityScore,
    isNoise: state.isNoise,
    evidence: state.evidence,
  }
}

function vmFromRecord(rec: DiagnosisRecord): DiagnoseVM {
  return {
    titleKind: rec.kind,
    titleName: rec.name,
    streaming: false,
    status: rec.status,
    error: rec.error ?? null,
    rootCause: rec.rootCause ?? '',
    report: rec.report ?? '',
    steps: [],
    validityScore: rec.validityScore,
    isNoise: rec.isNoise,
    evidence: rec.evidence ?? [],
  }
}

function ConfidenceBadge({ score }: { score: number }) {
  const pct = score <= 1 ? Math.round(score * 100) : Math.round(score)
  const tone = pct >= 67 ? 'text-green-400' : pct >= 34 ? 'text-amber-400' : 'text-red-400'
  return (
    <span className={`text-xs font-medium ${tone}`} title="OpenSRE confidence in this diagnosis">
      {pct}% confidence
    </span>
  )
}

function EvidenceTrail({ evidence }: { evidence: DiagnosisEvidence[] }) {
  const [open, setOpen] = useState(false)
  if (evidence.length === 0) return null
  return (
    <div className="rounded-md border border-theme-border">
      <button
        onClick={() => setOpen((v) => !v)}
        className="flex w-full items-center gap-1.5 p-2 text-xs font-medium text-theme-text-secondary hover:bg-theme-hover"
      >
        {open ? <ChevronDown className="w-3.5 h-3.5" /> : <ChevronRight className="w-3.5 h-3.5" />}
        How it investigated ({evidence.length} step{evidence.length === 1 ? '' : 's'})
      </button>
      {open && (
        <ul className="border-t border-theme-border p-2 space-y-1">
          {evidence.map((e, i) => (
            <li key={i} className="text-xs text-theme-text-tertiary flex gap-2">
              {e.source && (
                <span className="shrink-0 rounded bg-theme-elevated px-1.5 py-0.5 text-theme-text-secondary">
                  {e.source}
                </span>
              )}
              <span className="min-w-0">
                {e.tool && <span className="text-theme-text-secondary">{e.tool}</span>}
                {e.tool && e.summary ? ' — ' : ''}
                {e.summary}
              </span>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

// DiagnoseChat is a stateless follow-up thread for a stored diagnosis: the
// browser holds the turns and replays them; the server grounds answers in the
// record's RCA. Only rendered in record mode (we need a persisted id).
function DiagnoseChat({ id }: { id: string }) {
  const [turns, setTurns] = useState<DiagnoseChatTurn[]>([])
  const [input, setInput] = useState('')
  const [sending, setSending] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const send = useCallback(async () => {
    const q = input.trim()
    if (!q || sending) return
    setError(null)
    setInput('')
    const prior = turns
    setTurns((t) => [...t, { role: 'user', content: q }])
    setSending(true)
    try {
      const reply = await sendDiagnoseChat(id, q, prior)
      setTurns((t) => [...t, { role: 'assistant', content: reply }])
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Follow-up failed.')
    } finally {
      setSending(false)
    }
  }, [id, input, sending, turns])

  return (
    <div className="rounded-md border border-theme-border">
      <div className="flex items-center gap-1.5 border-b border-theme-border p-2 text-xs font-medium text-theme-text-secondary">
        <MessageSquare className="w-3.5 h-3.5" /> Ask a follow-up
      </div>
      <div className="space-y-3 p-2">
        {turns.map((t, i) => (
          <div key={i}>
            <div className="text-[10px] uppercase tracking-wide text-theme-text-tertiary">
              {t.role === 'user' ? 'You' : 'OpenSRE'}
            </div>
            {t.role === 'assistant' ? (
              <Markdown>{t.content}</Markdown>
            ) : (
              <div className="text-sm text-theme-text-primary">{t.content}</div>
            )}
          </div>
        ))}
        {sending && (
          <div className="flex items-center gap-2 text-xs text-theme-text-tertiary">
            <Loader2 className="w-3.5 h-3.5 animate-spin" /> Thinking…
          </div>
        )}
        {error && <div className="text-xs text-red-400">{error}</div>}
        <div className="flex gap-2">
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') void send()
            }}
            placeholder="e.g. why did you rule out the database?"
            disabled={sending}
            className="flex-1 rounded-md border border-theme-border bg-theme-base px-2 py-1.5 text-sm text-theme-text-primary placeholder:text-theme-text-tertiary disabled:opacity-50"
          />
          <button
            onClick={() => void send()}
            disabled={sending || !input.trim()}
            className="rounded-md border border-theme-border px-3 py-1.5 text-xs font-medium text-theme-text-primary hover:bg-theme-hover disabled:opacity-40"
          >
            Send
          </button>
        </div>
      </div>
    </div>
  )
}

function DiagnosePanel({
  target,
  record,
  onClose,
}: {
  target: DiagnoseTarget | null
  record: DiagnosisRecord | null
  onClose: () => void
}) {
  const live = useDiagnoseStream(target)
  const vm: DiagnoseVM | null = record ? vmFromRecord(record) : target ? vmFromLive(live, target) : null
  const streaming = vm?.streaming ?? false

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !streaming) onClose()
    }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [streaming, onClose])

  if (!vm) return null

  return createPortal(
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" onClick={streaming ? undefined : onClose} />
      <div className="relative bg-theme-surface border border-theme-border rounded-lg shadow-2xl mx-4 w-full max-w-3xl max-h-[90vh] flex flex-col">
        <div className="flex items-center justify-between p-4 border-b border-theme-border shrink-0">
          <div className="flex items-center gap-2 min-w-0">
            <Sparkles className="w-4 h-4 text-theme-text-secondary shrink-0" />
            <h3 className="text-base font-semibold text-theme-text-primary truncate">
              AI Diagnosis — {vm.titleKind}/{vm.titleName}
            </h3>
            {vm.isNoise && (
              <span className="shrink-0 rounded bg-amber-500/15 px-1.5 py-0.5 text-xs font-medium text-amber-400">
                Likely noise
              </span>
            )}
            {typeof vm.validityScore === 'number' && <ConfidenceBadge score={vm.validityScore} />}
          </div>
          <button
            onClick={onClose}
            disabled={streaming}
            className="text-theme-text-secondary hover:text-theme-text-primary disabled:opacity-40 shrink-0"
            title={streaming ? 'Investigation in progress…' : 'Close'}
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="flex-1 min-h-0 overflow-y-auto p-4 space-y-4">
          {vm.error && (
            <div className="flex items-start gap-2 text-sm text-red-400">
              <AlertTriangle className="w-4 h-4 mt-0.5 shrink-0" />
              <span>{vm.error}</span>
            </div>
          )}

          {vm.rootCause && (
            <div className="rounded-md border border-theme-border bg-theme-elevated p-3">
              <div className="text-xs font-medium uppercase tracking-wide text-theme-text-tertiary mb-1">
                Root cause
              </div>
              <div className="text-sm text-theme-text-primary">{vm.rootCause}</div>
            </div>
          )}

          {vm.report ? (
            <Markdown>{vm.report}</Markdown>
          ) : (
            !vm.error && (
              <div className="space-y-2">
                <div className="flex items-center gap-2 text-sm text-theme-text-secondary">
                  <Loader2 className="w-4 h-4 animate-spin" />
                  Investigating with OpenSRE…
                </div>
                {vm.steps.length > 0 && (
                  <ul className="text-xs text-theme-text-tertiary space-y-1 pl-6 list-disc">
                    {vm.steps.slice(-10).map((s, i) => (
                      <li key={`${i}-${s}`}>{s}</li>
                    ))}
                  </ul>
                )}
              </div>
            )
          )}

          {vm.evidence.length > 0 && <EvidenceTrail evidence={vm.evidence} />}

          {record && record.status !== 'error' && record.report && <DiagnoseChat id={record.id} />}
        </div>

        <div className="flex items-center justify-between gap-2 p-4 border-t border-theme-border shrink-0">
          <div className="text-xs text-theme-text-tertiary">
            {streaming ? (
              <span className="flex items-center gap-1.5">
                <Loader2 className="w-3.5 h-3.5 animate-spin" /> Streaming…
              </span>
            ) : vm.status === 'error' ? (
              <span className="flex items-center gap-1.5 text-red-400">
                <AlertTriangle className="w-3.5 h-3.5" /> Failed
              </span>
            ) : (
              <span className="flex items-center gap-1.5 text-theme-text-secondary">
                <CheckCircle2 className="w-3.5 h-3.5" /> {record ? 'Saved diagnosis' : 'Investigation complete'}
              </span>
            )}
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
  /** Wire into ResourceActionsBar's `onDiagnose` prop (starts a live investigation). */
  onDiagnose: (params: DiagnoseTarget) => void
  /** Open a stored diagnosis read-only (from the history list). */
  openRecord: (record: DiagnosisRecord) => void
  /** Wire into ResourceActionsBar's `canDiagnoseWithAI` prop. */
  canDiagnoseWithAI: boolean
  /** Wire into ResourceActionsBar's `isDiagnosing` prop. */
  isDiagnosing: boolean
  /** Render this in the component tree (e.g. alongside other dialogs). */
  panel: React.ReactNode
}

// useDiagnoseLauncher owns the panel state. A host renders `panel`, spreads
// onDiagnose/canDiagnoseWithAI/isDiagnosing into the action bar, and passes
// openRecord to the history list.
export function useDiagnoseLauncher(): DiagnoseLauncher {
  const canDiagnoseWithAI = useCanDiagnoseWithAI()
  const [target, setTarget] = useState<DiagnoseTarget | null>(null)
  const [record, setRecord] = useState<DiagnosisRecord | null>(null)
  const onDiagnose = useCallback((params: DiagnoseTarget) => {
    setRecord(null)
    setTarget(params)
  }, [])
  const openRecord = useCallback((rec: DiagnosisRecord) => {
    setTarget(null)
    setRecord(rec)
  }, [])
  const onClose = useCallback(() => {
    setTarget(null)
    setRecord(null)
  }, [])

  return {
    onDiagnose,
    openRecord,
    canDiagnoseWithAI,
    isDiagnosing: target !== null,
    panel: <DiagnosePanel target={target} record={record} onClose={onClose} />,
  }
}
