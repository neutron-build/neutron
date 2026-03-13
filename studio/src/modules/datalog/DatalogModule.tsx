import { useSignal } from '@preact/signals'
import { useEffect } from 'preact/hooks'
import { activeConnection, schema, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { DataGrid } from '../../components/DataGrid'
import type { QueryResult } from '../../lib/types'
import s from './DatalogModule.module.css'

const EXAMPLE_PROGRAMS = [
  `-- Ancestors example
parent(alice, bob).
parent(bob, charlie).
ancestor(X, Y) :- parent(X, Y).
ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z).
?- ancestor(alice, Who).`,
  `-- List all predicates
?- predicate(Name, Arity).`,
  `-- List all rules
?- rule(Head, Body).`,
]

export function DatalogModule() {
  const program = useSignal(EXAMPLE_PROGRAMS[0])
  const result = useSignal<QueryResult | null>(null)
  const running = useSignal(false)
  const stats = useSignal<{ predicates: number; rules: number } | null>(null)

  const conn = activeConnection.value!
  const dl = schema.value?.datalog

  useEffect(() => {
    if (dl) stats.value = { predicates: dl.predicateCount, rules: dl.ruleCount }
  }, [dl])

  async function evaluate() {
    const prog = program.value.trim()
    if (!prog) return
    running.value = true
    result.value = null
    try {
      const escaped = prog.replace(/'/g, "''")
      const r = await api.query(
        `SELECT * FROM datalog_eval('${escaped}')`,
        conn.id
      )
      result.value = r
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      running.value = false
    }
  }

  return (
    <div class={s.layout}>
      <div class={s.header}>
        <span class={s.title}>Datalog</span>
        {stats.value && (
          <div class={s.pills}>
            <span class={s.pill}>{stats.value.predicates} predicates</span>
            <span class={s.pill}>{stats.value.rules} rules</span>
          </div>
        )}
      </div>

      <div class={s.editorSection}>
        <div class={s.editorToolbar}>
          <div class={s.examples}>
            {EXAMPLE_PROGRAMS.map((p, i) => (
              <button key={i} class={s.exampleBtn} onClick={() => { program.value = p }}>
                eg{i + 1}
              </button>
            ))}
          </div>
          <span class={s.hint}>⌘↵ to evaluate</span>
          <button class={s.evalBtn} onClick={evaluate} disabled={running.value}>
            {running.value ? 'Evaluating…' : '▶ Evaluate'}
          </button>
        </div>
        <textarea
          class={s.editor}
          value={program.value}
          onInput={e => { program.value = (e.target as HTMLTextAreaElement).value }}
          spellcheck={false}
          onKeyDown={e => {
            if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { e.preventDefault(); evaluate() }
          }}
        />
      </div>

      <div class={s.results}>
        {result.value ? (
          result.value.error ? (
            <div class={s.error}>{result.value.error}</div>
          ) : (
            <>
              <div class={s.resultMeta}>{result.value.rowCount} tuples · {result.value.duration}ms</div>
              <div class={s.grid}><DataGrid result={result.value} /></div>
            </>
          )
        ) : !running.value && (
          <div class={s.hint2}>
            Write facts and rules, then query with <code>?-</code>
          </div>
        )}
      </div>
    </div>
  )
}
