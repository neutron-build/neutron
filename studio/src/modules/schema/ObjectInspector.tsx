import { useEffect } from 'preact/hooks'
import { useSignal } from '@preact/signals'
import { activeConnection, openTab, toast } from '../../lib/store'
import { api, ApiError } from '../../lib/api'
import type { SchemaFKEdge, SchemaObjectDetail } from '../../lib/types'
import s from './ObjectInspector.module.css'

/** Structure inspector (S05): one relation's catalog metadata from the
 * shared v2 introspection — columns, constraints, indexes and FK
 * relationships both ways, or a view's definition. Every "Open" action
 * navigates; nothing here mutates. */
export function ObjectInspector({ schema, table }: { schema: string; table: string }) {
  const conn = activeConnection.value
  const detail = useSignal<SchemaObjectDetail | null>(null)
  const error = useSignal<string | null>(null)
  const loading = useSignal(false)

  useEffect(() => {
    if (!conn) return
    loading.value = true
    error.value = null
    detail.value = null
    api.schemaObject(conn.id, schema, table)
      .then(d => { detail.value = d })
      .catch(e => {
        error.value = e instanceof ApiError && e.status === 404
          ? `${schema}.${table} is gone from the live catalog (dropped, renamed, or not visible to this role). Refresh the schema tree.`
          : e instanceof Error ? e.message : String(e)
      })
      .finally(() => { loading.value = false })
  }, [conn?.id, schema, table])

  if (!conn) return <div class={s.hint}>Connect to a database first</div>
  if (loading.value) return <div class={s.hint}>Loading {schema}.{table}…</div>
  if (error.value) {
    return <div class={s.error} role="alert">{error.value}</div>
  }
  const d = detail.value
  if (!d) return null

  return (
    <div class={s.wrap}>
      <header class={s.header}>
        <h2 class={s.title}>{schema !== 'public' ? `${schema}.` : ''}{table}</h2>
        <span class={s.kind} data-kind={d.kind}>{d.kind}</span>
        <code class={s.sha} title="canonical document SHA-256 — the CLI planner's identity for this catalog">doc {d.documentSHA256.slice(0, 12)}</code>
        <div class={s.actions}>
          <button class={s.action} onClick={() => openTab({
            id: crypto.randomUUID(), kind: 'sql-browser', label: table, objectSchema: schema, objectName: table,
          })}>Browse {d.kind === 'view' ? 'view' : 'rows'}</button>
          {d.kind === 'table' && (
            <>
              <button class={s.action} onClick={() => openTab({
                id: crypto.randomUUID(), kind: 'schema-designer', label: table, objectSchema: schema, objectName: table,
              })}>Design</button>
              <button class={s.action} onClick={() => openTab({
                id: crypto.randomUUID(), kind: 'diagnostics', label: table, objectSchema: schema, objectName: table,
              })}>Diagnose</button>
            </>
          )}
          {d.kind === 'table' && (
            <button class={s.action} onClick={() => openTab({
              id: crypto.randomUUID(), kind: 'journey', label: `Journey: ${table}`, objectSchema: schema, objectName: table,
            })}>Journey</button>
          )}
          <button class={s.action} onClick={() => {
            void navigator.clipboard?.writeText(`${schema}.${table}`)
              .then(() => toast('success', 'Copied'))
          }}>Copy name</button>
        </div>
      </header>

      {d.kind === 'opaque' && d.opaque && (
        <div class={s.opaque} role="note">
          <strong>{d.opaque.opaqueKind}</strong>: {d.opaque.reason}
          {d.opaque.owner && <> (owner: {d.opaque.owner})</>} — the object is inventoried, never planned.
        </div>
      )}

      {d.view && (
        <section class={s.section} aria-label="View definition">
          <h3>Definition</h3>
          <pre class={s.code}>{d.view.definition}</pre>
          {d.view.checkOption && <p class={s.meta}>check option: {d.view.checkOption}</p>}
          {d.view.securityInvoker !== undefined && <p class={s.meta}>security_invoker: {String(d.view.securityInvoker)}</p>}
        </section>
      )}

      {d.table && (
        <>
          <section class={s.section} aria-label="Columns">
            <h3>Columns ({d.table.columns.length})</h3>
            <table class={s.grid}>
              <thead><tr><th>Name</th><th>Type</th><th>Null</th><th>Default</th></tr></thead>
              <tbody>
                {d.table.columns.map(c => (
                  <tr key={c.name}>
                    <td>{c.isPrimaryKey && <span class={s.pk} title="primary key">PK </span>}{c.name}</td>
                    <td><code>{c.type}</code></td>
                    <td>{c.notNull ? 'not null' : 'nullable'}</td>
                    <td>{c.generated
                      ? <code>generated: {c.generated.expression}</code>
                      : c.default
                        ? <code>{c.default.kind}{c.default.sql ? ` ${c.default.sql}` : ''}</code>
                        : ''}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>

          <section class={s.section} aria-label="Constraints">
            <h3>Constraints ({d.table.constraints.length})</h3>
            {d.table.constraints.length === 0 && <p class={s.meta}>None.</p>}
            {d.table.constraints.map(c => (
              <div key={c.name} class={s.row}>
                <span class={s.rowName}>{c.name}</span>
                <span class={s.rowTag} data-type={c.type}>{c.type}</span>
                <span class={s.rowDetail}>
                  {c.columns && c.columns.length > 0 && `(${c.columns.join(', ')})`}
                  {c.expression && <code> {c.expression}</code>}
                  {c.references && ` → ${c.references.table}(${c.references.columns.join(', ')})`}
                  {c.deferrable && ' · deferrable'}
                </span>
              </div>
            ))}
          </section>

          <section class={s.section} aria-label="Indexes">
            <h3>Indexes ({d.table.indexes.length})</h3>
            {d.table.indexes.length === 0 && <p class={s.meta}>None.</p>}
            {d.table.indexes.map(ix => {
              const key = ix.key.map(k => k.expression ?? k.column ?? '?').join(', ')
              return (
                <div key={ix.name} class={s.row}>
                  <span class={s.rowName}>{ix.name}</span>
                  {ix.unique && <span class={s.unique}>UNIQUE</span>}
                  <span class={s.rowTag}>{ix.method}</span>
                  <span class={s.rowDetail}>({key}){ix.include && ix.include.length > 0 && ` include (${ix.include.join(', ')})`}{ix.where && <code> where {ix.where}</code>}</span>
                </div>
              )
            })}
          </section>

          <section class={s.section} aria-label="Relationships">
            <h3>Relationships</h3>
            <h4>References ({d.table.references.length})</h4>
            {d.table.references.length === 0 && <p class={s.meta}>No outgoing foreign keys.</p>}
            {d.table.references.map(r => (
              <RelationshipRow key={r.constraint} edge={r} direction="out" />
            ))}
            <h4>Referenced by ({d.table.referencedBy.length})</h4>
            {d.table.referencedBy.length === 0 && <p class={s.meta}>No incoming foreign keys.</p>}
            {d.table.referencedBy.map(r => (
              <RelationshipRow key={`${r.schema}.${r.name}.${r.constraint}`} edge={r} direction="in" />
            ))}
          </section>
        </>
      )}
    </div>
  )
}

/** One FK relationship. Outgoing: (columns) → other(refColumns); incoming:
 * other(columns) → (refColumns). The other table is addressed by its schema
 * and name as separate fields, so dotted or mixed-case names navigate. */
function RelationshipRow({ edge, direction }: { edge: SchemaFKEdge; direction: 'out' | 'in' }) {
  const other = edge.schema === 'public' ? edge.name : `${edge.schema}.${edge.name}`
  const label = direction === 'out'
    ? `${edge.constraint}: (${edge.columns.join(', ')}) → ${other}(${edge.refColumns.join(', ')})`
    : `${edge.constraint}: ${other}(${edge.columns.join(', ')}) → (${edge.refColumns.join(', ')})`
  return (
    <div class={s.row}>
      <span class={s.rowName}>{label}</span>
      <button
        class={s.link}
        aria-label={`Inspect ${other}`}
        onClick={() => openTab({
          id: crypto.randomUUID(), kind: 'schema-inspector', label: edge.name, objectSchema: edge.schema, objectName: edge.name,
        })}
      >→ {other}</button>
    </div>
  )
}
