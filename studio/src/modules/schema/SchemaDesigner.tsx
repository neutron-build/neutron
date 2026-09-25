import { useEffect } from 'preact/hooks'
import { useSignal, useComputed } from '@preact/signals'
import { schema, activeConnection, toast, refreshSchema } from '../../lib/store'
import { api, ApiError } from '../../lib/api'
import { PlanReview } from './PlanReview'
import type { SchemaChange, SchemaObjectDetail, SchemaPlanResponse } from '../../lib/types'
import s from './SchemaDesigner.module.css'

type Lang = 'go' | 'ts' | 'rust' | 'python'

/** One designer column. Loaded from the shared schema document v2 (GET
 * /api/schema/object), so types use the planner's own spelling and defaults
 * carry their contract kind. Identity/sequence defaults and generated
 * columns are database-managed and locked in the designer. */
interface EditCol {
  name: string
  dataType: string
  isNullable: boolean
  isPrimaryKey: boolean
  /** Literal/expression default SQL text, or null. */
  default: string | null
  /** Contract default kind of the loaded column (absent: no default). */
  defaultKind?: string
  /** Generation expression of a stored generated column. */
  generated?: string
  isNew: boolean
  isDeleted: boolean
  originalName: string
}

interface DesignIndex {
  name: string
  key: string
  unique: boolean
}

/** Column types the plan endpoint can represent (schema contract v2's
 * vocabulary, in the planner's DDL spelling). Anything else goes through
 * "custom…" and is refused with a precise message at plan time. */
const COMMON_TYPES = [
  'text', 'varchar(255)',
  'integer', 'bigint', 'smallint',
  'real', 'double precision', 'numeric(12,4)',
  'boolean',
  'timestamptz', 'timestamp', 'date',
  'uuid', 'jsonb', 'json', 'bytea',
]

function colsFromDetail(d: SchemaObjectDetail): EditCol[] {
  return (d.table?.columns ?? []).map(c => {
    const editable = c.default && (c.default.kind === 'literal' || c.default.kind === 'expression')
    return {
      name: c.name,
      dataType: c.type,
      isNullable: !c.notNull,
      isPrimaryKey: c.isPrimaryKey,
      default: editable ? c.default?.sql ?? null : null,
      defaultKind: c.default?.kind,
      generated: c.generated?.expression,
      isNew: false,
      isDeleted: false,
      originalName: c.name,
    }
  })
}

function indexesFromDetail(d: SchemaObjectDetail): DesignIndex[] {
  return (d.table?.indexes ?? []).map(ix => ({
    name: ix.name,
    key: ix.key.map(k => k.expression ?? k.column ?? '?').join(', '),
    unique: ix.unique,
  }))
}

/** Human message for a refused apply, by the server's machine state. */
function applyRefusal(err: ApiError): string {
  switch (err.state) {
    case 'stale-plan':
      return 'The live catalog changed since you reviewed this plan, so it now plans different statements. Nothing was applied — review the fresh plan below.'
    case 'migration-managed':
    case 'locked':
    case 'destructive-unacknowledged':
    case 'outcome-unknown':
    case 'sql-error':
      return err.message
    default:
      return err.message
  }
}

export function SchemaDesigner({ initialSchema, initialTable }: { initialSchema?: string; initialTable?: string }) {
  const conn = activeConnection.value
  const sc = schema.value

  const selectedTable = useSignal<string | null>(initialTable ?? null)
  const selectedSchema = useSignal(initialSchema ?? 'public')
  const cols = useSignal<EditCol[]>([])
  const originalCols = useSignal<EditCol[]>([])
  const indexes = useSignal<DesignIndex[]>([])
  const colsLoading = useSignal(false)
  const loadError = useSignal<string | null>(null)
  const reloadTick = useSignal(0)

  // The plan flow (S05): visual changes become a REVIEWABLE plan produced by
  // the CLI's own planner (POST /api/schema/plan). Apply sends the reviewed
  // planId; the server re-plans under the migration lock and runs exactly
  // those statements or refuses (stale plan, migration-managed database,
  // unacknowledged data loss). The designer never builds DDL.
  const pendingPlan = useSignal<SchemaPlanResponse | null>(null)
  const planError = useSignal<string | null>(null)
  const applyError = useSignal<string | null>(null)
  const plannedChanges = useSignal<SchemaChange[]>([])
  const planning = useSignal(false)
  const applying = useSignal(false)

  // New table form
  const isNewTable = useSignal(false)
  const newTableName = useSignal('')

  // Codegen
  const codegenLang = useSignal<Lang>('go')
  const codegenCode = useSignal('')
  const codegenLoading = useSignal(false)

  // New index form
  const newIdxCol = useSignal('')
  const newIdxUnique = useSignal(false)

  const dirty = useComputed(() =>
    cols.value.some(c => {
      if (c.isNew || c.isDeleted) return true
      const o = originalCols.value.find(x => x.name === c.originalName)
      return !o || c.name !== o.name || c.dataType !== o.dataType || c.isNullable !== o.isNullable ||
        c.isPrimaryKey !== o.isPrimaryKey || (c.default ?? '') !== (o.default ?? '')
    })
  )

  // Load the table's structure from the shared metadata when the selection
  // changes (or after an apply / a refused stale apply).
  useEffect(() => {
    if (!selectedTable.value || !conn) return
    colsLoading.value = true
    loadError.value = null
    api.schemaObject(conn.id, selectedSchema.value, selectedTable.value)
      .then(d => {
        if (d.kind !== 'table') {
          loadError.value = d.kind === 'opaque'
            ? `${d.schema}.${d.name} is not managed by the planner (${d.opaque?.reason ?? 'opaque object'}); change it in the SQL editor.`
            : `${d.schema}.${d.name} is a ${d.kind}; the designer edits tables.`
          cols.value = []
          originalCols.value = []
          indexes.value = []
          return
        }
        const loaded = colsFromDetail(d)
        originalCols.value = loaded
        cols.value = loaded.map(c => ({ ...c }))
        indexes.value = indexesFromDetail(d)
      })
      .catch(e => {
        loadError.value = e instanceof ApiError && e.status === 404
          ? `${selectedSchema.value}.${selectedTable.value} is gone from the live catalog (dropped or renamed). Refresh the schema.`
          : e instanceof Error ? e.message : String(e)
      })
      .finally(() => { colsLoading.value = false })
  }, [selectedTable.value, selectedSchema.value, conn?.id, reloadTick.value])

  // Load codegen when table or lang changes
  useEffect(() => {
    if (!selectedTable.value || !conn) return
    codegenLoading.value = true
    api.codegen(conn.id, selectedSchema.value, selectedTable.value, codegenLang.value)
      .then(r => { codegenCode.value = r.code })
      .catch(() => { codegenCode.value = '// error generating code' })
      .finally(() => { codegenLoading.value = false })
  }, [selectedTable.value, selectedSchema.value, codegenLang.value, conn?.id])

  if (!conn) {
    return <div class={s.hint}>Connect to a database to use Schema Designer</div>
  }

  const tables = sc?.sql ?? []

  // --- Handlers ---

  function resetPlan() {
    pendingPlan.value = null
    planError.value = null
    applyError.value = null
    plannedChanges.value = []
  }

  function selectTable(schemaName: string, tableName: string) {
    resetPlan()
    isNewTable.value = false
    selectedSchema.value = schemaName
    selectedTable.value = tableName
  }

  function startNewTable() {
    resetPlan()
    selectedTable.value = null
    isNewTable.value = true
    newTableName.value = ''
    cols.value = [{
      name: 'id', dataType: 'bigint', isNullable: false,
      default: null, isPrimaryKey: true,
      isNew: true, isDeleted: false, originalName: '',
    }]
    originalCols.value = []
    indexes.value = []
    codegenCode.value = ''
  }

  function addColumn() {
    cols.value = [...cols.value, {
      name: '', dataType: 'text', isNullable: true,
      default: null, isPrimaryKey: false,
      isNew: true, isDeleted: false, originalName: '',
    }]
  }

  function updateCol(idx: number, patch: Partial<EditCol>) {
    cols.value = cols.value.map((c, i) => i === idx ? { ...c, ...patch } : c)
  }

  function deleteCol(idx: number) {
    const c = cols.value[idx]
    if (c.isNew) {
      cols.value = cols.value.filter((_, i) => i !== idx)
    } else {
      updateCol(idx, { isDeleted: true })
    }
  }

  /** The visual edits as structured changes for the plan endpoint. */
  function computeChanges(): { changes: SchemaChange[]; error: string | null } {
    const changes: SchemaChange[] = []
    if (isNewTable.value) {
      const name = newTableName.value.trim()
      if (!name) return { changes: [], error: 'Table name is required' }
      const live = cols.value.filter(c => !c.isDeleted)
      if (live.length === 0) return { changes: [], error: 'Add at least one column' }
      for (const c of live) {
        if (!c.name.trim()) return { changes: [], error: 'Every column needs a name' }
      }
      changes.push({
        op: 'create-table', schema: selectedSchema.value, table: name,
        columns: live.map(c => ({
          name: c.name.trim(), type: c.dataType, notNull: !c.isNullable,
          default: c.default ?? undefined, isPrimaryKey: c.isPrimaryKey,
        })),
      })
      return { changes, error: null }
    }
    if (!selectedTable.value) return { changes: [], error: 'Select a table first' }
    const table = selectedTable.value
    const schemaName = selectedSchema.value
    for (const c of cols.value) {
      if (c.isDeleted) continue
      if (!c.name.trim()) return { changes: [], error: 'Every column needs a name' }
      const orig = originalCols.value.find(o => o.name === c.originalName)
      if ((c.isNew && c.isPrimaryKey) || (orig && orig.isPrimaryKey !== c.isPrimaryKey)) {
        return { changes: [], error: `Changing the primary key of ${table} is outside the designer's plan vocabulary — use the SQL editor` }
      }
    }
    // Renames first, so later ops address the final names.
    for (const c of cols.value) {
      if (c.isNew || c.isDeleted) continue
      if (c.name !== c.originalName) {
        changes.push({ op: 'rename-column', schema: schemaName, table, from: c.originalName, to: c.name })
      }
    }
    for (const c of cols.value) {
      if (c.isDeleted && !c.isNew) {
        changes.push({ op: 'drop-column', schema: schemaName, table, column: c.originalName })
        continue
      }
      if (c.isDeleted) continue
      if (c.isNew) {
        changes.push({
          op: 'add-column', schema: schemaName, table, column: c.name.trim(),
          type: c.dataType, notNull: !c.isNullable, default: c.default ?? undefined,
        })
        continue
      }
      const orig = originalCols.value.find(o => o.name === c.originalName)
      if (!orig) continue
      if (orig.dataType !== c.dataType) {
        changes.push({ op: 'alter-column-type', schema: schemaName, table, column: c.name, type: c.dataType })
      }
      if (orig.isNullable && !c.isNullable) {
        changes.push({ op: 'set-not-null', schema: schemaName, table, column: c.name })
      } else if (!orig.isNullable && c.isNullable) {
        changes.push({ op: 'drop-not-null', schema: schemaName, table, column: c.name })
      }
      const origDefault = orig.default ?? ''
      const newDefault = c.default ?? ''
      if (origDefault !== newDefault) {
        if (newDefault === '') {
          changes.push({ op: 'drop-default', schema: schemaName, table, column: c.name })
        } else {
          changes.push({ op: 'set-default', schema: schemaName, table, column: c.name, default: newDefault })
        }
      }
    }
    return { changes, error: null }
  }

  /** Plan changes and show the review; nothing executes. */
  async function requestPlan(changes: SchemaChange[]): Promise<boolean> {
    const c = activeConnection.value
    if (!c) return false
    resetPlan()
    planning.value = true
    try {
      pendingPlan.value = await api.schemaPlan({ connectionId: c.id, changes })
      plannedChanges.value = changes
      return true
    } catch (err: unknown) {
      planError.value = err instanceof Error ? err.message : String(err)
      if (err instanceof ApiError && err.status === 400 && /does not exist/.test(err.message)) {
        // The live catalog moved under the edit: refresh what we show.
        void refreshSchema(c.id)
      }
      return false
    } finally {
      planning.value = false
    }
  }

  async function saveChanges() {
    const { changes, error } = computeChanges()
    if (error) { toast('error', error); return }
    if (changes.length === 0) { toast('info', 'No changes to plan'); return }
    await requestPlan(changes)
  }

  async function applyPlan(acknowledged: boolean) {
    const c = activeConnection.value
    const plan = pendingPlan.value
    if (!c || !plan) return
    applying.value = true
    applyError.value = null
    try {
      const res = await api.schemaApply({
        connectionId: c.id,
        changes: plannedChanges.value,
        planId: plan.planId,
        allowDestructive: acknowledged,
      })
      if (res.verification === 'in-sync') {
        toast('success', res.applied ? `Applied ${res.up.length} statement(s) in one transaction` : 'Already in sync')
      } else if (res.verification === 'drift') {
        toast('error', `Applied, but the catalog does not match the plan (${res.residual?.length ?? 0} residual statement(s)) — refresh and review`)
      } else {
        toast('error', `Applied; ${res.verification ?? 'verification unavailable'}`)
      }
      const created = isNewTable.value ? newTableName.value.trim() : null
      resetPlan()
      await refreshSchema(c.id)
      if (created) {
        isNewTable.value = false
        selectedTable.value = created
      } else {
        reloadTick.value++
      }
    } catch (err: unknown) {
      if (err instanceof ApiError) {
        applyError.value = applyRefusal(err)
        if (err.state === 'stale-plan' && err.body && typeof err.body.plan === 'object' && err.body.plan) {
          // Show the fresh plan for review; the user's edits stay staged.
          pendingPlan.value = err.body.plan as SchemaPlanResponse
          void refreshSchema(c.id)
        } else if (err.state === 'outcome-unknown' || err.status === 400) {
          void refreshSchema(c.id)
        }
      } else {
        applyError.value = err instanceof Error ? err.message : String(err)
      }
    } finally {
      applying.value = false
    }
  }

  async function dropTable() {
    if (!selectedTable.value) return
    // Destructive: the plan review carries the DROP and requires an explicit
    // acknowledgement before Apply is enabled.
    await requestPlan([{ op: 'drop-table', schema: selectedSchema.value, table: selectedTable.value }])
  }

  async function addIndex() {
    if (!selectedTable.value || !newIdxCol.value) return
    const idxName = `${selectedTable.value}_${newIdxCol.value}_idx`
    const ok = await requestPlan([{
      op: 'add-index', schema: selectedSchema.value, table: selectedTable.value,
      index: idxName, column: newIdxCol.value, unique: newIdxUnique.value,
    }])
    if (ok) {
      newIdxCol.value = ''
      newIdxUnique.value = false
    }
  }

  async function dropIndexPlan(name: string) {
    if (!selectedTable.value) return
    await requestPlan([{ op: 'drop-index', schema: selectedSchema.value, table: selectedTable.value, index: name }])
  }

  function copyCodegen() {
    navigator.clipboard.writeText(codegenCode.value)
      .then(() => toast('success', 'Copied to clipboard'))
  }

  const visibleCols = cols.value.filter(c => !c.isDeleted)

  return (
    <div class={s.layout}>
      {/* Left sidebar: table list */}
      <div class={s.sidebar}>
        <div class={s.sidebarHeader}>
          <span class={s.sidebarTitle}>Tables</span>
          <button class={s.newTableBtn} onClick={startNewTable}>+ New</button>
        </div>
        <div class={s.tableList}>
          {tables.map(t => (
            <button
              key={t.schema + '.' + t.name}
              class={`${s.tableItem} ${selectedTable.value === t.name && selectedSchema.value === t.schema && !isNewTable.value ? s.tableItemActive : ''}`}
              onClick={() => selectTable(t.schema, t.name)}
            >
              {t.schema !== 'public' && <span class={s.schemaPrefix}>{t.schema}.</span>}
              {t.name}
            </button>
          ))}
          {tables.length === 0 && (
            <div class={s.emptyList}>No tables yet</div>
          )}
        </div>
      </div>

      {/* Right: editor */}
      <div class={s.main}>
        {!selectedTable.value && !isNewTable.value && (
          <div class={s.hint}>Select a table or create a new one</div>
        )}

        {/* Plan review panel (shared by every flow) */}
        {planError.value && (
          <div class={s.planError} role="alert">{planError.value}</div>
        )}
        {pendingPlan.value && (
          <PlanReview
            key={pendingPlan.value.planId}
            plan={pendingPlan.value}
            applying={applying.value}
            error={applyError.value}
            onApply={ack => { void applyPlan(ack) }}
            onCancel={resetPlan}
          />
        )}

        {/* New table form */}
        {isNewTable.value && (
          <div class={s.editor}>
            <div class={s.editorHeader}>
              <input
                class={s.tableNameInput}
                placeholder="table_name"
                value={newTableName.value}
                onInput={e => { newTableName.value = (e.target as HTMLInputElement).value }}
              />
              <span class={s.newBadge}>new table in {selectedSchema.value}</span>
            </div>

            <ColumnTable
              cols={cols.value}
              onUpdate={updateCol}
              onDelete={deleteCol}
              onAdd={addColumn}
            />

            <div class={s.actions}>
              <button class={s.cancelBtn} onClick={() => { isNewTable.value = false; resetPlan() }}>Cancel</button>
              <button class={s.createBtn} onClick={saveChanges} disabled={planning.value}>
                {planning.value ? 'Planning…' : 'Plan Create Table'}
              </button>
            </div>
          </div>
        )}

        {/* Existing table editor */}
        {selectedTable.value && !isNewTable.value && (
          <div class={s.editor}>
            {colsLoading.value ? (
              <div class={s.hint}>Loading…</div>
            ) : loadError.value ? (
              <div class={s.planError} role="alert">{loadError.value}</div>
            ) : (
              <>
                <div class={s.editorHeader}>
                  <span class={s.tableName}>{selectedSchema.value !== 'public' ? `${selectedSchema.value}.` : ''}{selectedTable.value}</span>
                  <span class={s.colCount}>{visibleCols.length} columns</span>
                </div>

                <ColumnTable
                  cols={cols.value}
                  onUpdate={updateCol}
                  onDelete={deleteCol}
                  onAdd={addColumn}
                />

                {dirty.value && (
                  <div class={s.dirtyBar}>
                    <span class={s.dirtyMsg}>Unsaved changes</span>
                    <button class={s.cancelBtn} onClick={() => { resetPlan(); cols.value = originalCols.value.map(c => ({ ...c })) }}>Revert</button>
                    <button class={s.saveBtn} onClick={saveChanges} disabled={planning.value}>
                      {planning.value ? 'Planning…' : 'Plan Changes'}
                    </button>
                  </div>
                )}

                {/* Indexes */}
                <div class={s.section}>
                  <div class={s.sectionTitle}>Indexes</div>
                  {indexes.value.length === 0 && (
                    <div class={s.emptyList}>No secondary indexes (primary-key and unique constraints are listed in the inspector)</div>
                  )}
                  {indexes.value.map(idx => (
                    <div key={idx.name} class={s.indexRow}>
                      <span class={s.indexName}>{idx.name}</span>
                      <span class={s.indexCols}>({idx.key})</span>
                      {idx.unique && <span class={s.uniqueBadge}>UNIQUE</span>}
                      <button class={s.dropIdxBtn} aria-label={`Plan dropping index ${idx.name}`} onClick={() => { void dropIndexPlan(idx.name) }}>✕</button>
                    </div>
                  ))}
                  <div class={s.addIndexRow}>
                    <select
                      class={s.idxColSelect}
                      value={newIdxCol.value}
                      onChange={e => { newIdxCol.value = (e.target as HTMLSelectElement).value }}
                    >
                      <option value="">Column…</option>
                      {originalCols.value.map(c => (
                        <option key={c.name} value={c.name}>{c.name}</option>
                      ))}
                    </select>
                    <label class={s.uniqueLabel}>
                      <input
                        type="checkbox"
                        checked={newIdxUnique.value}
                        onChange={e => { newIdxUnique.value = (e.target as HTMLInputElement).checked }}
                      />
                      Unique
                    </label>
                    <button
                      class={s.addIdxBtn}
                      onClick={addIndex}
                      disabled={!newIdxCol.value || planning.value}
                    >
                      + Plan Index
                    </button>
                  </div>
                </div>

                {/* Codegen */}
                <div class={s.section}>
                  <div class={s.codegenHeader}>
                    <span class={s.sectionTitle}>Codegen</span>
                    <div class={s.langTabs}>
                      {(['go', 'ts', 'rust', 'python'] as Lang[]).map(l => (
                        <button
                          key={l}
                          class={`${s.langTab} ${codegenLang.value === l ? s.langTabActive : ''}`}
                          onClick={() => { codegenLang.value = l }}
                        >
                          {l === 'ts' ? 'TypeScript' : l === 'go' ? 'Go' : l === 'rust' ? 'Rust' : 'Python'}
                        </button>
                      ))}
                    </div>
                    <button class={s.copyBtn} onClick={copyCodegen} disabled={!codegenCode.value}>
                      Copy
                    </button>
                  </div>
                  <pre class={s.codeBlock}>
                    {codegenLoading.value ? 'Generating…' : (codegenCode.value || '// select a table')}
                  </pre>
                </div>

                {/* Danger zone */}
                <div class={s.dangerZone}>
                  <span class={s.dangerLabel}>Drop this table permanently</span>
                  <button class={s.dropTableBtn} onClick={dropTable}>Plan Drop Table</button>
                </div>
              </>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

// --- ColumnTable subcomponent ---

interface ColumnTableProps {
  cols: EditCol[]
  onUpdate: (idx: number, patch: Partial<EditCol>) => void
  onDelete: (idx: number) => void
  onAdd: () => void
}

function ColumnTable({ cols, onUpdate, onDelete, onAdd }: ColumnTableProps) {
  // Indices always address the FULL column list (deleted columns stay in it
  // as staged drops), so an edit never lands on a neighbouring column.
  return (
    <div class={s.colTable}>
      <div class={s.colHeader}>
        <span class={s.chName}>Name</span>
        <span class={s.chType}>Type</span>
        <span class={s.chNull}>Nullable</span>
        <span class={s.chPk}>PK</span>
        <span class={s.chDef}>Default</span>
        <span class={s.chAct} />
      </div>
      {cols.map((col, i) => {
        if (col.isDeleted) return null
        const managedDefault = col.defaultKind === 'identity' || col.defaultKind === 'sequence'
        const locked = Boolean(col.generated)
        const label = col.name || `new column ${i + 1}`
        return (
          <div key={col.originalName || `new-${i}`} class={`${s.colRow} ${col.isNew ? s.colRowNew : ''}`}>
            <input
              class={s.colName}
              value={col.name}
              placeholder="column_name"
              aria-label={`Column name (${label})`}
              onInput={e => onUpdate(i, { name: (e.target as HTMLInputElement).value })}
            />
            <div class={s.typeCell}>
              <select
                class={s.typeSelect}
                aria-label={`Type of ${label}`}
                disabled={locked}
                value={COMMON_TYPES.includes(col.dataType) ? col.dataType : '__custom'}
                onChange={e => {
                  const v = (e.target as HTMLSelectElement).value
                  if (v !== '__custom') onUpdate(i, { dataType: v })
                }}
              >
                {COMMON_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
                <option value="__custom">custom…</option>
              </select>
              {!COMMON_TYPES.includes(col.dataType) && (
                <input
                  class={s.typeCustom}
                  value={col.dataType}
                  placeholder="type"
                  aria-label={`Custom type of ${label}`}
                  disabled={locked}
                  onInput={e => onUpdate(i, { dataType: (e.target as HTMLInputElement).value })}
                />
              )}
            </div>
            <input
              type="checkbox"
              class={s.checkbox}
              aria-label={`${label} nullable`}
              checked={col.isNullable}
              onChange={e => onUpdate(i, { isNullable: (e.target as HTMLInputElement).checked })}
            />
            <input
              type="checkbox"
              class={s.checkbox}
              aria-label={`${label} primary key`}
              checked={col.isPrimaryKey}
              onChange={e => onUpdate(i, { isPrimaryKey: (e.target as HTMLInputElement).checked })}
            />
            {locked || managedDefault ? (
              <span class={s.colDefault} title="database-managed; change it in the SQL editor">
                {locked ? `generated: ${col.generated}` : col.defaultKind}
              </span>
            ) : (
              <input
                class={s.colDefault}
                value={col.default ?? ''}
                placeholder="none"
                aria-label={`Default of ${label}`}
                onInput={e => onUpdate(i, { default: (e.target as HTMLInputElement).value || null })}
              />
            )}
            <button class={s.deleteColBtn} aria-label={`Drop column ${label}`} onClick={() => onDelete(i)}>✕</button>
          </div>
        )
      })}
      <button class={s.addColBtn} onClick={onAdd}>+ Add Column</button>
    </div>
  )
}
