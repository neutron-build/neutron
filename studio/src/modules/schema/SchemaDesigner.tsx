import { useEffect } from 'preact/hooks'
import { useSignal, useComputed } from '@preact/signals'
import { schema, activeConnection, toast } from '../../lib/store'
import { api } from '../../lib/api'
import type { ColumnDetail, IndexDetail } from '../../lib/types'
import s from './SchemaDesigner.module.css'

type Lang = 'go' | 'ts' | 'rust' | 'python'

interface EditCol extends ColumnDetail {
  isNew: boolean
  isDeleted: boolean
  originalName: string
}

const COMMON_TYPES = [
  'text', 'varchar(255)', 'char(1)',
  'integer', 'bigint', 'smallint', 'serial', 'bigserial',
  'real', 'double precision', 'numeric',
  'boolean',
  'timestamptz', 'timestamp', 'date', 'time',
  'uuid', 'jsonb', 'json', 'bytea',
]

export function SchemaDesigner() {
  const conn = activeConnection.value
  const sc = schema.value

  const selectedTable = useSignal<string | null>(null)
  const selectedSchema = useSignal('public')
  const cols = useSignal<EditCol[]>([])
  const indexes = useSignal<IndexDetail[]>([])
  const colsLoading = useSignal(false)
  const saving = useSignal(false)

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
  const addingIdx = useSignal(false)

  const dirty = useComputed(() =>
    cols.value.some(c => c.isNew || c.isDeleted ||
      c.name !== c.originalName ||
      c.dataType !== (cols.value.find(o => o.originalName === c.originalName)?.dataType ?? c.dataType)
    )
  )

  // Load columns when table selection changes
  useEffect(() => {
    if (!selectedTable.value || !conn) return
    colsLoading.value = true
    api.columns(conn.id, selectedSchema.value, selectedTable.value)
      .then(res => {
        cols.value = res.columns.map(c => ({
          ...c,
          isNew: false,
          isDeleted: false,
          originalName: c.name,
        }))
        indexes.value = res.indexes
      })
      .catch(e => toast('error', String(e)))
      .finally(() => { colsLoading.value = false })
  }, [selectedTable.value, conn?.id])

  // Load codegen when table or lang changes
  useEffect(() => {
    if (!selectedTable.value || !conn) return
    codegenLoading.value = true
    api.codegen(conn.id, selectedSchema.value, selectedTable.value, codegenLang.value)
      .then(r => { codegenCode.value = r.code })
      .catch(() => { codegenCode.value = '// error generating code' })
      .finally(() => { codegenLoading.value = false })
  }, [selectedTable.value, codegenLang.value, conn?.id])

  if (!conn) {
    return <div class={s.hint}>Connect to a database to use Schema Designer</div>
  }

  const tables = sc?.sql ?? []

  // --- Handlers ---

  function selectTable(schemaName: string, tableName: string) {
    isNewTable.value = false
    selectedSchema.value = schemaName
    selectedTable.value = tableName
  }

  function startNewTable() {
    selectedTable.value = null
    isNewTable.value = true
    newTableName.value = ''
    cols.value = [{
      name: 'id', dataType: 'bigserial', isNullable: false,
      default: null, isPrimaryKey: true, ordinal: 1,
      isNew: true, isDeleted: false, originalName: '',
    }]
    indexes.value = []
    codegenCode.value = ''
  }

  function addColumn() {
    const ordinal = cols.value.filter(c => !c.isDeleted).length + 1
    cols.value = [...cols.value, {
      name: '', dataType: 'text', isNullable: true,
      default: null, isPrimaryKey: false, ordinal,
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

  async function saveChanges() {
    if (!conn || !selectedTable.value) return
    saving.value = true
    const tbl = `"${selectedSchema.value}"."${selectedTable.value}"`

    const stmts: string[] = []

    for (const c of cols.value) {
      if (c.isNew && !c.isDeleted) {
        const nullStr = c.isNullable ? '' : ' NOT NULL'
        const defStr = c.default ? ` DEFAULT ${c.default}` : ''
        stmts.push(`ALTER TABLE ${tbl} ADD COLUMN "${c.name}" ${c.dataType}${defStr}${nullStr}`)
      } else if (c.isDeleted && !c.isNew) {
        stmts.push(`ALTER TABLE ${tbl} DROP COLUMN "${c.originalName}"`)
      } else if (!c.isNew && !c.isDeleted) {
        // Check for type change
        const orig = sc?.sql.find(t => t.name === selectedTable.value)
          ?.columns.find(col => col.name === c.originalName)
        if (orig && orig.type !== c.dataType) {
          stmts.push(`ALTER TABLE ${tbl} ALTER COLUMN "${c.name}" TYPE ${c.dataType} USING "${c.name}"::${c.dataType}`)
        }
        // Nullable change
        if (orig) {
          if (orig.nullable && !c.isNullable) {
            stmts.push(`ALTER TABLE ${tbl} ALTER COLUMN "${c.name}" SET NOT NULL`)
          } else if (!orig.nullable && c.isNullable) {
            stmts.push(`ALTER TABLE ${tbl} ALTER COLUMN "${c.name}" DROP NOT NULL`)
          }
        }
        // Rename
        if (c.name !== c.originalName) {
          stmts.push(`ALTER TABLE ${tbl} RENAME COLUMN "${c.originalName}" TO "${c.name}"`)
        }
      }
    }

    if (stmts.length === 0) {
      toast('info', 'No changes to save')
      saving.value = false
      return
    }

    for (const sql of stmts) {
      const res = await api.ddl(conn.id, sql)
      if (!res.ok) {
        toast('error', res.error ?? 'DDL failed')
        saving.value = false
        return
      }
    }

    toast('success', 'Changes saved')
    // Reload columns
    const res = await api.columns(conn.id, selectedSchema.value, selectedTable.value)
    cols.value = res.columns.map(c => ({ ...c, isNew: false, isDeleted: false, originalName: c.name }))
    saving.value = false
  }

  async function createTable() {
    if (!conn) return
    const name = newTableName.value.trim()
    if (!name) { toast('error', 'Table name is required'); return }
    if (cols.value.length === 0) { toast('error', 'Add at least one column'); return }

    const pkCols = cols.value.filter(c => c.isPrimaryKey)
    const colDefs = cols.value.map(c => {
      const nullStr = c.isNullable ? '' : ' NOT NULL'
      const defStr = c.default ? ` DEFAULT ${c.default}` : ''
      const pkStr = pkCols.length === 1 && c.isPrimaryKey ? ' PRIMARY KEY' : ''
      return `  "${c.name}" ${c.dataType}${defStr}${nullStr}${pkStr}`
    })
    if (pkCols.length > 1) {
      colDefs.push(`  PRIMARY KEY (${pkCols.map(c => `"${c.name}"`).join(', ')})`)
    }
    const sql = `CREATE TABLE "public"."${name}" (\n${colDefs.join(',\n')}\n)`

    saving.value = true
    const res = await api.ddl(conn.id, sql)
    saving.value = false

    if (!res.ok) {
      toast('error', res.error ?? 'CREATE TABLE failed')
      return
    }
    toast('success', `Table "${name}" created`)
    isNewTable.value = false
    // Reload schema (trigger re-fetch)
    await api.schema(conn.id).then(() => {
      window.dispatchEvent(new CustomEvent('studio:refresh-schema'))
    })
    selectedTable.value = name
  }

  async function dropTable() {
    if (!conn || !selectedTable.value) return
    if (!confirm(`Drop table "${selectedTable.value}"? This cannot be undone.`)) return
    const res = await api.ddl(conn.id, `DROP TABLE "${selectedSchema.value}"."${selectedTable.value}"`)
    if (!res.ok) {
      toast('error', res.error ?? 'DROP TABLE failed')
      return
    }
    toast('success', `Table "${selectedTable.value}" dropped`)
    selectedTable.value = null
    window.dispatchEvent(new CustomEvent('studio:refresh-schema'))
  }

  async function addIndex() {
    if (!conn || !selectedTable.value || !newIdxCol.value) return
    addingIdx.value = true
    const unique = newIdxUnique.value ? 'UNIQUE ' : ''
    const idxName = `${selectedTable.value}_${newIdxCol.value}_idx`
    const sql = `CREATE ${unique}INDEX "${idxName}" ON "${selectedSchema.value}"."${selectedTable.value}" ("${newIdxCol.value}")`
    const res = await api.ddl(conn.id, sql)
    addingIdx.value = false
    if (!res.ok) {
      toast('error', res.error ?? 'CREATE INDEX failed')
      return
    }
    toast('success', 'Index created')
    newIdxCol.value = ''
    newIdxUnique.value = false
    const updated = await api.columns(conn.id, selectedSchema.value, selectedTable.value)
    indexes.value = updated.indexes
  }

  async function dropIndex(name: string) {
    if (!conn) return
    const res = await api.ddl(conn.id, `DROP INDEX "${name}"`)
    if (!res.ok) { toast('error', res.error ?? 'DROP INDEX failed'); return }
    toast('success', 'Index dropped')
    indexes.value = indexes.value.filter(i => i.name !== name)
  }

  function copyCodegen() {
    navigator.clipboard.writeText(codegenCode.value)
      .then(() => toast('success', 'Copied to clipboard'))
  }

  function downloadMigration() {
    if (!selectedTable.value && !isNewTable.value) return

    const stmts: string[] = []
    const schemaName = selectedSchema.value
    const tableName = selectedTable.value ?? newTableName.value.trim()

    if (isNewTable.value) {
      const pkCols = cols.value.filter(c => c.isPrimaryKey)
      const colDefs = cols.value.filter(c => !c.isDeleted).map(c => {
        const nullStr = c.isNullable ? '' : ' NOT NULL'
        const defStr = c.default ? ` DEFAULT ${c.default}` : ''
        const pkStr = pkCols.length === 1 && c.isPrimaryKey ? ' PRIMARY KEY' : ''
        return `  "${c.name}" ${c.dataType}${defStr}${nullStr}${pkStr}`
      })
      if (pkCols.length > 1) {
        colDefs.push(`  PRIMARY KEY (${pkCols.map(c => `"${c.name}"`).join(', ')})`)
      }
      stmts.push(`CREATE TABLE "${schemaName}"."${tableName}" (\n${colDefs.join(',\n')}\n);`)
    } else {
      const tbl = `"${schemaName}"."${tableName}"`
      for (const c of cols.value) {
        if (c.isNew && !c.isDeleted) {
          const nullStr = c.isNullable ? '' : ' NOT NULL'
          const defStr = c.default ? ` DEFAULT ${c.default}` : ''
          stmts.push(`ALTER TABLE ${tbl} ADD COLUMN "${c.name}" ${c.dataType}${defStr}${nullStr};`)
        } else if (c.isDeleted && !c.isNew) {
          stmts.push(`ALTER TABLE ${tbl} DROP COLUMN "${c.originalName}";`)
        } else if (!c.isNew && !c.isDeleted && c.name !== c.originalName) {
          stmts.push(`ALTER TABLE ${tbl} RENAME COLUMN "${c.originalName}" TO "${c.name}";`)
        }
      }
    }

    if (stmts.length === 0) {
      toast('info', 'No changes to export')
      return
    }

    const ts = new Date().toISOString().replace(/[:.]/g, '').slice(0, 15)
    const filename = `${ts}_${tableName}.sql`
    const content = `-- Migration: ${tableName}\n-- Generated by Neutron Studio\n\n${stmts.join('\n\n')}\n`
    const blob = new Blob([content], { type: 'text/plain' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = filename
    a.click()
    URL.revokeObjectURL(url)
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
              class={`${s.tableItem} ${selectedTable.value === t.name && !isNewTable.value ? s.tableItemActive : ''}`}
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
              <span class={s.newBadge}>new table</span>
            </div>

            <ColumnTable
              cols={visibleCols}
              onUpdate={updateCol}
              onDelete={deleteCol}
              onAdd={addColumn}
            />

            <div class={s.actions}>
              <button class={s.cancelBtn} onClick={() => { isNewTable.value = false }}>Cancel</button>
              <button class={s.migrationBtn} onClick={downloadMigration}>↓ Migration</button>
              <button class={s.createBtn} onClick={createTable} disabled={saving.value}>
                {saving.value ? 'Creating…' : 'Create Table'}
              </button>
            </div>
          </div>
        )}

        {/* Existing table editor */}
        {selectedTable.value && !isNewTable.value && (
          <div class={s.editor}>
            {colsLoading.value ? (
              <div class={s.hint}>Loading…</div>
            ) : (
              <>
                <div class={s.editorHeader}>
                  <span class={s.tableName}>{selectedSchema.value !== 'public' ? `${selectedSchema.value}.` : ''}{selectedTable.value}</span>
                  <span class={s.colCount}>{visibleCols.length} columns</span>
                </div>

                <ColumnTable
                  cols={visibleCols}
                  onUpdate={updateCol}
                  onDelete={deleteCol}
                  onAdd={addColumn}
                />

                {dirty.value && (
                  <div class={s.dirtyBar}>
                    <span class={s.dirtyMsg}>Unsaved changes</span>
                    <button class={s.migrationBtn} onClick={downloadMigration}>↓ Migration</button>
                    <button class={s.saveBtn} onClick={saveChanges} disabled={saving.value}>
                      {saving.value ? 'Saving…' : 'Save Changes'}
                    </button>
                  </div>
                )}

                {/* Indexes */}
                <div class={s.section}>
                  <div class={s.sectionTitle}>Indexes</div>
                  {indexes.value.length === 0 && (
                    <div class={s.emptyList}>No secondary indexes</div>
                  )}
                  {indexes.value.map(idx => (
                    <div key={idx.name} class={s.indexRow}>
                      <span class={s.indexName}>{idx.name}</span>
                      <span class={s.indexCols}>({idx.columns.join(', ')})</span>
                      {idx.isUnique && <span class={s.uniqueBadge}>UNIQUE</span>}
                      <button class={s.dropIdxBtn} onClick={() => dropIndex(idx.name)}>✕</button>
                    </div>
                  ))}
                  <div class={s.addIndexRow}>
                    <select
                      class={s.idxColSelect}
                      value={newIdxCol.value}
                      onChange={e => { newIdxCol.value = (e.target as HTMLSelectElement).value }}
                    >
                      <option value="">Column…</option>
                      {visibleCols.map(c => (
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
                      disabled={!newIdxCol.value || addingIdx.value}
                    >
                      + Add Index
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
                  <button class={s.dropTableBtn} onClick={dropTable}>Drop Table</button>
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
      {cols.map((col, i) => (
        <div key={i} class={`${s.colRow} ${col.isNew ? s.colRowNew : ''}`}>
          <input
            class={s.colName}
            value={col.name}
            placeholder="column_name"
            onInput={e => onUpdate(i, { name: (e.target as HTMLInputElement).value })}
          />
          <div class={s.typeCell}>
            <select
              class={s.typeSelect}
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
                onInput={e => onUpdate(i, { dataType: (e.target as HTMLInputElement).value })}
              />
            )}
          </div>
          <input
            type="checkbox"
            class={s.checkbox}
            checked={col.isNullable}
            onChange={e => onUpdate(i, { isNullable: (e.target as HTMLInputElement).checked })}
          />
          <input
            type="checkbox"
            class={s.checkbox}
            checked={col.isPrimaryKey}
            onChange={e => onUpdate(i, { isPrimaryKey: (e.target as HTMLInputElement).checked })}
          />
          <input
            class={s.colDefault}
            value={col.default ?? ''}
            placeholder="none"
            onInput={e => onUpdate(i, { default: (e.target as HTMLInputElement).value || null })}
          />
          <button class={s.deleteColBtn} onClick={() => onDelete(i)}>✕</button>
        </div>
      ))}
      <button class={s.addColBtn} onClick={onAdd}>+ Add Column</button>
    </div>
  )
}
