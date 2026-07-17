import { useSignal } from '@preact/signals'
import { activeConnection, toast } from '../../lib/store'
import { api } from '../../lib/api'
import s from './GeoModule.module.css'

interface GeoModuleProps {
  name: string
}

type CalcType = 'distance' | 'within' | 'area'

// Nucleus has NO geo store to enumerate — only scalar geometry functions.
// This module is an honest calculator over user-entered coordinates:
//   GEO_DISTANCE(lat1,lon1,lat2,lon2)          → meters (haversine)
//   GEO_WITHIN(lat1,lon1,lat2,lon2,radius_m)   → bool
//   GEO_AREA(x1,y1,x2,y2,x3,y3,...)            → polygon area (>=3 pairs)

// Parse a textarea of "x,y" (or "x y") pairs, one per line, into a flat
// coordinate argument list for GEO_AREA.
export function parsePolygon(text: string): number[] {
  const coords: number[] = []
  for (const line of text.split('\n')) {
    const trimmed = line.trim()
    if (!trimmed) continue
    const parts = trimmed.split(/[,\s]+/).map(Number)
    if (parts.length < 2 || parts.some(isNaN)) continue
    coords.push(parts[0], parts[1])
  }
  return coords
}

export function GeoModule({ name }: GeoModuleProps) {
  const calcType = useSignal<CalcType>('distance')

  // Two-point inputs (distance + within)
  const lat1 = useSignal('37.7749')
  const lon1 = useSignal('-122.4194')
  const lat2 = useSignal('34.0522')
  const lon2 = useSignal('-118.2437')
  const radius = useSignal('600000')

  // Polygon input (area) — "x,y" per line
  const polygon = useSignal('0,0\n4,0\n4,3\n0,3')

  const result = useSignal<string | null>(null)
  const running = useSignal(false)

  const conn = activeConnection.value!

  async function runCalc() {
    running.value = true
    result.value = null
    try {
      let sql: string
      switch (calcType.value) {
        case 'distance':
          sql = `SELECT GEO_DISTANCE(${num(lat1.value)}, ${num(lon1.value)}, ${num(lat2.value)}, ${num(lon2.value)})`
          break
        case 'within':
          sql = `SELECT GEO_WITHIN(${num(lat1.value)}, ${num(lon1.value)}, ${num(lat2.value)}, ${num(lon2.value)}, ${num(radius.value)})`
          break
        case 'area': {
          const coords = parsePolygon(polygon.value)
          if (coords.length < 6) {
            throw new Error('GEO_AREA needs at least 3 coordinate pairs')
          }
          sql = `SELECT GEO_AREA(${coords.join(', ')})`
          break
        }
      }
      const r = await api.query(sql!, conn.id)
      if (r.error) throw new Error(r.error)
      const cell = r.rows.length > 0 ? r.rows[0][0] : null
      result.value = formatResult(calcType.value, cell)
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      running.value = false
    }
  }

  return (
    <div class={s.layout}>
      <div class={s.header}>
        <span class={s.layerName}>{name}</span>
        <span class={s.ptCount}>geometry calculator</span>
      </div>

      <div class={s.queryPanel}>
        <div class={s.tabs}>
          {(['distance', 'within', 'area'] as CalcType[]).map(t => (
            <button
              key={t}
              class={`${s.tab} ${calcType.value === t ? s.tabActive : ''}`}
              onClick={() => { calcType.value = t; result.value = null }}
            >
              {t === 'distance' ? 'Distance' : t === 'within' ? 'Within Radius' : 'Polygon Area'}
            </button>
          ))}
        </div>

        <div class={s.fields}>
          {(calcType.value === 'distance' || calcType.value === 'within') && (
            <>
              <Field label="Lat 1" value={lat1.value} onChange={v => { lat1.value = v }} />
              <Field label="Lon 1" value={lon1.value} onChange={v => { lon1.value = v }} />
              <Field label="Lat 2" value={lat2.value} onChange={v => { lat2.value = v }} />
              <Field label="Lon 2" value={lon2.value} onChange={v => { lon2.value = v }} />
              {calcType.value === 'within' && (
                <Field label="Radius (m)" value={radius.value} onChange={v => { radius.value = v }} />
              )}
            </>
          )}
          {calcType.value === 'area' && (
            <div class={s.field} style={{ flex: 1 }}>
              <label class={s.fieldLabel}>Polygon points (x,y per line, &ge; 3)</label>
              <textarea
                class={s.fieldInput}
                rows={5}
                value={polygon.value}
                onInput={e => { polygon.value = (e.target as HTMLTextAreaElement).value }}
              />
            </div>
          )}
          <button class={s.runBtn} onClick={runCalc} disabled={running.value}>
            {running.value ? 'Computing...' : 'Compute'}
          </button>
        </div>
      </div>

      <div class={s.results}>
        {result.value != null ? (
          <div class={s.resultMeta}>{result.value}</div>
        ) : !running.value && (
          <div class={s.hint}>Enter coordinates above and click Compute</div>
        )}
      </div>
    </div>
  )
}

// Coerce a user field to a numeric literal; fall back to 0 for empty input so
// the generated SQL is always valid.
function num(v: string): string {
  const n = Number(v.trim())
  return isNaN(n) ? '0' : String(n)
}

function formatResult(type: CalcType, cell: unknown): string {
  if (cell == null) return 'NULL'
  if (type === 'within') {
    const b = cell === true || cell === 'true' || cell === 't'
    return b ? 'Within radius: true' : 'Within radius: false'
  }
  const n = Number(cell)
  if (isNaN(n)) return String(cell)
  if (type === 'distance') {
    return `${n.toLocaleString(undefined, { maximumFractionDigits: 2 })} m (${(n / 1000).toLocaleString(undefined, { maximumFractionDigits: 3 })} km)`
  }
  return `Area: ${n.toLocaleString(undefined, { maximumFractionDigits: 6 })}`
}

function Field({ label, value, onChange }: { label: string; value: string; onChange: (v: string) => void }) {
  return (
    <div class={s.field}>
      <label class={s.fieldLabel}>{label}</label>
      <input
        class={s.fieldInput}
        value={value}
        onInput={e => onChange((e.target as HTMLInputElement).value)}
      />
    </div>
  )
}
