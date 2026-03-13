import { useSignal, useComputed } from '@preact/signals'
import { useEffect } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { DataGrid } from '../../components/DataGrid'
import type { QueryResult } from '../../lib/types'
import s from './GeoModule.module.css'

interface GeoModuleProps {
  name: string
}

type QueryType = 'radius' | 'bbox' | 'knn'
type ViewMode = 'results' | 'map'

// --- Parsed point from query results ---
interface GeoPoint {
  id: string
  lat: number
  lon: number
  extra: Record<string, unknown>
}

// --- Parse coordinates from query results ---
function parseGeoPoints(result: QueryResult): GeoPoint[] {
  const cols = result.columns.map(c => c.toLowerCase())
  const latIdx = cols.findIndex(c => c === 'lat' || c === 'latitude' || c === 'y')
  const lonIdx = cols.findIndex(c => c === 'lon' || c === 'lng' || c === 'longitude' || c === 'x')
  const idIdx = cols.findIndex(c => c === 'id' || c === 'point_id' || c === 'gid')

  if (latIdx < 0 || lonIdx < 0) return []

  const points: GeoPoint[] = []
  for (let i = 0; i < result.rows.length; i++) {
    const row = result.rows[i] as unknown[]
    const lat = Number(row[latIdx])
    const lon = Number(row[lonIdx])
    if (isNaN(lat) || isNaN(lon)) continue

    const id = idIdx >= 0 ? String(row[idIdx]) : String(i)
    const extra: Record<string, unknown> = {}
    for (let c = 0; c < result.columns.length; c++) {
      if (c !== latIdx && c !== lonIdx && c !== idIdx) {
        extra[result.columns[c]] = row[c]
      }
    }
    points.push({ id, lat, lon, extra })
  }
  return points
}

// --- Compute bounds from a set of points ---
function computeBounds(points: GeoPoint[]): { minLat: number; maxLat: number; minLon: number; maxLon: number } {
  let minLat = Infinity, maxLat = -Infinity
  let minLon = Infinity, maxLon = -Infinity
  for (const p of points) {
    if (p.lat < minLat) minLat = p.lat
    if (p.lat > maxLat) maxLat = p.lat
    if (p.lon < minLon) minLon = p.lon
    if (p.lon > maxLon) maxLon = p.lon
  }
  // Add padding (10% of range, minimum 0.001 degrees)
  const latPad = Math.max((maxLat - minLat) * 0.1, 0.001)
  const lonPad = Math.max((maxLon - minLon) * 0.1, 0.001)
  return {
    minLat: minLat - latPad,
    maxLat: maxLat + latPad,
    minLon: minLon - lonPad,
    maxLon: maxLon + lonPad,
  }
}

export function GeoModule({ name }: GeoModuleProps) {
  const queryType = useSignal<QueryType>('radius')
  const lat = useSignal('37.7749')
  const lon = useSignal('-122.4194')
  const radius = useSignal('10')
  const knn = useSignal('10')
  const minLat = useSignal('37.0')
  const minLon = useSignal('-123.0')
  const maxLat = useSignal('38.0')
  const maxLon = useSignal('-122.0')
  const result = useSignal<QueryResult | null>(null)
  const running = useSignal(false)
  const pointCount = useSignal<number | null>(null)
  const viewMode = useSignal<ViewMode>('results')
  const selectedPoint = useSignal<string | null>(null)

  const conn = activeConnection.value!

  useEffect(() => {
    async function loadCount() {
      try {
        const r = await api.query(`SELECT geo_count('${name}')`, conn.id)
        if (!r.error && r.rows.length > 0) pointCount.value = Number(r.rows[0][0])
      } catch { /* non-critical */ }
    }
    loadCount()
  }, [name])

  async function runQuery() {
    running.value = true
    result.value = null
    try {
      let sql: string
      switch (queryType.value) {
        case 'radius':
          sql = `SELECT id, lat, lon, distance_km
                 FROM geo_radius('${name}', ${lat.value}, ${lon.value}, ${radius.value})`
          break
        case 'knn':
          sql = `SELECT id, lat, lon, distance_km
                 FROM geo_knn('${name}', ${lat.value}, ${lon.value}, ${knn.value})`
          break
        case 'bbox':
          sql = `SELECT id, lat, lon
                 FROM geo_bbox('${name}', ${minLat.value}, ${minLon.value}, ${maxLat.value}, ${maxLon.value})`
          break
      }
      const r = await api.query(sql!, conn.id)
      result.value = r
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      running.value = false
    }
  }

  // Parse points from results
  const geoPoints = useComputed(() => {
    const r = result.value
    if (!r || r.error) return []
    return parseGeoPoints(r)
  })

  const hasMapView = geoPoints.value.length > 0

  return (
    <div class={s.layout}>
      <div class={s.header}>
        <span class={s.layerName}>{name}</span>
        {pointCount.value != null && (
          <span class={s.ptCount}>{pointCount.value.toLocaleString()} points</span>
        )}
      </div>

      <div class={s.queryPanel}>
        <div class={s.tabs}>
          {(['radius', 'knn', 'bbox'] as QueryType[]).map(t => (
            <button
              key={t}
              class={`${s.tab} ${queryType.value === t ? s.tabActive : ''}`}
              onClick={() => { queryType.value = t; result.value = null }}
            >
              {t === 'radius' ? 'Point + Radius' : t === 'knn' ? 'K-Nearest' : 'Bounding Box'}
            </button>
          ))}
        </div>

        <div class={s.fields}>
          {(queryType.value === 'radius' || queryType.value === 'knn') && (
            <>
              <Field label="Latitude" value={lat.value} onChange={v => { lat.value = v }} />
              <Field label="Longitude" value={lon.value} onChange={v => { lon.value = v }} />
              {queryType.value === 'radius'
                ? <Field label="Radius (km)" value={radius.value} onChange={v => { radius.value = v }} />
                : <Field label="K neighbors" value={knn.value} onChange={v => { knn.value = v }} />
              }
            </>
          )}
          {queryType.value === 'bbox' && (
            <>
              <Field label="Min Lat" value={minLat.value} onChange={v => { minLat.value = v }} />
              <Field label="Min Lon" value={minLon.value} onChange={v => { minLon.value = v }} />
              <Field label="Max Lat" value={maxLat.value} onChange={v => { maxLat.value = v }} />
              <Field label="Max Lon" value={maxLon.value} onChange={v => { maxLon.value = v }} />
            </>
          )}
          <button class={s.runBtn} onClick={runQuery} disabled={running.value}>
            {running.value ? 'Querying...' : 'Query'}
          </button>
        </div>
      </div>

      <div class={s.results}>
        {result.value ? (
          result.value.error ? (
            <div class={s.error}>{result.value.error}</div>
          ) : (
            <>
              <div class={s.resultToolbar}>
                <div class={s.resultMeta}>{result.value.rowCount} points &middot; {result.value.duration}ms</div>
                {hasMapView && (
                  <div class={s.viewToggle}>
                    <button
                      class={`${s.toggleBtn} ${viewMode.value === 'results' ? s.toggleActive : ''}`}
                      onClick={() => { viewMode.value = 'results' }}
                    >
                      Results
                    </button>
                    <button
                      class={`${s.toggleBtn} ${viewMode.value === 'map' ? s.toggleActive : ''}`}
                      onClick={() => { viewMode.value = 'map' }}
                    >
                      Map
                    </button>
                  </div>
                )}
              </div>

              {viewMode.value === 'results' ? (
                <div class={s.grid}><DataGrid result={result.value} /></div>
              ) : (
                <div class={s.svgWrap}>
                  <CoordinatePlot
                    points={geoPoints.value}
                    selectedPoint={selectedPoint.value}
                    onSelectPoint={(id) => { selectedPoint.value = selectedPoint.value === id ? null : id }}
                  />
                </div>
              )}
            </>
          )
        ) : !running.value && (
          <div class={s.hint}>Configure a spatial query above and click Query</div>
        )}
      </div>
    </div>
  )
}

// --- SVG Coordinate Plot ---
interface CoordinatePlotProps {
  points: GeoPoint[]
  selectedPoint: string | null
  onSelectPoint: (id: string) => void
}

const SVG_W = 700
const SVG_H = 500
const PAD = 60 // padding for axes

function CoordinatePlot({ points, selectedPoint, onSelectPoint }: CoordinatePlotProps) {
  if (points.length === 0) return null

  const bounds = computeBounds(points)
  const { minLat: bMinLat, maxLat: bMaxLat, minLon: bMinLon, maxLon: bMaxLon } = bounds

  const plotW = SVG_W - PAD * 2
  const plotH = SVG_H - PAD * 2

  // Map lon -> x, lat -> y (lat is inverted: higher lat = higher on screen = lower y)
  const toX = (lon: number) => PAD + ((lon - bMinLon) / (bMaxLon - bMinLon)) * plotW
  const toY = (lat: number) => PAD + plotH - ((lat - bMinLat) / (bMaxLat - bMinLat)) * plotH

  // Generate grid lines (5 divisions each axis)
  const GRID_DIVISIONS = 5
  const latStep = (bMaxLat - bMinLat) / GRID_DIVISIONS
  const lonStep = (bMaxLon - bMinLon) / GRID_DIVISIONS

  const gridLats: number[] = []
  const gridLons: number[] = []
  for (let i = 0; i <= GRID_DIVISIONS; i++) {
    gridLats.push(bMinLat + i * latStep)
    gridLons.push(bMinLon + i * lonStep)
  }

  // Find selected point data for tooltip
  const selPt = selectedPoint ? points.find(p => p.id === selectedPoint) : null

  return (
    <svg
      class={s.plotSvg}
      viewBox={`0 0 ${SVG_W} ${SVG_H}`}
      preserveAspectRatio="xMidYMid meet"
    >
      {/* Background */}
      <rect
        x={PAD}
        y={PAD}
        width={plotW}
        height={plotH}
        class={s.plotBg}
      />

      {/* Grid lines - horizontal (latitude) */}
      {gridLats.map(lat => {
        const y = toY(lat)
        return (
          <g key={`glat-${lat}`}>
            <line x1={PAD} y1={y} x2={PAD + plotW} y2={y} class={s.gridLine} />
            <text x={PAD - 8} y={y + 3} class={s.axisLabel} text-anchor="end">
              {lat.toFixed(3)}
            </text>
          </g>
        )
      })}

      {/* Grid lines - vertical (longitude) */}
      {gridLons.map(lon => {
        const x = toX(lon)
        return (
          <g key={`glon-${lon}`}>
            <line x1={x} y1={PAD} x2={x} y2={PAD + plotH} class={s.gridLine} />
            <text x={x} y={PAD + plotH + 16} class={s.axisLabel} text-anchor="middle">
              {lon.toFixed(3)}
            </text>
          </g>
        )
      })}

      {/* Axis labels */}
      <text
        x={SVG_W / 2}
        y={SVG_H - 6}
        class={s.axisTitle}
        text-anchor="middle"
      >
        Longitude
      </text>
      <text
        x={14}
        y={SVG_H / 2}
        class={s.axisTitle}
        text-anchor="middle"
        transform={`rotate(-90, 14, ${SVG_H / 2})`}
      >
        Latitude
      </text>

      {/* Axis border */}
      <rect
        x={PAD}
        y={PAD}
        width={plotW}
        height={plotH}
        fill="none"
        class={s.plotBorder}
      />

      {/* Data points */}
      {points.map(pt => {
        const cx = toX(pt.lon)
        const cy = toY(pt.lat)
        const isSelected = selectedPoint === pt.id

        return (
          <g key={pt.id} onClick={() => onSelectPoint(pt.id)} class={s.pointGroup}>
            {/* Larger invisible hit area */}
            <circle cx={cx} cy={cy} r={12} fill="transparent" />
            <circle
              cx={cx}
              cy={cy}
              r={isSelected ? 7 : 5}
              class={`${s.point} ${isSelected ? s.pointSelected : ''}`}
            />
          </g>
        )
      })}

      {/* Tooltip for selected point */}
      {selPt && (
        <g>
          {/* Tooltip background */}
          <rect
            x={toX(selPt.lon) + 12}
            y={toY(selPt.lat) - 44}
            width={180}
            height={40 + Object.keys(selPt.extra).length * 16}
            rx={4}
            class={s.tooltip}
          />
          <text
            x={toX(selPt.lon) + 20}
            y={toY(selPt.lat) - 28}
            class={s.tooltipTitle}
          >
            Point {selPt.id}
          </text>
          <text
            x={toX(selPt.lon) + 20}
            y={toY(selPt.lat) - 14}
            class={s.tooltipText}
          >
            {selPt.lat.toFixed(6)}, {selPt.lon.toFixed(6)}
          </text>
          {Object.entries(selPt.extra).map(([key, val], i) => (
            <text
              key={key}
              x={toX(selPt.lon) + 20}
              y={toY(selPt.lat) + i * 16}
              class={s.tooltipText}
            >
              {key}: {val === null ? 'null' : String(val)}
            </text>
          ))}
        </g>
      )}
    </svg>
  )
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
