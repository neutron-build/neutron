import { useSignal, useComputed } from '@preact/signals'
import { useEffect, useRef } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { DataGrid } from '../../components/DataGrid'
import type { QueryResult } from '../../lib/types'
import 'maplibre-gl/dist/maplibre-gl.css'
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

// --- MapLibre lazy loading (same pattern as CodeMirror in SQLEditor) ---
let maplibreLoaded = false
let maplibregl: typeof import('maplibre-gl')

async function loadMapLibre() {
  if (maplibreLoaded) return
  maplibregl = await import('maplibre-gl')
  maplibreLoaded = true
}

// --- MapLibre Map component ---
interface MapViewProps {
  points: GeoPoint[]
  selectedPoint: string | null
  onSelectPoint: (id: string) => void
}

function MapView({ points, selectedPoint, onSelectPoint }: MapViewProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const mapRef = useRef<import('maplibre-gl').Map | null>(null)
  const mapReady = useSignal(false)
  const mapError = useSignal<string | null>(null)

  // Initialize map
  useEffect(() => {
    let cancelled = false

    async function init() {
      try {
        await loadMapLibre()
      } catch (err) {
        mapError.value = 'Failed to load MapLibre GL'
        return
      }
      if (cancelled || !containerRef.current) return

      // Use a free tile source (OpenFreeMap / demotiles)
      const map = new maplibregl.Map({
        container: containerRef.current,
        style: {
          version: 8,
          name: 'Neutron Dark',
          sources: {
            'osm-raster': {
              type: 'raster',
              tiles: [
                'https://tile.openstreetmap.org/{z}/{x}/{y}.png',
              ],
              tileSize: 256,
              attribution: '&copy; OpenStreetMap contributors',
            },
          },
          layers: [
            {
              id: 'osm-raster-layer',
              type: 'raster',
              source: 'osm-raster',
              paint: {
                // Darken the tiles to match the dark UI
                'raster-brightness-max': 0.5,
                'raster-saturation': -0.3,
              },
            },
          ],
          // Dark background while tiles load
          glyphs: 'https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf',
        },
        center: [0, 20],
        zoom: 1,
        attributionControl: { compact: true },
      })

      mapRef.current = map

      map.on('load', () => {
        if (cancelled) return
        mapReady.value = true

        // Add source for points
        map.addSource('query-points', {
          type: 'geojson',
          data: buildGeoJSON(points),
        })

        // Circle layer for points
        map.addLayer({
          id: 'points-circle',
          type: 'circle',
          source: 'query-points',
          paint: {
            'circle-radius': [
              'case',
              ['boolean', ['feature-state', 'selected'], false], 9,
              6,
            ],
            'circle-color': [
              'case',
              ['boolean', ['feature-state', 'selected'], false],
              '#f59e0b',
              '#06b6d4',
            ],
            'circle-stroke-width': 2,
            'circle-stroke-color': [
              'case',
              ['boolean', ['feature-state', 'selected'], false],
              'rgba(255,255,255,0.9)',
              'rgba(255,255,255,0.4)',
            ],
            'circle-opacity': 0.9,
          },
        })

        // Label layer
        map.addLayer({
          id: 'points-label',
          type: 'symbol',
          source: 'query-points',
          layout: {
            'text-field': ['get', 'id'],
            'text-size': 10,
            'text-offset': [0, 1.5],
            'text-anchor': 'top',
            'text-font': ['Open Sans Regular'],
          },
          paint: {
            'text-color': 'rgba(255,255,255,0.7)',
            'text-halo-color': 'rgba(0,0,0,0.8)',
            'text-halo-width': 1,
          },
          minzoom: 10,
        })

        // Click handler
        map.on('click', 'points-circle', (e) => {
          if (e.features && e.features.length > 0) {
            const id = e.features[0].properties?.id
            if (id) onSelectPoint(String(id))
          }
        })

        // Cursor
        map.on('mouseenter', 'points-circle', () => {
          map.getCanvas().style.cursor = 'pointer'
        })
        map.on('mouseleave', 'points-circle', () => {
          map.getCanvas().style.cursor = ''
        })

        // Fit bounds to points
        if (points.length > 0) {
          fitToPoints(map, points)
        }
      })
    }

    init()

    return () => {
      cancelled = true
      if (mapRef.current) {
        mapRef.current.remove()
        mapRef.current = null
      }
    }
  }, []) // Initialize once

  // Update points data when they change
  useEffect(() => {
    const map = mapRef.current
    if (!map || !mapReady.value) return

    const source = map.getSource('query-points') as import('maplibre-gl').GeoJSONSource | undefined
    if (source) {
      source.setData(buildGeoJSON(points))
      if (points.length > 0) {
        fitToPoints(map, points)
      }
    }
  }, [points, mapReady.value])

  // Update selected point feature state
  useEffect(() => {
    const map = mapRef.current
    if (!map || !mapReady.value) return

    // Clear all selections
    map.removeFeatureState({ source: 'query-points' })

    // Set selected
    if (selectedPoint) {
      // Feature state uses numeric IDs; find index
      const idx = points.findIndex(p => p.id === selectedPoint)
      if (idx >= 0) {
        map.setFeatureState(
          { source: 'query-points', id: idx },
          { selected: true }
        )
      }
    }
  }, [selectedPoint, mapReady.value])

  // Show popup for selected point
  useEffect(() => {
    const map = mapRef.current
    if (!map || !mapReady.value) return

    // Remove existing popup
    const existing = containerRef.current?.querySelector('.maplibregl-popup')
    if (existing) existing.remove()

    if (!selectedPoint) return

    const pt = points.find(p => p.id === selectedPoint)
    if (!pt) return

    const extraLines = Object.entries(pt.extra)
      .map(([k, v]) => `<b>${k}:</b> ${v === null ? 'null' : String(v)}`)
      .join('<br/>')

    new maplibregl.Popup({ closeButton: true, closeOnClick: false, className: 'neutron-popup' })
      .setLngLat([pt.lon, pt.lat])
      .setHTML(`
        <div style="font-family:var(--font-mono);font-size:11px;line-height:1.6">
          <div style="font-weight:600;margin-bottom:2px">Point ${pt.id}</div>
          <div>${pt.lat.toFixed(6)}, ${pt.lon.toFixed(6)}</div>
          ${extraLines ? `<div style="margin-top:4px;color:var(--text-secondary,#aaa)">${extraLines}</div>` : ''}
        </div>
      `)
      .addTo(map)
  }, [selectedPoint, mapReady.value])

  return (
    <div class={s.mapWrap}>
      <div ref={containerRef} class={s.mapContainer} />
      {!mapReady.value && !mapError.value && (
        <div class={s.mapOverlay}>Loading map...</div>
      )}
      {mapError.value && (
        <div class={s.mapOverlay}>{mapError.value}</div>
      )}
    </div>
  )
}

function buildGeoJSON(points: GeoPoint[]): GeoJSON.FeatureCollection {
  return {
    type: 'FeatureCollection',
    features: points.map((pt, i) => ({
      type: 'Feature' as const,
      id: i,
      geometry: {
        type: 'Point' as const,
        coordinates: [pt.lon, pt.lat],
      },
      properties: {
        id: pt.id,
        lat: pt.lat,
        lon: pt.lon,
        ...pt.extra,
      },
    })),
  }
}

function fitToPoints(map: import('maplibre-gl').Map, points: GeoPoint[]) {
  if (points.length === 0) return
  if (points.length === 1) {
    map.flyTo({ center: [points[0].lon, points[0].lat], zoom: 14, duration: 800 })
    return
  }
  const bounds = computeBounds(points)
  map.fitBounds(
    [[bounds.minLon, bounds.minLat], [bounds.maxLon, bounds.maxLat]],
    { padding: 50, duration: 800 }
  )
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
  const viewMode = useSignal<ViewMode>('map')
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
    selectedPoint.value = null
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
      // Auto-switch to map view when results arrive
      if (!r.error && r.rows.length > 0) {
        viewMode.value = 'map'
      }
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

  const hasResults = geoPoints.value.length > 0

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
                {hasResults && (
                  <div class={s.viewToggle}>
                    <button
                      class={`${s.toggleBtn} ${viewMode.value === 'results' ? s.toggleActive : ''}`}
                      onClick={() => { viewMode.value = 'results' }}
                    >
                      Table
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
                <MapView
                  points={geoPoints.value}
                  selectedPoint={selectedPoint.value}
                  onSelectPoint={(id) => { selectedPoint.value = selectedPoint.value === id ? null : id }}
                />
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
