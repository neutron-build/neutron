/**
 * '@neutron/native/profiler' — performance profiling tools for Neutron Native.
 *
 * Provides FPS monitoring, render tracking, interaction timing, startup metrics,
 * and a visual performance overlay. All profiling tools are production-safe:
 * hooks return inert values and components render null when `__DEV__` is false.
 *
 * This module is intentionally excluded from the main barrel export to avoid
 * bundle impact — import it explicitly when needed:
 *
 * @example
 * ```ts
 * import {
 *   useFPSMonitor,
 *   useRenderTracker,
 *   PerformanceOverlay,
 *   useInteractionTiming,
 *   startupMetrics,
 * } from '@neutron/native/profiler'
 * ```
 *
 * @module @neutron/native/profiler
 */

import React, {
  useState,
  useEffect,
  useRef,
  useCallback,
  useMemo,
} from 'react'
import {
  View,
  Text,
  StyleSheet,
  PanResponder,
} from 'react-native'
import type { GestureResponderEvent, PanResponderGestureState } from 'react-native'

// ─── Globals ────────────────────────────────────────────────────────────────

declare const __DEV__: boolean

const isDev = typeof __DEV__ !== 'undefined' ? __DEV__ : process.env.NODE_ENV !== 'production'

// ─── FPS Monitor ────────────────────────────────────────────────────────────

/** Options for the FPS monitoring hook */
export interface FPSMonitorOptions {
  /** Whether the monitor is actively measuring. Defaults to true in __DEV__. */
  enabled?: boolean
  /** FPS threshold below which `isJanky` becomes true. Defaults to 45. */
  warningThreshold?: number
}

/** Values returned by useFPSMonitor */
export interface FPSMonitorResult {
  /** Rolling average FPS over the last 60 frames */
  fps: number
  /** Number of frames that took >33ms (dropped frames) */
  jankCount: number
  /** True when fps is below the warning threshold */
  isJanky: boolean
}

const ROLLING_WINDOW = 60
const JANK_THRESHOLD_MS = 33 // >33ms between frames = dropped frame

/**
 * Hook that measures real-time frame rate using `requestAnimationFrame`.
 *
 * Calculates a rolling average FPS over the last 60 frames and counts
 * "jank" frames where the gap between frames exceeds 33ms (indicating
 * a dropped frame at 30fps target).
 *
 * Returns inert values `{ fps: 60, jankCount: 0, isJanky: false }` in
 * production builds.
 *
 * @example
 * ```tsx
 * function MyScreen() {
 *   const { fps, isJanky } = useFPSMonitor({ warningThreshold: 50 })
 *   // fps updates reactively; isJanky is true when fps < 50
 * }
 * ```
 */
export function useFPSMonitor(options?: FPSMonitorOptions): FPSMonitorResult {
  const {
    enabled = isDev,
    warningThreshold = 45,
  } = options ?? {}

  const [fps, setFps] = useState(60)
  const [jankCount, setJankCount] = useState(0)

  const frameTimesRef = useRef<number[]>([])
  const lastFrameRef = useRef<number>(0)
  const jankCountRef = useRef(0)
  const rafIdRef = useRef<number>(0)

  useEffect(() => {
    if (!enabled || !isDev) return

    frameTimesRef.current = []
    lastFrameRef.current = 0
    jankCountRef.current = 0

    function onFrame(now: number) {
      if (lastFrameRef.current > 0) {
        const delta = now - lastFrameRef.current
        const frameTimes = frameTimesRef.current

        frameTimes.push(delta)
        if (frameTimes.length > ROLLING_WINDOW) {
          frameTimes.shift()
        }

        // Count jank
        if (delta > JANK_THRESHOLD_MS) {
          jankCountRef.current += 1
        }

        // Calculate rolling average FPS
        if (frameTimes.length >= 2) {
          const avgDelta = frameTimes.reduce((a, b) => a + b, 0) / frameTimes.length
          const currentFps = avgDelta > 0 ? Math.round(1000 / avgDelta) : 60
          setFps(currentFps)
          setJankCount(jankCountRef.current)
        }
      }

      lastFrameRef.current = now
      rafIdRef.current = requestAnimationFrame(onFrame)
    }

    rafIdRef.current = requestAnimationFrame(onFrame)

    return () => {
      if (rafIdRef.current) {
        cancelAnimationFrame(rafIdRef.current)
      }
    }
  }, [enabled])

  return useMemo(() => ({
    fps: isDev && enabled ? fps : 60,
    jankCount: isDev && enabled ? jankCount : 0,
    isJanky: isDev && enabled ? fps < warningThreshold : false,
  }), [fps, jankCount, enabled, warningThreshold])
}

// ─── Render Tracker ─────────────────────────────────────────────────────────

const RENDER_WARN_THRESHOLD = 10
const RENDER_WARN_INTERVAL_MS = 1000

/**
 * Hook that logs component renders in development mode and warns when a
 * component re-renders excessively (more than 10 times in 1 second).
 *
 * No-op in production builds.
 *
 * @param componentName - Human-readable name for logging
 *
 * @example
 * ```tsx
 * function ProductCard({ product }) {
 *   useRenderTracker('ProductCard')
 *   return <View>...</View>
 * }
 * ```
 */
export function useRenderTracker(componentName: string): void {
  const renderCountRef = useRef(0)
  const windowStartRef = useRef(Date.now())
  const windowCountRef = useRef(0)
  const hasWarnedRef = useRef(false)

  if (isDev) {
    // Increment on every render (intentionally outside useEffect)
    renderCountRef.current += 1
    windowCountRef.current += 1

    const now = Date.now()
    const elapsed = now - windowStartRef.current

    if (elapsed >= RENDER_WARN_INTERVAL_MS) {
      // Reset the window
      if (windowCountRef.current > RENDER_WARN_THRESHOLD && !hasWarnedRef.current) {
        console.warn(
          `[Neutron Profiler] "${componentName}" rendered ${windowCountRef.current} times in ${elapsed}ms. ` +
          `This may indicate unnecessary re-renders. Consider React.memo() or useCallback().`,
        )
        hasWarnedRef.current = true
      }
      windowStartRef.current = now
      windowCountRef.current = 0
    }
  }

  useEffect(() => {
    if (!isDev) return
    console.debug(`[Neutron Profiler] "${componentName}" mounted (render #${renderCountRef.current})`)
    return () => {
      console.debug(
        `[Neutron Profiler] "${componentName}" unmounted after ${renderCountRef.current} renders`,
      )
    }
    // Only run on mount/unmount
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])
}

// ─── Interaction Timing ─────────────────────────────────────────────────────

/** Aggregated timing statistics for a named interaction */
export interface InteractionStats {
  /** Average duration in milliseconds */
  avg: number
  /** Minimum duration in milliseconds */
  min: number
  /** Maximum duration in milliseconds */
  max: number
  /** Number of completed measurements */
  count: number
}

/** Return type of useInteractionTiming */
export interface InteractionTimingResult {
  /** Begin timing a named interaction */
  startInteraction: (name: string) => void
  /** End timing a named interaction and record the measurement */
  endInteraction: (name: string) => void
  /** Retrieve aggregated statistics for all tracked interactions */
  getTimings: () => Map<string, InteractionStats>
}

/**
 * Hook for measuring the duration of named user interactions.
 *
 * Call `startInteraction(name)` when an interaction begins (e.g., a button
 * press) and `endInteraction(name)` when the visual response is complete.
 * The hook maintains running min/max/avg statistics per interaction name.
 *
 * Returns no-op functions in production builds.
 *
 * @example
 * ```tsx
 * function SearchScreen() {
 *   const { startInteraction, endInteraction, getTimings } = useInteractionTiming()
 *
 *   async function onSearch(query: string) {
 *     startInteraction('search')
 *     const results = await api.search(query)
 *     setResults(results)
 *     endInteraction('search')
 *   }
 *
 *   // Later: getTimings().get('search')?.avg => 142
 * }
 * ```
 */
export function useInteractionTiming(): InteractionTimingResult {
  const pendingRef = useRef<Map<string, number>>(new Map())
  const statsRef = useRef<Map<string, InteractionStats>>(new Map())

  const startInteraction = useCallback((name: string) => {
    if (!isDev) return
    pendingRef.current.set(name, performance.now())
  }, [])

  const endInteraction = useCallback((name: string) => {
    if (!isDev) return

    const startTime = pendingRef.current.get(name)
    if (startTime == null) {
      console.warn(`[Neutron Profiler] endInteraction("${name}") called without a matching startInteraction()`)
      return
    }

    const duration = performance.now() - startTime
    pendingRef.current.delete(name)

    const existing = statsRef.current.get(name)
    if (existing) {
      const newCount = existing.count + 1
      statsRef.current.set(name, {
        avg: (existing.avg * existing.count + duration) / newCount,
        min: Math.min(existing.min, duration),
        max: Math.max(existing.max, duration),
        count: newCount,
      })
    } else {
      statsRef.current.set(name, {
        avg: duration,
        min: duration,
        max: duration,
        count: 1,
      })
    }
  }, [])

  const getTimings = useCallback((): Map<string, InteractionStats> => {
    return new Map(statsRef.current)
  }, [])

  return useMemo(() => ({
    startInteraction,
    endInteraction,
    getTimings,
  }), [startInteraction, endInteraction, getTimings])
}

// ─── Startup Time Tracker ───────────────────────────────────────────────────

/** Report of startup timing milestones */
export interface StartupReport {
  /** Time to First Render in milliseconds, or null if not yet marked */
  ttfr: number | null
  /** Time to Interactive in milliseconds, or null if not yet marked */
  tti: number | null
}

/** Startup metrics singleton — records app startup milestones */
export interface StartupMetrics {
  /** Timestamp (ms) when this module was first loaded */
  readonly appStartTime: number
  /** Timestamp of first render, or null if markFirstRender() hasn't been called */
  firstRenderTime: number | null
  /** Timestamp of interactive state, or null if markInteractive() hasn't been called */
  interactiveTime: number | null
  /** Call from your root component's first render to record TTFR */
  markFirstRender: () => void
  /** Call when navigation is ready / app is interactive to record TTI */
  markInteractive: () => void
  /** Returns computed TTFR and TTI durations */
  getReport: () => StartupReport
}

const appStartTime = Date.now()

/**
 * Singleton that tracks app startup milestones.
 *
 * `appStartTime` is captured at module-load time. Call `markFirstRender()`
 * from your root component's first render and `markInteractive()` when
 * navigation is ready. Use `getReport()` to retrieve TTFR and TTI.
 *
 * @example
 * ```tsx
 * // In your root App component
 * import { startupMetrics } from '@neutron/native/profiler'
 *
 * function App() {
 *   useEffect(() => {
 *     startupMetrics.markFirstRender()
 *   }, [])
 *
 *   return (
 *     <NavigationContainer onReady={() => startupMetrics.markInteractive()}>
 *       ...
 *     </NavigationContainer>
 *   )
 * }
 *
 * // Later, log startup performance:
 * const report = startupMetrics.getReport()
 * console.log(`TTFR: ${report.ttfr}ms, TTI: ${report.tti}ms`)
 * ```
 */
export const startupMetrics: StartupMetrics = {
  appStartTime,
  firstRenderTime: null,
  interactiveTime: null,

  markFirstRender() {
    if (this.firstRenderTime == null) {
      this.firstRenderTime = Date.now()
      if (isDev) {
        console.debug(
          `[Neutron Profiler] First render at +${this.firstRenderTime - this.appStartTime}ms`,
        )
      }
    }
  },

  markInteractive() {
    if (this.interactiveTime == null) {
      this.interactiveTime = Date.now()
      if (isDev) {
        console.debug(
          `[Neutron Profiler] Interactive at +${this.interactiveTime - this.appStartTime}ms`,
        )
      }
    }
  },

  getReport(): StartupReport {
    return {
      ttfr: this.firstRenderTime != null
        ? this.firstRenderTime - this.appStartTime
        : null,
      tti: this.interactiveTime != null
        ? this.interactiveTime - this.appStartTime
        : null,
    }
  },
}

// ─── Performance Overlay ────────────────────────────────────────────────────

/** Props for the PerformanceOverlay component */
export interface PerformanceOverlayProps {
  /** Whether the overlay is shown. Defaults to true in __DEV__. */
  enabled?: boolean
}

// Global render counter for the overlay
let _overlayRenderCount = 0

function getFPSColor(fps: number): string {
  if (fps >= 55) return '#4caf50' // green
  if (fps >= 40) return '#ff9800' // yellow/orange
  return '#f44336'                // red
}

/**
 * Try to read memory info from the JS runtime or React Native's PerfMonitor.
 * Returns null if no memory API is available.
 */
function getMemoryUsageMB(): number | null {
  try {
    // Standard performance.memory (Chromium / Hermes)
    const perf = performance as any
    if (perf?.memory?.usedJSHeapSize) {
      return Math.round(perf.memory.usedJSHeapSize / (1024 * 1024))
    }
  } catch { /* not available */ }

  return null
}

/**
 * Floating performance overlay that displays real-time FPS, memory usage,
 * and render count. Only renders in `__DEV__` mode.
 *
 * Features:
 * - Color-coded FPS indicator (green >= 55, yellow >= 40, red < 40)
 * - Memory usage when `performance.memory` is available
 * - Render count for the overlay's lifecycle
 * - Draggable via basic PanResponder touch handling
 * - Double-tap to toggle between expanded and collapsed views
 *
 * @example
 * ```tsx
 * function App() {
 *   return (
 *     <>
 *       <NavigationContainer>...</NavigationContainer>
 *       <PerformanceOverlay />
 *     </>
 *   )
 * }
 * ```
 */
export function PerformanceOverlay(props: PerformanceOverlayProps): React.ReactElement | null {
  const { enabled = isDev } = props

  // All hooks must be called unconditionally (Rules of Hooks)
  const { fps, jankCount, isJanky } = useFPSMonitor({ enabled: enabled && isDev })
  const [collapsed, setCollapsed] = useState(false)
  const [position, setPosition] = useState({ x: 0, y: 0 })
  const [memory, setMemory] = useState<number | null>(null)
  const lastTapRef = useRef(0)
  const positionRef = useRef({ x: 0, y: 0 })

  // Track renders
  _overlayRenderCount += 1
  const renderCount = _overlayRenderCount

  // Periodically poll memory
  useEffect(() => {
    if (!enabled || !isDev) return

    const id = setInterval(() => {
      setMemory(getMemoryUsageMB())
    }, 2000)

    return () => clearInterval(id)
  }, [enabled])

  // PanResponder for dragging
  const panResponder = useMemo(() => {
    if (!enabled || !isDev) {
      return PanResponder.create({})
    }

    return PanResponder.create({
      onStartShouldSetPanResponder: () => true,
      onMoveShouldSetPanResponder: (
        _evt: GestureResponderEvent,
        gestureState: PanResponderGestureState,
      ) => {
        return Math.abs(gestureState.dx) > 2 || Math.abs(gestureState.dy) > 2
      },
      onPanResponderGrant: () => {
        positionRef.current = { ...position }

        // Double-tap detection
        const now = Date.now()
        if (now - lastTapRef.current < 300) {
          setCollapsed((c) => !c)
        }
        lastTapRef.current = now
      },
      onPanResponderMove: (
        _evt: GestureResponderEvent,
        gestureState: PanResponderGestureState,
      ) => {
        setPosition({
          x: positionRef.current.x + gestureState.dx,
          y: positionRef.current.y + gestureState.dy,
        })
      },
    })
  // Position is captured in onPanResponderGrant via ref, not needed as dep
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [enabled])

  // Early return AFTER all hooks
  if (!enabled || !isDev) {
    return null
  }

  const fpsColor = getFPSColor(fps)

  if (collapsed) {
    return React.createElement(
      View,
      {
        ...panResponder.panHandlers,
        style: [
          styles.container,
          styles.collapsed,
          { transform: [{ translateX: position.x }, { translateY: position.y }] },
        ],
      },
      React.createElement(
        Text,
        { style: [styles.fpsText, { color: fpsColor }] },
        `${fps}`,
      ),
    )
  }

  const rows: React.ReactElement[] = [
    React.createElement(
      Text,
      { key: 'fps', style: [styles.label, { color: fpsColor }] },
      `FPS: ${fps}`,
    ),
    React.createElement(
      Text,
      { key: 'jank', style: [styles.label, isJanky ? styles.warn : null] },
      `Jank: ${jankCount}`,
    ),
  ]

  if (memory != null) {
    rows.push(
      React.createElement(
        Text,
        { key: 'mem', style: styles.label },
        `Mem: ${memory} MB`,
      ),
    )
  }

  rows.push(
    React.createElement(
      Text,
      { key: 'js', style: styles.label },
      `JS: ${fps >= 55 ? 'idle' : fps >= 40 ? 'busy' : 'blocked'}`,
    ),
  )

  rows.push(
    React.createElement(
      Text,
      { key: 'renders', style: styles.sublabel },
      `Renders: ${renderCount}`,
    ),
  )

  return React.createElement(
    View,
    {
      ...panResponder.panHandlers,
      style: [
        styles.container,
        { transform: [{ translateX: position.x }, { translateY: position.y }] },
      ],
      pointerEvents: 'box-only' as const,
    },
    ...rows,
  )
}

// ─── Styles ─────────────────────────────────────────────────────────────────

const styles = StyleSheet.create({
  container: {
    position: 'absolute',
    top: 50,
    right: 10,
    backgroundColor: 'rgba(0, 0, 0, 0.82)',
    borderRadius: 8,
    paddingHorizontal: 10,
    paddingVertical: 6,
    zIndex: 99999,
    elevation: 99999,
    minWidth: 100,
  },
  collapsed: {
    minWidth: 0,
    paddingHorizontal: 8,
    paddingVertical: 4,
    borderRadius: 12,
  },
  fpsText: {
    fontSize: 14,
    fontWeight: '700',
    fontVariant: ['tabular-nums'],
  },
  label: {
    color: '#e0e0e0',
    fontSize: 11,
    fontWeight: '600',
    fontVariant: ['tabular-nums'],
    lineHeight: 16,
  },
  sublabel: {
    color: '#9e9e9e',
    fontSize: 10,
    fontVariant: ['tabular-nums'],
    lineHeight: 14,
    marginTop: 2,
  },
  warn: {
    color: '#ff9800',
  },
})
