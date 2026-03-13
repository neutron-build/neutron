/**
 * Virtualized list types.
 */

import type { ComponentType, ReactNode, ReactElement } from 'react'
import type { NativeStyleProp } from '../types.js'

/** Information about a visible item */
export interface ViewToken<T = unknown> {
  item: T
  key: string
  index: number | null
  isViewable: boolean
}

/** Configuration for viewability tracking */
export interface ViewabilityConfig {
  /** Minimum percentage of item visible to count as "viewable" (0-100) */
  viewAreaCoveragePercentThreshold?: number
  /** Minimum time item must be visible (ms) */
  minimumViewTime?: number
  /** Include items partially visible at edges */
  waitForInteraction?: boolean
}

/** Props passed to custom cell renderers */
export interface CellRendererProps<T> {
  item: T
  index: number
  style: NativeStyleProp
  children: ReactNode
}

/** Render item info */
export interface ListRenderItemInfo<T> {
  item: T
  index: number
}

/** Base virtualized list props */
export interface VirtualizedListProps<T> {
  /** Data array */
  data: readonly T[]
  /** Render function for each item */
  renderItem: (info: ListRenderItemInfo<T>) => ReactElement | null
  /** Unique key for each item */
  keyExtractor: (item: T, index: number) => string

  // ── Dimensions ──────────────────────────────────────────────────────
  /** Estimated item height (required for initial render) */
  estimatedItemSize: number
  /** Number of items to render beyond the visible area */
  overscanCount?: number

  // ── Layout ──────────────────────────────────────────────────────────
  /** Horizontal scrolling */
  horizontal?: boolean
  /** Number of columns (grid mode) */
  numColumns?: number
  /** Inverted list (chat UI — newest at bottom) */
  inverted?: boolean

  // ── Header / Footer / Empty ─────────────────────────────────────────
  ListHeaderComponent?: ComponentType | null
  ListFooterComponent?: ComponentType | null
  ListEmptyComponent?: ComponentType | null
  ItemSeparatorComponent?: ComponentType | null

  // ── Style ──────────────────────────────────────────────────────────
  style?: NativeStyleProp
  contentContainerStyle?: NativeStyleProp

  // ── Scroll ──────────────────────────────────────────────────────────
  onScroll?: (event: { nativeEvent: { contentOffset: { x: number; y: number } } }) => void
  scrollEventThrottle?: number
  showsVerticalScrollIndicator?: boolean
  showsHorizontalScrollIndicator?: boolean

  // ── End / Refresh ──────────────────────────────────────────────────
  onEndReached?: () => void
  onEndReachedThreshold?: number
  refreshing?: boolean
  onRefresh?: () => void

  // ── Viewability ────────────────────────────────────────────────────
  viewabilityConfig?: ViewabilityConfig
  onViewableItemsChanged?: (info: { viewableItems: ViewToken<T>[]; changed: ViewToken<T>[] }) => void

  // ── Scroll-to methods (imperative) ─────────────────────────────────
  initialScrollIndex?: number

  // ── Custom cell renderer ──────────────────────────────────────────
  CellRendererComponent?: ComponentType<CellRendererProps<T>>

  testID?: string
}

/** FlashList-specific props (extends VirtualizedList) */
export interface FlashListProps<T> extends VirtualizedListProps<T> {
  /** Draw distance in px — how far ahead to pre-render (default: 250) */
  drawDistance?: number

  /** Estimated first item offset (for sticky headers) */
  estimatedFirstItemOffset?: number

  /** Override layout for specific items (return height for variable-height lists) */
  overrideItemLayout?: (
    layout: { span?: number; size?: number },
    item: T,
    index: number,
    maxColumns: number,
  ) => void
}
