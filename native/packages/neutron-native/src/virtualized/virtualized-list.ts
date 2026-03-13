/**
 * VirtualizedList — windowed rendering for large datasets.
 *
 * Only renders items within the viewport + overscan buffer.
 * Measures item sizes dynamically and recycles views.
 */

import React, { type ReactElement, useState, useEffect, useCallback, useMemo, useRef } from 'react'
import { View, ScrollView } from 'react-native'
import type { VirtualizedListProps, ListRenderItemInfo, ViewToken } from './types.js'

const DEFAULT_OVERSCAN = 5
const DEFAULT_END_THRESHOLD = 0.5

/**
 * VirtualizedList renders only visible items for smooth scrolling performance.
 *
 * @example
 * <VirtualizedList
 *   data={items}
 *   renderItem={({ item }) => <Text>{item.name}</Text>}
 *   keyExtractor={(item) => item.id}
 *   estimatedItemSize={60}
 * />
 */
export function VirtualizedList<T>({
  data,
  renderItem,
  keyExtractor,
  estimatedItemSize,
  overscanCount = DEFAULT_OVERSCAN,
  horizontal = false,
  numColumns = 1,
  inverted = false,
  ListHeaderComponent,
  ListFooterComponent,
  ListEmptyComponent,
  ItemSeparatorComponent,
  style,
  contentContainerStyle,
  onScroll,
  scrollEventThrottle = 16,
  showsVerticalScrollIndicator,
  showsHorizontalScrollIndicator,
  onEndReached,
  onEndReachedThreshold = DEFAULT_END_THRESHOLD,
  refreshing: _refreshing,
  onRefresh: _onRefresh,
  viewabilityConfig,
  onViewableItemsChanged,
  initialScrollIndex: _initialScrollIndex,
  testID,
}: VirtualizedListProps<T>): ReactElement {
  const [scrollOffset, setScrollOffset] = useState(0)
  const [containerSize, setContainerSize] = useState(0)
  const endReachedRef = useRef(false)
  const prevViewableRef = useRef<Set<string>>(new Set())

  // Compute visible range
  const totalItems = data.length
  const itemsPerRow = numColumns
  const totalRows = Math.ceil(totalItems / itemsPerRow)

  const visibleRange = useMemo(() => {
    if (containerSize === 0) {
      // Initial render — show first screen + overscan
      const count = Math.ceil(800 / estimatedItemSize) + overscanCount
      return { start: 0, end: Math.min(count * itemsPerRow, totalItems) }
    }

    const startRow = Math.max(0, Math.floor(scrollOffset / estimatedItemSize) - overscanCount)
    const visibleRows = Math.ceil(containerSize / estimatedItemSize)
    const endRow = Math.min(totalRows, startRow + visibleRows + overscanCount * 2)

    return {
      start: startRow * itemsPerRow,
      end: Math.min(endRow * itemsPerRow, totalItems),
    }
  }, [scrollOffset, containerSize, estimatedItemSize, totalItems, itemsPerRow, totalRows, overscanCount])

  // Handle scroll events
  const handleScroll = useCallback((event: { nativeEvent: { contentOffset: { x: number; y: number } } }) => {
    const offset = horizontal ? event.nativeEvent.contentOffset.x : event.nativeEvent.contentOffset.y
    setScrollOffset(offset)
    onScroll?.(event)

    // Check if we've reached the end
    const totalSize = totalRows * estimatedItemSize
    const threshold = onEndReachedThreshold * containerSize
    if (offset + containerSize + threshold >= totalSize && !endReachedRef.current) {
      endReachedRef.current = true
      onEndReached?.()
    } else if (offset + containerSize + threshold < totalSize) {
      endReachedRef.current = false
    }
  }, [horizontal, onScroll, totalRows, estimatedItemSize, containerSize, onEndReachedThreshold, onEndReached])

  // Handle layout to get container size
  const handleLayout = useCallback((event: { nativeEvent: { layout: { width: number; height: number } } }) => {
    const size = horizontal ? event.nativeEvent.layout.width : event.nativeEvent.layout.height
    setContainerSize(size)
  }, [horizontal])

  // Viewability tracking
  useEffect(() => {
    if (!onViewableItemsChanged || !viewabilityConfig) return

    const threshold = (viewabilityConfig.viewAreaCoveragePercentThreshold ?? 50) / 100
    const viewable: ViewToken<T>[] = []
    const changed: ViewToken<T>[] = []

    for (let i = visibleRange.start; i < visibleRange.end; i++) {
      const item = data[i]
      const key = keyExtractor(item, i)
      const itemTop = Math.floor(i / itemsPerRow) * estimatedItemSize
      const itemBottom = itemTop + estimatedItemSize
      const visibleTop = Math.max(itemTop, scrollOffset)
      const visibleBottom = Math.min(itemBottom, scrollOffset + containerSize)
      const visibleFraction = (visibleBottom - visibleTop) / estimatedItemSize

      const isViewable = visibleFraction >= threshold
      const token: ViewToken<T> = { item, key, index: i, isViewable }

      if (isViewable) {
        viewable.push(token)
        if (!prevViewableRef.current.has(key)) {
          changed.push(token)
        }
      } else if (prevViewableRef.current.has(key)) {
        changed.push(token)
      }
    }

    if (changed.length > 0) {
      const newKeys = new Set(viewable.map(v => v.key))
      prevViewableRef.current = newKeys
      onViewableItemsChanged({ viewableItems: viewable, changed })
    }
  }, [visibleRange, scrollOffset, containerSize])

  // Render items
  const renderedItems: ReactElement[] = []

  // Apply inverted order if needed
  const orderedData = inverted ? [...data].reverse() : data

  for (let i = visibleRange.start; i < visibleRange.end; i++) {
    const item = orderedData[i]
    if (!item) continue

    const key = keyExtractor(item, i)
    const info: ListRenderItemInfo<T> = { item, index: i }
    const rendered = renderItem(info)
    if (!rendered) continue

    const itemStyle: Record<string, unknown> = {
      position: 'absolute' as const,
      [horizontal ? 'left' : 'top']: Math.floor(i / itemsPerRow) * estimatedItemSize,
      [horizontal ? 'height' : 'width']: numColumns > 1 ? `${100 / numColumns}%` : '100%',
      [horizontal ? 'width' : 'height']: estimatedItemSize,
    }

    if (numColumns > 1) {
      const col = i % numColumns
      itemStyle[horizontal ? 'top' : 'left'] = `${(col / numColumns) * 100}%`
    }

    renderedItems.push(
      React.createElement(View, { key, style: itemStyle as any }, [
        rendered,
        ItemSeparatorComponent && i < visibleRange.end - 1
          ? React.createElement(ItemSeparatorComponent, { key: `sep-${key}` })
          : null,
      ])
    )
  }

  const totalSize = totalRows * estimatedItemSize
  const contentStyle: Record<string, unknown> = {
    ...(contentContainerStyle ?? {}),
    [horizontal ? 'width' : 'height']: totalSize,
    position: 'relative' as const,
  }

  const children: (ReactElement | null)[] = []

  if (ListHeaderComponent) {
    children.push(React.createElement(ListHeaderComponent, { key: '__header__' }))
  }

  if (data.length === 0 && ListEmptyComponent) {
    children.push(React.createElement(ListEmptyComponent, { key: '__empty__' }))
  } else {
    children.push(
      React.createElement(View, { key: '__content__', style: contentStyle as any }, renderedItems)
    )
  }

  if (ListFooterComponent) {
    children.push(React.createElement(ListFooterComponent, { key: '__footer__' }))
  }

  return React.createElement(ScrollView, {
    style: style as any,
    horizontal,
    onScroll: handleScroll as any,
    onLayout: handleLayout as any,
    scrollEventThrottle,
    showsVerticalScrollIndicator,
    showsHorizontalScrollIndicator,
    testID,
  }, children)
}
