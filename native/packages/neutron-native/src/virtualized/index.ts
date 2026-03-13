/**
 * '@neutron/native/virtualized' — High-performance virtualized lists.
 *
 * Uses native RecyclerView (Android) / UICollectionView (iOS) for
 * smooth scrolling with view recycling. Only renders items visible
 * in the viewport + overscan buffer.
 *
 * API is a superset of FlatList — drop-in replacement with better perf.
 */

export { VirtualizedList } from './virtualized-list.js'
export { FlashList } from './flash-list.js'
export type {
  VirtualizedListProps,
  FlashListProps,
  ViewToken,
  ViewabilityConfig,
  CellRendererProps,
} from './types.js'
