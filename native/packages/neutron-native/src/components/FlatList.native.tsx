import { FlatList as RNFlatList } from 'react-native'
import type { FlatListProps } from '../types.js'

export function FlatList<T>(props: FlatListProps<T>) {
  return <RNFlatList<T> {...props} />
}
