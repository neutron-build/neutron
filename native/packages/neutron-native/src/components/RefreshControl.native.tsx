import { RefreshControl as RNRefreshControl } from 'react-native'

export interface RefreshControlProps {
  refreshing: boolean
  onRefresh?: () => void
  colors?: string[]
  progressBackgroundColor?: string
  tintColor?: string
  title?: string
  titleColor?: string
  testID?: string
}

export function RefreshControl(props: RefreshControlProps) {
  return <RNRefreshControl {...props} />
}
