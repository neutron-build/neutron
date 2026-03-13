/**
 * Shared types for Neutron Native components.
 *
 * Uses React types — on web builds the bundler maps react → preact/compat,
 * so these types work in both environments.
 */

import type { ReactNode, ReactElement, ComponentType } from 'react'
import type { ViewStyle, TextStyle, ImageStyle, StyleProp, AccessibilityRole } from 'react-native'

// ─── Style helpers ───────────────────────────────────────────────────────────

export type NativeStyleProp = StyleProp<ViewStyle>
export type NativeTextStyleProp = StyleProp<TextStyle>
export type NativeImageStyleProp = StyleProp<ImageStyle>

// ─── View props shared by all Tier 1 components ───────────────────────────────

export interface BaseViewProps {
  style?: NativeStyleProp
  className?: string
  testID?: string
  accessible?: boolean
  accessibilityLabel?: string
  accessibilityHint?: string
  accessibilityRole?: AccessibilityRole
  pointerEvents?: 'box-none' | 'none' | 'box-only' | 'auto'
  children?: ReactNode
}

// ─── Component prop types ─────────────────────────────────────────────────────

export interface ViewProps extends BaseViewProps {
  onLayout?: (event: LayoutEvent) => void
}

export interface TextProps extends BaseViewProps {
  style?: NativeTextStyleProp
  numberOfLines?: number
  ellipsizeMode?: 'head' | 'middle' | 'tail' | 'clip'
  selectable?: boolean
  onPress?: (event: unknown) => void
}

export interface ImageProps extends Omit<BaseViewProps, 'children'> {
  style?: NativeImageStyleProp
  source: string | number | { uri: string }
  resizeMode?: 'cover' | 'contain' | 'stretch' | 'repeat' | 'center'
  onLoad?: (event?: unknown) => void
  onError?: (event?: unknown) => void
  onLoadEnd?: () => void
}

export interface PressableProps extends Omit<BaseViewProps, 'style' | 'children'> {
  onPress?: (event: unknown) => void
  onPressIn?: (event: unknown) => void
  onPressOut?: (event: unknown) => void
  onLongPress?: (event: unknown) => void
  disabled?: boolean
  hitSlop?: number | EdgeInsets
  style?: NativeStyleProp | ((state: { pressed: boolean }) => NativeStyleProp)
  children?: ReactNode | ((state: { pressed: boolean }) => ReactNode)
}

export interface TextInputProps extends BaseViewProps {
  value?: string
  defaultValue?: string
  placeholder?: string
  placeholderTextColor?: string
  onChangeText?: (text: string) => void
  onChange?: (event: { nativeEvent: { text: string } }) => void
  onSubmitEditing?: (event?: unknown) => void
  onFocus?: (event?: unknown) => void
  onBlur?: (event?: unknown) => void
  keyboardType?: 'default' | 'email-address' | 'numeric' | 'phone-pad' | 'url' | 'decimal-pad' | 'number-pad'
  returnKeyType?: 'done' | 'go' | 'next' | 'search' | 'send'
  secureTextEntry?: boolean
  autoCapitalize?: 'none' | 'sentences' | 'words' | 'characters'
  autoCorrect?: boolean
  autoFocus?: boolean
  editable?: boolean
  multiline?: boolean
  numberOfLines?: number
  maxLength?: number
}

export interface ScrollViewProps extends BaseViewProps {
  horizontal?: boolean
  showsHorizontalScrollIndicator?: boolean
  showsVerticalScrollIndicator?: boolean
  onScroll?: (event: ScrollEvent) => void
  scrollEventThrottle?: number
  onMomentumScrollEnd?: (event: ScrollEvent) => void
  onScrollBeginDrag?: (event: ScrollEvent) => void
  onScrollEndDrag?: (event: ScrollEvent) => void
  pagingEnabled?: boolean
  bounces?: boolean
  alwaysBounceVertical?: boolean
  alwaysBounceHorizontal?: boolean
  keyboardDismissMode?: 'none' | 'on-drag' | 'interactive'
  keyboardShouldPersistTaps?: 'always' | 'never' | 'handled'
  scrollEnabled?: boolean
  contentContainerStyle?: NativeStyleProp
}

export interface RenderItemInfo<T> {
  item: T
  index: number
  separators: {
    highlight: () => void
    unhighlight: () => void
    updateProps: (select: 'leading' | 'trailing', newProps: Record<string, unknown>) => void
  }
}

export interface FlatListProps<T> {
  data: T[] | null | undefined
  renderItem: (info: RenderItemInfo<T>) => ReactElement | null
  keyExtractor?: (item: T, index: number) => string
  style?: NativeStyleProp
  contentContainerStyle?: NativeStyleProp
  testID?: string
  horizontal?: boolean
  numColumns?: number
  onEndReached?: () => void
  onEndReachedThreshold?: number
  ListHeaderComponent?: ComponentType | null
  ListFooterComponent?: ComponentType | null
  ListEmptyComponent?: ComponentType | null
  ItemSeparatorComponent?: ComponentType | null
  refreshing?: boolean
  onRefresh?: () => void
  showsVerticalScrollIndicator?: boolean
  showsHorizontalScrollIndicator?: boolean
}

export interface ModalProps {
  visible: boolean
  onRequestClose?: () => void
  onShow?: () => void
  onDismiss?: () => void
  animationType?: 'none' | 'slide' | 'fade'
  transparent?: boolean
  statusBarTranslucent?: boolean
  testID?: string
  children?: ReactNode
}

export interface StatusBarProps {
  barStyle?: 'default' | 'light-content' | 'dark-content'
  backgroundColor?: string
  translucent?: boolean
  hidden?: boolean
  animated?: boolean
}

// ─── Event types ──────────────────────────────────────────────────────────────

export interface LayoutEvent {
  nativeEvent: {
    layout: { x: number; y: number; width: number; height: number }
  }
}

export interface ScrollEvent {
  nativeEvent: {
    contentOffset: { x: number; y: number }
    contentSize: { width: number; height: number }
    layoutMeasurement: { width: number; height: number }
  }
}

export interface EdgeInsets {
  top: number
  right: number
  bottom: number
  left: number
}
