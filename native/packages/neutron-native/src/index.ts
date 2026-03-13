/**
 * @neutron/native — public API
 *
 * This is the main entry point. Import universal (Tier 1) components,
 * the app entry helper, router, platform utilities, and navigation from here.
 *
 * For native-only (Tier 3) components, import from '@neutron/native/native'.
 * For navigation layouts, import from '@neutron/native/navigation'.
 * For router only, import from '@neutron/native/router'.
 * For platform only, import from '@neutron/native/platform'.
 * For preact/compat aliases (web builds), import from '@neutron/native/compat'.
 */

// ─── App Entry ───────────────────────────────────────────────────────────────
export { NeutronApp } from './render.js'

// ─── Tier 1 Components (universal — web + native) ─────────────────────────────
export { View } from './components/View.native.js'
export { Text } from './components/Text.native.js'
export { Image } from './components/Image.native.js'
export { Pressable } from './components/Pressable.native.js'
export { TextInput } from './components/TextInput.native.js'
export { Link } from './components/Link.native.js'

// ─── Tier 3 Components (native-only, also in @neutron/native/native) ──────────
export { ActivityIndicator } from './components/ActivityIndicator.native.js'
export { Switch } from './components/Switch.native.js'
export { Slider } from './components/Slider.native.js'
export { KeyboardAvoidingView } from './components/KeyboardAvoidingView.native.js'
export { RefreshControl } from './components/RefreshControl.native.js'

// ─── Navigation ───────────────────────────────────────────────────────────────
export { Stack, Tabs, Drawer } from './navigation/index.js'
export type { NavigatorProps, ScreenConfig, ScreenOptions } from './navigation/index.js'

// ─── Router ───────────────────────────────────────────────────────────────────
export {
  navigate, goBack, replace,
  useParams, usePathname, useRouter, useRoute,
  useSearchParams, useSearchParamsSetter,
  routerState, pathname, params, canGoBack, canGoForward,
  handleDeepLink, setNavigationRef,
  buildRouteTree, matchRoute,
  initDeepLinks,
} from './router/index.js'
export type { RouteRecord, RouterState, NavigateOptions, RouteManifest } from './router/index.js'

// ─── Platform ─────────────────────────────────────────────────────────────────
export { Platform } from './platform/index.js'
export { Capabilities } from './platform/capabilities.js'
export type { OS } from './platform/index.js'

// ─── Signals ──────────────────────────────────────────────────────────────────
export { persistedSignal, createEventBus } from './signals/bridge.js'

// ─── TurboModules (device APIs) ───────────────────────────────────────────────
export {
  useCamera, useLocation, useNotifications, useBiometrics, useHaptics,
  useClipboard, useAsyncStorage, useNetInfo, useDeviceInfo, usePermissions,
  getModule, requireModule, hasModule,
} from './turbomodule/index.js'
export type {
  CameraModule, LocationModule, NotificationsModule, BiometricsModule,
  HapticsModule, ClipboardModule, AsyncStorageModule, NetInfoModule,
  DeviceInfoModule, PermissionsModule, TurboModule,
} from './turbomodule/index.js'

// ─── Types ────────────────────────────────────────────────────────────────────
export type {
  NativeStyleProp, NativeTextStyleProp, NativeImageStyleProp,
  BaseViewProps, ViewProps, TextProps, ImageProps, PressableProps, TextInputProps,
  ScrollViewProps, FlatListProps, RenderItemInfo, ModalProps, StatusBarProps,
  LayoutEvent, ScrollEvent, EdgeInsets,
} from './types.js'
export type { ActivityIndicatorProps } from './components/ActivityIndicator.native.js'
export type { SwitchProps } from './components/Switch.native.js'
export type { SliderProps } from './components/Slider.native.js'
export type { KeyboardAvoidingViewProps } from './components/KeyboardAvoidingView.native.js'
export type { RefreshControlProps } from './components/RefreshControl.native.js'
export type { LinkProps } from './components/Link.native.js'
