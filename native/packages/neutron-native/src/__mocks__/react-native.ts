/**
 * Mock for react-native used in Jest tests.
 * Provides minimal stubs for all RN APIs used in the source.
 */

// --- StyleSheet ---
const StyleSheet = {
  create: (styles: Record<string, unknown>) => styles,
  flatten: (style: unknown) => (Array.isArray(style) ? Object.assign({}, ...style) : style),
  hairlineWidth: 1,
}

// --- Platform ---
const Platform = {
  OS: 'ios' as string,
  Version: '18.0',
  select: (specifics: Record<string, unknown>) => specifics.ios ?? specifics.default,
}

// --- Animated ---
class AnimatedValue {
  _value: number
  constructor(v: number) { this._value = v }
  setValue(v: number) { this._value = v }
  stopAnimation(cb?: (v: number) => void) { cb?.(this._value) }
}

const AnimatedTiming = {
  start: (cb?: (r: { finished: boolean }) => void) => cb?.({ finished: true }),
  stop: () => {},
}

const Animated = {
  Value: AnimatedValue,
  timing: (_av: AnimatedValue, _config: Record<string, unknown>) => ({
    start: (cb?: (r: { finished: boolean }) => void) => cb?.({ finished: true }),
    stop: () => {},
  }),
  spring: (_av: AnimatedValue, _config: Record<string, unknown>) => ({
    start: (cb?: (r: { finished: boolean }) => void) => cb?.({ finished: true }),
    stop: () => {},
  }),
  decay: (_av: AnimatedValue, _config: Record<string, unknown>) => ({
    start: (cb?: (r: { finished: boolean }) => void) => cb?.({ finished: true }),
    stop: () => {},
  }),
  sequence: (animations: Array<{ start: Function; stop: Function }>) => ({
    start: (cb?: (r: { finished: boolean }) => void) => {
      animations.forEach(a => a.start())
      cb?.({ finished: true })
    },
    stop: () => animations.forEach(a => a.stop()),
  }),
  delay: (ms: number) => ({
    start: (cb?: (r: { finished: boolean }) => void) => {
      setTimeout(() => cb?.({ finished: true }), 0)
    },
    stop: () => {},
  }),
  View: 'Animated.View',
  Text: 'Animated.Text',
  Image: 'Animated.Image',
  ScrollView: 'Animated.ScrollView',
  createAnimatedComponent: (comp: unknown) => comp,
}

// --- View, Text, Image, etc. ---
function makeMockComponent(name: string) {
  const comp = (props: Record<string, unknown>) => {
    return { type: name, props, children: props.children ?? null }
  }
  comp.displayName = name
  return comp
}

const View = makeMockComponent('View')
const Text = makeMockComponent('Text')
const Image = makeMockComponent('Image')
const TextInput = makeMockComponent('TextInput')
const ScrollView = makeMockComponent('ScrollView')
const FlatList = makeMockComponent('FlatList')
const Modal = makeMockComponent('Modal')
const StatusBar = makeMockComponent('StatusBar')
const SafeAreaView = makeMockComponent('SafeAreaView')
const ActivityIndicator = makeMockComponent('ActivityIndicator')
const Switch = makeMockComponent('Switch')
const Pressable = makeMockComponent('Pressable')
const TouchableOpacity = makeMockComponent('TouchableOpacity')
const KeyboardAvoidingView = makeMockComponent('KeyboardAvoidingView')

// --- Linking ---
const Linking = {
  getInitialURL: jest.fn().mockResolvedValue(null),
  addEventListener: jest.fn().mockReturnValue({ remove: jest.fn() }),
  openURL: jest.fn().mockResolvedValue(undefined),
  canOpenURL: jest.fn().mockResolvedValue(true),
}

// --- AppRegistry ---
const AppRegistry = {
  registerComponent: jest.fn(),
  getApplication: jest.fn(),
}

// --- Dimensions ---
const Dimensions = {
  get: jest.fn().mockReturnValue({ width: 375, height: 812 }),
  addEventListener: jest.fn().mockReturnValue({ remove: jest.fn() }),
}

// --- Keyboard ---
const Keyboard = {
  addListener: jest.fn().mockReturnValue({ remove: jest.fn() }),
  dismiss: jest.fn(),
}

// --- AccessibilityInfo ---
const AccessibilityInfo = {
  isScreenReaderEnabled: jest.fn().mockResolvedValue(false),
  isReduceMotionEnabled: jest.fn().mockResolvedValue(false),
  addEventListener: jest.fn().mockReturnValue({ remove: jest.fn() }),
  announceForAccessibility: jest.fn(),
  setAccessibilityFocus: jest.fn(),
}

// --- Vibration ---
const Vibration = {
  vibrate: jest.fn(),
  cancel: jest.fn(),
}

// --- PanResponder ---
const PanResponder = {
  create: jest.fn().mockReturnValue({
    panHandlers: {
      onStartShouldSetResponder: jest.fn(),
      onMoveShouldSetResponder: jest.fn(),
      onResponderGrant: jest.fn(),
      onResponderMove: jest.fn(),
      onResponderRelease: jest.fn(),
      onResponderTerminate: jest.fn(),
    },
  }),
}

// --- PixelRatio ---
const PixelRatio = {
  get: jest.fn().mockReturnValue(2),
  getFontScale: jest.fn().mockReturnValue(1),
  getPixelSizeForLayoutSize: jest.fn((size: number) => size * 2),
  roundToNearestPixel: jest.fn((size: number) => Math.round(size * 2) / 2),
}

// --- findNodeHandle ---
const findNodeHandle = jest.fn().mockReturnValue(1)

// --- UIManager ---
const UIManager = {
  measure: jest.fn(),
  dispatchViewManagerCommand: jest.fn(),
}

// --- Easing ---
const Easing = {
  linear: (t: number) => t,
  ease: (t: number) => t,
  quad: (t: number) => t * t,
  cubic: (t: number) => t * t * t,
  bezier: (x1: number, y1: number, x2: number, y2: number) => (t: number) => t,
}

// --- NativeModules ---
const NativeModules = {}

export {
  StyleSheet,
  Platform,
  Animated,
  View,
  Text,
  Image,
  TextInput,
  ScrollView,
  FlatList,
  Modal,
  StatusBar,
  SafeAreaView,
  ActivityIndicator,
  Switch,
  Pressable,
  TouchableOpacity,
  KeyboardAvoidingView,
  Linking,
  AppRegistry,
  Dimensions,
  Keyboard,
  AccessibilityInfo,
  Vibration,
  PanResponder,
  PixelRatio,
  findNodeHandle,
  UIManager,
  Easing,
  NativeModules,
}
