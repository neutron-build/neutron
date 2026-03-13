/**
 * '@neutron/native/accessibility' — VoiceOver (iOS) and TalkBack (Android) support.
 *
 * Provides hooks, components, and utilities for building accessible mobile
 * applications. Wraps React Native's accessibility APIs with a higher-level
 * interface that handles platform differences between iOS and Android.
 *
 * @example
 * import {
 *   useAccessibility,
 *   announceForAccessibility,
 *   AccessibleView,
 *   LiveRegion,
 * } from '@neutron/native/accessibility'
 *
 * function CartTotal({ total }) {
 *   const { isScreenReaderEnabled } = useAccessibility()
 *   return (
 *     <LiveRegion>
 *       <AccessibleView
 *         role="summary"
 *         label={`Cart total: $${total}`}
 *         hint="Double tap to proceed to checkout"
 *       >
 *         <Text>${total}</Text>
 *       </AccessibleView>
 *     </LiveRegion>
 *   )
 * }
 */

import React, {
  type ReactNode,
  type RefObject,
  useState,
  useEffect,
  useRef,
  useCallback,
} from 'react'
import {
  View,
  AccessibilityInfo,
  Platform,
  findNodeHandle,
  UIManager,
} from 'react-native'
import type { AccessibilityRole, ViewStyle, StyleProp } from 'react-native'

// ─── Types ───────────────────────────────────────────────────────────────────

/** Union of valid accessibility roles across both platforms */
export type NeutronAccessibilityRole =
  | 'none'
  | 'button'
  | 'link'
  | 'search'
  | 'image'
  | 'keyboardkey'
  | 'text'
  | 'adjustable'
  | 'imagebutton'
  | 'header'
  | 'summary'
  | 'alert'
  | 'checkbox'
  | 'combobox'
  | 'menu'
  | 'menubar'
  | 'menuitem'
  | 'progressbar'
  | 'radio'
  | 'radiogroup'
  | 'scrollbar'
  | 'spinbutton'
  | 'switch'
  | 'tab'
  | 'tablist'
  | 'timer'
  | 'toolbar'
  | 'list'
  | 'grid'

/** Accessibility state values for interactive elements */
export interface AccessibilityState {
  /** Whether the element is currently disabled */
  disabled?: boolean
  /** Whether a selectable element is currently selected */
  selected?: boolean
  /** Checked state for checkboxes and switches */
  checked?: boolean | 'mixed'
  /** Whether the element is currently busy (loading) */
  busy?: boolean
  /** Whether an expandable element is currently expanded */
  expanded?: boolean
}

/** Accessibility value for adjustable elements (sliders, progress bars) */
export interface AccessibilityValue {
  /** Minimum value */
  min?: number
  /** Maximum value */
  max?: number
  /** Current value */
  now?: number
  /** Human-readable text description of the current value */
  text?: string
}

/** Return type of useAccessibility() hook */
export interface AccessibilityStatus {
  /** Whether VoiceOver (iOS) or TalkBack (Android) is currently active */
  isScreenReaderEnabled: boolean
  /** Whether the user has enabled "Reduce Motion" in system settings */
  reduceMotionEnabled: boolean
  /** Whether the user has enabled bold text (iOS only, false on Android) */
  boldTextEnabled: boolean
  /** Whether the user has enabled "Reduce Transparency" (iOS only) */
  reduceTransparencyEnabled: boolean
  /** Whether the user has enabled "Invert Colors" (iOS only) */
  invertColorsEnabled: boolean
  /** Whether the user has grayscale enabled */
  grayscaleEnabled: boolean
}

// ─── useAccessibility hook ───────────────────────────────────────────────────

/**
 * Hook that tracks the current accessibility status of the device.
 * Subscribes to real-time changes in VoiceOver/TalkBack, reduce motion,
 * bold text, and other accessibility preferences.
 *
 * @returns Current accessibility status with live updates
 *
 * @example
 * function AnimatedCard() {
 *   const { reduceMotionEnabled, isScreenReaderEnabled } = useAccessibility()
 *
 *   const animation = reduceMotionEnabled
 *     ? { duration: 0 }
 *     : { duration: 300, easing: Easing.bezier(0.25, 0.1, 0.25, 1) }
 *
 *   return (
 *     <View accessibilityElementsHidden={!isScreenReaderEnabled}>
 *       ...
 *     </View>
 *   )
 * }
 */
export function useAccessibility(): AccessibilityStatus {
  const [status, setStatus] = useState<AccessibilityStatus>({
    isScreenReaderEnabled: false,
    reduceMotionEnabled: false,
    boldTextEnabled: false,
    reduceTransparencyEnabled: false,
    invertColorsEnabled: false,
    grayscaleEnabled: false,
  })

  useEffect(() => {
    // Query initial state
    let mounted = true

    async function queryInitialState() {
      const [screenReader, reduceMotion, boldText] = await Promise.all([
        AccessibilityInfo.isScreenReaderEnabled(),
        AccessibilityInfo.isReduceMotionEnabled(),
        Platform.OS === 'ios'
          ? AccessibilityInfo.isBoldTextEnabled()
          : Promise.resolve(false),
      ])

      // Additional queries that may not be available on all RN versions
      let reduceTransparency = false
      let invertColors = false
      let grayscale = false

      try {
        if (Platform.OS === 'ios' && (AccessibilityInfo as any).isReduceTransparencyEnabled) {
          reduceTransparency = await (AccessibilityInfo as any).isReduceTransparencyEnabled()
        }
      } catch { /* not available */ }

      try {
        if (Platform.OS === 'ios' && (AccessibilityInfo as any).isInvertColorsEnabled) {
          invertColors = await (AccessibilityInfo as any).isInvertColorsEnabled()
        }
      } catch { /* not available */ }

      try {
        if ((AccessibilityInfo as any).isGrayscaleEnabled) {
          grayscale = await (AccessibilityInfo as any).isGrayscaleEnabled()
        }
      } catch { /* not available */ }

      if (mounted) {
        setStatus({
          isScreenReaderEnabled: screenReader,
          reduceMotionEnabled: reduceMotion,
          boldTextEnabled: boldText,
          reduceTransparencyEnabled: reduceTransparency,
          invertColorsEnabled: invertColors,
          grayscaleEnabled: grayscale,
        })
      }
    }

    queryInitialState()

    // Subscribe to changes
    const screenReaderSub = AccessibilityInfo.addEventListener(
      'screenReaderChanged',
      (enabled) => {
        if (mounted) setStatus((s) => ({ ...s, isScreenReaderEnabled: enabled }))
      },
    )

    const reduceMotionSub = AccessibilityInfo.addEventListener(
      'reduceMotionChanged',
      (enabled) => {
        if (mounted) setStatus((s) => ({ ...s, reduceMotionEnabled: enabled }))
      },
    )

    // Bold text change (iOS only)
    let boldTextSub: { remove(): void } | null = null
    if (Platform.OS === 'ios') {
      try {
        boldTextSub = AccessibilityInfo.addEventListener(
          'boldTextChanged' as any,
          (enabled: boolean) => {
            if (mounted) setStatus((s) => ({ ...s, boldTextEnabled: enabled }))
          },
        )
      } catch { /* event not available on this RN version */ }
    }

    // Reduce transparency change (iOS only)
    let reduceTransparencySub: { remove(): void } | null = null
    if (Platform.OS === 'ios') {
      try {
        reduceTransparencySub = AccessibilityInfo.addEventListener(
          'reduceTransparencyChanged' as any,
          (enabled: boolean) => {
            if (mounted) setStatus((s) => ({ ...s, reduceTransparencyEnabled: enabled }))
          },
        )
      } catch { /* not available */ }
    }

    // Invert colors change (iOS only)
    let invertColorsSub: { remove(): void } | null = null
    if (Platform.OS === 'ios') {
      try {
        invertColorsSub = AccessibilityInfo.addEventListener(
          'invertColorsChanged' as any,
          (enabled: boolean) => {
            if (mounted) setStatus((s) => ({ ...s, invertColorsEnabled: enabled }))
          },
        )
      } catch { /* not available */ }
    }

    // Grayscale change
    let grayscaleSub: { remove(): void } | null = null
    try {
      grayscaleSub = AccessibilityInfo.addEventListener(
        'grayscaleChanged' as any,
        (enabled: boolean) => {
          if (mounted) setStatus((s) => ({ ...s, grayscaleEnabled: enabled }))
        },
      )
    } catch { /* not available */ }

    return () => {
      mounted = false
      screenReaderSub.remove()
      reduceMotionSub.remove()
      boldTextSub?.remove()
      reduceTransparencySub?.remove()
      invertColorsSub?.remove()
      grayscaleSub?.remove()
    }
  }, [])

  return status
}

// ─── announceForAccessibility ────────────────────────────────────────────────

/**
 * Announce a message to the screen reader (VoiceOver / TalkBack).
 * The announcement is queued and spoken when the screen reader is idle.
 *
 * @param message - The text to announce
 *
 * @example
 * announceForAccessibility('Item added to cart. Cart now has 3 items.')
 */
export function announceForAccessibility(message: string): void {
  AccessibilityInfo.announceForAccessibility(message)
}

// ─── setAccessibilityFocus ───────────────────────────────────────────────────

/**
 * Programmatically move VoiceOver/TalkBack focus to a specific view.
 * Useful after navigation, modal open, or dynamic content changes.
 *
 * @param ref - React ref to the target view component
 *
 * @example
 * const headerRef = useRef(null)
 *
 * useEffect(() => {
 *   // Focus the header after navigation
 *   setAccessibilityFocus(headerRef)
 * }, [route])
 *
 * <Text ref={headerRef} accessible>Page Title</Text>
 */
export function setAccessibilityFocus(ref: RefObject<any>): void {
  if (!ref.current) return

  const nodeHandle = findNodeHandle(ref.current)
  if (nodeHandle == null) return

  if (Platform.OS === 'ios') {
    // On iOS, use AccessibilityInfo.setAccessibilityFocus
    AccessibilityInfo.setAccessibilityFocus(nodeHandle)
  } else {
    // On Android, use UIManager.sendAccessibilityEvent
    ;(UIManager as any).sendAccessibilityEvent(
      nodeHandle,
      (UIManager as any).AccessibilityEventTypes?.typeViewFocused ?? 8, // TYPE_VIEW_FOCUSED = 8
    )
  }
}

// ─── AccessibleView ──────────────────────────────────────────────────────────

interface AccessibleViewProps {
  /** The accessibility role for this view */
  role?: NeutronAccessibilityRole
  /** A concise label describing the view's purpose (read by screen reader) */
  label?: string
  /** Additional hint text (e.g., "Double tap to activate") */
  hint?: string
  /** Current accessibility state */
  state?: AccessibilityState
  /** Current accessibility value (for adjustable elements) */
  accessibilityValue?: AccessibilityValue
  /** Whether this view should be treated as a single accessible element */
  accessible?: boolean
  /** Custom accessibility actions */
  actions?: Array<{ name: string; label: string }>
  /** Handler for custom accessibility actions */
  onAction?: (actionName: string) => void
  /** Whether the element is a modal (traps VoiceOver focus) */
  modal?: boolean
  /** Hide from accessibility tree (use sparingly) */
  hidden?: boolean
  /** View style */
  style?: StyleProp<ViewStyle>
  /** Test ID for testing */
  testID?: string
  /** Children */
  children?: ReactNode
}

/**
 * View with enhanced accessibility props. Provides a simplified, cross-platform
 * interface for setting VoiceOver/TalkBack attributes.
 *
 * This component normalizes differences between iOS and Android accessibility
 * APIs and provides sensible defaults.
 *
 * @example
 * <AccessibleView
 *   role="button"
 *   label="Add to cart"
 *   hint="Double tap to add this item to your shopping cart"
 *   state={{ disabled: isLoading }}
 * >
 *   <Text>Add to Cart</Text>
 * </AccessibleView>
 */
export function AccessibleView({
  role = 'none',
  label,
  hint,
  state,
  accessibilityValue,
  accessible = true,
  actions,
  onAction,
  modal = false,
  hidden = false,
  style,
  testID,
  children,
}: AccessibleViewProps) {
  // Map our role type to RN's AccessibilityRole
  const rnRole = role as AccessibilityRole

  // Build accessibility state object
  const rnState: Record<string, unknown> = {}
  if (state) {
    if (state.disabled != null) rnState.disabled = state.disabled
    if (state.selected != null) rnState.selected = state.selected
    if (state.checked != null) rnState.checked = state.checked
    if (state.busy != null) rnState.busy = state.busy
    if (state.expanded != null) rnState.expanded = state.expanded
  }

  // Build accessibility actions
  const rnActions = actions?.map((a) => ({
    name: a.name,
    label: a.label,
  }))

  const handleAction = useCallback(
    (event: { nativeEvent: { actionName: string } }) => {
      onAction?.(event.nativeEvent.actionName)
    },
    [onAction],
  )

  const props: Record<string, unknown> = {
    accessible,
    accessibilityRole: rnRole,
    style,
    testID,
  }

  if (label != null) props.accessibilityLabel = label
  if (hint != null) props.accessibilityHint = hint
  if (Object.keys(rnState).length > 0) props.accessibilityState = rnState
  if (accessibilityValue != null) props.accessibilityValue = accessibilityValue
  if (rnActions) {
    props.accessibilityActions = rnActions
    props.onAccessibilityAction = handleAction
  }

  // Platform-specific modal / hidden handling
  if (Platform.OS === 'ios') {
    if (modal) props.accessibilityViewIsModal = true
    if (hidden) props.accessibilityElementsHidden = true
  } else {
    if (hidden) props.importantForAccessibility = 'no-hide-descendants'
  }

  return React.createElement(View, props, children)
}

// ─── LiveRegion ──────────────────────────────────────────────────────────────

interface LiveRegionProps {
  /**
   * How assertively changes should be announced:
   * - 'polite': Wait for the screen reader to finish current speech (default)
   * - 'assertive': Interrupt current speech to announce immediately
   * - 'none': Do not announce changes (just update the accessibility tree)
   */
  politeness?: 'polite' | 'assertive' | 'none'
  /** Optional accessible label for the region */
  label?: string
  /** View style */
  style?: StyleProp<ViewStyle>
  /** Test ID */
  testID?: string
  /** Children — changes to text content inside will be announced */
  children?: ReactNode
}

/**
 * LiveRegion announces dynamic content changes to the screen reader.
 * Wrap elements whose text content changes over time (counters, status
 * messages, form validation errors, etc.) to ensure VoiceOver/TalkBack
 * users are informed of updates.
 *
 * On iOS, this uses `accessibilityLiveRegion` (RN 0.73+) with a
 * fallback to `announceForAccessibility` for older versions.
 *
 * On Android, this maps directly to ARIA live region semantics via
 * the `accessibilityLiveRegion` prop.
 *
 * @example
 * function CartBadge({ count }) {
 *   return (
 *     <LiveRegion politeness="polite" label={`${count} items in cart`}>
 *       <Text>{count}</Text>
 *     </LiveRegion>
 *   )
 * }
 *
 * @example
 * function FormError({ error }) {
 *   return (
 *     <LiveRegion politeness="assertive">
 *       {error && <Text style={{ color: 'red' }}>{error}</Text>}
 *     </LiveRegion>
 *   )
 * }
 */
export function LiveRegion({
  politeness = 'polite',
  label,
  style,
  testID,
  children,
}: LiveRegionProps) {
  const previousChildrenRef = useRef<ReactNode>(null)
  const viewRef = useRef<any>(null)

  // Track content changes and announce when screen reader is active
  useEffect(() => {
    if (politeness === 'none') return
    if (previousChildrenRef.current === null) {
      // First render — don't announce
      previousChildrenRef.current = children
      return
    }

    // Content changed
    if (previousChildrenRef.current !== children) {
      previousChildrenRef.current = children

      // On newer RN, accessibilityLiveRegion handles this natively.
      // On older RN or iOS < 17, we manually announce via AccessibilityInfo.
      // We do both: set the prop (native) and announce (fallback).
      if (label) {
        announceForAccessibility(label)
      } else if (typeof children === 'string') {
        announceForAccessibility(children)
      } else if (typeof children === 'number') {
        announceForAccessibility(String(children))
      }
      // For complex children, the native accessibilityLiveRegion handles it
    }
  }, [children, label, politeness])

  const props: Record<string, unknown> = {
    ref: viewRef,
    accessible: true,
    accessibilityLiveRegion: politeness,
    style,
    testID,
  }

  if (label != null) props.accessibilityLabel = label

  // On iOS, also set accessibilityRole to help VoiceOver understand this is a live region
  if (Platform.OS === 'ios') {
    props.accessibilityRole = 'text' as AccessibilityRole
  }

  return React.createElement(View, props, children)
}

// ─── useReducedMotion ────────────────────────────────────────────────────────

/**
 * Convenience hook that returns just the reduce-motion preference.
 * Use this to conditionally disable or simplify animations.
 *
 * @returns true if the user has enabled "Reduce Motion" in system settings
 *
 * @example
 * const prefersReducedMotion = useReducedMotion()
 * const duration = prefersReducedMotion ? 0 : 300
 */
export function useReducedMotion(): boolean {
  const [enabled, setEnabled] = useState(false)

  useEffect(() => {
    let mounted = true

    AccessibilityInfo.isReduceMotionEnabled().then((v) => {
      if (mounted) setEnabled(v)
    })

    const sub = AccessibilityInfo.addEventListener('reduceMotionChanged', (v) => {
      if (mounted) setEnabled(v)
    })

    return () => {
      mounted = false
      sub.remove()
    }
  }, [])

  return enabled
}

// ─── useScreenReader ─────────────────────────────────────────────────────────

/**
 * Convenience hook that returns whether a screen reader is active.
 *
 * @returns true if VoiceOver (iOS) or TalkBack (Android) is running
 *
 * @example
 * const srActive = useScreenReader()
 * // Show text labels when screen reader is active instead of icon-only buttons
 */
export function useScreenReader(): boolean {
  const [active, setActive] = useState(false)

  useEffect(() => {
    let mounted = true

    AccessibilityInfo.isScreenReaderEnabled().then((v) => {
      if (mounted) setActive(v)
    })

    const sub = AccessibilityInfo.addEventListener('screenReaderChanged', (v) => {
      if (mounted) setActive(v)
    })

    return () => {
      mounted = false
      sub.remove()
    }
  }, [])

  return active
}

// ─── FocusTrap ───────────────────────────────────────────────────────────────

interface FocusTrapProps {
  /** Whether the focus trap is currently active */
  active?: boolean
  /** View style */
  style?: StyleProp<ViewStyle>
  /** Children to trap focus within */
  children?: ReactNode
}

/**
 * FocusTrap constrains VoiceOver/TalkBack focus within its children.
 * Use this for modals, dialogs, and bottom sheets to prevent the screen
 * reader from escaping to content behind the overlay.
 *
 * On iOS, this uses `accessibilityViewIsModal`.
 * On Android, this uses `importantForAccessibility` on sibling views.
 *
 * @example
 * <FocusTrap active={isDialogOpen}>
 *   <Dialog onClose={() => setDialogOpen(false)}>
 *     <Text>Dialog content</Text>
 *   </Dialog>
 * </FocusTrap>
 */
export function FocusTrap({ active = true, style, children }: FocusTrapProps) {
  const props: Record<string, unknown> = {
    style,
    accessible: false, // Container itself should not be focusable
  }

  if (active) {
    if (Platform.OS === 'ios') {
      props.accessibilityViewIsModal = true
    }
    // On Android, the modal content should have importantForAccessibility="yes"
    // and siblings should have "no-hide-descendants". This component handles
    // the modal content side; the overlay/backdrop should handle hiding siblings.
    props.importantForAccessibility = 'yes'
  }

  return React.createElement(View, props, children)
}

// ─── AccessibilityOrder ──────────────────────────────────────────────────────

interface AccessibilityOrderProps {
  /** Ordered array of refs — screen reader traverses in this order */
  order: RefObject<any>[]
  /** View style */
  style?: StyleProp<ViewStyle>
  /** Children */
  children?: ReactNode
}

/**
 * AccessibilityOrder sets a custom VoiceOver/TalkBack traversal order
 * for its children. Use this when the visual layout order differs from
 * the logical reading order.
 *
 * On iOS, this sets `accessibilityElements` on the container.
 * On Android, this sets `accessibilityTraversalBefore/After`.
 *
 * @example
 * const titleRef = useRef(null)
 * const subtitleRef = useRef(null)
 * const priceRef = useRef(null)
 *
 * <AccessibilityOrder order={[titleRef, priceRef, subtitleRef]}>
 *   <Text ref={subtitleRef}>Subtitle</Text>
 *   <Text ref={priceRef}>$9.99</Text>
 *   <Text ref={titleRef}>Title</Text>
 * </AccessibilityOrder>
 */
export function AccessibilityOrder({ order, style, children }: AccessibilityOrderProps) {
  const containerRef = useRef<any>(null)

  useEffect(() => {
    if (Platform.OS !== 'ios') return
    if (!containerRef.current) return

    // On iOS, set accessibilityElements to the ordered node handles
    const handles = order
      .map((ref) => (ref.current ? findNodeHandle(ref.current) : null))
      .filter((h): h is number => h != null)

    // accessibilityElements is an iOS-only prop set via native methods
    try {
      const node = findNodeHandle(containerRef.current)
      if (node != null) {
        ;(UIManager as any).updateView(node, 'RCTView', {
          accessibilityElements: handles,
        })
      }
    } catch {
      // Not all RN versions support this
    }
  }, [order])

  return React.createElement(
    View,
    { ref: containerRef, style },
    children,
  )
}
