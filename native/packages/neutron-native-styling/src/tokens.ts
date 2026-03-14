/**
 * NeutronWind design tokens.
 *
 * Maps Tailwind-style class names to React Native style values.
 * Only a subset of Tailwind is supported — the intersection of what
 * CSS and React Native StyleSheet have in common.
 *
 * Classes are resolved at BUILD TIME by the Babel/Rspack plugin.
 * Zero runtime overhead — no className parsing at runtime.
 */

import { Dimensions } from 'react-native'

/** Mirrors NativeStyleProp from @neutron/native — avoid circular dep */
export type StyleProp = {
  [key: string]: string | number | null | { width?: number; height?: number } | undefined
}

type StyleMap = Record<string, StyleProp>

// ─── Spacing ──────────────────────────────────────────────────────────────────

const SPACING_BASE = 4 // 1 unit = 4px (matches Tailwind default)

function spacing(value: number): number {
  return value * SPACING_BASE
}

const SPACING_SCALE = [0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60, 64, 72, 80, 96]

export const PADDING_TOKENS: StyleMap = Object.fromEntries(
  SPACING_SCALE.flatMap(n => [
    [`p-${n}`,  { padding: spacing(n) }],
    [`px-${n}`, { paddingHorizontal: spacing(n) }],
    [`py-${n}`, { paddingVertical: spacing(n) }],
    [`pt-${n}`, { paddingTop: spacing(n) }],
    [`pb-${n}`, { paddingBottom: spacing(n) }],
    [`pl-${n}`, { paddingLeft: spacing(n) }],
    [`pr-${n}`, { paddingRight: spacing(n) }],
  ])
)

export const MARGIN_TOKENS: StyleMap = Object.fromEntries(
  SPACING_SCALE.flatMap(n => [
    [`m-${n}`,  { margin: spacing(n) }],
    [`mx-${n}`, { marginHorizontal: spacing(n) }],
    [`my-${n}`, { marginVertical: spacing(n) }],
    [`mt-${n}`, { marginTop: spacing(n) }],
    [`mb-${n}`, { marginBottom: spacing(n) }],
    [`ml-${n}`, { marginLeft: spacing(n) }],
    [`mr-${n}`, { marginRight: spacing(n) }],
  ])
)

// ─── Flex ─────────────────────────────────────────────────────────────────────

export const FLEX_TOKENS: StyleMap = {
  'flex':        { flex: 1 },
  'flex-1':      { flex: 1 },
  'flex-auto':   { flex: 1 },
  'flex-none':   { flex: 0 },
  'flex-row':    { flexDirection: 'row' },
  'flex-col':    { flexDirection: 'column' },
  'flex-wrap':   { flexWrap: 'wrap' },
  'flex-nowrap': { flexWrap: 'nowrap' },
  'flex-grow':     { flexGrow: 1 },
  'flex-grow-0':   { flexGrow: 0 },
  'flex-shrink':   { flexShrink: 1 },
  'flex-shrink-0': { flexShrink: 0 },
  // Flex basis
  'basis-auto': { flexBasis: 'auto' as unknown as number },
  'basis-full': { flexBasis: '100%' as unknown as number },
  ...Object.fromEntries(
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12].map(n => [
      `basis-${n}`, { flexBasis: spacing(n) },
    ])
  ),
  // Alignment
  'items-start':    { alignItems: 'flex-start' },
  'items-end':      { alignItems: 'flex-end' },
  'items-center':   { alignItems: 'center' },
  'items-stretch':  { alignItems: 'stretch' },
  'items-baseline': { alignItems: 'baseline' },
  'justify-start':   { justifyContent: 'flex-start' },
  'justify-end':     { justifyContent: 'flex-end' },
  'justify-center':  { justifyContent: 'center' },
  'justify-between': { justifyContent: 'space-between' },
  'justify-around':  { justifyContent: 'space-around' },
  'justify-evenly':  { justifyContent: 'space-evenly' },
  'self-auto':    { alignSelf: 'auto' },
  'self-start':   { alignSelf: 'flex-start' },
  'self-end':     { alignSelf: 'flex-end' },
  'self-center':  { alignSelf: 'center' },
  'self-stretch': { alignSelf: 'stretch' },
}

// ─── Sizing ───────────────────────────────────────────────────────────────────

export const SIZE_TOKENS: StyleMap = {
  ...Object.fromEntries(
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60, 64, 72, 80, 96].flatMap(n => [
      [`w-${n}`, { width: spacing(n) }],
      [`h-${n}`, { height: spacing(n) }],
    ])
  ),
  // Full / screen sizes
  'w-full':   { width: '100%' as unknown as number },
  'w-screen': { width: Dimensions.get('window').width },
  'h-full':   { height: '100%' as unknown as number },
  'h-screen': { height: Dimensions.get('window').height },
  // Min / max width
  'min-w-0':    { minWidth: 0 },
  'min-w-full': { minWidth: '100%' as unknown as number },
  'max-w-full':   { maxWidth: '100%' as unknown as number },
  'max-w-screen': { maxWidth: Dimensions.get('window').width },
  // Min / max height
  'min-h-0':      { minHeight: 0 },
  'min-h-full':   { minHeight: '100%' as unknown as number },
  'max-h-full':   { maxHeight: '100%' as unknown as number },
  'max-h-screen': { maxHeight: Dimensions.get('window').height },
  // Aspect ratio
  'aspect-square': { aspectRatio: 1 },
  'aspect-video':  { aspectRatio: 16 / 9 },
}

// ─── Layout ───────────────────────────────────────────────────────────────────

export const LAYOUT_TOKENS: StyleMap = {
  // Gap
  ...Object.fromEntries(
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12].map(n => [
      `gap-${n}`, { gap: spacing(n) },
    ])
  ),
  // Z-index
  'z-0':  { zIndex: 0 },
  'z-10': { zIndex: 10 },
  'z-20': { zIndex: 20 },
  'z-30': { zIndex: 30 },
  'z-40': { zIndex: 40 },
  'z-50': { zIndex: 50 },
  // Position
  'absolute': { position: 'absolute' },
  'relative': { position: 'relative' },
  // Inset
  'inset-0':   { top: 0, right: 0, bottom: 0, left: 0 },
  'inset-x-0': { left: 0, right: 0 },
  'inset-y-0': { top: 0, bottom: 0 },
  // Positional offsets
  ...Object.fromEntries(
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12].flatMap(n => [
      [`top-${n}`,    { top: spacing(n) }],
      [`right-${n}`,  { right: spacing(n) }],
      [`bottom-${n}`, { bottom: spacing(n) }],
      [`left-${n}`,   { left: spacing(n) }],
    ])
  ),
}

// ─── Typography ───────────────────────────────────────────────────────────────

export const TEXT_TOKENS: StyleMap = {
  // Font sizes
  'text-xs':   { fontSize: 12, lineHeight: 16 },
  'text-sm':   { fontSize: 14, lineHeight: 20 },
  'text-base': { fontSize: 16, lineHeight: 24 },
  'text-lg':   { fontSize: 18, lineHeight: 28 },
  'text-xl':   { fontSize: 20, lineHeight: 28 },
  'text-2xl':  { fontSize: 24, lineHeight: 32 },
  'text-3xl':  { fontSize: 30, lineHeight: 36 },
  'text-4xl':  { fontSize: 36, lineHeight: 40 },
  // Font weight — named
  'font-thin':       { fontWeight: '100' },
  'font-light':      { fontWeight: '300' },
  'font-normal':     { fontWeight: '400' },
  'font-medium':     { fontWeight: '500' },
  'font-semibold':   { fontWeight: '600' },
  'font-bold':       { fontWeight: '700' },
  'font-extrabold':  { fontWeight: '800' },
  'font-black':      { fontWeight: '900' },
  // Font weight — numeric
  'font-100': { fontWeight: '100' },
  'font-200': { fontWeight: '200' },
  'font-300': { fontWeight: '300' },
  'font-400': { fontWeight: '400' },
  'font-500': { fontWeight: '500' },
  'font-600': { fontWeight: '600' },
  'font-700': { fontWeight: '700' },
  'font-800': { fontWeight: '800' },
  'font-900': { fontWeight: '900' },
  // Font style
  'italic':          { fontStyle: 'italic' },
  'not-italic':      { fontStyle: 'normal' },
  // Text alignment
  'text-left':    { textAlign: 'left' },
  'text-center':  { textAlign: 'center' },
  'text-right':   { textAlign: 'right' },
  // Text transform
  'uppercase':    { textTransform: 'uppercase' },
  'lowercase':    { textTransform: 'lowercase' },
  'capitalize':   { textTransform: 'capitalize' },
  'normal-case':  { textTransform: 'none' },
  // Text decoration
  'underline':    { textDecorationLine: 'underline' },
  'line-through': { textDecorationLine: 'line-through' },
  'no-underline': { textDecorationLine: 'none' },
  // Text ellipsis (RN-specific — signals single-line truncation)
  'text-ellipsis': { numberOfLines: 1 } as unknown as StyleProp,
  // Line height
  'leading-none':    { lineHeight: 1 },
  'leading-tight':   { lineHeight: 1.25 },
  'leading-snug':    { lineHeight: 1.375 },
  'leading-normal':  { lineHeight: 1.5 },
  'leading-relaxed': { lineHeight: 1.625 },
  'leading-loose':   { lineHeight: 2 },
  // Letter spacing
  'tracking-tighter': { letterSpacing: -0.8 },
  'tracking-tight':   { letterSpacing: -0.4 },
  'tracking-normal':  { letterSpacing: 0 },
  'tracking-wide':    { letterSpacing: 0.4 },
  'tracking-wider':   { letterSpacing: 0.8 },
  'tracking-widest':  { letterSpacing: 1.6 },
}

// ─── Colors ───────────────────────────────────────────────────────────────────

// Full Tailwind CSS color palette with standard shade scales (50–950).
// Each color name maps to its hex value, used to generate bg-*, text-*, and border-* tokens.
const COLORS: Record<string, string> = {
  // ── Special ──
  'white': '#ffffff',
  'black': '#000000',
  'transparent': 'transparent',

  // ── Slate ──
  'slate-50':  '#f8fafc', 'slate-100': '#f1f5f9', 'slate-200': '#e2e8f0',
  'slate-300': '#cbd5e1', 'slate-400': '#94a3b8', 'slate-500': '#64748b',
  'slate-600': '#475569', 'slate-700': '#334155', 'slate-800': '#1e293b',
  'slate-900': '#0f172a', 'slate-950': '#020617',

  // ── Gray ──
  'gray-50':  '#f9fafb', 'gray-100': '#f3f4f6', 'gray-200': '#e5e7eb',
  'gray-300': '#d1d5db', 'gray-400': '#9ca3af', 'gray-500': '#6b7280',
  'gray-600': '#4b5563', 'gray-700': '#374151', 'gray-800': '#1f2937',
  'gray-900': '#111827', 'gray-950': '#030712',

  // ── Zinc ──
  'zinc-50':  '#fafafa', 'zinc-100': '#f4f4f5', 'zinc-200': '#e4e4e7',
  'zinc-300': '#d4d4d8', 'zinc-400': '#a1a1aa', 'zinc-500': '#71717a',
  'zinc-600': '#52525b', 'zinc-700': '#3f3f46', 'zinc-800': '#27272a',
  'zinc-900': '#18181b', 'zinc-950': '#09090b',

  // ── Neutral ──
  'neutral-50':  '#fafafa', 'neutral-100': '#f5f5f5', 'neutral-200': '#e5e5e5',
  'neutral-300': '#d4d4d4', 'neutral-400': '#a3a3a3', 'neutral-500': '#737373',
  'neutral-600': '#525252', 'neutral-700': '#404040', 'neutral-800': '#262626',
  'neutral-900': '#171717', 'neutral-950': '#0a0a0a',

  // ── Stone ──
  'stone-50':  '#fafaf9', 'stone-100': '#f5f5f4', 'stone-200': '#e7e5e4',
  'stone-300': '#d6d3d1', 'stone-400': '#a8a29e', 'stone-500': '#78716c',
  'stone-600': '#57534e', 'stone-700': '#44403c', 'stone-800': '#292524',
  'stone-900': '#1c1917', 'stone-950': '#0c0a09',

  // ── Red ──
  'red-50':  '#fef2f2', 'red-100': '#fee2e2', 'red-200': '#fecaca',
  'red-300': '#fca5a5', 'red-400': '#f87171', 'red-500': '#ef4444',
  'red-600': '#dc2626', 'red-700': '#b91c1c', 'red-800': '#991b1b',
  'red-900': '#7f1d1d', 'red-950': '#450a0a',

  // ── Orange ──
  'orange-50':  '#fff7ed', 'orange-100': '#ffedd5', 'orange-200': '#fed7aa',
  'orange-300': '#fdba74', 'orange-400': '#fb923c', 'orange-500': '#f97316',
  'orange-600': '#ea580c', 'orange-700': '#c2410c', 'orange-800': '#9a3412',
  'orange-900': '#7c2d12', 'orange-950': '#431407',

  // ── Amber ──
  'amber-50':  '#fffbeb', 'amber-100': '#fef3c7', 'amber-200': '#fde68a',
  'amber-300': '#fcd34d', 'amber-400': '#fbbf24', 'amber-500': '#f59e0b',
  'amber-600': '#d97706', 'amber-700': '#b45309', 'amber-800': '#92400e',
  'amber-900': '#78350f', 'amber-950': '#451a03',

  // ── Yellow ──
  'yellow-50':  '#fefce8', 'yellow-100': '#fef9c3', 'yellow-200': '#fef08a',
  'yellow-300': '#fde047', 'yellow-400': '#facc15', 'yellow-500': '#eab308',
  'yellow-600': '#ca8a04', 'yellow-700': '#a16207', 'yellow-800': '#854d0e',
  'yellow-900': '#713f12', 'yellow-950': '#422006',

  // ── Lime ──
  'lime-50':  '#f7fee7', 'lime-100': '#ecfccb', 'lime-200': '#d9f99d',
  'lime-300': '#bef264', 'lime-400': '#a3e635', 'lime-500': '#84cc16',
  'lime-600': '#65a30d', 'lime-700': '#4d7c0f', 'lime-800': '#3f6212',
  'lime-900': '#365314', 'lime-950': '#1a2e05',

  // ── Green ──
  'green-50':  '#f0fdf4', 'green-100': '#dcfce7', 'green-200': '#bbf7d0',
  'green-300': '#86efac', 'green-400': '#4ade80', 'green-500': '#22c55e',
  'green-600': '#16a34a', 'green-700': '#15803d', 'green-800': '#166534',
  'green-900': '#14532d', 'green-950': '#052e16',

  // ── Emerald ──
  'emerald-50':  '#ecfdf5', 'emerald-100': '#d1fae5', 'emerald-200': '#a7f3d0',
  'emerald-300': '#6ee7b7', 'emerald-400': '#34d399', 'emerald-500': '#10b981',
  'emerald-600': '#059669', 'emerald-700': '#047857', 'emerald-800': '#065f46',
  'emerald-900': '#064e3b', 'emerald-950': '#022c22',

  // ── Teal ──
  'teal-50':  '#f0fdfa', 'teal-100': '#ccfbf1', 'teal-200': '#99f6e4',
  'teal-300': '#5eead4', 'teal-400': '#2dd4bf', 'teal-500': '#14b8a6',
  'teal-600': '#0d9488', 'teal-700': '#0f766e', 'teal-800': '#115e59',
  'teal-900': '#134e4a', 'teal-950': '#042f2e',

  // ── Cyan ──
  'cyan-50':  '#ecfeff', 'cyan-100': '#cffafe', 'cyan-200': '#a5f3fc',
  'cyan-300': '#67e8f9', 'cyan-400': '#22d3ee', 'cyan-500': '#06b6d4',
  'cyan-600': '#0891b2', 'cyan-700': '#0e7490', 'cyan-800': '#155e75',
  'cyan-900': '#164e63', 'cyan-950': '#083344',

  // ── Sky ──
  'sky-50':  '#f0f9ff', 'sky-100': '#e0f2fe', 'sky-200': '#bae6fd',
  'sky-300': '#7dd3fc', 'sky-400': '#38bdf8', 'sky-500': '#0ea5e9',
  'sky-600': '#0284c7', 'sky-700': '#0369a1', 'sky-800': '#075985',
  'sky-900': '#0c4a6e', 'sky-950': '#082f49',

  // ── Blue ──
  'blue-50':  '#eff6ff', 'blue-100': '#dbeafe', 'blue-200': '#bfdbfe',
  'blue-300': '#93c5fd', 'blue-400': '#60a5fa', 'blue-500': '#3b82f6',
  'blue-600': '#2563eb', 'blue-700': '#1d4ed8', 'blue-800': '#1e40af',
  'blue-900': '#1e3a8a', 'blue-950': '#172554',

  // ── Indigo ──
  'indigo-50':  '#eef2ff', 'indigo-100': '#e0e7ff', 'indigo-200': '#c7d2fe',
  'indigo-300': '#a5b4fc', 'indigo-400': '#818cf8', 'indigo-500': '#6366f1',
  'indigo-600': '#4f46e5', 'indigo-700': '#4338ca', 'indigo-800': '#3730a3',
  'indigo-900': '#312e81', 'indigo-950': '#1e1b4b',

  // ── Violet ──
  'violet-50':  '#f5f3ff', 'violet-100': '#ede9fe', 'violet-200': '#ddd6fe',
  'violet-300': '#c4b5fd', 'violet-400': '#a78bfa', 'violet-500': '#8b5cf6',
  'violet-600': '#7c3aed', 'violet-700': '#6d28d9', 'violet-800': '#5b21b6',
  'violet-900': '#4c1d95', 'violet-950': '#2e1065',

  // ── Purple ──
  'purple-50':  '#faf5ff', 'purple-100': '#f3e8ff', 'purple-200': '#e9d5ff',
  'purple-300': '#d8b4fe', 'purple-400': '#c084fc', 'purple-500': '#a855f7',
  'purple-600': '#9333ea', 'purple-700': '#7e22ce', 'purple-800': '#6b21a8',
  'purple-900': '#581c87', 'purple-950': '#3b0764',

  // ── Fuchsia ──
  'fuchsia-50':  '#fdf4ff', 'fuchsia-100': '#fae8ff', 'fuchsia-200': '#f5d0fe',
  'fuchsia-300': '#f0abfc', 'fuchsia-400': '#e879f9', 'fuchsia-500': '#d946ef',
  'fuchsia-600': '#c026d3', 'fuchsia-700': '#a21caf', 'fuchsia-800': '#86198f',
  'fuchsia-900': '#701a75', 'fuchsia-950': '#4a044e',

  // ── Pink ──
  'pink-50':  '#fdf2f8', 'pink-100': '#fce7f3', 'pink-200': '#fbcfe8',
  'pink-300': '#f9a8d4', 'pink-400': '#f472b6', 'pink-500': '#ec4899',
  'pink-600': '#db2777', 'pink-700': '#be185d', 'pink-800': '#9d174d',
  'pink-900': '#831843', 'pink-950': '#500724',

  // ── Rose ──
  'rose-50':  '#fff1f2', 'rose-100': '#ffe4e6', 'rose-200': '#fecdd3',
  'rose-300': '#fda4af', 'rose-400': '#fb7185', 'rose-500': '#f43f5e',
  'rose-600': '#e11d48', 'rose-700': '#be123c', 'rose-800': '#9f1239',
  'rose-900': '#881337', 'rose-950': '#4c0519',
}

export const COLOR_TOKENS: StyleMap = Object.fromEntries(
  Object.entries(COLORS).flatMap(([name, hex]) => [
    [`text-${name}`,    { color: hex }],
    [`bg-${name}`,      { backgroundColor: hex }],
    [`border-${name}`,  { borderColor: hex }],
  ])
)

// ─── Border ───────────────────────────────────────────────────────────────────

const BORDER_RADIUS_SCALE: Record<string, number> = {
  'sm': 2,
  'md': 6,
  'lg': 8,
}

export const BORDER_TOKENS: StyleMap = {
  // Border width
  'border':     { borderWidth: 1 },
  'border-0':   { borderWidth: 0 },
  'border-2':   { borderWidth: 2 },
  'border-4':   { borderWidth: 4 },
  // Directional border width
  'border-t':   { borderTopWidth: 1 },
  'border-b':   { borderBottomWidth: 1 },
  'border-l':   { borderLeftWidth: 1 },
  'border-r':   { borderRightWidth: 1 },
  ...Object.fromEntries(
    [0, 1, 2, 3, 4].flatMap(n => [
      [`border-t-${n}`, { borderTopWidth: n }],
      [`border-b-${n}`, { borderBottomWidth: n }],
      [`border-l-${n}`, { borderLeftWidth: n }],
      [`border-r-${n}`, { borderRightWidth: n }],
    ])
  ),
  // Border radius — uniform
  'rounded':      { borderRadius: 4 },
  'rounded-sm':   { borderRadius: 2 },
  'rounded-md':   { borderRadius: 6 },
  'rounded-lg':   { borderRadius: 8 },
  'rounded-xl':   { borderRadius: 12 },
  'rounded-2xl':  { borderRadius: 16 },
  'rounded-full': { borderRadius: 9999 },
  'rounded-none': { borderRadius: 0 },
  // Border radius — directional top (topLeft + topRight)
  ...Object.fromEntries(
    Object.entries(BORDER_RADIUS_SCALE).flatMap(([name, value]) => [
      [`rounded-t-${name}`, { borderTopLeftRadius: value, borderTopRightRadius: value }],
      [`rounded-b-${name}`, { borderBottomLeftRadius: value, borderBottomRightRadius: value }],
    ])
  ),
}

// ─── Opacity / overflow / misc ────────────────────────────────────────────────

export const MISC_TOKENS: StyleMap = {
  // Opacity — standard Tailwind stops
  'opacity-0':   { opacity: 0 },
  'opacity-5':   { opacity: 0.05 },
  'opacity-10':  { opacity: 0.1 },
  'opacity-20':  { opacity: 0.2 },
  'opacity-25':  { opacity: 0.25 },
  'opacity-30':  { opacity: 0.3 },
  'opacity-40':  { opacity: 0.4 },
  'opacity-50':  { opacity: 0.5 },
  'opacity-60':  { opacity: 0.6 },
  'opacity-70':  { opacity: 0.7 },
  'opacity-75':  { opacity: 0.75 },
  'opacity-80':  { opacity: 0.8 },
  'opacity-90':  { opacity: 0.9 },
  'opacity-95':  { opacity: 0.95 },
  'opacity-100': { opacity: 1 },
  // Overflow
  'overflow-hidden':  { overflow: 'hidden' },
  'overflow-visible': { overflow: 'visible' },
  'overflow-scroll':  { overflow: 'scroll' },
  // Display
  'hidden': { display: 'none' },
  // Shadows
  'shadow':      { shadowColor: '#000', shadowOffset: { width: 0, height: 1 }, shadowOpacity: 0.2, shadowRadius: 2, elevation: 2 },
  'shadow-md':   { shadowColor: '#000', shadowOffset: { width: 0, height: 2 }, shadowOpacity: 0.25, shadowRadius: 4, elevation: 4 },
  'shadow-lg':   { shadowColor: '#000', shadowOffset: { width: 0, height: 4 }, shadowOpacity: 0.3, shadowRadius: 8, elevation: 8 },
  'shadow-none': { shadowColor: 'transparent', shadowOpacity: 0, elevation: 0 },
}

// ─── Master token map ─────────────────────────────────────────────────────────

export const ALL_TOKENS: StyleMap = {
  ...PADDING_TOKENS,
  ...MARGIN_TOKENS,
  ...FLEX_TOKENS,
  ...SIZE_TOKENS,
  ...LAYOUT_TOKENS,
  ...TEXT_TOKENS,
  ...COLOR_TOKENS,
  ...BORDER_TOKENS,
  ...MISC_TOKENS,
}

// ─── Arbitrary value support ──────────────────────────────────────────────────

// Maps class prefixes (e.g. 'w', 'h', 'p') to React Native style property names.
const ARBITRARY_PREFIX_MAP: Record<string, string> = {
  'w':       'width',
  'h':       'height',
  'p':       'padding',
  'px':      'paddingHorizontal',
  'py':      'paddingVertical',
  'pt':      'paddingTop',
  'pb':      'paddingBottom',
  'pl':      'paddingLeft',
  'pr':      'paddingRight',
  'm':       'margin',
  'mx':      'marginHorizontal',
  'my':      'marginVertical',
  'mt':      'marginTop',
  'mb':      'marginBottom',
  'ml':      'marginLeft',
  'mr':      'marginRight',
  'top':     'top',
  'right':   'right',
  'bottom':  'bottom',
  'left':    'left',
  'text':    'color',
  'bg':      'backgroundColor',
  'rounded': 'borderRadius',
  'leading': 'lineHeight',
  'tracking':'letterSpacing',
  'gap':     'gap',
  'basis':   'flexBasis',
  'min-w':   'minWidth',
  'max-w':   'maxWidth',
  'min-h':   'minHeight',
  'max-h':   'maxHeight',
  'border':  'borderWidth',
  'z':       'zIndex',
}

const ARBITRARY_RE = /^(w|h|p|px|py|pt|pb|pl|pr|m|mx|my|mt|mb|ml|mr|top|right|bottom|left|text|bg|rounded|leading|tracking|gap|basis|min-w|max-w|min-h|max-h|border|z)-\[(.+)\]$/

/**
 * Parse an arbitrary value class like `w-[42px]` or `bg-[#ff0000]`.
 *
 * Returns the corresponding RN style object, or null if the class
 * does not match the arbitrary value pattern.
 */
export function parseArbitraryValue(cls: string): StyleProp | null {
  const match = cls.match(ARBITRARY_RE)
  if (!match) return null

  const prefix = match[1]
  const rawValue = match[2]
  const prop = ARBITRARY_PREFIX_MAP[prefix]
  if (!prop) return null

  // For text-[...] prefixes, color-like values produce `color`, but
  // font-size-like values (e.g. `text-[18px]`) produce `fontSize`.
  if (prefix === 'text') {
    const isColor = rawValue.startsWith('#') || rawValue.startsWith('rgb') || rawValue.startsWith('hsl')
    if (!isColor) {
      // Treat as fontSize
      return { fontSize: _parseValue(rawValue) } as StyleProp
    }
  }

  return { [prop]: _parseValue(rawValue) } as StyleProp
}

/**
 * Convert a raw arbitrary value string to a number or string.
 * - '42px' → 42
 * - '100%' → '100%'
 * - '12' → 12
 * - '#ff0000' → '#ff0000'
 * - 'rgba(0,0,0,0.5)' → 'rgba(0,0,0,0.5)'
 */
function _parseValue(raw: string): string | number {
  // Strip 'px' suffix and convert to number
  if (raw.endsWith('px')) {
    const num = Number(raw.slice(0, -2))
    return Number.isNaN(num) ? raw : num
  }
  // Plain numeric
  if (/^-?\d+(\.\d+)?$/.test(raw)) {
    return Number(raw)
  }
  // Everything else (%, #hex, rgb(), hsl()) stays as string
  return raw
}

// ─── Opacity modifier support ─────────────────────────────────────────────────

/**
 * Convert a hex color (#rgb or #rrggbb) to rgba with the given opacity (0–100).
 */
export function hexToRgba(hex: string, opacity: number): string {
  let r: number, g: number, b: number

  const clean = hex.replace('#', '')
  if (clean.length === 3) {
    // Short-hand #rgb
    r = parseInt(clean[0] + clean[0], 16)
    g = parseInt(clean[1] + clean[1], 16)
    b = parseInt(clean[2] + clean[2], 16)
  } else {
    r = parseInt(clean.slice(0, 2), 16)
    g = parseInt(clean.slice(2, 4), 16)
    b = parseInt(clean.slice(4, 6), 16)
  }

  return `rgba(${r},${g},${b},${opacity / 100})`
}

/**
 * Parse opacity modifier classes like `bg-blue-500/50` or `text-white/80`.
 *
 * Splits on '/', looks up the base token, and converts the hex color
 * to rgba with the specified opacity.
 *
 * Returns null if the class doesn't match or the base token has no color.
 */
export function parseOpacityModifier(cls: string): StyleProp | null {
  const slashIdx = cls.lastIndexOf('/')
  if (slashIdx === -1) return null

  const base = cls.slice(0, slashIdx)
  const opacityStr = cls.slice(slashIdx + 1)
  const opacity = Number(opacityStr)
  if (Number.isNaN(opacity) || opacity < 0 || opacity > 100) return null

  // Look up the base class in our token map
  const token = ALL_TOKENS[base]
  if (!token) return null

  // Find the color property in the token
  const colorProps = ['color', 'backgroundColor', 'borderColor'] as const
  for (const prop of colorProps) {
    const value = token[prop]
    if (typeof value === 'string' && value.startsWith('#')) {
      return { [prop]: hexToRgba(value, opacity) } as StyleProp
    }
  }

  return null
}

// ─── Memoized className resolver ──────────────────────────────────────────────

const _classNameCache = new Map<string, Record<string, unknown>>()

/**
 * Clear the className resolution cache. Useful for testing.
 */
export function clearClassNameCache(): void {
  _classNameCache.clear()
}

/**
 * Resolve a space-separated className string to a merged NativeStyleProp.
 * Used at runtime as fallback (build-time plugin is preferred).
 *
 * Results are frozen and cached (up to 10,000 entries) to avoid
 * redundant allocations on repeated renders.
 */
export function resolveClassName(className: string): StyleProp {
  const cached = _classNameCache.get(className)
  if (cached) return cached as StyleProp

  const classes = className.trim().split(/\s+/)
  const result: Record<string, unknown> = {}
  for (const cls of classes) {
    const style = ALL_TOKENS[cls] ?? parseArbitraryValue(cls) ?? parseOpacityModifier(cls)
    if (style) Object.assign(result, style)
  }

  // Freeze to prevent mutation, cache for reuse
  Object.freeze(result)
  if (_classNameCache.size < 10000) {
    _classNameCache.set(className, result)
  }
  return result as StyleProp
}
