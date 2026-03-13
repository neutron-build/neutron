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

export const PADDING_TOKENS: StyleMap = Object.fromEntries(
  [0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60, 64, 72, 80, 96].flatMap(n => [
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
  [0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60, 64, 72, 80, 96].flatMap(n => [
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

export const SIZE_TOKENS: StyleMap = Object.fromEntries(
  [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60, 64, 72, 80, 96].flatMap(n => [
    [`w-${n}`, { width: spacing(n) }],
    [`h-${n}`, { height: spacing(n) }],
  ])
)

// ─── Typography ───────────────────────────────────────────────────────────────

export const TEXT_TOKENS: StyleMap = {
  'text-xs':   { fontSize: 12, lineHeight: 16 },
  'text-sm':   { fontSize: 14, lineHeight: 20 },
  'text-base': { fontSize: 16, lineHeight: 24 },
  'text-lg':   { fontSize: 18, lineHeight: 28 },
  'text-xl':   { fontSize: 20, lineHeight: 28 },
  'text-2xl':  { fontSize: 24, lineHeight: 32 },
  'text-3xl':  { fontSize: 30, lineHeight: 36 },
  'text-4xl':  { fontSize: 36, lineHeight: 40 },
  'font-thin':       { fontWeight: '100' },
  'font-light':      { fontWeight: '300' },
  'font-normal':     { fontWeight: '400' },
  'font-medium':     { fontWeight: '500' },
  'font-semibold':   { fontWeight: '600' },
  'font-bold':       { fontWeight: '700' },
  'font-extrabold':  { fontWeight: '800' },
  'font-black':      { fontWeight: '900' },
  'italic':          { fontStyle: 'italic' },
  'not-italic':      { fontStyle: 'normal' },
  'text-left':    { textAlign: 'left' },
  'text-center':  { textAlign: 'center' },
  'text-right':   { textAlign: 'right' },
  'uppercase':    { textTransform: 'uppercase' },
  'lowercase':    { textTransform: 'lowercase' },
  'capitalize':   { textTransform: 'capitalize' },
  'underline':    { textDecorationLine: 'underline' },
  'line-through': { textDecorationLine: 'line-through' },
  'no-underline': { textDecorationLine: 'none' },
}

// ─── Colors ───────────────────────────────────────────────────────────────────

// Core palette (Tailwind slate + minimal accent colors)
const COLORS: Record<string, string> = {
  'white': '#ffffff',
  'black': '#000000',
  'transparent': 'transparent',
  'slate-50': '#f8fafc',  'slate-100': '#f1f5f9',  'slate-200': '#e2e8f0',
  'slate-300': '#cbd5e1', 'slate-400': '#94a3b8',  'slate-500': '#64748b',
  'slate-600': '#475569', 'slate-700': '#334155',  'slate-800': '#1e293b',
  'slate-900': '#0f172a', 'slate-950': '#020617',
  'blue-50': '#eff6ff',   'blue-100': '#dbeafe',   'blue-500': '#3b82f6',
  'blue-600': '#2563eb',  'blue-700': '#1d4ed8',
  'red-50': '#fef2f2',    'red-500': '#ef4444',    'red-700': '#b91c1c',
  'green-50': '#f0fdf4',  'green-500': '#22c55e',  'green-700': '#15803d',
  'yellow-50': '#fefce8', 'yellow-500': '#eab308',
  'purple-500': '#a855f7', 'pink-500': '#ec4899',
}

export const COLOR_TOKENS: StyleMap = Object.fromEntries(
  Object.entries(COLORS).flatMap(([name, hex]) => [
    [`text-${name}`,    { color: hex }],
    [`bg-${name}`,      { backgroundColor: hex }],
    [`border-${name}`,  { borderColor: hex }],
  ])
)

// ─── Border ───────────────────────────────────────────────────────────────────

export const BORDER_TOKENS: StyleMap = {
  'border':     { borderWidth: 1 },
  'border-0':   { borderWidth: 0 },
  'border-2':   { borderWidth: 2 },
  'border-4':   { borderWidth: 4 },
  'border-t':   { borderTopWidth: 1 },
  'border-b':   { borderBottomWidth: 1 },
  'border-l':   { borderLeftWidth: 1 },
  'border-r':   { borderRightWidth: 1 },
  'rounded':    { borderRadius: 4 },
  'rounded-sm': { borderRadius: 2 },
  'rounded-md': { borderRadius: 6 },
  'rounded-lg': { borderRadius: 8 },
  'rounded-xl': { borderRadius: 12 },
  'rounded-2xl':{ borderRadius: 16 },
  'rounded-full':{ borderRadius: 9999 },
  'rounded-none':{ borderRadius: 0 },
}

// ─── Opacity / overflow ───────────────────────────────────────────────────────

export const MISC_TOKENS: StyleMap = {
  'opacity-0':   { opacity: 0 },
  'opacity-25':  { opacity: 0.25 },
  'opacity-50':  { opacity: 0.5 },
  'opacity-75':  { opacity: 0.75 },
  'opacity-100': { opacity: 1 },
  'overflow-hidden':  { overflow: 'hidden' },
  'overflow-visible': { overflow: 'visible' },
  'overflow-scroll':  { overflow: 'scroll' },
  'absolute': { position: 'absolute' },
  'relative': { position: 'relative' },
  'inset-0':  { top: 0, right: 0, bottom: 0, left: 0 },
  'hidden':   { display: 'none' },
  'shadow':   { shadowColor: '#000', shadowOffset: { width: 0, height: 1 }, shadowOpacity: 0.2, shadowRadius: 2, elevation: 2 },
  'shadow-md':{ shadowColor: '#000', shadowOffset: { width: 0, height: 2 }, shadowOpacity: 0.25, shadowRadius: 4, elevation: 4 },
  'shadow-lg':{ shadowColor: '#000', shadowOffset: { width: 0, height: 4 }, shadowOpacity: 0.3, shadowRadius: 8, elevation: 8 },
  'shadow-none': { shadowColor: 'transparent', shadowOpacity: 0, elevation: 0 },
}

// ─── Master token map ─────────────────────────────────────────────────────────

export const ALL_TOKENS: StyleMap = {
  ...PADDING_TOKENS,
  ...MARGIN_TOKENS,
  ...FLEX_TOKENS,
  ...SIZE_TOKENS,
  ...TEXT_TOKENS,
  ...COLOR_TOKENS,
  ...BORDER_TOKENS,
  ...MISC_TOKENS,
}

/**
 * Resolve a space-separated className string to a merged NativeStyleProp.
 * Used at runtime as fallback (build-time plugin is preferred).
 */
export function resolveClassName(className: string): StyleProp {
  const classes = className.trim().split(/\s+/)
  const result: Record<string, unknown> = {}
  for (const cls of classes) {
    const token = ALL_TOKENS[cls]
    if (token) Object.assign(result, token)
  }
  return result as StyleProp
}
