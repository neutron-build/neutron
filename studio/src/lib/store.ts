import { signal, computed } from '@preact/signals'
import type { Connection, Schema, NucleusFeatures, Tab, PendingChange } from './types'

// --- Connection state ---

export const connections = signal<Connection[]>([])
export const activeConnection = signal<Connection | null>(null)
export const connectionLoading = signal(false)
export const connectionError = signal<string | null>(null)

// --- Nucleus feature detection ---

export const features = signal<NucleusFeatures>({
  isNucleus: false,
  version: '',
  models: [],
})

export const isNucleus = computed(() => features.value.isNucleus)

// --- Schema ---

export const schema = signal<Schema | null>(null)
export const schemaLoading = signal(false)

// --- Tabs ---

export const tabs = signal<Tab[]>([])
export const activeTabId = signal<string | null>(null)

export const activeTab = computed(() =>
  tabs.value.find(t => t.id === activeTabId.value) ?? null
)

export function openTab(tab: Tab) {
  const existing = tabs.value.find(t =>
    t.kind === tab.kind &&
    t.objectSchema === tab.objectSchema &&
    t.objectName === tab.objectName
  )
  if (existing) {
    activeTabId.value = existing.id
    return
  }
  tabs.value = [...tabs.value, tab]
  activeTabId.value = tab.id
}

export function closeTab(id: string) {
  const idx = tabs.value.findIndex(t => t.id === id)
  tabs.value = tabs.value.filter(t => t.id !== id)
  if (activeTabId.value === id) {
    const next = tabs.value[Math.max(0, idx - 1)]
    activeTabId.value = next?.id ?? null
  }
}

// --- Pending changes (commit bar) ---

export const pendingChanges = signal<PendingChange[]>([])

export const pendingCount = computed(() => pendingChanges.value.length)

export function addPending(change: PendingChange) {
  pendingChanges.value = [...pendingChanges.value, change]
}

export function removePending(id: string) {
  pendingChanges.value = pendingChanges.value.filter(c => c.id !== id)
}

export function revertLast() {
  const last = pendingChanges.value[pendingChanges.value.length - 1]
  if (!last) return
  last.revert()
  pendingChanges.value = pendingChanges.value.slice(0, -1)
}

export function clearPending() {
  pendingChanges.value = []
}

// --- Theme ---

const storedTheme = typeof localStorage !== 'undefined'
  ? (localStorage.getItem('studio-theme') as 'dark' | 'light' | null)
  : null

export const theme = signal<'dark' | 'light'>(storedTheme ?? 'dark')

theme.subscribe(t => {
  document.documentElement.setAttribute('data-theme', t)
  if (typeof localStorage !== 'undefined') {
    localStorage.setItem('studio-theme', t)
  }
})

export function toggleTheme() {
  theme.value = theme.value === 'dark' ? 'light' : 'dark'
}

// --- Command palette ---

export const paletteOpen = signal(false)
export const paletteQuery = signal('')

export function openPalette() {
  paletteQuery.value = ''
  paletteOpen.value = true
}

export function closePalette() {
  paletteOpen.value = false
}

// --- Toast notifications ---

export interface Toast {
  id: string
  kind: 'success' | 'error' | 'info'
  message: string
}

export const toasts = signal<Toast[]>([])

export function toast(kind: Toast['kind'], message: string) {
  const id = crypto.randomUUID()
  toasts.value = [...toasts.value, { id, kind, message }]
  setTimeout(() => {
    toasts.value = toasts.value.filter(t => t.id !== id)
  }, 4000)
}
