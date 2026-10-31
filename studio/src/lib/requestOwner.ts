import { useEffect, useRef } from 'preact/hooks'
import { activeConnection } from './store'

/** Independent request channels share a view epoch, never a mutable response. */
export function createRequestOwner(identity: string) {
  let epoch = 0
  let current = identity
  let mounted = true
  const channels = new Map<string, number>()
  return {
    matches(expected: string) { return expected === current },
    update(next: string) { if (next !== current) { current = next; epoch++ } },
    invalidate(channel?: string) {
      if (channel) channels.set(channel, (channels.get(channel) ?? 0) + 1)
      else epoch++
    },
    mount() { mounted = true },
    dispose() { mounted = false; epoch++ },
    begin(channel = 'read') {
      const view = epoch
      const sequence = (channels.get(channel) ?? 0) + 1
      channels.set(channel, sequence)
      const connection = activeConnection.value?.id
      return () => mounted && epoch === view && channels.get(channel) === sequence && activeConnection.value?.id === connection
    },
  }
}

export function useRequestOwner(identity: string) {
  const ref = useRef<ReturnType<typeof createRequestOwner> | null>(null)
  if (!ref.current) ref.current = createRequestOwner(identity)
  const owner = ref.current
  owner.update(identity)
  useEffect(() => {
    owner.mount()
    let connection = activeConnection.value?.id
    const unsubscribe = activeConnection.subscribe(next => {
      if (next?.id !== connection) { connection = next?.id; owner.invalidate() }
    })
    return () => { unsubscribe(); owner.dispose() }
  }, [])
  // Bind tickets to this render's identity as well as the live controller.
  // A stale continuation calling an old load closure cannot adopt a new view.
  return { ...owner, begin(channel = 'read') {
    if (!owner.matches(identity)) return () => false
    const owns = owner.begin(channel)
    return () => owner.matches(identity) && owns()
  } }
}
