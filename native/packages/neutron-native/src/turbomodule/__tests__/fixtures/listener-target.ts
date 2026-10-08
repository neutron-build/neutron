/** Browser boundary double with native EventTarget callback-identity semantics. */
export function listenerTarget() {
  const listeners = new Map<string, Set<() => void>>()
  const addEventListener = jest.fn((event: string, callback: () => void) => {
    let callbacks = listeners.get(event)
    if (!callbacks) { callbacks = new Set(); listeners.set(event, callbacks) }
    callbacks.add(callback)
  })
  const removeEventListener = jest.fn((event: string, callback: () => void) => {
    listeners.get(event)?.delete(callback)
  })
  return {
    addEventListener, removeEventListener,
    dispatch(event: string) { for (const callback of [...(listeners.get(event) ?? [])]) callback() },
    count() { return [...listeners.values()].reduce((sum, callbacks) => sum + callbacks.size, 0) },
    registered(event: string) { return addEventListener.mock.calls.find(([name]) => name === event)![1] },
  }
}
