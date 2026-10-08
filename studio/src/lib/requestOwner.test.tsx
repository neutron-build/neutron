import { it, expect, afterEach } from 'vitest'
import { render, cleanup } from '@testing-library/preact'
import { activeConnection } from './store'
import { useRequestOwner } from './requestOwner'
afterEach(cleanup)
it('isolates channels, fences connection ABA, and refuses old render closures and unmount', () => {
  activeConnection.value = { id: 'c1', name: 'one', url: 'pg://one', isNucleus: false }
  let current!: ReturnType<typeof useRequestOwner>
  function Probe({ target }: { target: string }) { current = useRequestOwner(JSON.stringify([activeConnection.value?.id, target])); return <div>{target}</div> }
  const view = render(<Probe target="A" />)
  const oldRender = current
  const old = current.begin('rows'), stats = current.begin('stats'), latest = current.begin('rows')
  expect(old()).toBe(false); expect(stats()).toBe(true); expect(latest()).toBe(true)
  activeConnection.value = { id: 'c2', name: 'two', url: 'pg://two', isNucleus: false }
  activeConnection.value = { id: 'c1', name: 'one', url: 'pg://one', isNucleus: false }
  expect(stats()).toBe(false); expect(latest()).toBe(false)
  view.rerender(<Probe target="B" />)
  expect(oldRender.begin('rows')()).toBe(false)
  const originalQuery = current.begin('query'), independentCount = current.begin('count'), independentStats = current.begin('stats')
  // Input A -> B -> A invalidates the original A ticket even though text matches.
  current.invalidate('query'); const queryB = current.begin('query'); current.invalidate('query')
  expect(originalQuery()).toBe(false); expect(queryB()).toBe(false)
  expect(independentCount()).toBe(true); expect(independentStats()).toBe(true)
  const isolatedStats = current.begin('stats'), invalidatedRows = current.begin('rows')
  current.invalidate('rows'); expect(invalidatedRows()).toBe(false); expect(isolatedStats()).toBe(true)
  const pending = current.begin('rows'); expect(pending()).toBe(true)
  current.invalidate(); expect(pending()).toBe(false)
  const unmounted = current.begin('rows'); view.unmount(); expect(unmounted()).toBe(false)
})
