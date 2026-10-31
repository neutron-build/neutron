import { test } from 'node:test'
import assert from 'node:assert/strict'
import React, { act, createElement } from 'react'
import { createRoot } from 'react-dom/client'
import { Window } from 'happy-dom'
import { Stack } from '../packages/neutron-native/src/navigation/Stack.native.js'
import { navigate, replace, goBack, goForward, routerState, setNavigationRef, type NativeNavigationState } from '../packages/neutron-native/src/router/navigator.js'
import { useStackActions } from '../packages/neutron-native/src/navigation/scoped.js'
test('real React reconciles dynamic removal, nested actions and unmount ownership', async () => {
  const browser = new Window(), saved = Object.getOwnPropertyDescriptors(globalThis)
  for (const [name, value] of Object.entries({ window: browser, document: browser.document, navigator: browser.navigator, IS_REACT_ACT_ENVIRONMENT: true })) Object.defineProperty(globalThis, name, { value, configurable: true })
  const host = browser.document.createElement('div'); browser.document.body.appendChild(host)
  const root = createRoot(host as unknown as HTMLElement)
  const Home = () => createElement('p', null, 'home'), Other = () => createElement('p', null, 'other')
  let push: ((name: string) => void) | undefined
  const Child = () => { push = useStackActions().push; return createElement('p', null, 'nested') }
  const Nested = () => createElement(Stack, { initialRouteName: 'child' }, createElement(Stack.Screen, { name: 'child', component: Child }), createElement(Stack.Screen, { name: 'next', component: Other }))
  try {
    await act(async () => { navigate('/home'); root.render(createElement(Stack, {}, createElement(Stack.Screen, { name: 'home', component: Home }), createElement(Stack.Screen, { name: 'other', component: Other }))) })
    assert.match(host.textContent!, /home/)
    await act(async () => root.render(createElement(Stack, {}, createElement(Stack.Screen, { name: 'other', component: Other }))))
    assert.match(host.textContent!, /other/); assert.equal(routerState.value.pathname, '/other')
    await act(async () => { navigate('/parent/child'); root.render(createElement(Stack, {}, createElement(Stack.Screen, { name: 'parent', component: Nested }))) })
    await act(async () => push!('next'))
    assert.equal(routerState.value.pathname, '/parent/next'); assert.match(host.textContent!, /other/)
    await act(async () => root.unmount())
    navigate('/unmounted'); assert.equal(host.textContent, '')
  } finally { await act(async () => root.unmount()); for (const name of ['window', 'document', 'navigator', 'IS_REACT_ACT_ENVIRONMENT']) { if (saved[name]) Object.defineProperty(globalThis, name, saved[name]); else delete (globalThis as any)[name] }; await browser.happyDOM.close() }
})
test('public native state feedback and replace/back/forward use one root history', () => {
  let state: NativeNavigationState = { index: 0, routes: [{ name: 'home', params: { __neutronPath: '/home', __neutronParams: {} } }] }
  let notify: (() => void) | undefined, disposed = 0
  const calls: NativeNavigationState[] = []
  const detach = setNavigationRef({ isReady: () => true, getRootState: () => state,
    resetRoot: next => { state = next; calls.push(next); notify?.() },
    addListener: (_event, listener) => { notify = listener; return () => { disposed++; notify = undefined } } })
  navigate('/detail'); replace('/replacement'); assert.equal(state.routes.at(-1)?.params?.__neutronPath, '/replacement')
  goBack(); assert.equal(routerState.value.pathname, '/home'); goForward(); assert.equal(routerState.value.pathname, '/replacement')
  state = { ...state, index: 0 }; notify!(); assert.equal(routerState.value.pathname, '/home')
  goForward(); assert.equal(routerState.value.pathname, '/replacement'); assert.ok(calls.length >= 5)
  detach(); assert.equal(disposed, 1); navigate('/detached'); assert.equal(state.routes.at(-1)?.params?.__neutronPath, '/replacement')
})
