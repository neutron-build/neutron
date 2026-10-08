import { listenerTarget } from '../../turbomodule/__tests__/fixtures/listener-target.js'
jest.unmock('react-native')
jest.unmock('react')
jest.unmock('react/jsx-runtime')
import React from 'react'
import { act, create } from 'react-test-renderer'
import { Pressable } from '../Pressable.native.js'
import { Image } from '../Image.native.js'
import { View } from '../View.native.js'
;(globalThis as any).IS_REACT_ACT_ENVIRONMENT = true

it.each(['View', 'Text', 'Image', 'Pressable', 'TextInput', 'Link', 'ScrollView', 'FlatList', 'Modal', 'StatusBar', 'SafeAreaView', 'ActivityIndicator', 'Switch', 'KeyboardAvoidingView', 'RefreshControl'])('renders actual named %s export with real React', async name => {
  const Component = require('../' + name + '.native')[name]
  expect(typeof Component).toBe('function')
  let renderer: any
  await act(async () => { renderer = create(React.createElement(Component, { testID: 'actual', href: '/page', source: 'https://example.test/a.png', data: [] })) })
  expect(renderer.toJSON()).not.toBeNull()
  await act(async () => renderer.unmount())
})
it('normalizes actual Image string source and strips View className', async () => {
  let renderer: any
  await act(async () => { renderer = create(<View className="ignored" testID="view"><Image source="https://example.test/a.png" /></View>) })
  expect(renderer.root.findByType('View').props.className).toBeUndefined()
  expect(renderer.root.findByType('Image').props.source).toEqual({ uri: 'https://example.test/a.png' })
  await act(async () => renderer.unmount())
})
it('actual Pressable state rerenders and disabled handlers stay inert', async () => {
  const press = jest.fn(); let renderer: any
  await act(async () => { renderer = create(<Pressable onPress={press} style={({ pressed }) => ({ opacity: pressed ? 0.5 : 1 })}>{({ pressed }) => pressed ? 'down' : 'up'}</Pressable>) })
  expect(renderer.root.findByType('Pressable').props.style.opacity).toBe(1)
  await act(async () => renderer.root.findByType('Pressable').props.onPressIn({}))
  expect(renderer.root.findByType('Pressable').props.style.opacity).toBe(0.5)
  expect(renderer.toJSON().children).toEqual(['down'])
  await act(async () => renderer.update(<Pressable disabled onPress={press}>disabled</Pressable>))
  await act(async () => renderer.root.findByType('Pressable').props.onPress({}))
  expect(press).not.toHaveBeenCalled()
  await act(async () => renderer.unmount())
})
it('unmount releases a shipped browser subscription through real React effects', async () => {
  const browser = listenerTarget(), connection = listenerTarget(), delivery = jest.fn()
  ;(globalThis as any).window = browser
  ;(globalThis as any).document = {}
  Object.defineProperty(globalThis, 'navigator', { configurable: true, value: { onLine: true, connection } })
  const { getModule, clearCache } = require('../../turbomodule/registry')
  require('../../turbomodule/modules/net-info.web')
  clearCache(); const net = getModule('NeutronNetInfo')
  function Consumer() { React.useEffect(() => { const subscription = net.addEventListener(delivery); return () => subscription.remove() }, []); return <View /> }
  let renderer: any
  await act(async () => { renderer = create(<Consumer />) })
  await act(async () => { browser.dispatch('online'); browser.dispatch('offline'); connection.dispatch('change') })
  expect(delivery).toHaveBeenCalledTimes(3)
  await act(async () => renderer.unmount())
  for (const event of ['online', 'offline']) expect(browser.removeEventListener).toHaveBeenCalledWith(event, browser.registered(event))
  expect(connection.removeEventListener).toHaveBeenCalledWith('change', connection.registered('change'))
  expect(browser.count()).toBe(0); expect(connection.count()).toBe(0)
  await act(async () => { browser.dispatch('online'); browser.dispatch('offline'); connection.dispatch('change') })
  expect(delivery).toHaveBeenCalledTimes(3)
})
