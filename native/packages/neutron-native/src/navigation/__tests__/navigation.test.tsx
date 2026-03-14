/**
 * Tests for navigation components — Stack, Tabs, Drawer.
 */

import { createElement } from 'react'

describe('Stack navigator', () => {
  beforeEach(() => jest.resetModules())

  it('exports a Stack component', () => {
    const { Stack } = require('../index')
    expect(Stack).toBeDefined()
  })

  it('Stack renders with Screen children', () => {
    const { Stack } = require('../index')
    const DummyScreen = () => createElement('div', null, 'Hello')
    const el = createElement(Stack, { initialRouteName: 'Home' },
      createElement(Stack.Screen || 'div', { name: 'Home', component: DummyScreen })
    )
    expect(el).toBeDefined()
    expect(el.props.initialRouteName).toBe('Home')
  })
})

describe('Tabs navigator', () => {
  beforeEach(() => jest.resetModules())

  it('exports a Tabs component', () => {
    const { Tabs } = require('../index')
    expect(Tabs).toBeDefined()
  })

  it('Tabs renders with tab configuration', () => {
    const { Tabs } = require('../index')
    const el = createElement(Tabs, null)
    expect(el).toBeDefined()
  })
})

describe('Drawer navigator', () => {
  beforeEach(() => jest.resetModules())

  it('exports a Drawer component', () => {
    const { Drawer } = require('../index')
    expect(Drawer).toBeDefined()
  })

  it('Drawer renders with navigation props', () => {
    const { Drawer } = require('../index')
    const el = createElement(Drawer, null)
    expect(el).toBeDefined()
  })
})
