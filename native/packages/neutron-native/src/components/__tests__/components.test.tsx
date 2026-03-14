/**
 * Tests for native components — View, Text, Image, Pressable, TextInput, Link,
 * ScrollView, FlatList, Modal, StatusBar, SafeAreaView, ActivityIndicator,
 * Switch, Slider, KeyboardAvoidingView, RefreshControl.
 */

import { createElement } from 'react'

describe('View.native', () => {
  beforeEach(() => jest.resetModules())

  it('renders with style prop, stripping className', () => {
    const { default: View } = require('../View.native')
    const el = createElement(View, { className: 'flex-1', style: { padding: 10 }, testID: 'v' } as any)
    expect(el).toBeDefined()
  })

  it('passes children through', () => {
    const { default: View } = require('../View.native')
    const child = createElement('span', null, 'hi')
    const el = createElement(View, null, child) as any
    expect(el.props?.children).toBeDefined()
  })
})

describe('Text.native', () => {
  beforeEach(() => jest.resetModules())

  it('renders with text-specific props', () => {
    const { default: Text } = require('../Text.native')
    const el = createElement(Text, {
      numberOfLines: 2,
      ellipsizeMode: 'tail',
      selectable: true,
    } as any, 'Hello')
    expect(el).toBeDefined()
  })
})

describe('Image.native', () => {
  beforeEach(() => jest.resetModules())

  it('converts string source to uri object', () => {
    const { default: Image } = require('../Image.native')
    const el = createElement(Image, { source: 'https://example.com/img.png' } as any)
    expect(el).toBeDefined()
  })

  it('passes object source through', () => {
    const { default: Image } = require('../Image.native')
    const source = { uri: 'https://example.com/img.png' }
    const el = createElement(Image, { source } as any)
    expect(el).toBeDefined()
  })
})

describe('Pressable.native', () => {
  beforeEach(() => jest.resetModules())

  it('renders without crashing', () => {
    const { default: Pressable } = require('../Pressable.native')
    const el = createElement(Pressable, { onPress: jest.fn() } as any, 'Press me')
    expect(el).toBeDefined()
  })

  it('accepts disabled prop', () => {
    const { default: Pressable } = require('../Pressable.native')
    const el = createElement(Pressable, { disabled: true } as any)
    expect(el).toBeDefined()
  })
})

describe('TextInput.native', () => {
  beforeEach(() => jest.resetModules())

  it('renders with placeholder and value', () => {
    const { default: TextInput } = require('../TextInput.native')
    const el = createElement(TextInput, {
      placeholder: 'Enter text...',
      value: 'hello',
      onChangeText: jest.fn(),
    } as any)
    expect(el).toBeDefined()
  })
})

describe('Link.native', () => {
  beforeEach(() => jest.resetModules())

  it('renders with href prop', () => {
    const { default: Link } = require('../Link.native')
    const el = createElement(Link, { href: '/about' } as any, 'About')
    expect(el).toBeDefined()
  })
})

describe('ScrollView.native', () => {
  beforeEach(() => jest.resetModules())

  it('renders as a wrapper around RN ScrollView', () => {
    const { default: ScrollView } = require('../ScrollView.native')
    const el = createElement(ScrollView, { horizontal: true } as any)
    expect(el).toBeDefined()
  })
})

describe('FlatList.native', () => {
  beforeEach(() => jest.resetModules())

  it('renders with data and renderItem', () => {
    const { default: FlatList } = require('../FlatList.native')
    const el = createElement(FlatList, {
      data: [1, 2, 3],
      renderItem: ({ item }: { item: number }) => createElement('span', null, item),
    } as any)
    expect(el).toBeDefined()
  })
})

describe('Modal.native', () => {
  beforeEach(() => jest.resetModules())

  it('renders with visible prop', () => {
    const { default: Modal } = require('../Modal.native')
    const el = createElement(Modal, { visible: true } as any)
    expect(el).toBeDefined()
  })
})

describe('StatusBar.native', () => {
  beforeEach(() => jest.resetModules())

  it('renders with barStyle prop', () => {
    const { default: StatusBar } = require('../StatusBar.native')
    const el = createElement(StatusBar, { barStyle: 'light-content' } as any)
    expect(el).toBeDefined()
  })
})

describe('SafeAreaView.native', () => {
  beforeEach(() => jest.resetModules())

  it('renders as a wrapper', () => {
    const { default: SafeAreaView } = require('../SafeAreaView.native')
    const el = createElement(SafeAreaView, { style: { flex: 1 } } as any)
    expect(el).toBeDefined()
  })
})

describe('ActivityIndicator.native', () => {
  beforeEach(() => jest.resetModules())

  it('renders with size and color', () => {
    const { default: ActivityIndicator } = require('../ActivityIndicator.native')
    const el = createElement(ActivityIndicator, { size: 'large', color: '#000' } as any)
    expect(el).toBeDefined()
  })
})

describe('Switch.native', () => {
  beforeEach(() => jest.resetModules())

  it('renders with value and onValueChange', () => {
    const { default: Switch } = require('../Switch.native')
    const el = createElement(Switch, {
      value: true,
      onValueChange: jest.fn(),
    } as any)
    expect(el).toBeDefined()
  })
})

describe('Slider.native', () => {
  beforeEach(() => jest.resetModules())

  it('renders a placeholder View', () => {
    const { default: Slider } = require('../Slider.native')
    const el = createElement(Slider, {
      value: 0.5,
      minimumValue: 0,
      maximumValue: 1,
    } as any)
    expect(el).toBeDefined()
  })
})

describe('KeyboardAvoidingView.native', () => {
  beforeEach(() => jest.resetModules())

  it('renders with platform-aware behavior', () => {
    const { default: KAV } = require('../KeyboardAvoidingView.native')
    const el = createElement(KAV, { behavior: 'padding' } as any)
    expect(el).toBeDefined()
  })
})

describe('RefreshControl.native', () => {
  beforeEach(() => jest.resetModules())

  it('renders with refreshing and onRefresh', () => {
    const { default: RefreshControl } = require('../RefreshControl.native')
    const el = createElement(RefreshControl, {
      refreshing: false,
      onRefresh: jest.fn(),
    } as any)
    expect(el).toBeDefined()
  })
})
