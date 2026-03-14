/**
 * Tests for render module — NeutronApp.
 */

import { AppRegistry } from 'react-native'
import { NeutronApp } from './render'

describe('NeutronApp', () => {
  it('registers the component with AppRegistry', () => {
    const MockComponent = () => null
    NeutronApp({ component: MockComponent, appName: 'TestApp' })
    expect(AppRegistry.registerComponent).toHaveBeenCalledWith('TestApp', expect.any(Function))
  })

  it('the factory returns the provided component', () => {
    const MockComponent = () => null
    NeutronApp({ component: MockComponent, appName: 'AnotherApp' })
    const calls = (AppRegistry.registerComponent as jest.Mock).mock.calls
    const lastCall = calls[calls.length - 1]
    const factory = lastCall[1]
    expect(factory()).toBe(MockComponent)
  })
})
