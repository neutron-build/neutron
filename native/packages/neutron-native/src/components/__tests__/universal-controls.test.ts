/**
 * NF-NR-14 regressions — universal controls keep essential navigation,
 * disabled and keyboard behavior on both platforms.
 */

import { Switch } from '../Switch.web'
import { Link } from '../Link.web'
import { TextInput } from '../TextInput.web'
import { routerState } from '../../router/navigator.js'

export {}

describe('NF-NR-14 universal controls', () => {
  it('Switch is keyboard-operable (Space/Enter) and exposes disabled state', () => {
    const on = jest.fn()
    const element = Switch({ value: false, onValueChange: on, testID: 's1' }) as unknown as { props: Record<string, unknown>; onKeyDown?: (e: KeyboardEvent) => void }
    expect(element.props.role).toBe('switch')
    expect(element.props.tabIndex).toBe(0)
    // Preact invokes the component function; keydown goes through props.
    const keydown = element.props.onKeyDown as (e: KeyboardEvent) => void
    keydown({ key: ' ', preventDefault: jest.fn() } as unknown as KeyboardEvent)
    expect(on).toHaveBeenCalledWith(true)

    const disabledElement = Switch({ value: false, onValueChange: on, disabled: true, testID: 's2' }) as unknown as { props: Record<string, unknown> }
    expect(disabledElement.props['aria-disabled']).toBe(true)
    expect(disabledElement.props.tabIndex).toBe(-1)
    expect(disabledElement.props.onClick).toBeUndefined()
  })

  it('a disabled web Link blocks the browser navigation too', () => {
    const element = Link({ href: '/x', disabled: true, children: null }) as unknown as { props: Record<string, unknown> }
    const click = element.props.onClick as (e: MouseEvent) => void
    const preventDefault = jest.fn()
    click({ preventDefault } as unknown as MouseEvent)
    expect(preventDefault).toHaveBeenCalled()
  })

  it('an enabled internal Link routes through the signal navigator', () => {
    const element = Link({ href: '/y', children: null }) as unknown as { props: Record<string, unknown> }
    const click = element.props.onClick as (e: MouseEvent) => void
    const preventDefault = jest.fn()
    click({ preventDefault } as unknown as MouseEvent)
    expect(preventDefault).toHaveBeenCalled()
    expect(routerState.value.pathname).toBe('/y')
    expect(element.props.role).toBe('link')
  })

  it('TextInput maps RN props onto HTML semantics and both text events', () => {
    const onChangeText = jest.fn()
    const onChange = jest.fn()
    const input = TextInput({ value: '', onChangeText, onChange, autoCapitalize: 'none', keyboardType: 'email-address', accessibilityLabel: 'Email', testID: 'email' } as never) as unknown as { props: Record<string, unknown> }
    expect(input.props.autoCapitalize).toBe('off')
    expect(input.props.inputMode).toBe('email')
    expect(input.props['aria-label']).toBe('Email')
    const onInput = input.props.onInput as (e: { target: { value: string } }) => void
    onInput({ target: { value: 'typed' } })
    expect(onChangeText).toHaveBeenCalledWith('typed')
    expect(onChange).toHaveBeenCalledWith({ nativeEvent: { text: 'typed' } })
  })

  it('multiline TextInput renders a textarea with rows', () => {
    const area = TextInput({ value: '', multiline: true, numberOfLines: 6 } as never) as unknown as { type: string; props: Record<string, unknown> }
    expect(area.type).toBe('textarea')
    expect(area.props.rows).toBe(6)
  })
})
