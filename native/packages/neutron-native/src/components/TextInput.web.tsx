import type { TextInputProps } from '../types.js'
import { styleToCSS } from '../web-compat/style.js'

const KEYBOARD_TO_INPUT_MODE: Record<string, string> = {
  default: 'text',
  numeric: 'numeric',
  email: 'email',
  'email-address': 'email',
  'phone-pad': 'tel',
  url: 'url',
  'decimal-pad': 'decimal',
  'number-pad': 'numeric',
}

export function TextInput({
  value,
  defaultValue,
  onChangeText,
  onSubmitEditing,
  onFocus,
  onBlur,
  placeholder,
  secureTextEntry,
  keyboardType,
  autoCapitalize,
  autoCorrect,
  multiline,
  numberOfLines,
  style,
  editable,
  maxLength,
  testID,
}: TextInputProps) {
  const sharedProps = {
    value,
    defaultValue,
    placeholder,
    disabled: editable === false,
    maxLength,
    'data-testid': testID,
    autoCapitalize: autoCapitalize ?? 'sentences',
    autoCorrect: String(autoCorrect ?? true),
    style: styleToCSS(style) as preact.JSX.CSSProperties,
    onInput: onChangeText
      ? (e: Event) => onChangeText((e.target as HTMLInputElement).value)
      : undefined,
    onFocus: onFocus ? () => onFocus() : undefined,
    onBlur: onBlur ? () => onBlur() : undefined,
    onKeyDown: onSubmitEditing
      ? (e: KeyboardEvent) => { if (e.key === 'Enter' && !multiline) onSubmitEditing() }
      : undefined,
    inputMode: KEYBOARD_TO_INPUT_MODE[keyboardType ?? 'default'] as preact.JSX.HTMLAttributes<HTMLInputElement>['inputMode'],
  }

  if (multiline) {
    return <textarea rows={numberOfLines ?? 4} {...sharedProps as preact.JSX.HTMLAttributes<HTMLTextAreaElement>} />
  }

  return <input type={secureTextEntry ? 'password' : 'text'} {...sharedProps as preact.JSX.HTMLAttributes<HTMLInputElement>} />
}
