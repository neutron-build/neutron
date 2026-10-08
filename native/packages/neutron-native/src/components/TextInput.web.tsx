/** @jsxImportSource preact */
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

/** RN autoCapitalize values map onto the HTML attribute's own enum. */
const AUTOCAPITALIZE: Record<string, string> = {
  none: 'off',
  sentences: 'sentences',
  words: 'words',
  characters: 'characters',
}

export function TextInput({
  value,
  defaultValue,
  onChangeText,
  onChange,
  onSubmitEditing,
  onFocus,
  onBlur,
  placeholder,
  placeholderTextColor,
  secureTextEntry,
  keyboardType,
  autoCapitalize,
  autoCorrect,
  multiline,
  numberOfLines,
  style,
  editable,
  maxLength,
  autoFocus,
  accessibilityLabel,
  testID,
}: TextInputProps) {
  const sharedProps = {
    value,
    defaultValue,
    placeholder,
    disabled: editable === false,
    maxLength,
    autoFocus,
    'data-testid': testID,
    'aria-label': accessibilityLabel,
    autoCapitalize: AUTOCAPITALIZE[autoCapitalize ?? 'sentences'],
    autoCorrect: String(autoCorrect ?? true),
    style: styleToCSS(style) as preact.JSX.CSSProperties,
    onInput: (onChangeText || onChange)
      ? (e: Event) => {
          const text = (e.target as HTMLInputElement).value
          onChangeText?.(text)
          // RN's onChange carries { nativeEvent: { text } } (NF-NR-14).
          onChange?.({ nativeEvent: { text } })
        }
      : undefined,
    onFocus: onFocus ? (e: FocusEvent) => onFocus(e) : undefined,
    onBlur: onBlur ? (e: FocusEvent) => onBlur(e) : undefined,
    onKeyDown: onSubmitEditing
      ? (e: KeyboardEvent) => { if (e.key === 'Enter' && !multiline) onSubmitEditing(e) }
      : undefined,
    inputMode: KEYBOARD_TO_INPUT_MODE[keyboardType ?? 'default'] as preact.JSX.HTMLAttributes<HTMLInputElement>['inputMode'],
  }

  // placeholderTextColor has no CSS equivalent — the ::placeholder pseudo
  //element needs a stylesheet; surfaced as a CSS variable for one.
  const styleWithPlaceholder = placeholderTextColor
    ? { ...sharedProps.style, '--placeholder-color': placeholderTextColor } as preact.JSX.CSSProperties
    : sharedProps.style

  if (multiline) {
    return <textarea rows={numberOfLines ?? 4} {...sharedProps as preact.JSX.HTMLAttributes<HTMLTextAreaElement>} style={styleWithPlaceholder} />
  }

  return <input type={secureTextEntry ? 'password' : 'text'} {...sharedProps as preact.JSX.HTMLAttributes<HTMLInputElement>} style={styleWithPlaceholder} />
}