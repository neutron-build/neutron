import { TextInput as RNTextInput } from 'react-native'
import type { TextInputProps } from '../types.js'

export function TextInput({
  style, value, defaultValue, placeholder, placeholderTextColor,
  onChangeText, onChange, onSubmitEditing, onFocus, onBlur,
  keyboardType, returnKeyType, secureTextEntry, autoCapitalize,
  autoCorrect, autoFocus, editable, multiline, numberOfLines,
  maxLength, className: _className, ...rest
}: TextInputProps) {
  return (
    <RNTextInput
      style={style}
      value={value}
      defaultValue={defaultValue}
      placeholder={placeholder}
      placeholderTextColor={placeholderTextColor}
      onChangeText={onChangeText}
      onChange={onChange}
      onSubmitEditing={onSubmitEditing}
      onFocus={onFocus}
      onBlur={onBlur}
      keyboardType={keyboardType}
      returnKeyType={returnKeyType}
      secureTextEntry={secureTextEntry}
      autoCapitalize={autoCapitalize}
      autoCorrect={autoCorrect}
      autoFocus={autoFocus}
      editable={editable}
      multiline={multiline}
      numberOfLines={numberOfLines}
      maxLength={maxLength}
      {...rest}
    />
  )
}
