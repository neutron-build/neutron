import { Image as RNImage } from 'react-native'
import type { ImageProps } from '../types.js'

export function Image({ source, style, resizeMode, onLoad, onError, onLoadEnd, className: _className, ...rest }: ImageProps) {
  const src = typeof source === 'string' ? { uri: source } : source
  return (
    <RNImage
      source={src}
      style={style}
      resizeMode={resizeMode}
      onLoad={onLoad}
      onError={onError}
      onLoadEnd={onLoadEnd}
      {...rest}
    />
  )
}
