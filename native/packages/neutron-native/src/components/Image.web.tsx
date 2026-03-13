import type { ImageProps } from '../types.js'
import { styleToCSS } from '../web-compat/style.js'

const RESIZE_MAP: Record<string, string> = {
  cover: 'cover',
  contain: 'contain',
  stretch: 'fill',
  center: 'none',
  repeat: 'none',
}

export function Image({ source, style, resizeMode, onLoad, onError, testID, accessibilityLabel, ...rest }: ImageProps) {
  const src = typeof source === 'object' && source !== null && 'uri' in source
    ? (source as { uri: string }).uri
    : String(source)

  return (
    <img
      src={src}
      alt={accessibilityLabel ?? ''}
      data-testid={testID}
      style={{
        objectFit: RESIZE_MAP[resizeMode ?? 'cover'] as preact.JSX.CSSProperties['objectFit'],
        ...styleToCSS(style),
      } as preact.JSX.CSSProperties}
      onLoad={onLoad ? () => onLoad() : undefined}
      onError={onError ? () => onError() : undefined}
      {...(rest as Record<string, unknown>)}
    />
  )
}
