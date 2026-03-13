import type { ActivityIndicatorProps } from './ActivityIndicator.native.js'
import { styleToCSS } from '../web-compat/style.js'

export function ActivityIndicator({ animating, color, size, style, testID }: ActivityIndicatorProps) {
  if (animating === false) return null
  const px = size === 'large' ? 36 : size === 'small' ? 20 : typeof size === 'number' ? size : 20

  return (
    <div
      data-testid={testID}
      role="progressbar"
      aria-label="Loading"
      style={{
        width: px,
        height: px,
        borderRadius: '50%',
        border: `3px solid ${color ?? '#999'}`,
        borderTopColor: 'transparent',
        animation: 'neutron-spin 0.7s linear infinite',
        ...styleToCSS(style),
      } as preact.JSX.CSSProperties}
    />
  )
}
