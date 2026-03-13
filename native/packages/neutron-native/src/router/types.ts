import type { ComponentType, ReactNode } from 'react'

export interface RouteRecord {
  /** File-system path segment, e.g. 'home', '[id]', '(tabs)', '_layout' */
  segment: string
  /** The component for this route, if it's a leaf */
  component?: ComponentType
  /** Nested routes */
  children?: RouteRecord[]
  /** True if this is a dynamic segment like [id] */
  dynamic?: boolean
  /** True if this is a group/layout segment like (tabs) */
  group?: boolean
  /** Layout component wrapping children */
  layout?: ComponentType<{ children: ReactNode }>
}

export interface RouterState {
  /** Current path segments */
  segments: string[]
  /** Dynamic params extracted from segments */
  params: Record<string, string>
  /** Full path string */
  pathname: string
}

export interface NavigateOptions {
  replace?: boolean
}
