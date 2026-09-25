// S05 hash deep-links: every browsable surface has a shareable URL.
//
//   #/c/<connId>/t/<schema>/<table>          browse a table's rows
//   #/c/<connId>/v/<schema>/<view>          browse a view (read-only)
//   #/c/<connId>/designer/<schema>/<table>  schema designer on a table
//   #/c/<connId>/inspect/<schema>/<table>   object structure detail
//   #/c/<connId>/diagnostics                performance diagnosis
//   #/c/<connId>/diagnostics/<schema>/<table>
//   #/c/<connId>/sql                        SQL editor
//   #/c/<connId>/journey/<schema>/<table>   X06 cross-model inspection journey
//
// Names are URL-encoded, so mixed-case names and names with spaces survive.
// Opening a browsable tab updates the hash (history.replaceState: the URL
// is the tab's address, not a history step).

import type { Tab } from './types'

export interface DeepLink {
  connectionId: string
  tab: Omit<Tab, 'id'>
}

export function parseDeepLink(hash: string): DeepLink | null {
  const path = hash.replace(/^#\/?/, '')
  if (!path) return null
  const parts = path.split('/').map(decodeURIComponent)
  if (parts.length < 3 || parts[0] !== 'c' || !parts[1]) return null
  const connectionId = parts[1]
  const rest = parts.slice(2)
  const names = (i: number): [string, string] => {
    const schema = rest[i] ?? ''
    const name = rest[i + 1] ?? ''
    return [schema, name]
  }
  const withNames = (kind: Tab['kind'], i: number): DeepLink | null => {
    const [schema, name] = names(i)
    if (!schema || !name) return null
    return { connectionId, tab: { kind, label: name, objectSchema: schema, objectName: name } }
  }
  switch (rest[0]) {
    case 't':
      return withNames('sql-browser', 1)
    case 'v':
      return withNames('sql-browser', 1)
    case 'designer':
      if (rest.length >= 3) return withNames('schema-designer', 1)
      return { connectionId, tab: { kind: 'schema-designer', label: 'Schema Designer' } }
    case 'inspect':
      return withNames('schema-inspector', 1)
    case 'diagnostics':
      if (rest.length >= 3) return withNames('diagnostics', 1)
      return { connectionId, tab: { kind: 'diagnostics', label: 'Diagnostics' } }
    case 'sql':
      return { connectionId, tab: { kind: 'sql-editor', label: 'SQL' } }
    case 'journey': {
      const link = withNames('journey', 1)
      if (link) link.tab.label = `Journey: ${link.tab.objectName}`
      return link
    }
    default:
      return null
  }
}

export function serializeDeepLink(tab: Tab, connectionId: string): string | null {
  const enc = encodeURIComponent
  const name = tab.objectName ? enc(tab.objectName) : ''
  const schema = tab.objectSchema ? enc(tab.objectSchema) : ''
  const base = `#/c/${enc(connectionId)}`
  switch (tab.kind) {
    case 'sql-browser':
      if (!tab.objectSchema || !tab.objectName) return null
      // Filtered views (FK follow) are per-open contexts, not addresses.
      if (tab.filter || tab.match) return null
      return `${base}/t/${schema}/${name}`
    case 'schema-designer':
      return tab.objectSchema && tab.objectName
        ? `${base}/designer/${schema}/${name}`
        : `${base}/designer`
    case 'schema-inspector':
      if (!tab.objectSchema || !tab.objectName) return null
      return `${base}/inspect/${schema}/${name}`
    case 'diagnostics':
      return tab.objectSchema && tab.objectName
        ? `${base}/diagnostics/${schema}/${name}`
        : `${base}/diagnostics`
    case 'sql-editor':
      // An editor seeded with a specific statement is a context, not an
      // address; a plain editor tab still deep-links.
      return tab.initialSql ? null : `${base}/sql`
    case 'journey':
      if (!tab.objectSchema || !tab.objectName) return null
      return `${base}/journey/${schema}/${name}`
    default:
      return null
  }
}
