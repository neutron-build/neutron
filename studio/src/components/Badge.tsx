import s from './Badge.module.css'

type BadgeKind =
  | 'sql' | 'kv' | 'vector' | 'ts' | 'doc' | 'graph'
  | 'fts' | 'geo' | 'blob' | 'pubsub' | 'streams'
  | 'columnar' | 'datalog' | 'cdc'

interface BadgeProps {
  kind: BadgeKind
  label?: string
}

const LABELS: Record<BadgeKind, string> = {
  sql: 'SQL', kv: 'KV', vector: 'Vector', ts: 'TimeSeries',
  doc: 'Document', graph: 'Graph', fts: 'FTS', geo: 'Geo',
  blob: 'Blob', pubsub: 'PubSub', streams: 'Streams',
  columnar: 'Columnar', datalog: 'Datalog', cdc: 'CDC',
}

export function Badge({ kind, label }: BadgeProps) {
  return (
    <span class={s.badge} data-kind={kind}>
      {label ?? LABELS[kind]}
    </span>
  )
}
