import { Badge } from './Badge'
import s from './ModelStub.module.css'

type BadgeKind = 'kv' | 'vector' | 'ts' | 'doc' | 'graph' | 'fts' | 'geo' | 'blob' | 'pubsub' | 'streams' | 'columnar' | 'datalog' | 'cdc'

interface ModelStubProps {
  kind: BadgeKind
  name?: string
  description: string
  phase?: string
}

export function ModelStub({ kind, name, description, phase = 'Phase 2' }: ModelStubProps) {
  return (
    <div class={s.stub}>
      <Badge kind={kind} />
      {name && <h2 class={s.name}>{name}</h2>}
      <p class={s.desc}>{description}</p>
      <span class={s.phase}>Coming in {phase}</span>
    </div>
  )
}
