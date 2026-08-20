import { RLS_EXPLANATION, RLS_FIX_SQL } from '../lib/rls'
import s from './RlsNotice.module.css'

interface RlsNoticeProps {
  detail?: string
}

/**
 * Shown in place of a specialty-store browser's data when the engine denied
 * the load because row-level security is active. Explains why and shows the
 * exact SQL that restores access instead of showing the raw error.
 */
export function RlsNotice({ detail }: RlsNoticeProps) {
  return (
    <div class={s.layout}>
      <div class={s.title}>Store unavailable while row-level security is active</div>
      <div class={s.body}>{RLS_EXPLANATION}</div>
      <div class={s.sqlList}>
        {RLS_FIX_SQL.map(sql => (
          <code key={sql} class={s.sqlLine}>{sql}</code>
        ))}
      </div>
      {detail && <div class={s.detail}>{detail}</div>}
    </div>
  )
}
