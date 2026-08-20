import { RLS_EXPLANATION } from '../lib/rls'
import s from './RlsNotice.module.css'

interface RlsNoticeProps {
  detail?: string
}

/**
 * Shown in place of a specialty-store browser's data when the engine denied
 * the load because row-level security is active. Explains why instead of
 * showing the raw error.
 */
export function RlsNotice({ detail }: RlsNoticeProps) {
  return (
    <div class={s.layout}>
      <div class={s.title}>Store unavailable while row-level security is active</div>
      <div class={s.body}>{RLS_EXPLANATION}</div>
      {detail && <div class={s.detail}>{detail}</div>}
    </div>
  )
}
