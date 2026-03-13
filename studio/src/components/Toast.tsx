import { toasts } from '../lib/store'
import s from './Toast.module.css'

export function Toasts() {
  const list = toasts.value
  if (list.length === 0) return null

  return (
    <div class={s.container}>
      {list.map(t => (
        <div key={t.id} class={`${s.toast} ${s[t.kind]}`}>
          {t.message}
        </div>
      ))}
    </div>
  )
}
