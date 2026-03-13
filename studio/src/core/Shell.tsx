import { Sidebar } from './Sidebar'
import { TabBar } from './TabBar'
import { CommitBar } from './CommitBar'
import { ContentArea } from './ContentArea'
import s from './Shell.module.css'

export function Shell() {
  return (
    <div class={s.shell}>
      <Sidebar />
      <div class={s.main}>
        <TabBar />
        <ContentArea />
        <CommitBar />
      </div>
    </div>
  )
}
