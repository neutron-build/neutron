import { lazy, Suspense } from 'preact/compat'
import { activeTab } from '../lib/store'
import s from './ContentArea.module.css'

// Lazy-load every module to keep initial bundle small
const SQLBrowser    = lazy(() => import('../modules/sql/SQLBrowser').then(m => ({ default: m.SQLBrowser })))
const SQLEditor     = lazy(() => import('../modules/sql/SQLEditor').then(m => ({ default: m.SQLEditor })))
const KVModule      = lazy(() => import('../modules/kv/KVModule').then(m => ({ default: m.KVModule })))
const VectorModule  = lazy(() => import('../modules/vector/VectorModule').then(m => ({ default: m.VectorModule })))
const TSModule      = lazy(() => import('../modules/timeseries/TSModule').then(m => ({ default: m.TSModule })))
const DocModule     = lazy(() => import('../modules/document/DocModule').then(m => ({ default: m.DocModule })))
const GraphModule   = lazy(() => import('../modules/graph/GraphModule').then(m => ({ default: m.GraphModule })))
const FTSModule     = lazy(() => import('../modules/fts/FTSModule').then(m => ({ default: m.FTSModule })))
const GeoModule     = lazy(() => import('../modules/geo/GeoModule').then(m => ({ default: m.GeoModule })))
const BlobModule    = lazy(() => import('../modules/blob/BlobModule').then(m => ({ default: m.BlobModule })))
const PubSubModule  = lazy(() => import('../modules/pubsub/PubSubModule').then(m => ({ default: m.PubSubModule })))
const StreamsModule = lazy(() => import('../modules/streams/StreamsModule').then(m => ({ default: m.StreamsModule })))
const ColumnarModule= lazy(() => import('../modules/columnar/ColumnarModule').then(m => ({ default: m.ColumnarModule })))
const DatalogModule = lazy(() => import('../modules/datalog/DatalogModule').then(m => ({ default: m.DatalogModule })))
const CDCModule       = lazy(() => import('../modules/cdc/CDCModule').then(m => ({ default: m.CDCModule })))
const SchemaDesigner  = lazy(() => import('../modules/schema/SchemaDesigner').then(m => ({ default: m.SchemaDesigner })))

function Fallback() {
  return <div class={s.loading}>Loading…</div>
}

function Empty() {
  return (
    <div class={s.empty}>
      <div class={s.emptyIcon}>⬡</div>
      <p class={s.emptyText}>Select a table or data store from the sidebar</p>
    </div>
  )
}

export function ContentArea() {
  const tab = activeTab.value

  if (!tab) return <Empty />

  let content: preact.VNode | null = null

  switch (tab.kind) {
    case 'sql-browser':
      content = <SQLBrowser schema={tab.objectSchema!} table={tab.objectName!} />
      break
    case 'sql-editor':
      content = <SQLEditor tabId={tab.id} />
      break
    case 'kv':
      content = <KVModule name={tab.objectName!} />
      break
    case 'vector':
      content = <VectorModule name={tab.objectName!} />
      break
    case 'timeseries':
      content = <TSModule name={tab.objectName!} />
      break
    case 'document':
      content = <DocModule name={tab.objectName!} />
      break
    case 'graph':
      content = <GraphModule name={tab.objectName!} />
      break
    case 'fts':
      content = <FTSModule name={tab.objectName!} />
      break
    case 'geo':
      content = <GeoModule name={tab.objectName!} />
      break
    case 'blob':
      content = <BlobModule name={tab.objectName!} />
      break
    case 'pubsub':
      content = <PubSubModule name={tab.objectName!} />
      break
    case 'streams':
      content = <StreamsModule name={tab.objectName!} />
      break
    case 'columnar':
      content = <ColumnarModule name={tab.objectName!} />
      break
    case 'datalog':
      content = <DatalogModule />
      break
    case 'cdc':
      content = <CDCModule />
      break
    case 'schema-designer':
      content = <SchemaDesigner />
      break
    default:
      content = <Empty />
  }

  return (
    <div class={s.area}>
      <Suspense fallback={<Fallback />}>
        {content}
      </Suspense>
    </div>
  )
}
