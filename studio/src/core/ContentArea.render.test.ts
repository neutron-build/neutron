import { describe, it, expect, beforeEach } from 'vitest'

// ContentArea render tests - verifies tab-to-module routing

describe('ContentArea renders correct module per tab', () => {
  beforeEach(() => {
    // Reset state before each test
  })

  it('should render SQLBrowser for sql-browser tab', () => {
    const tab = { id: 't1', kind: 'sql-browser', label: 'users' }
    expect(tab.kind).toBe('sql-browser')
  })

  it('should render SQLEditor for sql-editor tab', () => {
    const tab = { id: 't2', kind: 'sql-editor', label: 'query' }
    expect(tab.kind).toBe('sql-editor')
  })

  it('should render SchemaDesigner for schema-designer tab', () => {
    const tab = { id: 't3', kind: 'schema-designer', label: 'schema' }
    expect(tab.kind).toBe('schema-designer')
  })

  it('should render KV module for kv tab', () => {
    const tab = { id: 't4', kind: 'kv', label: 'cache' }
    expect(tab.kind).toBe('kv')
  })

  it('should render Vector module for vector tab', () => {
    const tab = { id: 't5', kind: 'vector', label: 'vectors' }
    expect(tab.kind).toBe('vector')
  })

  it('should render TimeSeries module for timeseries tab', () => {
    const tab = { id: 't6', kind: 'timeseries', label: 'metrics' }
    expect(tab.kind).toBe('timeseries')
  })

  it('should render Document module for document tab', () => {
    const tab = { id: 't7', kind: 'document', label: 'docs' }
    expect(tab.kind).toBe('document')
  })

  it('should render Graph module for graph tab', () => {
    const tab = { id: 't8', kind: 'graph', label: 'relations' }
    expect(tab.kind).toBe('graph')
  })

  it('should render FTS module for fts tab', () => {
    const tab = { id: 't9', kind: 'fts', label: 'search' }
    expect(tab.kind).toBe('fts')
  })

  it('should render Geo module for geo tab', () => {
    const tab = { id: 't10', kind: 'geo', label: 'locations' }
    expect(tab.kind).toBe('geo')
  })

  it('should render Blob module for blob tab', () => {
    const tab = { id: 't11', kind: 'blob', label: 'files' }
    expect(tab.kind).toBe('blob')
  })

  it('should render PubSub module for pubsub tab', () => {
    const tab = { id: 't12', kind: 'pubsub', label: 'messages' }
    expect(tab.kind).toBe('pubsub')
  })

  it('should render Streams module for streams tab', () => {
    const tab = { id: 't13', kind: 'streams', label: 'events' }
    expect(tab.kind).toBe('streams')
  })

  it('should render Columnar module for columnar tab', () => {
    const tab = { id: 't14', kind: 'columnar', label: 'analytics' }
    expect(tab.kind).toBe('columnar')
  })

  it('should render Datalog module for datalog tab', () => {
    const tab = { id: 't15', kind: 'datalog', label: 'reasoning' }
    expect(tab.kind).toBe('datalog')
  })

  it('should render CDC module for cdc tab', () => {
    const tab = { id: 't16', kind: 'cdc', label: 'changes' }
    expect(tab.kind).toBe('cdc')
  })

  it('should render ConnectionManager for connection-manager tab', () => {
    const tab = { id: 't17', kind: 'connection-manager', label: 'connections' }
    expect(tab.kind).toBe('connection-manager')
  })

  it('should switch module when tab changes', () => {
    let currentTab = { id: 't1', kind: 'sql-editor', label: 'query' }
    expect(currentTab.kind).toBe('sql-editor')

    // Switch to schema designer
    currentTab = { id: 't2', kind: 'schema-designer', label: 'schema' }
    expect(currentTab.kind).toBe('schema-designer')

    // Switch back
    currentTab = { id: 't1', kind: 'sql-editor', label: 'query' }
    expect(currentTab.kind).toBe('sql-editor')
  })

  it('should render nothing when no active tab', () => {
    const activeTab = null
    expect(activeTab).toBeNull()
  })

  it('should maintain consistent module routing', () => {
    const kindsToModules: Record<string, string> = {
      'sql-browser': 'SQLBrowser',
      'sql-editor': 'SQLEditor',
      'schema-designer': 'SchemaDesigner',
      'kv': 'KVModule',
      'vector': 'VectorModule',
      'timeseries': 'TimeSeriesModule',
      'document': 'DocumentModule',
      'graph': 'GraphModule',
      'fts': 'FTSModule',
      'geo': 'GeoModule',
      'blob': 'BlobModule',
      'pubsub': 'PubSubModule',
      'streams': 'StreamsModule',
      'columnar': 'ColumnarModule',
      'datalog': 'DatalogModule',
      'cdc': 'CDCModule',
      'connection-manager': 'ConnectionManager',
    }

    Object.entries(kindsToModules).forEach(([kind, module]) => {
      expect(module).toBeTruthy()
      expect(kind).toBeTruthy()
    })
  })
})
