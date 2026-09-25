import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { render, screen, cleanup, fireEvent } from '@testing-library/preact'
import { limitsReport, limitsError } from '../lib/store'
import limitsFixture from '../lib/limits.fixture.json'
import type { LimitsReport } from '../lib/types'
import { ModelLimits, modelForTabKind, availabilityText, transactionText, durabilityText } from './ModelLimits'

// X06: the limits strip renders the server's limits (the fixture is the
// Go registry's exact output, pinned by cli/internal/inspect's fixture
// test) and never states more than they do.

const pg = limitsFixture.postgres as LimitsReport
const nucleus = limitsFixture.nucleus as LimitsReport
const unmeasured = limitsFixture.nucleusUnmeasured as LimitsReport

beforeEach(() => {
  cleanup()
  limitsReport.value = null
  limitsError.value = null
})
afterEach(() => cleanup())

describe('ModelLimits (X06)', () => {
  it('shows every Nucleus model without implying atomicity or availability it lacks', () => {
    limitsReport.value = nucleus
    for (const m of nucleus.models) {
      cleanup()
      const { container } = render(<ModelLimits model={m.model} />)
      const text = container.textContent ?? ''
      if (m.transaction !== 'atomic') expect(text).not.toMatch(/transactions: atomic/)
      if (m.availability !== 'supported') expect(text).not.toMatch(/\bavailable\b(?! on)/)
      if (m.model !== 'sql' && m.atomicWithSql !== 'supported') expect(text).not.toMatch(/(?<!not )atomic with SQL rows/)
      for (const w of m.warnings) expect(text).toContain(w)
      expect(text).toContain(m.transactionNote)
      expect(text).toContain(m.durabilityNote)
    }
  })

  it('names Nucleus SQL transactions partial, with the DDL hazard visible', () => {
    limitsReport.value = nucleus
    render(<ModelLimits model="sql" />)
    expect(screen.getByText('transactions: partial')).toBeTruthy()
    expect(screen.getByText(/DDL runs outside the transaction/)).toBeTruthy()
    expect(screen.getByText(/Nucleus 1\.0\.2/)).toBeTruthy()
  })

  it('names PostgreSQL SQL atomic and shows the live durability settings', () => {
    limitsReport.value = pg
    render(<ModelLimits model="sql" />)
    expect(screen.getByText('transactions: atomic')).toBeTruthy()
    expect(screen.getByText(/fsync=on, synchronous_commit=on/)).toBeTruthy()
  })

  it('reports Nucleus models unavailable on PostgreSQL', () => {
    limitsReport.value = pg
    render(<ModelLimits model="graph" />)
    expect(screen.getByText('unavailable on this engine')).toBeTruthy()
  })

  it('demotes everything on an unmeasured build and says why', () => {
    limitsReport.value = unmeasured
    render(<ModelLimits model="document" />)
    expect(screen.getByRole('alert').textContent).toMatch(/9\.9\.9/)
    expect(screen.getByText('availability unknown')).toBeTruthy()
    expect(screen.getByText('transactions: unknown')).toBeTruthy()
    expect(screen.getByText('durability: unknown')).toBeTruthy()
  })

  it('claims nothing while limits are missing', () => {
    limitsError.value = 'boom'
    render(<ModelLimits model="kv" />)
    expect(screen.getByText('limits not loaded')).toBeTruthy()
    expect(screen.getByText(/no transaction or durability guarantee is assumed/)).toBeTruthy()
  })

  it('lists the evidence behind a limit on request', () => {
    limitsReport.value = nucleus
    render(<ModelLimits model="cdc" />)
    fireEvent.click(screen.getByText('Evidence'))
    const list = screen.getByLabelText('Evidence')
    expect(list.textContent).toContain('cdc.delivery_shape')
    expect(list.textContent).toContain('supports availability, warnings')
    expect(list.textContent).toContain('nucleus/ tree 3313729ae513')
    expect(list.textContent).toContain('matched by version string only')
  })

  it('never says crash for a restart whose kill signal was not recorded (X06 review 1)', () => {
    limitsReport.value = nucleus
    for (const model of ['sql', 'kv', 'blob']) {
      cleanup()
      const { container } = render(<ModelLimits model={model} />)
      const text = container.textContent ?? ''
      expect(text).toContain('survived engine kill + restart (measured)')
      expect(text).not.toMatch(/crash/)
      expect(text).toContain('SIGKILL is not claimed')
    }
  })

  it('maps chip tones only from the reported status', () => {
    for (const m of [...pg.models, ...nucleus.models, ...unmeasured.models]) {
      expect(availabilityText(m)[1] === 'ok').toBe(m.availability === 'supported')
      expect(transactionText(m)[0].includes('atomic')).toBe(m.transaction === 'atomic')
      expect(durabilityText(m)[1] === 'ok').toBe(m.durability === 'survives-restart' || m.durability === 'engine-documented')
    }
  })

  it('maps every acting tab kind to its model', () => {
    expect(modelForTabKind('sql-browser')).toBe('sql')
    expect(modelForTabKind('sql-editor')).toBe('sql')
    expect(modelForTabKind('schema-designer')).toBe('sql')
    for (const k of ['kv', 'vector', 'timeseries', 'document', 'graph', 'fts', 'geo', 'blob', 'pubsub', 'streams', 'columnar', 'datalog', 'cdc']) {
      expect(modelForTabKind(k)).toBe(k)
      expect(nucleus.models.some(m => m.model === k)).toBe(true)
    }
    expect(modelForTabKind('journey')).toBeNull()
  })
})
