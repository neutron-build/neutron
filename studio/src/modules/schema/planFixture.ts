import type { PlanOperation, SchemaPlanResponse } from '../../lib/types'

// Test fixture: a SchemaPlanResponse whose derived fields (up/down/risk)
// are computed from its operations, the way the server's M03 plan builder
// derives them.

export function op(index: number, sql: string, extra: Partial<PlanOperation> = {}): PlanOperation {
  return { index, sql, down: `-- down ${index}`, destructive: false, dataLoss: false, reversibility: 'reversible', ...extra }
}

export function makePlan(p: Partial<SchemaPlanResponse> & { operations?: PlanOperation[] }): SchemaPlanResponse {
  const operations = p.operations ?? []
  const hasDestructive = operations.some(o => o.destructive)
  const hasDataLoss = operations.some(o => o.dataLoss)
  return {
    planId: 'a'.repeat(64),
    baseSha256: 'b'.repeat(64),
    targetSha256: 'c'.repeat(64),
    up: operations.map(o => o.sql),
    down: operations.map(o => o.down),
    warnings: [],
    risk: {
      hasDestructive, hasDataLoss, statementCount: operations.length,
      irreversibleCount: operations.filter(o => o.reversibility === 'irreversible').length,
      overallReversibility: 'reversible',
    },
    transactionMode: 'single',
    renameFlags: [],
    designerNotes: [],
    cliEquivalent: 'neutron db push --dry-run --schema target.schema.json',
    target: { version: 2 },
    applied: false,
    ...p,
    operations,
  }
}
