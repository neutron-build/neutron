import { describe, it, expect, afterEach, vi } from 'vitest'
import { render, screen, cleanup, fireEvent } from '@testing-library/preact'
import { PlanReview, planSqlText } from './PlanReview'
import { makePlan, op } from './planFixture'

// S05 plan review: the designer's visual changes surface as the CLI
// planner's own statements with the M03 per-operation risk; plans that drop
// objects or lose data need an explicit acknowledgement before Apply is
// enabled, and the CLI equivalent is shown for reproduction.

afterEach(cleanup)

describe('PlanReview (S05)', () => {
  it('lists the planned statements, the base identity and the CLI equivalent', () => {
    const onApply = vi.fn()
    render(<PlanReview plan={makePlan({
      operations: [
        op(1, 'alter table "public"."orders" add column "channel" varchar(40)'),
        op(2, 'create index "orders_channel_idx" on "public"."orders" using btree ("channel")'),
      ],
    })} applying={false} onApply={onApply} onCancel={vi.fn()} />)
    expect(screen.getByText(/Migration plan — 2 statements/)).toBeTruthy()
    expect(screen.getByText(/add column "channel"/)).toBeTruthy()
    expect(screen.getByText('bbbbbbbbbbbb')).toBeTruthy()
    expect(screen.getByText('neutron db push --dry-run --schema target.schema.json')).toBeTruthy()
    expect(screen.queryByText('destructive')).toBeNull()
    expect(screen.queryByRole('checkbox')).toBeNull()
    // A safe plan applies without acknowledgement.
    fireEvent.click(screen.getByText('Apply plan'))
    expect(onApply).toHaveBeenCalledWith(false)
  })

  it('requires an explicit acknowledgement for data loss before Apply is enabled', () => {
    const onApply = vi.fn()
    render(<PlanReview plan={makePlan({
      operations: [op(1, 'alter table "public"."orders" drop column if exists "note"', { destructive: true, dataLoss: true })],
      designerNotes: ['index "orders_note_idx" on public.orders involves column "note" and is dropped with it'],
    })} applying={false} onApply={onApply} onCancel={vi.fn()} />)
    expect(screen.getAllByText('data loss').length).toBeGreaterThan(0)
    expect(screen.getByText(/orders_note_idx/)).toBeTruthy()
    const apply = screen.getByText('Apply plan') as HTMLButtonElement
    expect(apply.disabled).toBe(true)
    fireEvent.click(apply)
    expect(onApply).not.toHaveBeenCalled()
    const ack = screen.getByRole('checkbox') as HTMLInputElement
    expect(ack.closest('label')?.textContent).toContain('can lose data')
    fireEvent.click(ack)
    expect((screen.getByText('Apply plan') as HTMLButtonElement).disabled).toBe(false)
    fireEvent.click(screen.getByText('Apply plan'))
    expect(onApply).toHaveBeenCalledWith(true)
  })

  it('renders planner warnings, down statements on demand, and an apply error', () => {
    render(<PlanReview plan={makePlan({
      operations: [op(1, 'alter table "public"."orders" rename column "note" to "remark"', { down: 'alter table "public"."orders" rename column "remark" to "note"' })],
      warnings: ['view public.v is unchanged but is dropped and recreated around the table alterations'],
    })} applying={false} error="The live catalog changed since you reviewed this plan" onApply={vi.fn()} onCancel={vi.fn()} />)
    expect(screen.getByText(/dropped and recreated/)).toBeTruthy()
    expect(screen.getByRole('alert').textContent).toContain('changed since you reviewed')
    expect(screen.queryByText(/rename column "remark" to "note"/)).toBeNull()
    fireEvent.click(screen.getByText(/Show reverse/))
    expect(screen.getByText(/rename column "remark" to "note"/)).toBeTruthy()
  })

  it('an empty plan says the catalog already matches and offers no Apply', () => {
    render(<PlanReview plan={makePlan({ operations: [] })} applying={false} onApply={vi.fn()} onCancel={vi.fn()} />)
    expect(screen.getByRole('status').textContent).toContain('already matches')
    expect(screen.queryByText('Apply plan')).toBeNull()
  })

  it('the SQL download is the plan: one transaction, identity and down as comments', () => {
    const text = planSqlText(makePlan({
      operations: [op(1, 'alter table "t" add column "x" text', { down: 'alter table "t" drop column if exists "x"' })],
      warnings: ['w1'],
    }))
    expect(text).toContain('-- Neutron Studio migration plan aaaaaaaaaaaa')
    expect(text).toContain('-- Reproduce: neutron db push --dry-run --schema target.schema.json')
    expect(text).toContain('-- WARNING: w1')
    expect(text).toMatch(/BEGIN;\nalter table "t" add column "x" text;\nCOMMIT;/)
    expect(text).toContain('-- alter table "t" drop column if exists "x";')
  })
})
