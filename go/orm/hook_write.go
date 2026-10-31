package orm

import (
	"context"
	"errors"
)

// HookInsert performs one explicitly hooked INSERT RETURNING workflow. Its
// returned model is the statement snapshot, before any extra after-hook SQL.
func HookInsert[M any](ctx context.Context, s *WriteSession, repo HookRepository[M], assignments ...Assignment[M]) (M, error) {
	var zero M
	intent := snapshotHookIntent(HookCreate, repo.table, Predicate[M]{}, assignments)
	if _, _, err := insertSQL(repo.table, intent.assignments); err != nil {
		return zero, err
	}
	return hookWrite(ctx, s, repo, intent, func() (WriteEvent[M], error) {
		model, err := InsertOne(ctx, s.scope, repo.table, intent.assignments...)
		return WriteEvent[M]{Operation: HookCreate, AffectedRows: 1, Model: &model}, err
	})
}
func HookUpdate[M any](ctx context.Context, s *WriteSession, repo HookRepository[M], predicate Predicate[M], assignments ...Assignment[M]) (int64, error) {
	intent := snapshotHookIntent(HookModify, repo.table, predicate, assignments)
	if _, _, err := updateSQL(repo.table, predicate, intent.assignments); err != nil {
		return 0, err
	}
	_, event, err := hookStatement(ctx, s, repo, intent, func() (WriteEvent[M], error) {
		count, err := Update(ctx, s.scope, repo.table, predicate, intent.assignments...)
		return WriteEvent[M]{Operation: HookModify, AffectedRows: count}, err
	})
	return event.AffectedRows, err
}
func HookDelete[M any](ctx context.Context, s *WriteSession, repo HookRepository[M], predicate Predicate[M]) (int64, error) {
	intent := snapshotHookIntent(HookRemove, repo.table, predicate, nil)
	if _, _, err := deleteSQL(repo.table, predicate); err != nil {
		return 0, err
	}
	_, event, err := hookStatement(ctx, s, repo, intent, func() (WriteEvent[M], error) {
		count, err := Delete(ctx, s.scope, repo.table, predicate)
		return WriteEvent[M]{Operation: HookRemove, AffectedRows: count}, err
	})
	return event.AffectedRows, err
}
func hookWrite[M any](ctx context.Context, s *WriteSession, repo HookRepository[M], intent WriteIntent[M], sql func() (WriteEvent[M], error)) (M, error) {
	model, _, err := hookStatement(ctx, s, repo, intent, sql)
	return model, err
}
func hookStatement[M any](ctx context.Context, s *WriteSession, repo HookRepository[M], intent WriteIntent[M], sql func() (WriteEvent[M], error)) (model M, event WriteEvent[M], result error) {
	if err := s.begin(ctx); err != nil {
		return model, event, err
	}
	finished := false
	defer func() {
		if !finished {
			s.end(ErrHookWorkflow)
		}
	}()
	before := repo.hooks.BeforeCreate
	after := repo.hooks.AfterCreate
	if intent.operation == HookModify {
		before = repo.hooks.BeforeUpdate
		after = repo.hooks.AfterUpdate
	}
	if intent.operation == HookRemove {
		before = repo.hooks.BeforeDelete
		after = repo.hooks.AfterDelete
	}
	for _, group := range [][]BeforeHook[M]{repo.hooks.BeforeWrite, before} {
		for _, callback := range group {
			result = s.invoke(ctx, func(h *HookContext) error { return callback(h.token.ctx, h, intent) })
			if result != nil {
				result = s.end(result)
				finished = true
				return
			}
		}
	}
	if err := ctx.Err(); err != nil {
		result = s.end(err)
		finished = true
		return
	}
	event, result = sql()
	if result == nil {
		for _, group := range [][]AfterHook[M]{after, repo.hooks.AfterWrite} {
			for _, callback := range group {
				result = s.invoke(ctx, func(h *HookContext) error { return callback(h.token.ctx, h, cloneHookEvent(event, repo.table.info)) })
				if result != nil {
					break
				}
			}
			if result != nil {
				break
			}
		}
	}
	result = errors.Join(result, ctx.Err())
	if result == nil {
		snapshot := cloneHookEvent(event, repo.table.info)
		queued := queuedHookEvent{}
		for _, callback := range repo.hooks.AfterCommit {
			hook := callback
			queued.callbacks = append(queued.callbacks, func(dispatch context.Context) error { return hook(dispatch, cloneHookEvent(snapshot, repo.table.info)) })
		}
		s.scope.owner.mu.Lock()
		scopeErr := s.scope.failed
		s.scope.owner.mu.Unlock()
		s.mu.Lock()
		if s.failed != nil || scopeErr != nil {
			result = errors.Join(s.failed, scopeErr)
		}
		if s.closed {
			result = ErrHookClosed
			s.failed = errors.Join(s.failed, ErrHookWorkflow, result)
		} else if result == nil {
			s.events = append(s.events, queued)
		}
		s.mu.Unlock()
		if event.Model != nil {
			model = cloneHookModel(*event.Model, repo.table.info)
		}
	}
	result = s.end(result)
	finished = true
	return
}
