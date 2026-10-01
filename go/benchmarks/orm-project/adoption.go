package main

import (
	"context"
	"errors"
)

// IssueService is an executable tiny application boundary, not a benchmark API.
// Tenant is fixed by the application service instance; this fixture does not
// implement authentication. Callers cannot supply another tenant to its methods.
type IssueService struct {
	tenant string
	data   Provider
}

var ErrEditConflict = errors.New("issue changed concurrently")

func (s IssueService) Create(ctx context.Context, id, project int32, note string) error {
	d := Document{Tenant: s.tenant, ID: id, ProjectID: project, Version: largeVersion, Amount: Decimal(exactAmount), Note: &note, Payload: []byte{0, 255, 23}}
	n, e := s.data.Write(ctx, "insert", d, false)
	if e == nil && n != 1 {
		return errors.New("issue create affected unexpected rows")
	}
	return e
}
func (s IssueService) Edit(ctx context.Context, id int32, version int64, note string) error {
	n, e := s.data.Write(ctx, "cas", Document{Tenant: s.tenant, ID: id, Version: version, Note: &note}, false)
	if e == nil && n == 0 {
		return ErrEditConflict
	}
	if e == nil && n != 1 {
		return errors.New("edit affected unexpected rows")
	}
	return e
}
func (s IssueService) Get(ctx context.Context, id int32) ([]Document, error) {
	return s.data.Point(ctx, s.tenant, id)
}
func (s IssueService) List(ctx context.Context, after int32) ([]Document, error) {
	return s.data.Page(ctx, s.tenant, after)
}
func (s IssueService) Project(ctx context.Context, id int32) (Relation, error) {
	return s.data.Relation(ctx, s.tenant, id)
}
func (s IssueService) DiscardDraft(ctx context.Context, id int32) error {
	note := "discarded"
	_, e := s.data.Write(ctx, "insert", Document{Tenant: s.tenant, ID: id, ProjectID: 1, Version: largeVersion, Amount: Decimal(exactAmount), Note: &note, Payload: []byte{1}}, true)
	return e
}
func (s IssueService) Delete(ctx context.Context, id int32) error {
	n, e := s.data.Write(ctx, "delete", Document{Tenant: s.tenant, ID: id}, false)
	if e == nil && n != 1 {
		return errors.New("delete affected unexpected rows")
	}
	return e
}
func adoptionScenario(ctx context.Context, f *Fixture, p Provider) error {
	a, b := IssueService{"a", p}, IssueService{"b", p}
	const id int32 = 12000
	if e := a.Create(ctx, id, 1, "new issue"); e != nil {
		return e
	}
	if e := b.Create(ctx, id, 1, "other tenant"); e != nil {
		return e
	}
	if e := a.Edit(ctx, id, largeVersion, "resolved"); e != nil {
		return e
	}
	if e := a.Edit(ctx, id, largeVersion, "stale"); e != ErrEditConflict {
		return errors.New("stale application edit did not conflict")
	}
	rows, e := a.Get(ctx, id)
	if e != nil || len(rows) != 1 || *rows[0].Note != "resolved" || rows[0].Version != largeVersion+1 {
		return errors.New("application read mismatch")
	}
	other, e := b.Get(ctx, id)
	if e != nil || len(other) != 1 || *other[0].Note != "other tenant" || other[0].Version != largeVersion {
		return errors.New("application tenant isolation mismatch")
	}
	list, e := a.List(ctx, 11999)
	if e != nil || len(list) != 1 || list[0].ID != id {
		return errors.New("application list mismatch")
	}
	relation, e := a.Project(ctx, 1)
	if e != nil || relation.Project == nil || len(relation.Documents) != 21 {
		return errors.New("application relation mismatch")
	}
	if e = a.DiscardDraft(ctx, 12001); e != nil {
		return e
	}
	draft, e := a.Get(ctx, 12001)
	if e != nil || len(draft) != 0 {
		return errors.New("application draft rollback failed")
	}
	for _, d := range append(rows, other...) {
		got, e := extendedOracle(ctx, f, d.Tenant, d.ID)
		if e != nil {
			return e
		}
		if e = same(got, []Document{d}); e != nil {
			return e
		}
	}
	if e = a.Delete(ctx, id); e != nil {
		return e
	}
	if e = b.Delete(ctx, id); e != nil {
		return e
	}
	count := 0
	if e = f.Oracle.QueryRow(ctx, "SELECT count(*) FROM documents WHERE id IN (12000,12001)").Scan(&count); e != nil || count != 0 {
		return errors.New("application cleanup oracle failed")
	}
	return nil
}
