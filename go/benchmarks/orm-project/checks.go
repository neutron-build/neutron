package main

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
)

type Check struct {
	Provider string `json:"provider"`
	Name     string `json:"name"`
	Passed   bool   `json:"passed"`
	Detail   string `json:"detail,omitempty"`
}

func correctness(ctx context.Context, f *Fixture, providers []Provider) ([]Check, error) {
	checks := make([]Check, 0)
	for _, p := range providers {
		run := func(name string, fn func() error) error {
			err := fn()
			c := Check{Provider: p.Name(), Name: name, Passed: err == nil}
			if err != nil {
				c.Detail = errorCategory(err)
			}
			checks = append(checks, c)
			if err != nil {
				return fmt.Errorf("%s %s failed", p.Name(), name)
			}
			return nil
		}
		if err := run("point_exact_values_tenant_isolation", func() error {
			for _, tenant := range []string{"a", "b"} {
				for _, id := range []int32{1, 2, 3, 4, 5, 6, 7, 8, 20, 2000} {
					got, err := p.Point(ctx, tenant, id)
					if err != nil {
						return err
					}
					if err := same(got, []Document{f.Docs[key(tenant, id)]}); err != nil {
						return err
					}
				}
			}
			return nil
		}); err != nil {
			return checks, err
		}
		if err := run("point_missing_and_keyset", func() error {
			missing, err := p.Point(ctx, "a", 8000)
			if err != nil {
				return err
			}
			if err := same(missing, []Document{}); err != nil {
				return err
			}
			for _, after := range []int32{0, 37, 1987, 2000} {
				got, err := p.Page(ctx, "b", after)
				if err != nil {
					return err
				}
				if err := same(got, f.expectedPage("b", after)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return checks, err
		}
		if err := run("parent_children_empty_missing_composite_key", func() error {
			for _, tenant := range []string{"a", "b"} {
				for _, id := range []int32{1, 100, 101, 102} {
					got, err := p.Relation(ctx, tenant, id)
					if err != nil {
						return err
					}
					if err := same(got, f.expectedRelation(tenant, id)); err != nil {
						return err
					}
				}
			}
			return nil
		}); err != nil {
			return checks, err
		}
		if err := run("create_read_native_oracle_delete", func() error {
			note := ""
			d := Document{Tenant: "a", ID: 9000, ProjectID: 1, Version: largeVersion, Amount: Decimal(exactAmount), Note: &note, Payload: []byte{0, 255, 23}}
			n, err := p.Write(ctx, "insert", d, false)
			if err != nil || n != 1 {
				return errors.New("insert did not affect one row")
			}
			got, err := p.Point(ctx, d.Tenant, d.ID)
			if err != nil {
				return err
			}
			if err := same(got, []Document{d}); err != nil {
				return err
			}
			stored, err := oracleDocuments(ctx, f.Oracle)
			if err != nil {
				return err
			}
			if err := same(stored[key(d.Tenant, d.ID)], d); err != nil {
				return err
			}
			n, err = p.Write(ctx, "delete", d, false)
			if err != nil || n != 1 {
				return errors.New("delete did not affect one row")
			}
			var count int
			if err := f.Oracle.QueryRow(ctx, "SELECT count(*) FROM documents WHERE tenant=$1 AND id=$2", d.Tenant, d.ID).Scan(&count); err != nil || count != 0 {
				return errors.New("delete persisted incorrectly")
			}
			return nil
		}); err != nil {
			return checks, err
		}
		if err := run("explicit_transaction_rollback", func() error {
			d := Document{Tenant: "a", ID: 9001, ProjectID: 1, Version: largeVersion, Amount: Decimal(exactAmount), Payload: nil}
			n, err := p.Write(ctx, "insert", d, true)
			if err != nil || n != 1 {
				return errors.New("rollback mutation failed")
			}
			var count int
			if err := f.Oracle.QueryRow(ctx, "SELECT count(*) FROM documents WHERE tenant=$1 AND id=$2", d.Tenant, d.ID).Scan(&count); err != nil || count != 0 {
				return errors.New("rollback leaked a row")
			}
			return nil
		}); err != nil {
			return checks, err
		}
		if err := run("foreign_key_error_has_no_effect", func() error {
			d := Document{Tenant: "a", ID: 9001, ProjectID: 999, Version: largeVersion, Amount: Decimal(exactAmount)}
			_, err := p.Write(ctx, "insert", d, false)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "23503" {
				return errors.New("expected actual foreign-key SQLSTATE 23503")
			}
			var count int
			if err := f.Oracle.QueryRow(ctx, "SELECT count(*) FROM documents WHERE tenant=$1 AND id=$2", d.Tenant, d.ID).Scan(&count); err != nil || count != 0 {
				return errors.New("FK error leaked a row")
			}
			return nil
		}); err != nil {
			return checks, err
		}
		if err := run("concurrent_cas_exactly_one_winner", func() error {
			d := Document{Tenant: "a", ID: 9002, ProjectID: 1, Version: largeVersion, Amount: Decimal(exactAmount), Payload: []byte{0, 255}}
			if _, err := p.Write(ctx, "insert", d, false); err != nil {
				return err
			}
			var wg sync.WaitGroup
			start := make(chan struct{})
			results := make([]int64, 2)
			errs := make([]error, 2)
			for i := range 2 {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					note := fmt.Sprintf("winner-%d", i)
					update := d
					update.Note = &note
					results[i], errs[i] = p.Write(ctx, "cas", update, false)
				}(i)
			}
			close(start)
			wg.Wait()
			if errs[0] != nil || errs[1] != nil || results[0]+results[1] != 1 {
				return errors.New("CAS did not have exactly one successful writer")
			}
			stored, err := oracleDocuments(ctx, f.Oracle)
			if err != nil {
				return err
			}
			winner := stored[key(d.Tenant, d.ID)]
			idx := 0
			if results[1] == 1 {
				idx = 1
			}
			note := fmt.Sprintf("winner-%d", idx)
			expected := d
			expected.Version++
			expected.Note = &note
			if err := same(winner, expected); err != nil {
				return err
			}
			n, err := p.Write(ctx, "cas", d, false)
			if err != nil || n != 0 {
				return errors.New("stale CAS changed a row")
			}
			_, err = p.Write(ctx, "delete", d, false)
			return err
		}); err != nil {
			return checks, err
		}
	}
	digest, err := nativeDigest(ctx, f.Oracle)
	if err != nil || digest != f.Digest {
		return checks, errors.New("correctness left fixture mutations")
	}
	return checks, nil
}

func associationProbe(ctx context.Context, f *Fixture, p *gormProvider, audit *queryAudit) ([]Check, []string) {
	checks := make([]Check, 0)
	audit.reset()
	var projects []Project
	err := p.db.WithContext(ctx).Where("id IN ?", []int32{1, 101}).Order("tenant,id").Preload("Documents", func(db *gorm.DB) *gorm.DB { return db.Order("id") }).Find(&projects).Error
	queries := audit.snapshot()
	if err == nil && len(projects) != 4 {
		err = errors.New("unexpected project count")
	}
	if err == nil {
		for _, project := range projects {
			want := f.expectedRelation(project.Tenant, project.ID)
			if same(project.Documents, want.Documents) != nil {
				err = errors.New("composite association differed")
				break
			}
		}
	}
	c := Check{Provider: "gorm", Name: "native_preload_composite_key_both_tenants_and_empty", Passed: err == nil}
	if err != nil {
		c.Detail = errorCategory(err)
	}
	checks = append(checks, c)
	// A single parent is a separate shape from the composite tuple IN above.
	audit.reset()
	projects = nil
	err = p.db.WithContext(ctx).Where("tenant = ? AND id = ?", "a", 1).Preload("Documents", func(db *gorm.DB) *gorm.DB { return db.Order("id") }).Find(&projects).Error
	if err == nil {
		if len(projects) != 1 || same(projects[0].Documents, f.expectedRelation("a", 1).Documents) != nil {
			err = errors.New("single association differed")
		}
	}
	c = Check{Provider: "gorm", Name: "native_preload_single_parent", Passed: err == nil}
	if err != nil {
		c.Detail = errorCategory(err)
	}
	checks = append(checks, c)
	queries = append(queries, audit.snapshot()...)
	return checks, queries
}
