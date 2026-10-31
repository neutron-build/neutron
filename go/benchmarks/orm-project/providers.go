package main

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neutron-build/neutron/go/nucleus"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Decimal is an exact textual decimal mapping, not a float64 approximation.
// GORM uses the documented Scanner/Valuer extension point; Neutron scans its
// string kind. The native oracle separately checks the stored NUMERIC text.
type Decimal string

func (d *Decimal) Scan(value any) error {
	switch v := value.(type) {
	case string:
		*d = Decimal(v)
	case []byte:
		*d = Decimal(string(v))
	default:
		return fmt.Errorf("decimal: unexpected source %T", value)
	}
	return nil
}
func (d Decimal) Value() (driver.Value, error) { return string(d), nil }
func (Decimal) GormDataType() string           { return "numeric" }

type Document struct {
	Tenant    string  `db:"tenant" gorm:"primaryKey;autoIncrement:false"`
	ID        int32   `db:"id" gorm:"primaryKey;autoIncrement:false"`
	ProjectID int32   `db:"project_id"`
	Version   int64   `db:"version"`
	Amount    Decimal `db:"amount" gorm:"type:numeric(40,18)"`
	Note      *string `db:"note"`
	Payload   []byte  `db:"payload"`
}

type Project struct {
	Tenant    string     `db:"tenant" gorm:"primaryKey;autoIncrement:false"`
	ID        int32      `db:"id" gorm:"primaryKey;autoIncrement:false"`
	Title     string     `db:"title"`
	Documents []Document `gorm:"foreignKey:Tenant,ProjectID;references:Tenant,ID" db:"-"`
}

type Relation struct {
	Project   *Project
	Documents []Document
}

type Provider interface {
	Name() string
	Point(context.Context, string, int32) ([]Document, error)
	Page(context.Context, string, int32) ([]Document, error)
	Relation(context.Context, string, int32) (Relation, error)
	Write(context.Context, string, Document, bool) (int64, error)
	Close()
}

const columns = "tenant,id,project_id,version,amount,note,payload"
const pointSQL = "SELECT " + columns + " FROM documents WHERE tenant=$1 AND id=$2"
const pageSQL = "SELECT " + columns + " FROM documents WHERE tenant=$1 AND id>$2 ORDER BY id LIMIT 20"
const relationSQL = "SELECT " + columns + " FROM documents WHERE tenant=$1 AND project_id=$2 ORDER BY id"
const projectSQL = "SELECT tenant,id,title FROM projects WHERE tenant=$1 AND id=$2"
const insertSQL = "INSERT INTO documents(" + columns + ") VALUES($1,$2,$3,$4,$5,$6,$7)"
const deleteSQL = "DELETE FROM documents WHERE tenant=$1 AND id=$2"
const casSQL = "UPDATE documents SET version=version+1,note=$4 WHERE tenant=$1 AND id=$2 AND version=$3"

func poolConfig(url string, tracer *queryAudit) (*pgxpool.Config, error) {
	c, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, errors.New("invalid database configuration")
	}
	c.MaxConns, c.MinConns = 4, 0
	c.MaxConnLifetime, c.MaxConnLifetimeJitter = time.Hour, 0
	c.MaxConnIdleTime, c.HealthCheckPeriod = 5*time.Minute, time.Minute
	// Match Neutron's public SQL protocol rather than comparing its simple
	// protocol with pgx/GORM's implicit prepared statement cache.
	c.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	if tracer != nil {
		c.ConnConfig.Tracer = tracer
	}
	return c, nil
}

type nativeProvider struct{ client *nucleus.Client }

func (p *nativeProvider) Name() string { return "neutron-native-sql" }
func (p *nativeProvider) Close()       { p.client.Close() }
func (p *nativeProvider) Point(ctx context.Context, tenant string, id int32) ([]Document, error) {
	return nucleus.Query[Document](ctx, p.client.SQL(), pointSQL, tenant, id)
}
func (p *nativeProvider) Page(ctx context.Context, tenant string, after int32) ([]Document, error) {
	return nucleus.Query[Document](ctx, p.client.SQL(), pageSQL, tenant, after)
}
func (p *nativeProvider) Relation(ctx context.Context, tenant string, id int32) (Relation, error) {
	projects, err := nucleus.Query[Project](ctx, p.client.SQL(), projectSQL, tenant, id)
	if err != nil || len(projects) == 0 {
		return Relation{Documents: []Document{}}, err
	}
	docs, err := nucleus.Query[Document](ctx, p.client.SQL(), relationSQL, tenant, id)
	return Relation{Project: &projects[0], Documents: docs}, err
}
func (p *nativeProvider) Write(ctx context.Context, op string, d Document, rollback bool) (int64, error) {
	tx, err := p.client.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	query, args := writeArgs(op, d)
	n, err := tx.SQL().Exec(ctx, query, args...)
	if err != nil {
		return n, err
	}
	if rollback {
		return n, tx.Rollback(ctx)
	}
	return n, tx.Commit(ctx)
}

type rawProvider struct{ pool *pgxpool.Pool }

func (p *rawProvider) Name() string { return "raw-pgx" }
func (p *rawProvider) Close()       { p.pool.Close() }
func rawDocuments(ctx context.Context, pool *pgxpool.Pool, query string, args ...any) ([]Document, error) {
	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	docs := make([]Document, 0)
	for rows.Next() {
		var d Document
		var amount string
		if err := rows.Scan(&d.Tenant, &d.ID, &d.ProjectID, &d.Version, &amount, &d.Note, &d.Payload); err != nil {
			return nil, err
		}
		d.Amount = Decimal(amount)
		docs = append(docs, d)
	}
	return docs, rows.Err()
}
func (p *rawProvider) Point(ctx context.Context, tenant string, id int32) ([]Document, error) {
	return rawDocuments(ctx, p.pool, pointSQL, tenant, id)
}
func (p *rawProvider) Page(ctx context.Context, tenant string, after int32) ([]Document, error) {
	return rawDocuments(ctx, p.pool, pageSQL, tenant, after)
}
func (p *rawProvider) Relation(ctx context.Context, tenant string, id int32) (Relation, error) {
	var project Project
	err := p.pool.QueryRow(ctx, projectSQL, tenant, id).Scan(&project.Tenant, &project.ID, &project.Title)
	if errors.Is(err, pgx.ErrNoRows) {
		return Relation{Documents: []Document{}}, nil
	}
	if err != nil {
		return Relation{}, err
	}
	docs, err := rawDocuments(ctx, p.pool, relationSQL, tenant, id)
	return Relation{Project: &project, Documents: docs}, err
}
func (p *rawProvider) Write(ctx context.Context, op string, d Document, rollback bool) (int64, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	query, args := writeArgs(op, d)
	tag, err := tx.Exec(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	if rollback {
		return tag.RowsAffected(), tx.Rollback(ctx)
	}
	return tag.RowsAffected(), tx.Commit(ctx)
}

func writeArgs(op string, d Document) (string, []any) {
	switch op {
	case "insert":
		return insertSQL, []any{d.Tenant, d.ID, d.ProjectID, d.Version, string(d.Amount), d.Note, d.Payload}
	case "delete":
		return deleteSQL, []any{d.Tenant, d.ID}
	case "cas":
		return casSQL, []any{d.Tenant, d.ID, d.Version, d.Note}
	default:
		panic("unknown internal write operation")
	}
}

type gormProvider struct{ db *gorm.DB }

func (p *gormProvider) Name() string { return "gorm" }
func (p *gormProvider) Close() {
	db, _ := p.db.DB()
	if db != nil {
		db.Close()
	}
}
func (p *gormProvider) Point(ctx context.Context, tenant string, id int32) ([]Document, error) {
	docs := make([]Document, 0)
	err := p.db.WithContext(ctx).Where("tenant = ? AND id = ?", tenant, id).Find(&docs).Error
	return docs, err
}
func (p *gormProvider) Page(ctx context.Context, tenant string, after int32) ([]Document, error) {
	docs := make([]Document, 0)
	err := p.db.WithContext(ctx).Where("tenant = ? AND id > ?", tenant, after).Order("id").Limit(20).Find(&docs).Error
	return docs, err
}
func (p *gormProvider) Relation(ctx context.Context, tenant string, id int32) (Relation, error) {
	projects := make([]Project, 0)
	err := p.db.WithContext(ctx).Omit("Documents").Where("tenant = ? AND id = ?", tenant, id).Find(&projects).Error
	if err != nil || len(projects) == 0 {
		return Relation{Documents: []Document{}}, err
	}
	docs := make([]Document, 0)
	err = p.db.WithContext(ctx).Where("tenant = ? AND project_id = ?", tenant, id).Order("id").Find(&docs).Error
	return Relation{Project: &projects[0], Documents: docs}, err
}
func (p *gormProvider) Write(ctx context.Context, op string, d Document, rollback bool) (int64, error) {
	var n int64
	sentinel := errors.New("intentional rollback")
	err := p.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var result *gorm.DB
		switch op {
		case "insert":
			result = tx.Create(&d)
		case "delete":
			result = tx.Where("tenant = ? AND id = ?", d.Tenant, d.ID).Delete(&Document{})
		case "cas":
			result = tx.Model(&Document{}).Where("tenant = ? AND id = ? AND version = ?", d.Tenant, d.ID, d.Version).
				Updates(map[string]any{"version": gorm.Expr("version + 1"), "note": d.Note})
		default:
			panic("unknown internal write operation")
		}
		n = result.RowsAffected
		if result.Error != nil {
			return result.Error
		}
		if rollback {
			return sentinel
		}
		return nil
	})
	if errors.Is(err, sentinel) {
		err = nil
	}
	return n, err
}

func openProviders(ctx context.Context, url string, trace *queryAudit) ([]Provider, error) {
	var providers []Provider
	cleanup := func() {
		for _, p := range providers {
			p.Close()
		}
	}
	cfg, err := poolConfig(url, trace)
	if err != nil {
		return nil, err
	}
	native, err := nucleus.Connect(ctx, url, nucleus.WithPoolConfig(cfg))
	if err != nil {
		return nil, err
	}
	providers = append(providers, &nativeProvider{native})
	cfg, err = poolConfig(url, trace)
	if err != nil {
		cleanup()
		return nil, err
	}
	raw, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		cleanup()
		return nil, err
	}
	providers = append(providers, &rawProvider{raw})
	db, err := gorm.Open(postgres.New(postgres.Config{DSN: url, PreferSimpleProtocol: true}), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
		// Keep default write transactions. Correctness uses an explicit outer
		// transaction for each provider; timed workloads are reads only.
	})
	if err != nil {
		cleanup()
		return nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		cleanup()
		return nil, err
	}
	sqlDB.SetMaxOpenConns(4)
	sqlDB.SetMaxIdleConns(4)
	sqlDB.SetConnMaxLifetime(time.Hour)
	sqlDB.SetConnMaxIdleTime(5 * time.Minute)
	if trace != nil {
		if err := db.Callback().Query().After("gorm:query").Register("comparison:audit", func(tx *gorm.DB) {
			trace.record(tx.Statement.SQL.String())
		}); err != nil {
			sqlDB.Close()
			cleanup()
			return nil, err
		}
	}
	providers = append(providers, &gormProvider{db})
	return providers, nil
}

type queryAudit struct {
	mu  sync.Mutex
	SQL []string
}

func (a *queryAudit) record(query string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.SQL = append(a.SQL, query)
}
func (a *queryAudit) reset() { a.mu.Lock(); a.SQL = nil; a.mu.Unlock() }
func (a *queryAudit) snapshot() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string{}, a.SQL...)
}
func (a *queryAudit) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	a.record(data.SQL)
	return ctx
}
func (*queryAudit) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func errorCategory(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return "SQLSTATE:" + pgErr.Code
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return fmt.Sprintf("%T", err)
}

func parseInt32(s string) (int32, error) {
	v, err := strconv.ParseInt(s, 10, 32)
	return int32(v), err
}
