package orm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Explicit endpoint profiles (NP01). The core Executor stays caller-owned and
// unchanged; nothing here runs unless a caller selects a profile.
//
// AdmitPostgresDirect admits a reported PostgreSQL identity only.
// AdmitNucleusFinite admits an UNCERTIFIED finite Nucleus candidate (startup
// 16.0 (Nucleus), SQL 1.2.0) and returns a guarded Executor that dispatches
// only generated point CRUD over registered, schema-qualified models whose
// every field is int32/int64/bool/string/time.Time/JSON (nullable pointers
// allowed). Raw SQL, joins, ORDER BY/LIMIT/OFFSET (so SelectOne, Stream and
// keyset paging), aggregates, catalog access, custom or numeric scalars and
// stronger isolation are refused before dispatch. Reported identity is not
// binary, TLS or intermediary attestation and the profile never enables a
// package support matrix. Gap: owned transactions are only guarded through
// FiniteExecutor.WithTransaction and FiniteScope.Savepoint; a raw *Scope or
// executor obtained elsewhere bypasses the guard.
const (
	PostgresDirectProfile   = "postgres-direct"
	NucleusCandidateProfile = "nucleus-relational-rc-v1-candidate"
	NucleusCandidateVersion = "1.2.2"
)

// ErrProfileRefused matches every refusal by an explicit execution profile.
var ErrProfileRefused = errors.New("orm: operation outside the selected execution profile")

// ProfileError carries a static refusal reason that never includes SQL text,
// parameter values or server messages.
type ProfileError struct{ Reason string }

func (e *ProfileError) Error() string        { return "orm: profile refused: " + e.Reason }
func (e *ProfileError) Is(target error) bool { return target == ErrProfileRefused }

func refuseProfile(reason string) error { return &ProfileError{reason} }

// EndpointIdentity is the reported engine/version admission. It is immutable:
// fields are unexported and Capabilities returns a copy.
type EndpointIdentity struct {
	engine, version, profile, qualification string
	capabilities                            []string
	packageEnabled                          bool
}

func (i EndpointIdentity) Engine() string        { return i.engine }
func (i EndpointIdentity) Version() string       { return i.version }
func (i EndpointIdentity) Profile() string       { return i.profile }
func (i EndpointIdentity) Qualification() string { return i.qualification }
func (i EndpointIdentity) PackageEnabled() bool  { return i.packageEnabled }
func (i EndpointIdentity) Capabilities() []string {
	return append([]string(nil), i.capabilities...)
}

var (
	startupVersionPattern  = regexp.MustCompile(`^([0-9]+(?:\.[0-9]+){1,2})(?:\s+\([^\r\n]*\))?$`)
	reportedVersionPattern = regexp.MustCompile(`^PostgreSQL ([0-9]+(?:\.[0-9]+){1,2})(?:\s|$)`)
	unsupportedEngines     = []string{"nucleus", "cockroach", "yugabyte", "redshift", "greenplum", "materialize", "questdb", "cratedb"}
)

const (
	nucleusStartup  = "16.0 (Nucleus)"
	nucleusReported = "PostgreSQL 16.0 (Nucleus " + NucleusCandidateVersion + " — The Definitive Database)"
)

func unsupportedEngine(text string) bool {
	folded := strings.ToLower(text)
	for _, marker := range unsupportedEngines {
		if strings.Contains(folded, marker) {
			return true
		}
	}
	return false
}

func admitEndpoint(startup, reported, profile string) (EndpointIdentity, error) {
	switch profile {
	case NucleusCandidateProfile:
		if startup != nucleusStartup {
			return EndpointIdentity{}, refuseProfile("unknown Nucleus candidate startup identity")
		}
		if reported != nucleusReported {
			return EndpointIdentity{}, refuseProfile("unknown or contradictory Nucleus candidate reported identity")
		}
		return EndpointIdentity{engine: "nucleus", version: NucleusCandidateVersion, profile: profile,
			qualification: "uncertified-finite-candidate", capabilities: []string{"point-crud", "read-committed-transaction", "savepoint"}}, nil
	case PostgresDirectProfile:
		if unsupportedEngine(startup) || unsupportedEngine(reported) {
			return EndpointIdentity{}, refuseProfile("unsupported or unknown endpoint engine identity")
		}
		start := startupVersionPattern.FindStringSubmatch(startup)
		report := reportedVersionPattern.FindStringSubmatch(reported)
		if start == nil || report == nil {
			return EndpointIdentity{}, refuseProfile("unsupported or unknown endpoint engine identity")
		}
		if start[1] != report[1] {
			return EndpointIdentity{}, refuseProfile("contradictory or unknown endpoint engine identity")
		}
		return EndpointIdentity{engine: "postgresql", version: start[1], profile: profile,
			qualification: "reported-identity-only", packageEnabled: true}, nil
	}
	return EndpointIdentity{}, refuseProfile("unsupported/unknown execution profile; operation refused")
}

func queryOneText(ctx context.Context, db Executor, sql string) (string, error) {
	rows, err := db.Query(ctx, sql)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var text string
	count := 0
	for rows.Next() {
		count++
		if count > 1 {
			return "", fmt.Errorf("orm: identity query returned several rows")
		}
		if err := rows.Scan(&text); err != nil {
			return "", err
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if count != 1 {
		return "", fmt.Errorf("orm: identity query returned no row")
	}
	return text, nil
}

// probeEndpoint issues two fixed read-only identity queries that precede every
// caller statement.
func probeEndpoint(ctx context.Context, db Executor, profile string) (EndpointIdentity, error) {
	if err := ready(ctx, db); err != nil {
		return EndpointIdentity{}, wrap("admit endpoint", err)
	}
	startup, err := queryOneText(ctx, db, "SELECT current_setting('server_version')")
	if err != nil {
		return EndpointIdentity{}, wrap("admit endpoint", err)
	}
	reported, err := queryOneText(ctx, db, "SELECT version()")
	if err != nil {
		return EndpointIdentity{}, wrap("admit endpoint", err)
	}
	return admitEndpoint(startup, reported, profile)
}

// AdmitPostgresDirect admits only a reported PostgreSQL identity; Nucleus and
// other pgwire servers are refused. It does not wrap the executor.
func AdmitPostgresDirect(ctx context.Context, db Executor) (EndpointIdentity, error) {
	return probeEndpoint(ctx, db, PostgresDirectProfile)
}

// FiniteTable is validated metadata and the generated statement grammar of one
// model for the finite profile. Its zero value is invalid.
type FiniteTable struct{ patterns []*regexp.Regexp }

// NewFiniteTable refuses models whose fields leave the finite scalar families:
// int32 (int4), int64 (int8), bool, string (text), JSON (jsonb) and time.Time
// (timestamptz, UTC arguments only), optionally nullable. int, floats, Decimal,
// UUID, Bytea, temporal and enum helpers and named user types are refused. The
// database column types themselves are verified by native qualification, not
// here.
func NewFiniteTable[M any](table Table[M]) (FiniteTable, error) {
	if table.info == nil {
		return FiniteTable{}, refuseProfile("uninitialized table")
	}
	info := table.info
	if info.schema == "" || info.catalogOIDs != nil {
		return FiniteTable{}, refuseProfile("finite profile requires an ordinary schema-qualified table")
	}
	for _, field := range info.fields {
		if !finiteFieldType(field) {
			return FiniteTable{}, refuseProfile("column type outside uncertified Nucleus finite profile")
		}
	}
	return FiniteTable{finitePatterns(info)}, nil
}

func finiteFieldType(field fieldInfo) bool {
	base := field.typ
	if field.nullable {
		if base.Kind() != reflect.Pointer {
			return false
		}
		base = base.Elem()
	}
	switch base {
	case reflect.TypeOf(time.Time{}), reflect.TypeOf(JSON{}):
		return true
	}
	if base.PkgPath() != "" {
		return false
	}
	switch base.Kind() {
	case reflect.String, reflect.Bool, reflect.Int32, reflect.Int64:
		return true
	}
	return false
}

func finitePatterns(info *modelInfo) []*regexp.Regexp {
	qualified := regexp.QuoteMeta(info.sqlName())
	names := make([]string, len(info.fields))
	for i, field := range info.fields {
		names[i] = regexp.QuoteMeta(quote(field.name))
	}
	column := "(?:" + strings.Join(names, "|") + ")"
	list := func(item string) string { return item + "(?:, " + item + ")*" }
	param := `\$[1-9][0-9]*`
	atom := "(?:" + param + "|DEFAULT)"
	point := column + " = " + param
	returning := "(?: RETURNING " + list(column) + ")?"
	sources := []string{
		"^SELECT " + list(column) + " FROM " + qualified + " WHERE " + point + "$",
		"^INSERT INTO " + qualified + `(?: DEFAULT VALUES| \(` + list(column) + `\) VALUES \(` + list(atom) + `\))` + returning + "$",
		"^UPDATE " + qualified + " SET " + list(column+" = "+atom) + " WHERE " + point + returning + "$",
		"^DELETE FROM " + qualified + " WHERE " + point + returning + "$",
	}
	patterns := make([]*regexp.Regexp, len(sources))
	for i, source := range sources {
		patterns[i] = regexp.MustCompile(source)
	}
	return patterns
}

var (
	quotedIdentifier = regexp.MustCompile(`"(?:[^"]|"")*"`)
	placeholder      = regexp.MustCompile(`\$([1-9][0-9]*)`)
)

func finiteArgument(value any) bool {
	switch v := value.(type) {
	case nil, bool, string, int32, int64:
		return true
	case time.Time:
		_, offset := v.Zone()
		return offset == 0 && v.Nanosecond()%1000 == 0
	case JSON:
		return v.valid
	}
	return false
}

func admitFinite(patterns []*regexp.Regexp, sql string, args []any) error {
	matched := false
	for _, pattern := range patterns {
		if pattern.MatchString(sql) {
			matched = true
			break
		}
	}
	if !matched {
		return refuseProfile("SQL shape outside uncertified Nucleus finite point profile; refused before dispatch")
	}
	found := placeholder.FindAllStringSubmatch(quotedIdentifier.ReplaceAllString(sql, `""`), -1)
	if len(found) != len(args) {
		return refuseProfile("parameter count outside uncertified Nucleus finite point profile")
	}
	for i, match := range found {
		if match[1] != fmt.Sprint(i+1) {
			return refuseProfile("placeholders outside uncertified Nucleus finite point profile")
		}
	}
	for _, arg := range args {
		if !finiteArgument(arg) {
			return refuseProfile("parameter type outside uncertified Nucleus finite profile")
		}
	}
	return nil
}

// FiniteExecutor is the guarded Executor returned by AdmitNucleusFinite. It
// is safe to pass anywhere an Executor is accepted; refused operations return
// a *ProfileError before reaching the wrapped executor.
type FiniteExecutor struct {
	inner    Executor
	identity EndpointIdentity
	patterns []*regexp.Regexp
}

// AdmitNucleusFinite verifies the exact reported Nucleus candidate identity
// through db and returns a guarded executor for the registered tables. Tables
// are checked before any statement is dispatched.
func AdmitNucleusFinite(ctx context.Context, db Executor, tables ...FiniteTable) (*FiniteExecutor, error) {
	if err := ready(ctx, db); err != nil {
		return nil, wrap("admit endpoint", err)
	}
	var patterns []*regexp.Regexp
	for _, table := range tables {
		if len(table.patterns) == 0 {
			return nil, refuseProfile("uninitialized finite table")
		}
		patterns = append(patterns, table.patterns...)
	}
	identity, err := probeEndpoint(ctx, db, NucleusCandidateProfile)
	if err != nil {
		return nil, err
	}
	return &FiniteExecutor{inner: db, identity: identity, patterns: patterns}, nil
}

// Identity returns the immutable admitted identity. PackageEnabled is false.
func (e *FiniteExecutor) Identity() EndpointIdentity { return e.identity }

func (e *FiniteExecutor) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := admitFinite(e.patterns, sql, args); err != nil {
		return nil, err
	}
	return e.inner.Query(ctx, sql, args...)
}

func (e *FiniteExecutor) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := admitFinite(e.patterns, sql, args); err != nil {
		return pgconn.CommandTag{}, err
	}
	return e.inner.Exec(ctx, sql, args...)
}

func (e *FiniteExecutor) rebind(inner Executor) *FiniteExecutor {
	return &FiniteExecutor{inner: inner, identity: e.identity, patterns: e.patterns}
}

// FiniteScope is the guarded view of an owned transaction Scope.
type FiniteScope struct {
	*FiniteExecutor
	scope *Scope
}

// WithTransaction runs WithTransaction on the admitted pool, which must be the
// executor passed to AdmitNucleusFinite. Only the default or READ COMMITTED
// isolation, read-write access and no DEFERRABLE are admitted; anything else is
// refused before a connection is pinned.
func (e *FiniteExecutor) WithTransaction(ctx context.Context, pool *pgxpool.Pool, options TransactionOptions, callback func(*FiniteScope) error) error {
	if admitted, ok := e.inner.(*pgxpool.Pool); !ok || pool == nil || admitted != pool {
		return refuseProfile("owned transactions require the admitted pool")
	}
	if (options.Isolation != "" && options.Isolation != pgx.ReadCommitted) ||
		(options.Access != "" && options.Access != pgx.ReadWrite) || options.Deferrable != "" {
		return refuseProfile("transaction options outside uncertified Nucleus finite profile")
	}
	if callback == nil {
		return refuseProfile("transaction callback required")
	}
	return WithTransaction(ctx, pool, options, func(scope *Scope) error {
		return callback(&FiniteScope{e.rebind(scope), scope})
	})
}

// Savepoint runs a child savepoint scope with the same guard.
func (s *FiniteScope) Savepoint(ctx context.Context, callback func(*FiniteScope) error) error {
	if callback == nil {
		return refuseProfile("savepoint callback required")
	}
	return s.scope.Savepoint(ctx, func(child *Scope) error {
		return callback(&FiniteScope{s.FiniteExecutor.rebind(child), child})
	})
}
