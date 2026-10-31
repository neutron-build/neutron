package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaFiles embed.FS

const exactAmount = "1234567890123456789012.123456789012345679"
const largeVersion int64 = 9007199254740993

type Fixture struct {
	Database string
	URL      string              `json:"-"`
	Admin    *pgx.Conn           `json:"-"`
	Oracle   *pgxpool.Pool       `json:"-"`
	Docs     map[string]Document `json:"-"`
	Projects map[string]Project  `json:"-"`
	Digest   string
	DDLHash  string
	Closed   bool
	Cleaned  bool
}

func key(tenant string, id int32) string { return tenant + ":" + strconv.FormatInt(int64(id), 10) }

func createFixture(ctx context.Context, adminURL string) (*Fixture, error) {
	parsed, err := url.Parse(adminURL)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Host == "" {
		return nil, errors.New("ORM_ADMIN_URL must be an absolute PostgreSQL URL")
	}
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		return nil, errors.New("could not connect to fixture administrator")
	}
	var version int
	if err := admin.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&version); err != nil || version/10000 != 17 {
		admin.Close(ctx)
		return nil, errors.New("comparison requires PostgreSQL 17")
	}
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		admin.Close(ctx)
		return nil, err
	}
	name := "neutron_go_compare_" + hex.EncodeToString(nonce[:])
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		admin.Close(ctx)
		return nil, errors.New("could not create owned fixture database")
	}
	parsed.Path, parsed.RawPath = "/"+name, ""
	fixture := &Fixture{Database: name, URL: parsed.String(), Admin: admin}
	var activeTx pgx.Tx
	fail := func(err error) (*Fixture, error) {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if activeTx != nil {
			activeTx.Rollback(cleanupCtx)
		}
		fixture.Close(cleanupCtx)
		return fixture, err
	}
	cfg, err := poolConfig(fixture.URL, nil)
	if err != nil {
		return fail(err)
	}
	fixture.Oracle, err = pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return fail(errors.New("could not open owned fixture database"))
	}
	ddl, err := schemaFiles.ReadFile("schema.sql")
	if err != nil {
		return fail(err)
	}
	h := sha256.Sum256(ddl)
	fixture.DDLHash = hex.EncodeToString(h[:])
	if _, err := fixture.Oracle.Exec(ctx, string(ddl)); err != nil {
		return fail(errors.New("fixture DDL failed"))
	}
	tx, err := fixture.Oracle.Begin(ctx)
	if err != nil {
		return fail(err)
	}
	activeTx = tx
	defer tx.Rollback(ctx)
	projectRows := make([][]any, 0, 202)
	documentRows := make([][]any, 0, 4000)
	for _, tenant := range []string{"a", "b"} {
		for project := int32(1); project <= 101; project++ {
			projectRows = append(projectRows, []any{tenant, project, tenant + "-project-" + strconv.Itoa(int(project))})
			if project == 101 {
				continue
			}
			for j := int32(1); j <= 20; j++ {
				id := (project-1)*20 + j
				version := largeVersion
				if id == 1 {
					version = -9223372036854775808
				}
				if id == 2 {
					version = 9223372036854775807
				}
				var note *string
				if j%3 == 1 {
					v := ""
					note = &v
				}
				if j%3 == 2 {
					v := tenant + "'\\\n" + strconv.Itoa(int(id))
					note = &v
				}
				var payload []byte
				if j%4 == 1 {
					payload = []byte{}
				}
				if j%4 == 2 {
					payload = []byte{0, 255, byte(id), 34, 92}
				}
				if j%4 == 3 {
					payload = make([]byte, 256)
					for k := range payload {
						payload[k] = byte(k)
					}
				}
				documentRows = append(documentRows, []any{tenant, id, project, version, exactAmount, note, payload})
			}
		}
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"projects"}, []string{"tenant", "id", "title"}, pgx.CopyFromRows(projectRows)); err != nil {
		return fail(errors.New("project fixture copy failed"))
	}
	// COPY requires an actual numeric codec input, not a string with uncertain
	// driver coercion. Parameterized INSERT uses the same exact decimal text.
	for _, args := range documentRows {
		if _, err := tx.Exec(ctx, insertSQL, args...); err != nil {
			return fail(errors.New("document fixture insert failed"))
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fail(err)
	}
	if _, err := fixture.Oracle.Exec(ctx, "ANALYZE projects; ANALYZE documents"); err != nil {
		return fail(err)
	}
	fixture.Docs, err = oracleDocuments(ctx, fixture.Oracle)
	if err != nil {
		return fail(err)
	}
	fixture.Projects = make(map[string]Project, 202)
	rows, err := fixture.Oracle.Query(ctx, "SELECT tenant,id,title FROM projects ORDER BY tenant,id")
	if err != nil {
		return fail(err)
	}
	for rows.Next() {
		var p Project
		if err := rows.Scan(&p.Tenant, &p.ID, &p.Title); err != nil {
			rows.Close()
			return fail(err)
		}
		fixture.Projects[key(p.Tenant, p.ID)] = p
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fail(err)
	}
	if err := validateNativeSeed(fixture); err != nil {
		return fail(err)
	}
	fixture.Digest, err = nativeDigest(ctx, fixture.Oracle)
	if err != nil {
		return fail(err)
	}
	return fixture, nil
}

func (f *Fixture) Close(ctx context.Context) error {
	if f.Closed {
		return nil
	}
	f.Closed = true
	if f.Oracle != nil {
		f.Oracle.Close()
	}
	defer f.Admin.Close(ctx)
	// The name is generated only after successful CREATE. Never drop a
	// caller-supplied database. FORCE also handles an interrupted own pool.
	_, err := f.Admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{f.Database}.Sanitize()+" WITH (FORCE)")
	f.Cleaned = err == nil
	return err
}

func oracleDocuments(ctx context.Context, pool *pgxpool.Pool) (map[string]Document, error) {
	rows, err := pool.Query(ctx, "SELECT tenant,id::text,project_id::text,version::text,amount::text,note,encode(payload,'hex') FROM documents ORDER BY tenant,id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]Document)
	for rows.Next() {
		var d Document
		var id, project, version, amount string
		var bytes *string
		if err := rows.Scan(&d.Tenant, &id, &project, &version, &amount, &d.Note, &bytes); err != nil {
			return nil, err
		}
		d.ID, err = parseInt32(id)
		if err != nil {
			return nil, err
		}
		d.ProjectID, err = parseInt32(project)
		if err != nil {
			return nil, err
		}
		d.Version, err = strconv.ParseInt(version, 10, 64)
		if err != nil {
			return nil, err
		}
		d.Amount = Decimal(amount)
		if bytes != nil {
			d.Payload, err = hex.DecodeString(*bytes)
			if err != nil {
				return nil, err
			}
		}
		result[key(d.Tenant, d.ID)] = d
	}
	return result, rows.Err()
}

func nativeDigest(ctx context.Context, pool *pgxpool.Pool) (string, error) {
	h := sha256.New()
	queries := []string{
		"SELECT row_to_json(x)::text FROM (SELECT tenant,id,title FROM projects ORDER BY tenant,id) x",
		"SELECT row_to_json(x)::text FROM (SELECT tenant,id,project_id,version::text,amount::text,note,encode(payload,'hex') AS payload FROM documents ORDER BY tenant,id) x",
	}
	for index, query := range queries {
		fmt.Fprintln(h, index)
		rows, err := pool.Query(ctx, query)
		if err != nil {
			return "", err
		}
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				rows.Close()
				return "", err
			}
			fmt.Fprintln(h, line)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Validate explicit expected seed properties before using native values as the
// oracle. Agreement among providers must not hide a degraded fixture.
func validateNativeSeed(f *Fixture) error {
	if len(f.Docs) != 4000 || len(f.Projects) != 202 {
		return errors.New("native fixture cardinality differs")
	}
	for _, tenant := range []string{"a", "b"} {
		for _, id := range []int32{1, 101} {
			p, ok := f.Projects[key(tenant, id)]
			if !ok || p.Tenant != tenant || p.ID != id || p.Title != tenant+"-project-"+strconv.Itoa(int(id)) {
				return errors.New("native project seed differs")
			}
		}
		empty := ""
		literal := tenant + "'\\\n2"
		allBytes := make([]byte, 256)
		for i := range allBytes {
			allBytes[i] = byte(i)
		}
		anchors := []Document{
			{Tenant: tenant, ID: 1, ProjectID: 1, Version: -9223372036854775808, Amount: Decimal(exactAmount), Note: &empty, Payload: []byte{}},
			{Tenant: tenant, ID: 2, ProjectID: 1, Version: 9223372036854775807, Amount: Decimal(exactAmount), Note: &literal, Payload: []byte{0, 255, 2, 34, 92}},
			{Tenant: tenant, ID: 3, ProjectID: 1, Version: largeVersion, Amount: Decimal(exactAmount), Note: nil, Payload: allBytes},
			{Tenant: tenant, ID: 4, ProjectID: 1, Version: largeVersion, Amount: Decimal(exactAmount), Note: &empty, Payload: nil},
		}
		for _, want := range anchors {
			got, ok := f.Docs[key(tenant, want.ID)]
			if !ok || same(got, want) != nil {
				return errors.New("native exact-value seed differs")
			}
		}
	}
	for _, d := range f.Docs {
		if (d.Tenant != "a" && d.Tenant != "b") || d.ID < 1 || d.ID > 2000 || d.ProjectID != (d.ID-1)/20+1 || d.Amount != Decimal(exactAmount) {
			return errors.New("native fixture row invariant differs")
		}
	}
	return nil
}

func (f *Fixture) expectedPage(tenant string, after int32) []Document {
	result := make([]Document, 0, 20)
	for id := after + 1; id <= 2000 && len(result) < 20; id++ {
		result = append(result, f.Docs[key(tenant, id)])
	}
	return result
}
func (f *Fixture) expectedRelation(tenant string, id int32) Relation {
	p, ok := f.Projects[key(tenant, id)]
	if !ok {
		return Relation{Documents: []Document{}}
	}
	docs := make([]Document, 0, 20)
	if id <= 100 {
		for j := int32(1); j <= 20; j++ {
			docs = append(docs, f.Docs[key(tenant, (id-1)*20+j)])
		}
	}
	return Relation{Project: &p, Documents: docs}
}
func same(got, want any) error {
	if reflect.DeepEqual(got, want) {
		return nil
	}
	// Error text contains no credentials; capped values are not retained.
	a, _ := json.Marshal(got)
	b, _ := json.Marshal(want)
	return fmt.Errorf("result differs (actual %d bytes, expected %d bytes)", len(a), len(b))
}
