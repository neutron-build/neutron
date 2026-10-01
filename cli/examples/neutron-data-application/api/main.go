// PostgreSQL reference service. Schema provisioning is a separate operator step.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neutron-build/neutron/go/neutron"
	"github.com/neutron-build/neutron/go/neutronauth"
	"github.com/neutron-build/neutron/go/nucleus"
)

type tenantConfig struct {
	URL  string `json:"url"`
	Role string `json:"role"`
}
type service struct {
	tenants map[string]*nucleus.Client
	logger  *slog.Logger
	timeout time.Duration
	secrets []string
}
type principalKey struct{}
type principal struct {
	tenant string
	client *nucleus.Client
}

func connectTenant(ctx context.Context, tenant string, conf tenantConfig) (*nucleus.Client, error) {
	if (tenant != "tenant-a" && tenant != "tenant-b") || !strings.HasSuffix(conf.Role, "_api_"+strings.TrimPrefix(tenant, "tenant-")) {
		return nil, errors.New("invalid configured tenant/role")
	}
	cfg, err := pgxpool.ParseConfig(conf.URL)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 2
	cfg.MinConns = 0
	cfg.ConnConfig.ConnectTimeout = 3 * time.Second
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "5000"
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = "2000"
	cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "5000"
	cfg.ConnConfig.RuntimeParams["timezone"] = "UTC"
	cfg.ConnConfig.RuntimeParams["search_path"] = "public,pg_catalog"
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		var current, session, version string
		var privileged, member, owner bool
		var revision, rows, managed int
		err := conn.QueryRow(ctx, `SELECT current_user,session_user,version(),
   r.rolsuper OR r.rolbypassrls OR r.rolcreatedb OR r.rolcreaterole OR r.rolinherit OR NOT r.rolcanlogin,
   EXISTS(SELECT 1 FROM pg_catalog.pg_auth_members WHERE member=r.oid),
   EXISTS(SELECT 1 FROM pg_catalog.pg_database WHERE datname=current_database() AND datdba=r.oid)
    OR EXISTS(SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname='public' AND nspowner=r.oid)
    OR EXISTS(SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relowner=r.oid)
   FROM pg_catalog.pg_roles r WHERE r.rolname=current_user`).Scan(&current, &session, &version, &privileged, &member, &owner)
		if err != nil {
			return err
		}
		if current != conf.Role || session != conf.Role || privileged || member || owner || !strings.HasPrefix(version, "PostgreSQL 17.") {
			return errors.New("runtime identity/profile refused")
		}
		if err = conn.QueryRow(ctx, `SELECT count(*)::int,coalesce(min(revision),0) FROM public.app_schema_revision`).Scan(&rows, &revision); err != nil {
			return err
		}
		if rows != 1 || revision != 1 {
			return errors.New("schema revision refused")
		}
		if err = conn.QueryRow(ctx, `SELECT count(*)::int FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname=ANY(ARRAY['projects','documents','processing_requests','jobs','results']) AND c.relkind='r' AND c.relrowsecurity AND c.relforcerowsecurity`).Scan(&managed); err != nil {
			return err
		}
		if managed != 5 {
			return errors.New("managed RLS profile refused")
		}
		return nil
	}
	c, err := nucleus.Connect(ctx, conf.URL, nucleus.WithPoolConfig(cfg))
	if err != nil {
		return nil, err
	}
	if c.IsNucleus() {
		c.Close()
		return nil, errors.New("PostgreSQL required")
	}
	return c, nil
}
func (s *service) close() {
	for _, c := range s.tenants {
		c.Close()
	}
}
func (s *service) authorize(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, err := neutronauth.ClaimsFromContext(r.Context())
		tenant, _ := claims["tenant_id"].(string)
		sub, _ := claims["sub"].(string)
		if err != nil || claims["iss"] != "neutron-data-reference" || claims["aud"] != "neutron-data-reference-api" || strings.TrimSpace(sub) == "" {
			neutron.WriteError(w, r, neutron.ErrUnauthorized("invalid application principal"))
			return
		}
		c, ok := s.tenants[tenant]
		if !ok {
			neutron.WriteError(w, r, neutron.ErrForbidden("tenant is not configured"))
			return
		}
		for _, v := range r.URL.Query()["tenant_id"] {
			if v != tenant {
				neutron.WriteError(w, r, neutron.ErrForbidden("tenant mismatch"))
				return
			}
		}
		ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(context.WithValue(ctx, principalKey{}, principal{tenant, c})))
	})
}
func (s *service) app(secret string) *neutron.App {
	app := neutron.New(neutron.WithoutDefaultRoutes(), neutron.WithLogger(s.logger), neutron.WithMiddleware(neutron.DefaultStack(neutron.DefaultStackConfig{Logger: s.logger})...))
	app.Router().HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		respond(w, map[string]any{"status": "ok", "nucleus": false, "version": "1"})
	})
	api := app.Router().Group("/api", neutronauth.JWTMiddleware(secret), s.authorize)
	api.HandleFunc("POST /projects", s.wrap(s.createProject))
	api.HandleFunc("GET /projects", s.wrap(s.listProjects))
	api.HandleFunc("POST /documents", s.wrap(s.createDocument))
	api.HandleFunc("GET /documents", s.wrap(s.listDocuments))
	api.HandleFunc("GET /documents/{id}", s.wrap(s.getDocument))
	api.HandleFunc("POST /documents/{id}/note", s.wrap(s.updateNote))
	return app
}

type endpoint func(*http.Request) (any, error)

var urlSecretRE = regexp.MustCompile(`postgres(?:ql)?://[^\s]+`)

func (s *service) safe(err error) string {
	value := err.Error()
	for _, secret := range s.secrets {
		if secret != "" {
			value = strings.ReplaceAll(value, secret, "[REDACTED]")
		}
	}
	return urlSecretRE.ReplaceAllString(value, "[REDACTED_URL]")
}
func (s *service) wrap(fn endpoint) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		out, err := fn(r)
		if err != nil {
			var appErr *neutron.AppError
			var failed *dbFailure
			if errors.As(err, &failed) {
				appErr = failed.public
				s.logger.Error("database operation failed", "cause", s.safe(failed.cause), "path", r.URL.Path)
			} else if !errors.As(err, &appErr) {
				s.logger.Error("database operation failed", "cause", s.safe(err), "path", r.URL.Path)
				appErr = neutron.ErrServiceUnavailable("database dependency unavailable")
			}
			neutron.WriteError(w, r, appErr)
			return
		}
		respond(w, out)
	}
}
func respond(w http.ResponseWriter, out any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}
func who(r *http.Request) principal { return r.Context().Value(principalKey{}).(principal) }
func bad() error                    { return neutron.ErrBadRequest("invalid request values") }
func conflict() error               { return neutron.ErrConflict("request conflicts with existing state") }

type dbFailure struct {
	public *neutron.AppError
	cause  error
}

func (e *dbFailure) Error() string { return e.public.Error() }
func (e *dbFailure) Unwrap() error { return e.cause }
func unknownOutcome(err error, detail string) error {
	var pg *pgconn.PgError
	if errors.As(err, &pg) || errors.Is(err, pgx.ErrTxCommitRollback) {
		return &dbFailure{public: neutron.ErrServiceUnavailable("Transaction did not commit; database dependency unavailable. No automatic retry was attempted."), cause: err}
	}

	return &dbFailure{public: &neutron.AppError{Status: 503, Code: "https://neutron.dev/errors/unknown-outcome", Title: "Unknown transaction outcome", Detail: detail + " No automatic retry was attempted."}, cause: err}
}
func unknownCommit(err error) error {
	return unknownOutcome(err, "Look up the request by its idempotency key before deciding another action.")
}

// Decode once, reject duplicate/unknown keys and trailing JSON. Required nullable
// fields retain presence separately from their null value.
func input(r *http.Request, allowed []string) (map[string]json.RawMessage, error) {
	mediaType, _, mediaErr := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mediaErr != nil || mediaType != "application/json" {
		return nil, bad()
	}
	rawBody, readErr := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
	if readErr != nil || len(rawBody) > 1<<20 || !utf8.Valid(rawBody) {
		return nil, bad()
	}
	d := json.NewDecoder(bytes.NewReader(rawBody))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return nil, bad()
	}
	m := map[string]json.RawMessage{}
	keys := map[string]bool{"tenant_id": true}
	for _, k := range allowed {
		keys[k] = true
	}
	for d.More() {
		t, e := d.Token()
		key, ok := t.(string)
		if e != nil || !ok || !keys[key] {
			return nil, bad()
		}
		if _, exists := m[key]; exists {
			return nil, bad()
		}
		var raw json.RawMessage
		if d.Decode(&raw) != nil {
			return nil, bad()
		}
		m[key] = raw
	}
	if t, e := d.Token(); e != nil || t != json.Delim('}') {
		return nil, bad()
	}
	if _, e := d.Token(); e != io.EOF {
		return nil, bad()
	}
	if raw, ok := m["tenant_id"]; ok {
		var tenant string
		if json.Unmarshal(raw, &tenant) != nil || tenant != who(r).tenant {
			return nil, neutron.ErrForbidden("tenant mismatch")
		}
	}
	return m, nil
}

// encoding/json replaces lone UTF-16 surrogates with U+FFFD. Reject those
// escapes instead of silently changing an immutable document's content.
func validSurrogates(raw []byte) bool {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if i >= len(raw) {
			return false
		}
		if raw[i] != 'u' {
			continue
		}
		if i+4 >= len(raw) {
			return false
		}
		v, err := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		if err != nil {
			return false
		}
		i += 4
		if v >= 0xdc00 && v <= 0xdfff {
			return false
		}
		if v >= 0xd800 && v <= 0xdbff {
			if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
				return false
			}
			low, err := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 6
		}
	}
	return true
}
func text(m map[string]json.RawMessage, key string) (string, error) {
	raw, ok := m[key]
	var out string
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, &out) != nil || !utf8.ValidString(out) || !validSurrogates(raw) || strings.ContainsRune(out, 0) {
		return "", bad()
	}
	return out, nil
}
func note(m map[string]json.RawMessage) (*string, error) {
	raw, ok := m["note"]
	if !ok {
		return nil, bad()
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	v, err := text(m, "note")
	if err != nil {
		return nil, err
	}
	return &v, nil
}

var uuidRE = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func uuid(v string) (string, error) {
	if !uuidRE.MatchString(v) {
		return "", bad()
	}
	return strings.ToLower(v), nil
}
func id(m map[string]json.RawMessage, key string) (string, error) {
	v, e := text(m, key)
	if e != nil {
		return "", e
	}
	return uuid(v)
}

var decimalRE = regexp.MustCompile(`^-?[0-9]{1,22}(\.[0-9]{1,18})?$`)

func decimal(v string) (string, error) {
	if !decimalRE.MatchString(v) {
		return "", bad()
	}
	negative := strings.HasPrefix(v, "-")
	v = strings.TrimPrefix(v, "-")
	parts := strings.SplitN(v, ".", 2)
	integer := strings.TrimLeft(parts[0], "0")
	if integer == "" {
		integer = "0"
	}
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
	}
	fraction += strings.Repeat("0", 18-len(fraction))
	out := integer + "." + fraction
	if negative && out != "0.000000000000000000" {
		out = "-" + out
	}
	return out, nil
}
func after(r *http.Request) (string, error) {
	values := r.URL.Query()["after"]
	if len(values) > 1 {
		return "", bad()
	}
	if len(values) == 0 {
		return "", nil
	}
	return uuid(values[0])
}
func pgcode(err error, code string) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == code
}
func missing(err error) bool { var a *neutron.AppError; return errors.As(err, &a) && a.Status == 404 }
func dependency(err error) error {
	if pgcode(err, "23503") || pgcode(err, "23505") || pgcode(err, "23514") {
		return &dbFailure{public: neutron.ErrConflict("request conflicts with existing state"), cause: err}
	}
	return err
}
func cleanup(tx *nucleus.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

type Project struct {
	Tenant string `json:"tenant_id" db:"tenant_id"`
	ID     string `json:"id" db:"id"`
	Title  string `json:"title" db:"title"`
}
type Document struct {
	Tenant    string  `json:"tenant_id" db:"tenant_id"`
	ID        string  `json:"id" db:"id"`
	ProjectID string  `json:"project_id" db:"project_id"`
	Content   string  `json:"content" db:"content"`
	Amount    string  `json:"amount" db:"amount"`
	Note      *string `json:"note" db:"note"`
	Payload   []byte  `json:"payload" db:"payload"`
	Version   string  `json:"version" db:"version"`
	CreatedAt string  `json:"created_at" db:"created_at"`
}

const docColumns = `tenant_id,id::text,project_id::text,content,amount::text,note,payload,version::text,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS created_at`

func (s *service) createProject(r *http.Request) (any, error) {
	m, e := input(r, []string{"id", "title"})
	if e != nil {
		return nil, e
	}
	key, e := id(m, "id")
	if e != nil {
		return nil, e
	}
	title, e := text(m, "title")
	if e != nil || strings.TrimSpace(title) == "" {
		return nil, bad()
	}
	p := who(r)
	tx, e := p.client.Begin(r.Context())
	if e != nil {
		return nil, e
	}
	defer cleanup(tx)
	out, e := nucleus.QueryOne[Project](r.Context(), tx.SQL(), `INSERT INTO public.projects(tenant_id,id,title) VALUES($1,$2::uuid,$3) RETURNING tenant_id,id::text,title`, p.tenant, key, title)
	if e != nil {
		return nil, dependency(e)
	}
	if e = tx.Commit(r.Context()); e != nil {
		return nil, unknownOutcome(e, "Look up the project by its UUID before deciding another action.")
	}
	return out, nil
}
func (s *service) listProjects(r *http.Request) (any, error) {
	cursor, e := after(r)
	if e != nil {
		return nil, e
	}
	p := who(r)
	return nucleus.Query[Project](r.Context(), p.client.SQL(), `SELECT tenant_id,id::text,title FROM public.projects WHERE tenant_id=$1 AND ($2='' OR id>NULLIF($2,'')::uuid) ORDER BY id LIMIT 50`, p.tenant, cursor)
}
func document(ctx context.Context, sql *nucleus.SQLModel, tenant, key string) (Document, error) {
	return nucleus.QueryOne[Document](ctx, sql, `SELECT `+docColumns+` FROM public.documents WHERE tenant_id=$1 AND id=$2::uuid`, tenant, key)
}

type requestRow struct {
	DocumentID string `db:"document_id"`
	Digest     string `db:"payload_digest"`
}

func request(ctx context.Context, sql *nucleus.SQLModel, tenant, key string) (requestRow, error) {
	return nucleus.QueryOne[requestRow](ctx, sql, `SELECT document_id::text,payload_digest FROM public.processing_requests WHERE tenant_id=$1 AND idempotency_key=$2`, tenant, key)
}

type normalized struct {
	ID        string  `json:"id"`
	ProjectID string  `json:"project_id"`
	Content   string  `json:"content"`
	Amount    string  `json:"amount"`
	Note      *string `json:"note"`
	Payload   string  `json:"payload"`
}

func (s *service) replay(r *http.Request, p principal, key, digest string) (any, error) {
	old, err := request(r.Context(), p.client.SQL(), p.tenant, key)
	if err != nil {
		return nil, dependency(err)
	}
	if old.Digest != digest {
		return nil, conflict()
	}
	return document(r.Context(), p.client.SQL(), p.tenant, old.DocumentID)
}
func (s *service) createDocument(r *http.Request) (any, error) {
	m, e := input(r, []string{"id", "project_id", "content", "amount", "note", "payload", "idempotency_key"})
	if e != nil {
		return nil, e
	}
	key, e := id(m, "id")
	if e != nil {
		return nil, e
	}
	project, e := id(m, "project_id")
	if e != nil {
		return nil, e
	}
	content, e := text(m, "content")
	if e != nil || len(content) > 65536 {
		return nil, bad()
	}
	amount, e := text(m, "amount")
	if e != nil {
		return nil, e
	}
	amount, e = decimal(amount)
	if e != nil {
		return nil, e
	}
	n, e := note(m)
	if e != nil {
		return nil, e
	}
	raw, e := text(m, "payload")
	if e != nil {
		return nil, e
	}
	payload, e := base64.StdEncoding.Strict().DecodeString(raw)
	if e != nil {
		return nil, bad()
	}
	idem, e := text(m, "idempotency_key")
	if e != nil || len([]rune(idem)) < 1 || len([]rune(idem)) > 128 {
		return nil, bad()
	}
	normalizedInput := normalized{key, project, content, amount, n, base64.StdEncoding.EncodeToString(payload)}
	canonical, _ := json.Marshal(normalizedInput)
	sum := sha256.Sum256(canonical)
	digest := hex.EncodeToString(sum[:])
	p := who(r)
	old, e := request(r.Context(), p.client.SQL(), p.tenant, idem)
	if e == nil {
		if old.Digest != digest {
			return nil, conflict()
		}
		return document(r.Context(), p.client.SQL(), p.tenant, old.DocumentID)
	}
	if !missing(e) {
		return nil, e
	}
	tx, e := p.client.Begin(r.Context())
	if e != nil {
		return nil, e
	}
	defer cleanup(tx)
	_, e = tx.SQL().Exec(r.Context(), `INSERT INTO public.documents(tenant_id,id,project_id,content,amount,note,payload) VALUES($1,$2::uuid,$3::uuid,$4,$5::numeric,$6,$7::bytea)`, p.tenant, key, project, content, amount, n, payload)
	if e == nil {
		_, e = tx.SQL().Exec(r.Context(), `INSERT INTO public.processing_requests(tenant_id,idempotency_key,document_id,payload_digest) VALUES($1,$2,$3::uuid,$4)`, p.tenant, idem, key, digest)
	}
	if e == nil {
		_, e = tx.SQL().Exec(r.Context(), `INSERT INTO public.jobs(tenant_id,document_id) VALUES($1,$2::uuid)`, p.tenant, key)
	}
	if e != nil {
		cleanup(tx)
		if pgcode(e, "23505") {
			out, winner := s.replay(r, p, idem, digest)
			if missing(winner) {
				return nil, conflict()
			}
			return out, winner
		}
		return nil, dependency(e)
	}
	out, e := document(r.Context(), tx.SQL(), p.tenant, key)
	if e != nil {
		return nil, e
	}
	if e = tx.Commit(r.Context()); e != nil {
		return nil, unknownCommit(e)
	}
	return out, nil
}
func (s *service) listDocuments(r *http.Request) (any, error) {
	project, e := uuid(r.URL.Query().Get("project_id"))
	if e != nil {
		return nil, e
	}
	cursor, e := after(r)
	if e != nil {
		return nil, e
	}
	p := who(r)
	return nucleus.Query[Document](r.Context(), p.client.SQL(), `SELECT `+docColumns+` FROM public.documents WHERE tenant_id=$1 AND project_id=$2::uuid AND ($3='' OR id>NULLIF($3,'')::uuid) ORDER BY id LIMIT 50`, p.tenant, project, cursor)
}

type Job struct {
	Status      string  `json:"status" db:"status"`
	Attempts    int     `json:"attempts" db:"attempts"`
	FailureCode *string `json:"failure_code" db:"failure_code"`
	LeaseUntil  *string `json:"lease_until" db:"lease_until"`
}
type Result struct {
	ContentDigest string `json:"content_digest" db:"content_digest"`
	WordCount     int    `json:"word_count" db:"word_count"`
	ProcessedAt   string `json:"processed_at" db:"processed_at"`
}
type Detail struct {
	Document
	Job    *Job    `json:"job"`
	Result *Result `json:"result"`
}

func (s *service) getDocument(r *http.Request) (any, error) {
	key, e := uuid(r.PathValue("id"))
	if e != nil {
		return nil, e
	}
	p := who(r)
	out, e := document(r.Context(), p.client.SQL(), p.tenant, key)
	if e != nil {
		return nil, e
	}
	detail := Detail{Document: out}
	job, e := nucleus.QueryOne[Job](r.Context(), p.client.SQL(), `SELECT status,attempts,failure_code,to_char(lease_until AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS lease_until FROM public.jobs WHERE tenant_id=$1 AND document_id=$2::uuid`, p.tenant, key)
	if e == nil {
		detail.Job = &job
	} else if !missing(e) {
		return nil, e
	}
	result, e := nucleus.QueryOne[Result](r.Context(), p.client.SQL(), `SELECT content_digest,word_count,to_char(processed_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS processed_at FROM public.results WHERE tenant_id=$1 AND document_id=$2::uuid`, p.tenant, key)
	if e == nil {
		detail.Result = &result
	} else if !missing(e) {
		return nil, e
	}
	return detail, nil
}
func (s *service) updateNote(r *http.Request) (any, error) {
	m, e := input(r, []string{"expected_version", "note"})
	if e != nil {
		return nil, e
	}
	n, e := note(m)
	if e != nil {
		return nil, e
	}
	raw, e := text(m, "expected_version")
	if e != nil {
		return nil, e
	}
	v, e := strconv.ParseInt(raw, 10, 64)
	if e != nil || v < 1 || strconv.FormatInt(v, 10) != raw {
		return nil, bad()
	}
	key, e := uuid(r.PathValue("id"))
	if e != nil {
		return nil, e
	}
	p := who(r)
	tx, e := p.client.Begin(r.Context())
	if e != nil {
		return nil, e
	}
	defer cleanup(tx)
	out, e := nucleus.QueryOne[Document](r.Context(), tx.SQL(), `UPDATE public.documents SET note=$3,version=version+1 WHERE tenant_id=$1 AND id=$2::uuid AND version=$4 AND version<9223372036854775807 RETURNING `+docColumns, p.tenant, key, n, v)
	if missing(e) {
		return nil, conflict()
	}
	if e != nil {
		return nil, dependency(e)
	}
	if e = tx.Commit(r.Context()); e != nil {
		return nil, unknownOutcome(e, "Read the document and its current version before deciding another action.")
	}
	return out, nil
}
func main() {
	var config map[string]tenantConfig
	if json.Unmarshal([]byte(os.Getenv("DATA_API_TENANTS")), &config) != nil || len(config) == 0 {
		fmt.Fprintln(os.Stderr, "private tenant configuration required")
		os.Exit(1)
	}
	secret := os.Getenv("DATA_API_JWT_SECRET")
	if len(secret) < 32 {
		fmt.Fprintln(os.Stderr, "random JWT secret required")
		os.Exit(1)
	}
	path := os.Getenv("DATA_API_ERROR_LOG")
	if path == "" {
		fmt.Fprintln(os.Stderr, "private error log required")
		os.Exit(1)
	}
	file, e := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if e != nil {
		os.Exit(1)
	}
	defer file.Close()
	if info, e := file.Stat(); e != nil || info.Mode().Perm()&0077 != 0 {
		os.Exit(1)
	}
	s := &service{tenants: map[string]*nucleus.Client{}, timeout: 5 * time.Second}
	defer s.close()
	if raw := os.Getenv("DATA_API_REQUEST_TIMEOUT_MS"); raw != "" {
		ms, e := strconv.Atoi(raw)
		if e != nil || ms < 100 || ms > 30000 {
			os.Exit(1)
		}
		s.timeout = time.Duration(ms) * time.Millisecond
	}
	s.secrets = append(s.secrets, secret)
	for _, conf := range config {
		s.secrets = append(s.secrets, conf.URL)
		if u, e := url.Parse(conf.URL); e == nil && u.User != nil {
			if pass, ok := u.User.Password(); ok {
				s.secrets = append(s.secrets, pass)
			}
		}
	}
	logger := slog.New(slog.NewJSONHandler(file, &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Value.Kind() == slog.KindString {
			a.Value = slog.StringValue(s.safe(errors.New(a.Value.String())))
		} else if err, ok := a.Value.Any().(error); ok {
			a.Value = slog.StringValue(s.safe(err))
		}
		return a
	}}))
	s.logger = logger
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for tenant, conf := range config {
		c, e := connectTenant(ctx, tenant, conf)
		if e != nil {
			logger.Error("startup refused", "cause", s.safe(e))
			fmt.Fprintln(os.Stderr, "runtime database profile refused")
			os.Exit(1)
		}
		s.tenants[tenant] = c
	}
	if e = s.app(secret).Run(""); e != nil {
		logger.Error("server stopped", "cause", s.safe(e))
		os.Exit(1)
	}
}
