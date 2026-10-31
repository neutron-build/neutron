package converted

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	mycasbin "github.com/go-admin-team/go-admin-core/v2/casbin"
	"github.com/go-admin-team/go-admin-core/v2/sdk"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neutron-build/neutron/go/orm"
	ormhttp "github.com/neutron-build/neutron/go/orm/http"
	"go-admin/app/admin/models"
	"go-admin/app/admin/service/dto"
	"go-admin/common/actions"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlog "gorm.io/gorm/logger"
)

// The unchanged application schema is created by the original application's own
// GORM AutoMigrate of its pinned models, exactly as the original harness does.
// GORM and Casbin's GORM adapter remain here only for that baseline DDL and for
// the application-owned policy engine; every service operation under test goes
// through the Neutron ORM.
type convertedFixture struct {
	pool   *pgxpool.Pool
	schema string
	tenant string
}

func convertedPostgres(t *testing.T) convertedFixture {
	t.Helper()
	raw := os.Getenv("NEUTRON_GO_APP_DATABASE_URL")
	if raw == "" {
		t.Fatal("disposable native application URL required")
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		t.Fatal("application native profile requires PostgreSQL URI")
	}
	admin, err := gorm.Open(postgres.Open(raw), &gorm.Config{Logger: gormlog.Default.LogMode(gormlog.Silent)})
	if err != nil {
		t.Fatal("native administrative connection failed")
	}
	adminDB, err := admin.DB()
	if err != nil {
		t.Fatal("native administrative database handle unavailable")
	}
	ctx, done := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(done)
	name := fmt.Sprintf("go_app_converted_%d", time.Now().UnixNano())
	if err := admin.WithContext(ctx).Exec(`CREATE SCHEMA "` + name + `"`).Error; err != nil {
		t.Fatal("native corpus schema creation failed")
	}
	t.Cleanup(func() {
		clean, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := adminDB.ExecContext(clean, `DROP SCHEMA "`+name+`" CASCADE`); err != nil {
			t.Error("native corpus cleanup failed")
		}
		adminDB.Close()
	})
	query := parsed.Query()
	query.Set("search_path", name)
	parsed.RawQuery = query.Encode()
	db, err := gorm.Open(postgres.Open(parsed.String()), &gorm.Config{Logger: gormlog.Default.LogMode(gormlog.Silent)})
	if err != nil {
		t.Fatal("baseline PostgreSQL driver profile failed")
	}
	native, err := db.DB()
	if err != nil {
		t.Fatal("baseline native database handle unavailable")
	}
	t.Cleanup(func() { native.Close() })
	if err := db.AutoMigrate(&models.SysDept{}, &models.SysUser{}, &models.SysRole{}); err != nil {
		t.Fatal("original embedded schema migration failed")
	}
	tenant := name
	interval := mycasbin.ReloadInterval
	mycasbin.ReloadInterval = 0
	t.Cleanup(func() { mycasbin.ReloadInterval = interval })
	enforcer := mycasbin.Setup(db, tenant)
	previous := sdk.Runtime.GetCasbinByTenant(tenant)
	sdk.Runtime.SetCasbinByTenant(tenant, enforcer)
	t.Cleanup(func() { sdk.Runtime.SetCasbinByTenant(tenant, previous) })
	config, err := pgxpool.ParseConfig(parsed.String())
	if err != nil {
		t.Fatal("converted pool configuration failed")
	}
	config.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal("converted pool connection failed")
	}
	t.Cleanup(pool.Close)
	return convertedFixture{pool: pool, schema: name, tenant: tenant}
}

func (f convertedFixture) services(t *testing.T) (enabled, disabled *Service) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	enabled, err := NewService(ctx, f.pool, f.schema, true)
	if err != nil {
		t.Fatalf("converted catalog qualification failed: %v", err)
	}
	disabled, err = NewService(ctx, f.pool, f.schema, false)
	if err != nil {
		t.Fatalf("converted catalog qualification failed: %v", err)
	}
	return enabled, disabled
}

func seedConvertedCorpus(t *testing.T, pool *pgxpool.Pool, scenario corpusScenario) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, statement := range corpusSeedStatements(t, scenario) {
		if _, err := pool.Exec(ctx, statement.SQL, statement.Args...); err != nil {
			t.Fatal("converted corpus seed failed")
		}
	}
}

func convertedPermission(p corpusPermission) *actions.DataPermission {
	return &actions.DataPermission{DataScope: p.Scope, UserId: p.UserID, DeptId: p.DeptID, RoleId: p.RoleID}
}

func convertedPageRequest(search corpusSearch) dto.SysUserGetPageReq {
	req := dto.SysUserGetPageReq{}
	req.UserId = search.UserID
	req.Username = search.Username
	req.NickName = search.NickName
	req.Phone = search.Phone
	req.RoleId = search.RoleID
	req.Sex = search.Sex
	req.Email = search.Email
	req.PostId = search.PostID
	req.Status = search.Status
	req.DeptId = search.DeptID
	req.UserIdOrder = search.UserIDOrder
	req.UsernameOrder = search.UsernameOrder
	req.StatusOrder = search.StatusOrder
	req.CreatedAtOrder = search.CreatedAtOrder
	req.PageIndex = search.PageIndex
	req.PageSize = search.PageSize
	return req
}

func TestNeutronCorpusConvertedPostgresScopeMatrix(t *testing.T) {
	fixture := convertedPostgres(t)
	scenario := loadCorpusScenario(t)
	seedConvertedCorpus(t, fixture.pool, scenario)
	enabled, disabled := fixture.services(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	observations := make([]corpusObservation, 0, len(scenario.Queries))
	for _, query := range scenario.Queries {
		application := enabled
		if !query.EnableDataPermission {
			application = disabled
		}
		req := convertedPageRequest(query.Search)
		page, count, err := application.GetPage(ctx, fixture.pool, &req, convertedPermission(query.Permission))
		if err != nil {
			t.Fatalf("converted page %s failed: %v", query.Name, err)
		}
		observation := corpusObservation{Name: query.Name, Count: count, IDs: make([]int, 0, len(page)), DeptIDs: make([]int, 0, len(page))}
		for _, user := range page {
			department := 0
			if user.Dept != nil {
				department = user.Dept.DeptId
			}
			observation.IDs = append(observation.IDs, user.UserId)
			observation.DeptIDs = append(observation.DeptIDs, department)
		}
		observations = append(observations, observation)
	}
	transcript := newCorpusTranscript("scope-matrix")
	transcript.record("observations", observations)
	transcript.write(t)
}

func convertedCredentials(ctx context.Context, pool *pgxpool.Pool, userID int) string {
	var password, salt *string
	if err := pool.QueryRow(ctx, "SELECT password, salt FROM sys_user WHERE user_id = $1", userID).Scan(&password, &salt); err != nil {
		return "absent"
	}
	return fmt.Sprintf("present:%v:%v", value(password), value(salt)) + fmt.Sprint(password == nil, salt == nil)
}

func convertedSnapshot(ctx context.Context, pool *pgxpool.Pool, userID int) map[string]any {
	values := make([]*string, len(corpusUserSnapshotColumns))
	destinations := make([]any, len(values))
	for i := range values {
		destinations[i] = &values[i]
	}
	if err := pool.QueryRow(ctx, corpusUserSnapshotSQL, userID).Scan(destinations...); err != nil {
		return nil
	}
	return corpusSnapshotRecord(values)
}

func convertedNotFound(err error) bool { return errors.Is(err, orm.ErrNotFound) }

// inTransaction runs one service call in a caller-owned native transaction, the
// same shape the HTTP adapter gives a request, and returns the call's own error.
func inTransaction(ctx context.Context, t *testing.T, pool *pgxpool.Pool, work func(orm.Executor) error) error {
	t.Helper()
	var failure error
	err := orm.WithTransaction(ctx, pool, orm.TransactionOptions{}, func(scope *orm.Scope) error {
		failure = work(scope)
		return failure
	})
	if err != nil && failure == nil {
		t.Fatal("converted transaction did not complete")
	}
	return failure
}

func runConvertedOperation(ctx context.Context, t *testing.T, pool *pgxpool.Pool, application *Service, operation corpusOperation) corpusOperationResult {
	t.Helper()
	permission := convertedPermission(operation.Permission)
	result := corpusOperationResult{Name: operation.Name, UserID: operation.UserID, Extra: map[string]any{}}
	credentialsBefore := convertedCredentials(ctx, pool, operation.UserID)
	var err error
	switch operation.Kind {
	case "get", "getself":
		var model models.SysUser
		target := dto.SysUserById{}
		target.Id = operation.UserID
		if operation.Kind == "get" {
			model, err = application.Get(ctx, pool, &target, permission)
		} else {
			model, err = application.GetSelf(ctx, pool, &target)
		}
		if err == nil {
			result.Extra["model"] = map[string]any{"user_id": model.UserId, "username": model.Username, "dept_ids": model.DeptIds, "role_ids": model.RoleIds, "post_ids": model.PostIds, "has_dept": model.Dept != nil}
		}
	case "insert":
		r := operation.Request
		req := dto.SysUserInsertReq{UserId: r.UserID, Username: r.Username, Password: r.Password, NickName: r.NickName, Phone: r.Phone, RoleId: r.RoleID, Avatar: r.Avatar, Sex: r.Sex, Email: r.Email, DeptId: r.DeptID, PostId: r.PostID, Remark: r.Remark, Status: r.Status}
		req.CreateBy = r.CreateBy
		err = inTransaction(ctx, t, pool, func(db orm.Executor) error { return application.Insert(ctx, db, &req) })
		var id int
		if scanErr := pool.QueryRow(ctx, "SELECT user_id FROM sys_user WHERE username = $1 ORDER BY user_id LIMIT 1", r.Username).Scan(&id); scanErr == nil {
			result.UserID = id
		}
		var stored *string
		if scanErr := pool.QueryRow(ctx, "SELECT password FROM sys_user WHERE user_id = $1", result.UserID).Scan(&stored); scanErr == nil {
			result.Extra["password_verifies"] = stored != nil && bcrypt.CompareHashAndPassword([]byte(*stored), []byte(r.Password)) == nil
			result.Extra["password_is_plaintext"] = stored != nil && *stored == r.Password
		}
	case "update":
		r := operation.Request
		req := dto.SysUserUpdateReq{UserId: r.UserID, Username: r.Username, NickName: r.NickName, Phone: r.Phone, RoleId: r.RoleID, Avatar: r.Avatar, Sex: r.Sex, Email: r.Email, DeptId: r.DeptID, PostId: r.PostID, Remark: r.Remark, Status: r.Status}
		err = inTransaction(ctx, t, pool, func(db orm.Executor) error {
			return application.Update(ctx, db, &req, permission, operation.CallerID)
		})
	case "remove":
		target := dto.SysUserById{}
		target.Id = operation.UserID
		err = inTransaction(ctx, t, pool, func(db orm.Executor) error { return application.Remove(ctx, db, &target, permission) })
	case "native":
		if _, execErr := pool.Exec(ctx, operation.SQL); execErr != nil {
			t.Fatal("converted native seed operation failed")
		}
	default:
		t.Fatal("unknown shared operation kind")
	}
	result.Error = corpusErrorClass(err, convertedNotFound)
	result.Snapshot = convertedSnapshot(ctx, pool, result.UserID)
	if operation.Kind == "update" || operation.Kind == "remove" {
		result.Extra["credentials_unchanged"] = credentialsBefore == convertedCredentials(ctx, pool, operation.UserID)
	}
	return result
}

func TestNeutronCorpusConvertedPostgresOperations(t *testing.T) {
	fixture := convertedPostgres(t)
	scenario := loadCorpusScenario(t)
	seedConvertedCorpus(t, fixture.pool, scenario)
	enabled, _ := fixture.services(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	results := make([]corpusOperationResult, 0, len(scenario.Operations))
	for _, operation := range scenario.Operations {
		results = append(results, runConvertedOperation(ctx, t, fixture.pool, enabled, operation))
	}
	transcript := newCorpusTranscript("operations")
	transcript.record("operations", results)
	transcript.write(t)
}

func callConvertedUpdate(t *testing.T, handler http.Handler, callerID int, body map[string]any) int {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal("marshal request body failed")
	}
	request := httptest.NewRequest(http.MethodPut, "/api/v1/sys-user", bytes.NewReader(raw))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Corpus-Caller", strconv.Itoa(callerID))
	request.Header.Set("X-Corpus-Role", "ordinary-role")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder.Code
}

// The anti-privilege-escalation scenario: a request-owned Scope update through
// the HTTP adapter, with actual Casbin enforcement from the application's own
// enforcer (empty policy, so an ordinary role holds no grant), the privileged
// self-edit fields, credential preservation, soft-delete markers and rollback.
func TestNeutronCorpusConvertedPostgresUserServiceAndAuthorization(t *testing.T) {
	transcript := newCorpusTranscript("authorization")
	fixture := convertedPostgres(t)
	enabled, disabled := fixture.services(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := fixture.pool.Exec(ctx, "INSERT INTO sys_dept (dept_id, dept_name, dept_path) VALUES (1, 'Original Department', '/0/1/')"); err != nil {
		t.Fatal("converted department create failed")
	}
	user := models.SysUser{UserId: 101, Username: "original-self", Password: "correct-horse-battery-staple", Salt: "preserve-salt", NickName: "Before", RoleId: 2, DeptId: 1, PostId: 3, Status: "1"}
	user.CreateBy = 101
	if err := disabled.InsertUser(ctx, fixture.pool, &user); err != nil {
		t.Fatal("converted password hook create failed")
	}
	var storedHash, storedSalt string
	if err := fixture.pool.QueryRow(ctx, "SELECT password, salt FROM sys_user WHERE user_id = 101").Scan(&storedHash, &storedSalt); err != nil {
		t.Fatal("native credentials oracle failed")
	}
	if bcrypt.CompareHashAndPassword([]byte(storedHash), []byte("correct-horse-battery-staple")) != nil {
		t.Fatal("converted stored password does not verify")
	}
	transcript.record("created_password_verifies", true)
	transcript.record("created_salt", storedSalt)
	victim := models.SysUser{UserId: 102, Username: "original-victim", NickName: "Victim", RoleId: 2, DeptId: 1, Status: "1"}
	victim.CreateBy = 102
	if err := disabled.InsertUser(ctx, fixture.pool, &victim); err != nil {
		t.Fatal("converted victim create failed")
	}
	requests, err := ormhttp.NewTransactions(fixture.pool, ormhttp.Options{MaxResponseBytes: 4096})
	if err != nil {
		t.Fatal("request transaction configuration failed")
	}
	t.Cleanup(func() {
		clean, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if err := requests.Shutdown(clean); err != nil {
			t.Error("request transaction shutdown failed")
		}
	})
	identity := func(r *http.Request) (Caller, *actions.DataPermission, bool) {
		id, err := strconv.Atoi(r.Header.Get("X-Corpus-Caller"))
		if err != nil {
			return Caller{}, nil, false
		}
		return Caller{UserID: id, RoleKey: r.Header.Get("X-Corpus-Role")}, &actions.DataPermission{}, true
	}
	authorize := func(_ context.Context, caller Caller, path, method string) (bool, error) {
		return sdk.Runtime.GetCasbinByTenant(fixture.tenant).Enforce(caller.RoleKey, path, method)
	}
	handler, err := requests.Handler(disabled.UpdateHandler(identity, authorize))
	if err != nil {
		t.Fatal("request handler configuration failed")
	}
	if code := callConvertedUpdate(t, handler, 101, map[string]any{"userId": 102, "username": victim.Username, "nickName": "pwned", "phone": "13800000000", "email": "victim@example.com", "roleId": 1, "deptId": 1, "status": "0"}); code != http.StatusForbidden {
		t.Fatalf("converted other-user update was not forbidden: %d", code)
	}
	var role, dept int
	var nick, status string
	if err := fixture.pool.QueryRow(ctx, "SELECT role_id, dept_id, nick_name, status FROM sys_user WHERE user_id = 102").Scan(&role, &dept, &nick, &status); err != nil || role != 2 || dept != 1 || nick != "Victim" || status != "1" {
		t.Fatal("native unauthorized other-user mutation")
	}
	transcript.record("other_user_after", map[string]any{"role_id": role, "dept_id": dept, "nick_name": nick, "status": status})
	if code := callConvertedUpdate(t, handler, 101, map[string]any{"userId": 101, "username": user.Username, "nickName": "After", "phone": "13900000000", "email": "self@example.com", "roleId": 1, "deptId": 999, "status": "0"}); code != http.StatusOK {
		t.Fatalf("converted self update was refused: %d", code)
	}
	var afterHash, afterSalt string
	if err := fixture.pool.QueryRow(ctx, "SELECT role_id, dept_id, nick_name, status, password, salt FROM sys_user WHERE user_id = 101").Scan(&role, &dept, &nick, &status, &afterHash, &afterSalt); err != nil || role != 2 || dept != 1 || nick != "After" || status != "1" || afterHash != storedHash || afterSalt != storedSalt {
		t.Fatal("native converted self-edit/credential oracle")
	}
	transcript.record("self_after", map[string]any{"role_id": role, "dept_id": dept, "nick_name": nick, "status": status, "hash_unchanged": afterHash == storedHash, "salt_unchanged": afterSalt == storedSalt})
	loaded, err := disabled.GetProfile(ctx, fixture.pool, 101)
	if err != nil || loaded.Dept == nil || loaded.Dept.DeptName != "Original Department" || len(loaded.DeptIds) != 1 || loaded.DeptIds[0] != 1 || len(loaded.RoleIds) != 1 || loaded.RoleIds[0] != 2 || len(loaded.PostIds) != 1 || loaded.PostIds[0] != 3 {
		t.Fatal("converted Preload/AfterFind behavior")
	}
	transcript.record("preload", map[string]any{"dept_name": loaded.Dept.DeptName, "dept_ids": loaded.DeptIds, "role_ids": loaded.RoleIds, "post_ids": loaded.PostIds})
	if err := loaded.BeforeUpdate(nil); err != nil || loaded.Password != storedHash {
		t.Fatal("converted stored hash was rehashed")
	}
	transcript.record("before_update_rehashes_stored_hash", false)
	req := dto.SysUserGetPageReq{}
	req.UserIdOrder = "asc"
	req.PageIndex = 1
	req.PageSize = 1
	permission := &actions.DataPermission{DataScope: actions.DataScopeSelf, UserId: 101}
	page, count, err := enabled.GetPage(ctx, fixture.pool, &req, permission)
	if err != nil || count != 1 || len(page) != 1 || page[0].UserId != 101 || page[0].Dept == nil {
		t.Fatal("converted scoped page/count/association")
	}
	transcript.record("scoped_page", map[string]any{"count": count, "ids": []int{page[0].UserId}, "dept_present": page[0].Dept != nil})
	permission.DataScope = "unknown"
	page, count, err = enabled.GetPage(ctx, fixture.pool, &req, permission)
	if err != nil || count != 0 || len(page) != 0 {
		t.Fatal("converted invalid scope failed open")
	}
	transcript.record("invalid_scope_page", map[string]any{"count": count, "rows": len(page)})
	var marker int64
	if err := fixture.pool.QueryRow(ctx, "SELECT deleted_at FROM sys_user WHERE user_id = 101").Scan(&marker); err != nil || marker != 0 {
		t.Fatal("converted zero-live deletion schema")
	}
	transcript.record("live_marker_is_zero", true)
	removal := dto.SysUserById{}
	removal.Id = 102
	if err := enabled.Remove(ctx, fixture.pool, &removal, &actions.DataPermission{DataScope: actions.DataScopeAll}); err != nil {
		t.Fatal("converted soft delete failed")
	}
	if err := fixture.pool.QueryRow(ctx, "SELECT deleted_at FROM sys_user WHERE user_id = 102").Scan(&marker); err != nil || marker <= 0 {
		t.Fatal("native converted millisecond soft-delete oracle")
	}
	transcript.record("deleted_marker_is_positive_milliseconds", true)
	if _, err := disabled.GetProfile(ctx, fixture.pool, 102); !errors.Is(err, orm.ErrNotFound) {
		t.Fatal("converted record-not-found translation")
	}
	transcript.record("deleted_lookup", "record_not_found")
	// A failing actual password hook rolls back earlier writes in one native
	// transaction; no mock callback represents the failure.
	failure := orm.WithTransaction(ctx, fixture.pool, orm.TransactionOptions{}, func(scope *orm.Scope) error {
		if _, err := scope.Exec(ctx, "UPDATE sys_user SET nick_name = 'must rollback' WHERE user_id = 101"); err != nil {
			return err
		}
		bad := models.SysUser{UserId: 103, Username: "bad-password", Password: strings.Repeat("x", 73), DeptId: 1}
		return disabled.InsertUser(ctx, scope, &bad)
	})
	if failure == nil {
		t.Fatal("converted hook failure missing")
	}
	if err := fixture.pool.QueryRow(ctx, "SELECT nick_name FROM sys_user WHERE user_id = 101").Scan(&nick); err != nil || nick != "After" {
		t.Fatal("native converted hook rollback oracle")
	}
	transcript.record("rollback_hook_error", true)
	transcript.record("rollback_nick_name", nick)
	transcript.write(t)
}

// Request-owned Scope refusals commit nothing and an unauthenticated request
// never reaches the service; both are converted-only HTTP boundary checks.
func TestNeutronCorpusConvertedPostgresHTTPBoundary(t *testing.T) {
	fixture := convertedPostgres(t)
	_, disabled := fixture.services(t)
	requests, err := ormhttp.NewTransactions(fixture.pool, ormhttp.Options{MaxResponseBytes: 4096})
	if err != nil {
		t.Fatal("request transaction configuration failed")
	}
	t.Cleanup(func() {
		clean, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if err := requests.Shutdown(clean); err != nil {
			t.Error("request transaction shutdown failed")
		}
	})
	identity := func(r *http.Request) (Caller, *actions.DataPermission, bool) {
		id, err := strconv.Atoi(r.Header.Get("X-Corpus-Caller"))
		if err != nil {
			return Caller{}, nil, false
		}
		return Caller{UserID: id, RoleKey: "ordinary-role"}, &actions.DataPermission{}, true
	}
	denied := func(context.Context, Caller, string, string) (bool, error) { return false, nil }
	handler, err := requests.Handler(disabled.UpdateHandler(identity, denied))
	if err != nil {
		t.Fatal("request handler configuration failed")
	}
	anonymous := httptest.NewRequest(http.MethodPut, "/api/v1/sys-user", strings.NewReader("{}"))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, anonymous)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated request was not refused: %d", recorder.Code)
	}
	if code := callConvertedUpdate(t, handler, 1, map[string]any{"userId": 2}); code != http.StatusBadRequest {
		t.Fatalf("invalid body was not refused before authorization: %d", code)
	}
	if _, err := disabled.GetProfile(context.Background(), fixture.pool, 1); !errors.Is(err, orm.ErrNotFound) {
		t.Fatal("empty schema lookup did not report not found")
	}
}
