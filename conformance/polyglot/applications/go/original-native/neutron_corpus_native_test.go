// Added corpus harness: upstream selected files and tests remain unchanged.
package apis

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	mycasbin "github.com/go-admin-team/go-admin-core/v2/casbin"
	"github.com/go-admin-team/go-admin-core/v2/logger"
	"github.com/go-admin-team/go-admin-core/v2/sdk"
	"github.com/go-admin-team/go-admin-core/v2/sdk/config"
	"go-admin/app/admin/models"
	"go-admin/app/admin/service"
	"go-admin/app/admin/service/dto"
	"go-admin/common/actions"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlog "gorm.io/gorm/logger"
)

func corpusPostgres(t *testing.T) (*gorm.DB, *sql.DB, string) {
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
	name := fmt.Sprintf("go_app_original_%d", time.Now().UnixNano())
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
		t.Fatal("original PostgreSQL driver profile failed")
	}
	native, err := db.DB()
	if err != nil {
		t.Fatal("original native database handle unavailable")
	}
	native.SetMaxOpenConns(1)
	t.Cleanup(func() { native.Close() })
	// SysRole carries the actual many2many:sys_role_dept mapping used by the
	// custom data scope; migrating it also creates its recursive dependencies.
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
	return db, native, tenant
}

func TestNeutronCorpusOriginalPostgresUserServiceAndAuthorization(t *testing.T) {
	transcript := newCorpusTranscript("authorization")
	db, native, tenant := corpusPostgres(t)
	previousDP := config.ApplicationConfig.EnableDP
	t.Cleanup(func() { config.ApplicationConfig.EnableDP = previousDP })
	config.ApplicationConfig.EnableDP = false
	department := models.SysDept{DeptId: 1, DeptName: "Original Department", DeptPath: "/0/1/"}
	if err := db.Create(&department).Error; err != nil {
		t.Fatal("original department create failed")
	}
	user := models.SysUser{UserId: 101, Username: "original-self", Password: "correct-horse-battery-staple", Salt: "preserve-salt", NickName: "Before", RoleId: 2, DeptId: 1, PostId: 3, Status: "1"}
	user.CreateBy = 101
	if err := db.Create(&user).Error; err != nil {
		t.Fatal("original password hook create failed")
	}
	var storedHash, storedSalt string
	if err := native.QueryRow("SELECT password,salt FROM sys_user WHERE user_id=101").Scan(&storedHash, &storedSalt); err != nil {
		t.Fatal("native credentials oracle failed")
	}
	if bcrypt.CompareHashAndPassword([]byte(storedHash), []byte("correct-horse-battery-staple")) != nil {
		t.Fatal("original stored password does not verify")
	}
	transcript.record("created_password_verifies", true)
	transcript.record("created_salt", storedSalt)
	victim := models.SysUser{UserId: 102, Username: "original-victim", NickName: "Victim", RoleId: 2, DeptId: 1, Status: "1"}
	victim.CreateBy = 102
	if err := db.Create(&victim).Error; err != nil {
		t.Fatal("original victim create failed")
	}
	callUpdate(t, db, tenant, 101, map[string]interface{}{"userId": 102, "username": victim.Username, "nickName": "pwned", "phone": "13800000000", "email": "victim@example.com", "roleId": 1, "deptId": 1, "status": "0"})
	var role, dept int
	var nick, status string
	if err := native.QueryRow("SELECT role_id,dept_id,nick_name,status FROM sys_user WHERE user_id=102").Scan(&role, &dept, &nick, &status); err != nil || role != 2 || dept != 1 || nick != "Victim" || status != "1" {
		t.Fatal("native unauthorized other-user mutation")
	}
	transcript.record("other_user_after", map[string]any{"role_id": role, "dept_id": dept, "nick_name": nick, "status": status})
	callUpdate(t, db, tenant, 101, map[string]interface{}{"userId": 101, "username": user.Username, "nickName": "After", "phone": "13900000000", "email": "self@example.com", "roleId": 1, "deptId": 999, "status": "0"})
	var afterHash, afterSalt string
	if err := native.QueryRow("SELECT role_id,dept_id,nick_name,status,password,salt FROM sys_user WHERE user_id=101").Scan(&role, &dept, &nick, &status, &afterHash, &afterSalt); err != nil || role != 2 || dept != 1 || nick != "After" || status != "1" || afterHash != storedHash || afterSalt != storedSalt {
		t.Fatal("native original self-edit/credential oracle")
	}
	transcript.record("self_after", map[string]any{"role_id": role, "dept_id": dept, "nick_name": nick, "status": status, "hash_unchanged": afterHash == storedHash, "salt_unchanged": afterSalt == storedSalt})
	var loaded models.SysUser
	if err := db.Preload("Dept").First(&loaded, 101).Error; err != nil || loaded.Dept == nil || loaded.Dept.DeptName != department.DeptName || len(loaded.DeptIds) != 1 || loaded.DeptIds[0] != 1 || len(loaded.RoleIds) != 1 || loaded.RoleIds[0] != 2 || len(loaded.PostIds) != 1 || loaded.PostIds[0] != 3 {
		t.Fatal("original Preload/AfterFind native behavior")
	}
	transcript.record("preload", map[string]any{"dept_name": loaded.Dept.DeptName, "dept_ids": loaded.DeptIds, "role_ids": loaded.RoleIds, "post_ids": loaded.PostIds})
	if err := loaded.BeforeUpdate(db); err != nil || loaded.Password != storedHash {
		t.Fatal("original stored hash was rehashed")
	}
	transcript.record("before_update_rehashes_stored_hash", false)
	config.ApplicationConfig.EnableDP = true
	application := service.SysUser{}
	application.Orm = db
	application.Log = logger.NewHelper(logger.DefaultLogger)
	req := dto.SysUserGetPageReq{}
	req.UserIdOrder = "asc"
	req.PageIndex = 1
	req.PageSize = 1
	permission := &actions.DataPermission{DataScope: actions.DataScopeSelf, UserId: 101}
	var page []models.SysUser
	var count int64
	if err := application.GetPage(&req, permission, &page, &count); err != nil || count != 1 || len(page) != 1 || page[0].UserId != 101 || page[0].Dept == nil {
		t.Fatal("original scoped page/count/association")
	}
	transcript.record("scoped_page", map[string]any{"count": count, "ids": []int{page[0].UserId}, "dept_present": page[0].Dept != nil})
	permission.DataScope = "unknown"
	if err := application.GetPage(&req, permission, &page, &count); err != nil || count != 0 || len(page) != 0 {
		t.Fatal("original invalid scope failed open")
	}
	transcript.record("invalid_scope_page", map[string]any{"count": count, "rows": len(page)})
	// Preserve actual uint millisecond zero-live deletion, not nullable datetime.
	var marker int64
	if err := native.QueryRow("SELECT deleted_at FROM sys_user WHERE user_id=101").Scan(&marker); err != nil || marker != 0 {
		t.Fatal("original zero-live deletion schema")
	}
	transcript.record("live_marker_is_zero", true)
	if err := db.Delete(&victim).Error; err != nil {
		t.Fatal("original soft delete failed")
	}
	if err := native.QueryRow("SELECT deleted_at FROM sys_user WHERE user_id=102").Scan(&marker); err != nil || marker <= 0 {
		t.Fatal("native original millisecond soft-delete oracle")
	}
	transcript.record("deleted_marker_is_positive_milliseconds", true)
	if err := db.First(&models.SysUser{}, 102).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatal("original record-not-found translation")
	}
	transcript.record("deleted_lookup", "record_not_found")
	// A failing actual application password hook rolls back earlier writes in
	// an explicit native transaction; it is not represented by a mock callback.
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("UPDATE sys_user SET nick_name='must rollback' WHERE user_id=101").Error; err != nil {
			return err
		}
		bad := models.SysUser{UserId: 103, Username: "bad-password", Password: strings.Repeat("x", 73), DeptId: 1}
		return tx.Create(&bad).Error
	})
	if err == nil {
		t.Fatal("original hook failure missing")
	}
	if err := native.QueryRow("SELECT nick_name FROM sys_user WHERE user_id=101").Scan(&nick); err != nil || nick != "After" {
		t.Fatal("native original hook rollback oracle")
	}
	transcript.record("rollback_hook_error", true)
	transcript.record("rollback_nick_name", nick)
	transcript.write(t)
}

func originalService(db *gorm.DB) *service.SysUser {
	application := &service.SysUser{}
	application.Orm = db
	application.Log = logger.NewHelper(logger.DefaultLogger)
	return application
}

func seedOriginalCorpus(t *testing.T, native *sql.DB, scenario corpusScenario) {
	t.Helper()
	for _, statement := range corpusSeedStatements(t, scenario) {
		if _, err := native.Exec(statement.SQL, statement.Args...); err != nil {
			t.Fatal("original corpus seed failed")
		}
	}
}

func originalPermission(p corpusPermission) *actions.DataPermission {
	return &actions.DataPermission{DataScope: p.Scope, UserId: p.UserID, DeptId: p.DeptID, RoleId: p.RoleID}
}

func originalPageRequest(search corpusSearch) dto.SysUserGetPageReq {
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

// The scope matrix runs every upstream data scope, invalid scopes, department
// zero, search and ordering variants against one seeded original schema that
// includes sys_role_dept, deleted creators/departments and NULL columns.
func TestNeutronCorpusOriginalPostgresScopeMatrix(t *testing.T) {
	db, native, _ := corpusPostgres(t)
	scenario := loadCorpusScenario(t)
	seedOriginalCorpus(t, native, scenario)
	previousDP := config.ApplicationConfig.EnableDP
	t.Cleanup(func() { config.ApplicationConfig.EnableDP = previousDP })
	application := originalService(db)
	observations := make([]corpusObservation, 0, len(scenario.Queries))
	for _, query := range scenario.Queries {
		config.ApplicationConfig.EnableDP = query.EnableDataPermission
		req := originalPageRequest(query.Search)
		var page []models.SysUser
		var count int64
		if err := application.GetPage(&req, originalPermission(query.Permission), &page, &count); err != nil {
			t.Fatalf("original page %s failed", query.Name)
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

func originalCredentials(native *sql.DB, userID int) (string, string) {
	var password, salt sql.NullString
	if err := native.QueryRow("SELECT password, salt FROM sys_user WHERE user_id = $1", userID).Scan(&password, &salt); err != nil {
		return "absent", "absent"
	}
	return "present:" + password.String + ":" + fmt.Sprint(password.Valid), "present:" + salt.String + ":" + fmt.Sprint(salt.Valid)
}

func originalSnapshot(native *sql.DB, userID int) map[string]any {
	values := make([]sql.NullString, len(corpusUserSnapshotColumns))
	destinations := make([]any, len(values))
	for i := range values {
		destinations[i] = &values[i]
	}
	if err := native.QueryRow(corpusUserSnapshotSQL, userID).Scan(destinations...); err != nil {
		return nil
	}
	pointers := make([]*string, len(values))
	for i := range values {
		if values[i].Valid {
			text := values[i].String
			pointers[i] = &text
		}
	}
	return corpusSnapshotRecord(pointers)
}

func originalNotFound(err error) bool { return errors.Is(err, gorm.ErrRecordNotFound) }

func runOriginalOperation(t *testing.T, native *sql.DB, application *service.SysUser, operation corpusOperation) corpusOperationResult {
	t.Helper()
	permission := originalPermission(operation.Permission)
	result := corpusOperationResult{Name: operation.Name, UserID: operation.UserID, Extra: map[string]any{}}
	passwordBefore, saltBefore := originalCredentials(native, operation.UserID)
	var err error
	switch operation.Kind {
	case "get", "getself":
		var model models.SysUser
		target := dto.SysUserById{}
		target.Id = operation.UserID
		if operation.Kind == "get" {
			err = application.Get(&target, permission, &model)
		} else {
			err = application.GetSelf(&target, &model)
		}
		if err == nil {
			result.Extra["model"] = map[string]any{"user_id": model.UserId, "username": model.Username, "dept_ids": model.DeptIds, "role_ids": model.RoleIds, "post_ids": model.PostIds, "has_dept": model.Dept != nil}
		}
	case "insert":
		r := operation.Request
		req := dto.SysUserInsertReq{UserId: r.UserID, Username: r.Username, Password: r.Password, NickName: r.NickName, Phone: r.Phone, RoleId: r.RoleID, Avatar: r.Avatar, Sex: r.Sex, Email: r.Email, DeptId: r.DeptID, PostId: r.PostID, Remark: r.Remark, Status: r.Status}
		req.CreateBy = r.CreateBy
		err = application.Insert(&req)
		var id int
		if scanErr := native.QueryRow("SELECT user_id FROM sys_user WHERE username = $1 ORDER BY user_id LIMIT 1", r.Username).Scan(&id); scanErr == nil {
			result.UserID = id
		}
		var stored sql.NullString
		if scanErr := native.QueryRow("SELECT password FROM sys_user WHERE user_id = $1", result.UserID).Scan(&stored); scanErr == nil {
			result.Extra["password_verifies"] = stored.Valid && bcrypt.CompareHashAndPassword([]byte(stored.String), []byte(r.Password)) == nil
			result.Extra["password_is_plaintext"] = stored.Valid && stored.String == r.Password
		}
	case "update":
		r := operation.Request
		req := dto.SysUserUpdateReq{UserId: r.UserID, Username: r.Username, NickName: r.NickName, Phone: r.Phone, RoleId: r.RoleID, Avatar: r.Avatar, Sex: r.Sex, Email: r.Email, DeptId: r.DeptID, PostId: r.PostID, Remark: r.Remark, Status: r.Status}
		err = application.Update(&req, permission, operation.CallerID)
	case "remove":
		target := dto.SysUserById{}
		target.Id = operation.UserID
		err = application.Remove(&target, permission)
	case "native":
		_, err = native.Exec(operation.SQL)
		if err != nil {
			t.Fatal("original native seed operation failed")
		}
	default:
		t.Fatal("unknown shared operation kind")
	}
	result.Error = corpusErrorClass(err, originalNotFound)
	result.Snapshot = originalSnapshot(native, result.UserID)
	if operation.Kind == "update" || operation.Kind == "remove" {
		passwordAfter, saltAfter := originalCredentials(native, operation.UserID)
		result.Extra["credentials_unchanged"] = passwordBefore == passwordAfter && saltBefore == saltAfter
	}
	return result
}

// The operations replay get, self lookup, insert, update, soft deletion and the
// privileged-field/credential rules through the actual original service layer.
func TestNeutronCorpusOriginalPostgresOperations(t *testing.T) {
	db, native, _ := corpusPostgres(t)
	scenario := loadCorpusScenario(t)
	seedOriginalCorpus(t, native, scenario)
	previousDP := config.ApplicationConfig.EnableDP
	t.Cleanup(func() { config.ApplicationConfig.EnableDP = previousDP })
	config.ApplicationConfig.EnableDP = true
	application := originalService(db)
	results := make([]corpusOperationResult, 0, len(scenario.Operations))
	for _, operation := range scenario.Operations {
		results = append(results, runOriginalOperation(t, native, application, operation))
	}
	transcript := newCorpusTranscript("operations")
	transcript.record("operations", results)
	transcript.write(t)
}
