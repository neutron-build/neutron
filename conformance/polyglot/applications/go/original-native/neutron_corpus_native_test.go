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
	if err := db.AutoMigrate(&models.SysDept{}, &models.SysUser{}); err != nil {
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
	victim := models.SysUser{UserId: 102, Username: "original-victim", NickName: "Victim", RoleId: 2, DeptId: 1, Status: "1"}
	victim.CreateBy = 102
	if err := db.Create(&victim).Error; err != nil {
		t.Fatal("original victim create failed")
	}
	callUpdate(t, db, tenant, 101, map[string]interface{}{"userId": 102, "username": victim.Username, "nickName": "pwned", "roleId": 1, "deptId": 1, "status": "0"})
	var role, dept int
	var nick, status string
	if err := native.QueryRow("SELECT role_id,dept_id,nick_name,status FROM sys_user WHERE user_id=102").Scan(&role, &dept, &nick, &status); err != nil || role != 2 || dept != 1 || nick != "Victim" || status != "1" {
		t.Fatal("native unauthorized other-user mutation")
	}
	callUpdate(t, db, tenant, 101, map[string]interface{}{"userId": 101, "username": user.Username, "nickName": "After", "roleId": 1, "deptId": 999, "status": "0"})
	var afterHash, afterSalt string
	if err := native.QueryRow("SELECT role_id,dept_id,nick_name,status,password,salt FROM sys_user WHERE user_id=101").Scan(&role, &dept, &nick, &status, &afterHash, &afterSalt); err != nil || role != 2 || dept != 1 || nick != "After" || status != "1" || afterHash != storedHash || afterSalt != storedSalt {
		t.Fatal("native original self-edit/credential oracle")
	}
	var loaded models.SysUser
	if err := db.Preload("Dept").First(&loaded, 101).Error; err != nil || loaded.Dept == nil || loaded.Dept.DeptName != department.DeptName || len(loaded.DeptIds) != 1 || loaded.DeptIds[0] != 1 || len(loaded.RoleIds) != 1 || loaded.RoleIds[0] != 2 || len(loaded.PostIds) != 1 || loaded.PostIds[0] != 3 {
		t.Fatal("original Preload/AfterFind native behavior")
	}
	if err := loaded.BeforeUpdate(db); err != nil || loaded.Password != storedHash {
		t.Fatal("original stored hash was rehashed")
	}
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
	permission.DataScope = "unknown"
	if err := application.GetPage(&req, permission, &page, &count); err != nil || count != 0 || len(page) != 0 {
		t.Fatal("original invalid scope failed open")
	}
	// Preserve actual uint millisecond zero-live deletion, not nullable datetime.
	var marker int64
	if err := native.QueryRow("SELECT deleted_at FROM sys_user WHERE user_id=101").Scan(&marker); err != nil || marker != 0 {
		t.Fatal("original zero-live deletion schema")
	}
	if err := db.Delete(&victim).Error; err != nil {
		t.Fatal("original soft delete failed")
	}
	if err := native.QueryRow("SELECT deleted_at FROM sys_user WHERE user_id=102").Scan(&marker); err != nil || marker <= 0 {
		t.Fatal("native original millisecond soft-delete oracle")
	}
	if err := db.First(&models.SysUser{}, 102).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatal("original record-not-found translation")
	}
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
}
