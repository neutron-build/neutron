// Package converted is the bounded Neutron conversion of go-admin's sys_user
// service, data-scope authorization and user-update API. It retains the pinned
// upstream business models, DTOs and permission constants; only database access
// moves from GORM to the typed Neutron ORM. It is not the whole go-admin product.
//
// Nullable columns follow the permissive DDL GORM's AutoMigrate produced for the
// unchanged application, so every mapped value is a nullable SDK field. NULL is
// normalized to the upstream zero value only at the explicit userModel and
// departmentModel boundary, never inside ORM decoding. Predicates that share a
// column type across a subquery (create_by IN (SELECT user_id ...)) therefore
// use the same nullable Go type on both sides.
package converted

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neutron-build/neutron/go/orm"
	"go-admin/app/admin/models"
	"go-admin/app/admin/service/dto"
	"go-admin/common/actions"
	"gorm.io/plugin/soft_delete"
)

// Messages are the upstream service strings, kept byte-identical so that
// application callers observe the same refusal text.
const (
	messageNotVisible   = "查看对象不存在或无权查看"
	messageRemoveDenied = "无权删除该数据"
	messageDuplicate    = "用户名已存在！"
	messageUpdateFailed = "update userinfo error"
)

var errBatchLookup = errors.New("converted application serves single-id lookups only")

type userRow struct {
	UserId    *int       `db:"user_id,nullable"`
	Username  *string    `db:"username,nullable"`
	Password  *string    `db:"password,nullable"`
	NickName  *string    `db:"nick_name,nullable"`
	Phone     *string    `db:"phone,nullable"`
	RoleId    *int       `db:"role_id,nullable"`
	Salt      *string    `db:"salt,nullable"`
	Avatar    *string    `db:"avatar,nullable"`
	Sex       *string    `db:"sex,nullable"`
	Email     *string    `db:"email,nullable"`
	DeptId    *int       `db:"dept_id,nullable"`
	PostId    *int       `db:"post_id,nullable"`
	Remark    *string    `db:"remark,nullable"`
	Status    *string    `db:"status,nullable"`
	CreateBy  *int       `db:"create_by,nullable"`
	UpdateBy  *int       `db:"update_by,nullable"`
	CreatedAt *time.Time `db:"created_at,nullable"`
	UpdatedAt *time.Time `db:"updated_at,nullable"`
	DeletedAt *int64     `db:"deleted_at,nullable"`
}

type deptRow struct {
	DeptId    int        `db:"dept_id"`
	ParentId  *int       `db:"parent_id,nullable"`
	DeptPath  *string    `db:"dept_path,nullable"`
	DeptName  *string    `db:"dept_name,nullable"`
	Sort      *int       `db:"sort,nullable"`
	Leader    *string    `db:"leader,nullable"`
	Phone     *string    `db:"phone,nullable"`
	Email     *string    `db:"email,nullable"`
	Status    *int       `db:"status,nullable"`
	CreateBy  *int       `db:"create_by,nullable"`
	UpdateBy  *int       `db:"update_by,nullable"`
	CreatedAt *time.Time `db:"created_at,nullable"`
	UpdatedAt *time.Time `db:"updated_at,nullable"`
	DeletedAt *int64     `db:"deleted_at,nullable"`
}

// roleDeptRow maps the many2many:sys_role_dept join table GORM derives from the
// pinned SysRole. The native columns are NOT NULL primary-key parts; they are
// mapped nullable only so subquery projections share the *int type of the
// nullable sys_user.dept_id column they are compared with.
type roleDeptRow struct {
	RoleId *int `db:"role_id,nullable"`
	DeptId *int `db:"dept_id,nullable"`
}

// deptScopeRow projects sys_dept with a nullable identifier for the
// department-tree scope and the department-path search, whose results are
// compared with the nullable sys_user.dept_id.
type deptScopeRow struct {
	DeptId   *int    `db:"dept_id,nullable"`
	DeptPath *string `db:"dept_path,nullable"`
}

// departmentKey is the non-nullable parent identity the Preload("Dept")
// relation requires; users without a department simply have no key.
type departmentKey struct {
	DeptId int `db:"dept_id"`
}

// columns holds every typed column binding once, so no call site can panic on a
// field-name mismatch after construction.
type columns struct {
	userID, deptID, roleID, postID, createBy, updateBy     orm.Column[userRow, *int]
	username, password, nickName, phone, salt, avatar, sex orm.Column[userRow, *string]
	email, remark, status                                  orm.Column[userRow, *string]
	createdAt, updatedAt                                   orm.Column[userRow, *time.Time]
	deletedAt                                              orm.Column[userRow, *int64]
	roleDeptRole, roleDeptDept                             orm.Column[roleDeptRow, *int]
	scopeDeptKey                                           orm.Column[deptScopeRow, *int]
	scopeDeptPath                                          orm.Column[deptScopeRow, *string]
	deptDeletedAt                                          orm.Column[deptRow, *int64]
	keyDeptID                                              orm.Column[departmentKey, int]
	departmentID                                           orm.Column[deptRow, int]
}

type binder struct{ err error }

func bind[M, T any](b *binder, table orm.Table[M], field string) orm.Column[M, T] {
	column, err := orm.NewColumn[M, T](table, field)
	if err != nil && b.err == nil {
		b.err = err
	}
	return column
}

func ptr[T any](v T) *T { return &v }

func value[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}
	return *p
}

// Service owns qualified table bindings. It holds no connection, transaction or
// session: every method receives the Executor (pool, request Scope or
// transaction) it must use, so request ownership stays with the caller.
type Service struct {
	enabled          bool
	users            orm.Table[userRow]
	departments      orm.Table[deptRow]
	roleDepartments  orm.Table[roleDeptRow]
	scopeDepartments orm.Table[deptScopeRow]
	relation         orm.Relation[departmentKey, deptRow]
	c                columns
}

// NewService catalog-qualifies the actual original schema (sys_user, sys_dept
// and the sys_role_dept join table) before returning. dataPermission mirrors
// the original process-wide EnableDP flag as an explicit constructor input.
func NewService(ctx context.Context, pool *pgxpool.Pool, schema string, dataPermission bool) (*Service, error) {
	if pool == nil {
		return nil, errors.New("converted application pool required")
	}
	users, err := orm.NewPostgresTable[userRow](ctx, pool, schema, "sys_user")
	if err != nil {
		return nil, err
	}
	departments, err := orm.NewPostgresTable[deptRow](ctx, pool, schema, "sys_dept")
	if err != nil {
		return nil, err
	}
	roleDepartments, err := orm.NewPostgresTable[roleDeptRow](ctx, pool, schema, "sys_role_dept")
	if err != nil {
		return nil, err
	}
	scopeDepartments, err := orm.NewPostgresTable[deptScopeRow](ctx, pool, schema, "sys_dept")
	if err != nil {
		return nil, err
	}
	keys, err := orm.NewTable[departmentKey](schema, "sys_user")
	if err != nil {
		return nil, err
	}
	var b binder
	bound := columns{
		userID:        bind[userRow, *int](&b, users, "UserId"),
		deptID:        bind[userRow, *int](&b, users, "DeptId"),
		roleID:        bind[userRow, *int](&b, users, "RoleId"),
		postID:        bind[userRow, *int](&b, users, "PostId"),
		createBy:      bind[userRow, *int](&b, users, "CreateBy"),
		updateBy:      bind[userRow, *int](&b, users, "UpdateBy"),
		username:      bind[userRow, *string](&b, users, "Username"),
		password:      bind[userRow, *string](&b, users, "Password"),
		nickName:      bind[userRow, *string](&b, users, "NickName"),
		phone:         bind[userRow, *string](&b, users, "Phone"),
		salt:          bind[userRow, *string](&b, users, "Salt"),
		avatar:        bind[userRow, *string](&b, users, "Avatar"),
		sex:           bind[userRow, *string](&b, users, "Sex"),
		email:         bind[userRow, *string](&b, users, "Email"),
		remark:        bind[userRow, *string](&b, users, "Remark"),
		status:        bind[userRow, *string](&b, users, "Status"),
		createdAt:     bind[userRow, *time.Time](&b, users, "CreatedAt"),
		updatedAt:     bind[userRow, *time.Time](&b, users, "UpdatedAt"),
		deletedAt:     bind[userRow, *int64](&b, users, "DeletedAt"),
		roleDeptRole:  bind[roleDeptRow, *int](&b, roleDepartments, "RoleId"),
		roleDeptDept:  bind[roleDeptRow, *int](&b, roleDepartments, "DeptId"),
		scopeDeptKey:  bind[deptScopeRow, *int](&b, scopeDepartments, "DeptId"),
		scopeDeptPath: bind[deptScopeRow, *string](&b, scopeDepartments, "DeptPath"),
		deptDeletedAt: bind[deptRow, *int64](&b, departments, "DeletedAt"),
		keyDeptID:     bind[departmentKey, int](&b, keys, "DeptId"),
		departmentID:  bind[deptRow, int](&b, departments, "DeptId"),
	}
	if b.err != nil {
		return nil, b.err
	}
	relation, err := orm.NewRelation(keys, departments, orm.Join(bound.keyDeptID, bound.departmentID))
	if err != nil {
		return nil, err
	}
	return &Service{
		enabled:          dataPermission,
		users:            users,
		departments:      departments,
		roleDepartments:  roleDepartments,
		scopeDepartments: scopeDepartments,
		relation:         relation,
		c:                bound,
	}, nil
}

func (s *Service) live() orm.Predicate[userRow] {
	return s.c.deletedAt.Eq(ptr(int64(0)))
}

// creatorIn restricts rows to creators selected by members. Like the upstream
// raw subqueries, members deliberately includes soft-deleted users and
// departments; only the outer user rows receive the live predicate.
func (s *Service) creatorIn(members orm.Query[userRow]) (orm.Predicate[userRow], error) {
	source, err := orm.NewScalarQuery(s.c.userID, members)
	if err != nil {
		return orm.Predicate[userRow]{}, err
	}
	return orm.And(s.live(), orm.InSubquery(s.c.createBy, source)), nil
}

// visibility is actions.Permission translated: zero-live outer predicate plus
// the five upstream create_by scopes. An unknown scope, a department scope with
// department <= 0 and a nil permission all fail closed to no rows.
func (s *Service) visibility(p *actions.DataPermission) (orm.Predicate[userRow], error) {
	live := s.live()
	if !s.enabled {
		return live, nil
	}
	none := orm.And(live, s.c.userID.In())
	if p == nil {
		return none, nil
	}
	switch p.DataScope {
	case actions.DataScopeAll:
		return live, nil
	case actions.DataScopeCustom:
		departments, err := orm.NewScalarQuery(s.c.roleDeptDept, orm.Query[roleDeptRow]{}.Where(s.c.roleDeptRole.Eq(ptr(p.RoleId))))
		if err != nil {
			return orm.Predicate[userRow]{}, err
		}
		return s.creatorIn(orm.Query[userRow]{}.Where(orm.InSubquery(s.c.deptID, departments)))
	case actions.DataScopeDept:
		if p.DeptId <= 0 {
			return none, nil
		}
		return s.creatorIn(orm.Query[userRow]{}.Where(s.c.deptID.Eq(ptr(p.DeptId))))
	case actions.DataScopeDeptTree:
		if p.DeptId <= 0 {
			return none, nil
		}
		departments, err := orm.NewScalarQuery(s.c.scopeDeptKey, orm.Query[deptScopeRow]{}.Where(orm.LikeNullable(s.c.scopeDeptPath, fmt.Sprintf("%%/%d/%%", p.DeptId))))
		if err != nil {
			return orm.Predicate[userRow]{}, err
		}
		return s.creatorIn(orm.Query[userRow]{}.Where(orm.InSubquery(s.c.deptID, departments)))
	case actions.DataScopeSelf:
		return orm.And(live, s.c.createBy.Eq(ptr(p.UserId))), nil
	default:
		return none, nil
	}
}

// contains and exact reproduce the postgres branch of go-admin-core's search
// tags: contains is LIKE '%v%' with wildcards left active (not escaped), exact
// is equality, and a zero DTO value means the filter is absent.
func contains(column orm.Column[userRow, *string], text string) orm.Predicate[userRow] {
	return orm.LikeNullable(column, "%"+text+"%")
}

func exactNumber(column orm.Column[userRow, *int], text string) (orm.Predicate[userRow], error) {
	number, err := strconv.Atoi(strings.TrimSpace(text))
	if err != nil {
		return orm.Predicate[userRow]{}, errors.New("invalid numeric search value")
	}
	return column.Eq(ptr(number)), nil
}

func (s *Service) search(req *dto.SysUserGetPageReq) ([]orm.Predicate[userRow], error) {
	var predicates []orm.Predicate[userRow]
	if req.UserId != 0 {
		predicates = append(predicates, s.c.userID.Eq(ptr(req.UserId)))
	}
	for _, filter := range []struct {
		column orm.Column[userRow, *string]
		text   string
	}{{s.c.username, req.Username}, {s.c.nickName, req.NickName}, {s.c.phone, req.Phone}, {s.c.email, req.Email}} {
		if filter.text != "" {
			predicates = append(predicates, contains(filter.column, filter.text))
		}
	}
	for _, filter := range []struct {
		column orm.Column[userRow, *string]
		text   string
	}{{s.c.sex, req.Sex}, {s.c.status, req.Status}} {
		if filter.text != "" {
			predicates = append(predicates, filter.column.Eq(ptr(filter.text)))
		}
	}
	for _, filter := range []struct {
		column orm.Column[userRow, *int]
		text   string
	}{{s.c.roleID, req.RoleId}, {s.c.postID, req.PostId}} {
		if filter.text == "" {
			continue
		}
		predicate, err := exactNumber(filter.column, filter.text)
		if err != nil {
			return nil, err
		}
		predicates = append(predicates, predicate)
	}
	if req.DeptId != "" {
		// The upstream LEFT JOIN sys_dept ... WHERE dept_path LIKE acts as an
		// inner join on the primary key dept_id, so it is equivalent to this
		// semi-join without duplicating user rows.
		departments, err := orm.NewScalarQuery(s.c.scopeDeptKey, orm.Query[deptScopeRow]{}.Where(orm.LikeNullable(s.c.scopeDeptPath, "%"+req.DeptId+"%")))
		if err != nil {
			return nil, err
		}
		predicates = append(predicates, orm.InSubquery(s.c.deptID, departments))
	}
	return predicates, nil
}

// orders follow the upstream tag order and ignore any direction other than
// asc/desc (case-insensitive), exactly like the original search resolver.
func (s *Service) orders(req *dto.SysUserGetPageReq) []orm.Order[userRow] {
	var orders []orm.Order[userRow]
	add := func(direction string, ascending, descending orm.Order[userRow]) {
		switch strings.ToLower(direction) {
		case "asc":
			orders = append(orders, ascending)
		case "desc":
			orders = append(orders, descending)
		}
	}
	add(req.UserIdOrder, s.c.userID.Asc(), s.c.userID.Desc())
	add(req.UsernameOrder, s.c.username.Asc(), s.c.username.Desc())
	add(req.StatusOrder, s.c.status.Asc(), s.c.status.Desc())
	add(req.CreatedAtOrder, s.c.createdAt.Asc(), s.c.createdAt.Desc())
	return orders
}

func userModel(row userRow) (models.SysUser, error) {
	deleted := value(row.DeletedAt)
	if deleted < 0 {
		return models.SysUser{}, errors.New("negative original unsigned deletion marker")
	}
	user := models.SysUser{
		UserId:   value(row.UserId),
		Username: value(row.Username),
		Password: value(row.Password),
		NickName: value(row.NickName),
		Phone:    value(row.Phone),
		RoleId:   value(row.RoleId),
		Salt:     value(row.Salt),
		Avatar:   value(row.Avatar),
		Sex:      value(row.Sex),
		Email:    value(row.Email),
		DeptId:   value(row.DeptId),
		PostId:   value(row.PostId),
		Remark:   value(row.Remark),
		Status:   value(row.Status),
	}
	user.CreateBy = value(row.CreateBy)
	user.UpdateBy = value(row.UpdateBy)
	user.CreatedAt = value(row.CreatedAt)
	user.UpdatedAt = value(row.UpdatedAt)
	user.DeletedAt = soft_delete.DeletedAt(deleted)
	if err := user.AfterFind(nil); err != nil {
		return models.SysUser{}, err
	}
	return user, nil
}

func departmentModel(row deptRow) (models.SysDept, error) {
	deleted := value(row.DeletedAt)
	if deleted < 0 {
		return models.SysDept{}, errors.New("negative original unsigned deletion marker")
	}
	department := models.SysDept{
		DeptId:   row.DeptId,
		ParentId: value(row.ParentId),
		DeptPath: value(row.DeptPath),
		DeptName: value(row.DeptName),
		Sort:     value(row.Sort),
		Leader:   value(row.Leader),
		Phone:    value(row.Phone),
		Email:    value(row.Email),
		Status:   value(row.Status),
	}
	department.CreateBy = value(row.CreateBy)
	department.UpdateBy = value(row.UpdateBy)
	department.CreatedAt = value(row.CreatedAt)
	department.UpdatedAt = value(row.UpdatedAt)
	department.DeletedAt = soft_delete.DeletedAt(deleted)
	return department, nil
}

// toModels converts rows and, when preload is set, attaches Dept the way
// Preload("Dept") does: a live department only, absent for dept_id 0/NULL or a
// missing or soft-deleted department.
func (s *Service) toModels(ctx context.Context, db orm.Executor, rows []userRow, preload bool) ([]models.SysUser, error) {
	result := make([]models.SysUser, len(rows))
	keys := make([]departmentKey, 0, len(rows))
	positions := make([]int, 0, len(rows))
	for i, row := range rows {
		user, err := userModel(row)
		if err != nil {
			return nil, err
		}
		result[i] = user
		if preload && user.DeptId != 0 {
			keys = append(keys, departmentKey{DeptId: user.DeptId})
			positions = append(positions, i)
		}
	}
	if len(keys) == 0 {
		return result, nil
	}
	live := orm.Query[deptRow]{}.Where(s.c.deptDeletedAt.Eq(ptr(int64(0))))
	loaded, err := orm.LoadOne(ctx, db, s.relation, keys, live, orm.LoadBudget{MaxParents: len(keys), MaxRows: len(keys), BatchSize: 256})
	if err != nil {
		return nil, err
	}
	for i, association := range loaded {
		if len(association.Children) == 0 {
			continue
		}
		department, err := departmentModel(association.Children[0])
		if err != nil {
			return nil, err
		}
		result[positions[i]].Dept = &department
	}
	return result, nil
}

// GetPage applies identical search and permission predicates to the count and
// the page. Without an upstream order key the row order is unspecified, as it
// was in the original.
func (s *Service) GetPage(ctx context.Context, db orm.Executor, req *dto.SysUserGetPageReq, p *actions.DataPermission) ([]models.SysUser, int64, error) {
	visibility, err := s.visibility(p)
	if err != nil {
		return nil, 0, err
	}
	predicates, err := s.search(req)
	if err != nil {
		return nil, 0, err
	}
	query := orm.Query[userRow]{}.Where(orm.And(append([]orm.Predicate[userRow]{visibility}, predicates...)...))
	pageSize, pageIndex := req.GetPageSize(), req.GetPageIndex()
	count, err := orm.AggregateOne(ctx, db, orm.CountAll(s.users), query)
	if err != nil {
		return nil, 0, err
	}
	rows, err := orm.Select(ctx, db, s.users, query.OrderBy(s.orders(req)...).Limit(pageSize).Offset((pageIndex-1)*pageSize))
	if err != nil {
		return nil, 0, err
	}
	list, err := s.toModels(ctx, db, rows, true)
	if err != nil {
		return nil, 0, err
	}
	return list, count, nil
}

func (s *Service) visible(ctx context.Context, db orm.Executor, id int, p *actions.DataPermission) (userRow, error) {
	visibility, err := s.visibility(p)
	if err != nil {
		return userRow{}, err
	}
	return orm.SelectOne(ctx, db, s.users, orm.Query[userRow]{}.Where(orm.And(visibility, s.c.userID.Eq(ptr(id)))))
}

func (s *Service) one(ctx context.Context, db orm.Executor, row userRow, err error) (models.SysUser, error) {
	if err != nil {
		if errors.Is(err, orm.ErrNotFound) {
			return models.SysUser{}, errors.New(messageNotVisible)
		}
		return models.SysUser{}, err
	}
	users, err := s.toModels(ctx, db, []userRow{row}, false)
	if err != nil {
		return models.SysUser{}, err
	}
	return users[0], nil
}

// Get applies the data scope. Batch ids are outside this bounded conversion.
func (s *Service) Get(ctx context.Context, db orm.Executor, req *dto.SysUserById, p *actions.DataPermission) (models.SysUser, error) {
	if len(req.Ids) > 0 {
		return models.SysUser{}, errBatchLookup
	}
	row, err := s.visible(ctx, db, req.Id, p)
	return s.one(ctx, db, row, err)
}

// GetSelf reads the caller's own row without a data scope, like the upstream
// token-identity lookup; only the soft-delete predicate applies.
func (s *Service) GetSelf(ctx context.Context, db orm.Executor, req *dto.SysUserById) (models.SysUser, error) {
	if len(req.Ids) > 0 {
		return models.SysUser{}, errBatchLookup
	}
	row, err := orm.SelectOne(ctx, db, s.users, orm.Query[userRow]{}.Where(orm.And(s.live(), s.c.userID.Eq(ptr(req.Id)))))
	return s.one(ctx, db, row, err)
}

// GetProfile is the user and Dept portion of the upstream GetProfile. Roles and
// posts are outside the bounded conversion. A missing user keeps the underlying
// not-found error, as the upstream GORM error was returned unchanged.
func (s *Service) GetProfile(ctx context.Context, db orm.Executor, id int) (models.SysUser, error) {
	row, err := orm.SelectOne(ctx, db, s.users, orm.Query[userRow]{}.Where(orm.And(s.live(), s.c.userID.Eq(ptr(id)))))
	if err != nil {
		return models.SysUser{}, err
	}
	users, err := s.toModels(ctx, db, []userRow{row}, true)
	if err != nil {
		return models.SysUser{}, err
	}
	return users[0], nil
}

// Insert mirrors the upstream username-uniqueness check and Create. Run it in a
// caller-owned transaction when it must roll back with other writes.
func (s *Service) Insert(ctx context.Context, db orm.Executor, req *dto.SysUserInsertReq) error {
	existing, err := orm.AggregateOne(ctx, db, orm.CountAll(s.users), orm.Query[userRow]{}.Where(orm.And(s.live(), s.c.username.Eq(ptr(req.Username)))))
	if err != nil {
		return err
	}
	if existing > 0 {
		return errors.New(messageDuplicate)
	}
	var user models.SysUser
	req.Generate(&user)
	return s.InsertUser(ctx, db, &user)
}

// InsertUser runs the upstream BeforeCreate password hook and then writes every
// column, as GORM Create does, including empty strings and zero identifiers.
// A hook failure happens before any statement, so a surrounding transaction can
// roll back earlier writes exactly as the original did.
func (s *Service) InsertUser(ctx context.Context, db orm.Executor, user *models.SysUser) error {
	if err := user.BeforeCreate(nil); err != nil {
		return err
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	created, updated := user.CreatedAt.UTC().Truncate(time.Microsecond), user.UpdatedAt.UTC().Truncate(time.Microsecond)
	if user.CreatedAt.IsZero() {
		created = now
	}
	if user.UpdatedAt.IsZero() {
		updated = now
	}
	assignments := []orm.Assignment[userRow]{
		orm.Set(s.c.username, orm.Some(ptr(user.Username))),
		orm.Set(s.c.password, orm.Some(ptr(user.Password))),
		orm.Set(s.c.nickName, orm.Some(ptr(user.NickName))),
		orm.Set(s.c.phone, orm.Some(ptr(user.Phone))),
		orm.Set(s.c.roleID, orm.Some(ptr(user.RoleId))),
		orm.Set(s.c.salt, orm.Some(ptr(user.Salt))),
		orm.Set(s.c.avatar, orm.Some(ptr(user.Avatar))),
		orm.Set(s.c.sex, orm.Some(ptr(user.Sex))),
		orm.Set(s.c.email, orm.Some(ptr(user.Email))),
		orm.Set(s.c.deptID, orm.Some(ptr(user.DeptId))),
		orm.Set(s.c.postID, orm.Some(ptr(user.PostId))),
		orm.Set(s.c.remark, orm.Some(ptr(user.Remark))),
		orm.Set(s.c.status, orm.Some(ptr(user.Status))),
		orm.Set(s.c.createBy, orm.Some(ptr(user.CreateBy))),
		orm.Set(s.c.updateBy, orm.Some(ptr(user.UpdateBy))),
		orm.Set(s.c.createdAt, orm.Some(ptr(created))),
		orm.Set(s.c.updatedAt, orm.Some(ptr(updated))),
		orm.Set(s.c.deletedAt, orm.Some(ptr(int64(user.DeletedAt)))),
	}
	if user.UserId != 0 {
		assignments = append(assignments, orm.Set(s.c.userID, orm.Some(ptr(user.UserId))))
	}
	row, err := orm.InsertOne(ctx, db, s.users, assignments...)
	if err != nil {
		return err
	}
	user.UserId = value(row.UserId)
	return nil
}

// updateAssignments reproduces GORM struct Updates with Omit("password",
// "salt"): only non-zero model fields are written (so an empty request value
// leaves the stored value, including SQL NULL, untouched), and updated_at is
// refreshed. create_by, update_by and created_at are never changed by the
// upstream Generate, so their loaded values are not rewritten.
func (s *Service) updateAssignments(user *models.SysUser, now time.Time) []orm.Assignment[userRow] {
	var assignments []orm.Assignment[userRow]
	text := func(column orm.Column[userRow, *string], v string) {
		if v != "" {
			assignments = append(assignments, orm.Set(column, orm.Some(ptr(v))))
		}
	}
	number := func(column orm.Column[userRow, *int], v int) {
		if v != 0 {
			assignments = append(assignments, orm.Set(column, orm.Some(ptr(v))))
		}
	}
	text(s.c.username, user.Username)
	text(s.c.nickName, user.NickName)
	text(s.c.phone, user.Phone)
	number(s.c.roleID, user.RoleId)
	text(s.c.avatar, user.Avatar)
	text(s.c.sex, user.Sex)
	text(s.c.email, user.Email)
	number(s.c.deptID, user.DeptId)
	number(s.c.postID, user.PostId)
	text(s.c.remark, user.Remark)
	text(s.c.status, user.Status)
	return append(assignments, orm.Set(s.c.updatedAt, orm.Some(ptr(now))))
}

// Update is the upstream service update: scoped read, privileged self fields
// preserved, the BeforeUpdate hook run on the loaded model, password and salt
// never assigned, and exactly the non-zero fields written. Use one transaction
// (for HTTP, the request-owned Scope) so the read and write share a snapshot.
func (s *Service) Update(ctx context.Context, db orm.Executor, req *dto.SysUserUpdateReq, p *actions.DataPermission, callerID int) error {
	row, err := s.visible(ctx, db, req.UserId, p)
	if err != nil {
		return err
	}
	user, err := userModel(row)
	if err != nil {
		return err
	}
	if user.UserId == callerID {
		req.RoleId = user.RoleId
		req.DeptId = user.DeptId
		req.Status = user.Status
	}
	req.Generate(&user)
	if err := user.BeforeUpdate(nil); err != nil {
		return err
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	affected, err := orm.Update(ctx, db, s.users, orm.And(s.live(), s.c.userID.Eq(ptr(user.UserId))), s.updateAssignments(&user, now)...)
	if err != nil {
		return err
	}
	if affected == 0 {
		return errors.New(messageUpdateFailed)
	}
	return nil
}

// Remove soft-deletes within the data scope: deleted_at becomes the current
// epoch milliseconds, only for live rows, matching GORM's soft_delete:milli.
func (s *Service) Remove(ctx context.Context, db orm.Executor, req *dto.SysUserById, p *actions.DataPermission) error {
	ids := []int{req.Id}
	if len(req.Ids) > 0 {
		ids = append(append([]int{}, req.Ids...), req.Id)
	}
	pointers := make([]*int, len(ids))
	for i := range ids {
		pointers[i] = ptr(ids[i])
	}
	visibility, err := s.visibility(p)
	if err != nil {
		return err
	}
	affected, err := orm.Update(ctx, db, s.users, orm.And(visibility, s.c.userID.In(pointers...)), orm.Set(s.c.deletedAt, orm.Some(ptr(time.Now().UnixMilli()))))
	if err != nil {
		return err
	}
	if affected == 0 {
		return errors.New(messageRemoveDenied)
	}
	return nil
}
