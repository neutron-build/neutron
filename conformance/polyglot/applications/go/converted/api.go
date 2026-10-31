package converted

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	vd "github.com/bytedance/go-tagexpr/v2/validator"
	"github.com/neutron-build/neutron/go/orm"
	ormhttp "github.com/neutron-build/neutron/go/orm/http"
	"go-admin/app/admin/service/dto"
	"go-admin/common/actions"
)

const maxUpdateBody = 1 << 20

// Caller is what the application's authentication produced for a request. The
// original read these values from decoded JWT claims in the gin context;
// authentication stays application-owned and is not part of this conversion.
type Caller struct {
	UserID  int
	RoleKey string
}

// Identity resolves the authenticated caller and the data scope PermissionAction
// would have stored for the request. It reports false for an unauthenticated one.
type Identity func(*http.Request) (Caller, *actions.DataPermission, bool)

// Authorizer asks the application's Casbin enforcer whether the role may call
// the route. Policy evaluation stays application-owned.
type Authorizer func(ctx context.Context, caller Caller, path, method string) (bool, error)

func respond(status int, message string) ormhttp.Response {
	body, err := json.Marshal(map[string]any{"code": status, "msg": message})
	if err != nil {
		body = []byte(`{"code":500}`)
	}
	return ormhttp.Response{Status: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: body}
}

// UpdateHandler is the PUT /api/v1/sys-user handler. It runs inside the request-
// owned transaction Scope that ormhttp leases for the request: a body userId
// that differs from the caller requires an explicit Casbin grant (the route is
// excluded from the Casbin middleware upstream, so the handler enforces it),
// and the service then reads and writes through that one Scope. Returning an
// error rolls the request transaction back; refusals are committed responses
// because they performed no write.
func (s *Service) UpdateHandler(identity Identity, authorize Authorizer) ormhttp.Handler {
	return func(ctx context.Context, session ormhttp.RequestSession, r *http.Request) (ormhttp.Response, error) {
		caller, permission, ok := identity(r)
		if !ok {
			return respond(http.StatusUnauthorized, "unauthenticated"), nil
		}
		var req dto.SysUserUpdateReq
		if err := json.NewDecoder(io.LimitReader(r.Body, maxUpdateBody)).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			return respond(http.StatusBadRequest, "invalid request body"), nil
		}
		if err := vd.Validate(&req); err != nil {
			return respond(http.StatusBadRequest, "invalid request"), nil
		}
		if req.UserId != caller.UserID {
			allowed := caller.RoleKey == "admin"
			if !allowed {
				var err error
				allowed, err = authorize(ctx, caller, r.URL.Path, r.Method)
				if err != nil {
					return ormhttp.Response{}, err
				}
			}
			if !allowed {
				return respond(http.StatusForbidden, "无权更新其他用户数据"), nil
			}
		}
		req.SetUpdateBy(caller.UserID)
		if err := s.Update(ctx, session.Executor, &req, permission, caller.UserID); err != nil {
			if errors.Is(err, orm.ErrNotFound) {
				return respond(http.StatusNotFound, "record not found"), nil
			}
			return ormhttp.Response{}, err
		}
		return respond(http.StatusOK, "更新成功"), nil
	}
}
