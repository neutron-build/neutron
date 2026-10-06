package db

import (
	"strings"
	"testing"
)

func TestV3RolesSQLSelectsMembershipOptionsByServerVersion(t *testing.T) {
	for _, version := range []int{160000, 160004, 170002, 180000} {
		query, err := v3RolesSQL(version)
		if err != nil {
			t.Fatalf("version %d: %v", version, err)
		}
		if !strings.Contains(query, "m.inherit_option") || !strings.Contains(query, "m.set_option") {
			t.Fatalf("version %d must read inherit_option and set_option", version)
		}
		if strings.Contains(query, "unavailable-before-pg16") || strings.Contains(query, v3RoleOptionsToken) {
			t.Fatalf("version %d query has a legacy sentinel or an unreplaced token", version)
		}
	}
	for _, version := range []int{140000, 140013, 150000, 150008} {
		query, err := v3RolesSQL(version)
		if err != nil {
			t.Fatalf("version %d: %v", version, err)
		}
		if strings.Contains(query, "inherit_option") || strings.Contains(query, "set_option") {
			t.Fatalf("version %d must not reference pg_auth_members columns that do not exist", version)
		}
		if !strings.Contains(query, "m.admin_option") || strings.Count(query, "unavailable-before-pg16") != 2 || strings.Contains(query, v3RoleOptionsToken) {
			t.Fatalf("version %d must read ADMIN and mark INHERIT/SET explicitly unavailable", version)
		}
	}
	for _, version := range []int{0, 90600, 130014, 139999} {
		if _, err := v3RolesSQL(version); err == nil {
			t.Fatalf("version %d must be refused", version)
		}
	}
}
