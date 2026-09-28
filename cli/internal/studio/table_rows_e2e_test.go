package studio

import (
	"net/url"
	"testing"
)

// deriveStudioDatabaseURL points the e2e base URL at one disposable
// database.
func deriveStudioDatabaseURL(t *testing.T, base, dbName string) string {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse NEUTRON_E2E_DATABASE_URL: %v", err)
	}
	u.Path = "/" + dbName
	return u.String()
}
