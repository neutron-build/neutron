package db

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"testing"
)

func TestBackfillNativePrivilegeAndReadOnlyRefusals(t *testing.T) {
	h, job := newBackfillHarness(t)
	ctx := context.Background()
	h.exec(`INSERT INTO public.source VALUES(1,'private',NULL)`)
	role := "backfill_role_" + h.dbName
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		t.Fatal(err)
	}
	password := hex.EncodeToString(bytes)
	// The fixture password is generated only in memory and never printed.
	if err := h.client.Exec(ctx, `CREATE ROLE "`+role+`" LOGIN PASSWORD '`+password+`'`); err != nil {
		t.Fatal("fixture role provisioning failed")
	}
	t.Cleanup(func() {
		if err := h.client.Exec(ctx, `DROP OWNED BY "`+role+`";DROP ROLE "`+role+`"`); err != nil {
			t.Error("fixture role cleanup failed")
		}
	})
	h.exec(`GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO "` + role + `"`)
	h.exec(`GRANT USAGE ON SCHEMA public TO "` + role + `"`)
	h.exec(`GRANT SELECT ON public.source,public.progress TO "` + role + `"`)
	h.exec(`GRANT UPDATE(dst) ON public.source TO "` + role + `"`)
	h.exec(`GRANT UPDATE ON public.progress TO "` + role + `"`)
	u, err := url.Parse(h.client.url)
	if err != nil {
		t.Fatal("fixture URL invalid")
	}
	u.User = url.UserPassword(role, password)
	worker, err := Connect(ctx, u.String())
	if err != nil {
		t.Fatal("fixture worker connection failed")
	}
	defer worker.Close()
	// Prove cluster admission is available so failure cannot accidentally pass
	// solely because the low-privilege role cannot read the cluster identifier.
	var cluster string
	if err := worker.QueryRow(ctx, `SELECT system_identifier::pg_catalog.text FROM pg_catalog.pg_control_system()`).Scan(&cluster); err != nil || cluster == "" {
		t.Fatal("fixture worker cluster authority unavailable")
	}
	if _, err := InspectBackfillJob(ctx, worker, job.Spec); err == nil {
		t.Fatal("checkpoint lacking INSERT privilege admitted")
	}
	if h.queryOne(`SELECT count(*)::text FROM public.progress`) != "0" {
		t.Fatal("inspection had checkpoint effects")
	}
	h.exec(`GRANT INSERT ON public.progress TO "` + role + `"`)
	admitted, err := InspectBackfillJob(ctx, worker, job.Spec)
	if err != nil {
		t.Fatal(err)
	}
	h.exec(`REVOKE UPDATE(dst) ON public.source FROM "` + role + `"`)
	if _, err := RunBackfillChunk(ctx, worker, admitted); err == nil {
		t.Fatal("revoked source write privilege accepted")
	}
	if h.queryOne(`SELECT count(*)::text FROM public.progress`) != "0" || h.queryOne(`SELECT count(*)::text FROM public.source WHERE dst IS NOT NULL`) != "0" {
		t.Fatal("privilege refusal mutated state")
	}
	// A connection whose default transaction mode is read-only cannot perform
	// the executor's locking/writes even when its role has SQL privileges.
	ownerURL, err := url.Parse(h.client.url)
	if err != nil {
		t.Fatal("fixture URL invalid")
	}
	q := ownerURL.Query()
	q.Set("default_transaction_read_only", "on")
	ownerURL.RawQuery = q.Encode()
	readonly, err := Connect(ctx, ownerURL.String())
	if err != nil {
		t.Fatal("readonly fixture connection failed")
	}
	defer readonly.Close()
	if _, err := RunBackfillChunk(ctx, readonly, job); err == nil {
		t.Fatal("readonly session performed a chunk")
	}
	if h.queryOne(`SELECT count(*)::text FROM public.progress`) != "0" {
		t.Fatal("readonly refusal advanced checkpoint")
	}
}

func TestBackfillNativeCatalogAndPolicyAdmission(t *testing.T) {
	cases := map[string]string{
		"source RLS":              `ALTER TABLE public.source ENABLE ROW LEVEL SECURITY`,
		"checkpoint RLS":          `ALTER TABLE public.progress ENABLE ROW LEVEL SECURITY`,
		"source trigger":          `CREATE FUNCTION public.reject_profile() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$; CREATE TRIGGER user_trigger BEFORE UPDATE ON public.source FOR EACH ROW EXECUTE FUNCTION public.reject_profile()`,
		"checkpoint trigger":      `CREATE FUNCTION public.reject_profile() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$; CREATE TRIGGER user_trigger BEFORE UPDATE ON public.progress FOR EACH ROW EXECUTE FUNCTION public.reject_profile()`,
		"extra checkpoint column": `ALTER TABLE public.progress ADD COLUMN unknown text`,
		"checkpoint domain":       `CREATE DOMAIN public.text AS pg_catalog.text;ALTER TABLE public.progress ALTER COLUMN format TYPE public.text`,
		"unlogged checkpoint":     `ALTER TABLE public.progress SET UNLOGGED`,
	}
	for name, ddl := range cases {
		t.Run(name, func(t *testing.T) {
			h, job := newBackfillHarness(t)
			h.exec(`INSERT INTO public.source VALUES(1,'unchanged',NULL)`)
			h.exec(ddl)
			if _, err := InspectBackfillJob(context.Background(), h.client, job.Spec); err == nil {
				t.Fatal("unsafe catalog profile admitted")
			}
			if _, err := RunBackfillChunk(context.Background(), h.client, job); err == nil {
				t.Fatal("stale approved job bypassed policy admission")
			}
			if h.queryOne(`SELECT count(*)::text FROM public.source WHERE dst IS NOT NULL`) != "0" || h.queryOne(`SELECT count(*)::text FROM public.progress`) != "0" {
				t.Fatal("admission refusal mutated native state")
			}
		})
	}
}
