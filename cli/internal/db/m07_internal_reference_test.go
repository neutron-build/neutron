package db

import (
	"strings"
	"testing"
)

// jobResultsDocJSON is what live introspection returns for a database where
// a user table keeps a foreign key into the job queues' _neutron_jobs
// table, next to a foreign key between user tables.
const jobResultsDocJSON = `{
	"version": 2, "dialect": "postgresql", "capabilities": [],
	"schemas": [{"name": "public"}],
	"tables": [
		{
			"identity": {"schema": "public", "name": "_neutron_jobs"},
			"managed": true,
			"columns": [{"name": "id", "type": {"name": "text", "codec": "string"}, "notNull": true}],
			"constraints": [{"type": "primary-key", "name": "_neutron_jobs_pkey", "columns": ["id"]}],
			"indexes": []
		},
		{
			"identity": {"schema": "public", "name": "job_results"},
			"managed": true,
			"columns": [
				{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true},
				{"name": "job_id", "type": {"name": "text", "codec": "string"}, "notNull": false},
				{"name": "user_id", "type": {"name": "int4", "codec": "number"}, "notNull": false}
			],
			"constraints": [
				{"type": "primary-key", "name": "job_results_pkey", "columns": ["id"]},
				{"type": "foreign-key", "name": "job_results_job_id_fkey", "columns": ["job_id"],
				 "references": {"table": {"schema": "public", "name": "_neutron_jobs"}, "columns": ["id"]}},
				{"type": "foreign-key", "name": "job_results_user_id_fkey", "columns": ["user_id"],
				 "references": {"table": {"schema": "public", "name": "users"}, "columns": ["id"]}}
			],
			"indexes": []
		},
		{
			"identity": {"schema": "public", "name": "users"},
			"managed": true,
			"columns": [{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true}],
			"constraints": [{"type": "primary-key", "name": "users_pkey", "columns": ["id"]}],
			"indexes": []
		}
	],
	"enums": [], "views": [], "opaque": []
}`

// M07 review-1 F3: a user foreign key into a neutron-internal table is
// refused with the table, the key and the internal target named, and the
// reason; foreign keys between user tables are not listed.
func TestWithoutInternalMetadataNamesForeignKeysIntoInternalTables(t *testing.T) {
	doc, err := ParseV2Document([]byte(jobResultsDocJSON))
	if err != nil {
		t.Fatal(err)
	}
	out, removed, err := WithoutInternalMetadata(doc)
	if err == nil {
		t.Fatalf("a user foreign key into _neutron_jobs must refuse the document (removed %v, tables %v)", removed, tableNames(t, out))
	}
	msg := err.Error()
	for _, want := range []string{
		"table public.job_results has foreign key job_results_job_id_fkey referencing public._neutron_jobs",
		"_neutron_* tables are neutron-managed",
		"cannot be declared in a schema document",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal must say %q:\n%s", want, msg)
		}
	}
	if strings.Contains(msg, "job_results_user_id_fkey") {
		t.Errorf("a foreign key between user tables must not be listed:\n%s", msg)
	}
}
