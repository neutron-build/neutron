"""Verify the converted mappings against the migrated database catalog.

Run with the converted environment's interpreter from the prepared backend directory, with
the same environment the application uses (DATABASE_URL, NEUTRON_PYAPP_SCHEMA). It admits
both tables through Database.catalog_table (exact column OIDs and NOT NULL), then compares
the complete native column set, primary keys, the unique email index and the item.owner_id
ON DELETE CASCADE foreign key using an independent psycopg connection. Prints one JSON
line and exits non-zero on any mismatch.
"""
import json

import psycopg
from neutron.orm import Database

from app import persistence

UDT = {"uuid": "uuid", "varchar": "varchar", "bool": "bool", "timestamptz": "timestamptz"}


def main() -> None:
    url = persistence.database_url()
    schema = persistence.SCHEMA
    tables = {"user": persistence.USER_TABLE, "item": persistence.ITEM_TABLE}
    with Database.connect(url) as database:
        for name, table in tables.items():
            database.catalog_table(name, {column: item.spec for column, item in table.columns.items()}, schema=schema)
    quoted = lambda name: '"' + schema.replace('"', '""') + '"."' + name + '"'
    facts: dict[str, object] = {}
    with psycopg.connect(url) as connection:
        for name, table in tables.items():
            expected = sorted((column, UDT[item.spec.sql_type], "YES" if item.spec.nullable else "NO") for column, item in table.columns.items())
            actual = sorted(connection.execute(
                "SELECT column_name, udt_name, is_nullable FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
                (schema, name)).fetchall())
            assert actual == expected, f"column set mismatch for {name}"
            primary = connection.execute(
                "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
                "WHERE i.indrelid = %s::regclass AND i.indisprimary", (quoted(name),)).fetchall()
            assert primary == [("id",)], f"primary key mismatch for {name}"
            facts[name + "_columns"] = len(actual)
        email_index = connection.execute(
            "SELECT i.indisunique, a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
            "WHERE i.indrelid = %s::regclass AND NOT i.indisprimary", (quoted("user"),)).fetchall()
        assert (True, "email") in email_index, "unique email index missing"
        cascade = connection.execute(
            "SELECT c.confdeltype, a.attname FROM pg_constraint c JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey) "
            "WHERE c.conrelid = %s::regclass AND c.contype = 'f' AND c.confrelid = %s::regclass", (quoted("item"), quoted("user"))).fetchall()
        assert cascade == [("c", "owner_id")], "item.owner_id ON DELETE CASCADE foreign key missing"
    print(json.dumps({"catalog_admission": True, "native_catalog_equal": True, "email_unique": True, "owner_fk_cascade": True, **facts}))


if __name__ == "__main__":
    main()
