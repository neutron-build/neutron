-- Minimal upstream reproducers for the Nucleus defects found by the X00 ORM
-- probes (conformance/live/orm/). Plain SQL, no driver involved, so each one
-- isolates the engine from the client. Run against a DISPOSABLE engine and,
-- as the oracle, a throwaway PostgreSQL database:
--
--   psql "$URL" -X -At -f conformance/live/orm/reproducers.sql
--
-- Every block echoes its id; ORM_CONFORMANCE.md in this directory describes
-- each defect. N1 and N3 have standalone, fully characterised reproducers in
-- upstream/ (expected and observed output recorded in ORM_CONFORMANCE.md).

\echo N2 ddl-survives-rollback
begin;
create table x00r_rb (id int);
rollback;
select count(*) from pg_class where relname = 'x00r_rb';  -- PostgreSQL: 0

\echo N2b failed-migration-partially-applied
create table x00r_base (id int primary key);
begin;
alter table x00r_base add column m int;
create table x00r_base (id int);  -- fails 42P07
commit;
select count(*) from information_schema.columns where table_name = 'x00r_base' and column_name = 'm';  -- PostgreSQL: 0

-- N3: see upstream/N3-timestamptz-offset.sql

\echo N4 catalog-introspection
create table x00r_cat (id bigserial primary key, code varchar(20) not null, amount numeric(10,2) not null default 0);
select column_name, data_type, is_nullable from information_schema.columns where table_name = 'x00r_cat' order by ordinal_position;
select format_type(atttypid, atttypmod) from pg_attribute where attrelid = 'x00r_cat'::regclass and attnum > 0 order by attnum;
select pg_get_expr(adbin, adrelid) from pg_attrdef where adrelid = 'x00r_cat'::regclass order by adnum;

\echo N5 generated-and-identity-columns
create table x00r_gen (a int not null, b int generated always as (a * 2) stored);
insert into x00r_gen (a) values (3);
select b from x00r_gen;  -- PostgreSQL: 6
create table x00r_ident (id int generated always as identity primary key, v text);
insert into x00r_ident (id, v) values (50, 'explicit');  -- PostgreSQL: ERROR 428C9

\echo N6 deferrable-fk
create table x00r_pa (id int primary key);
create table x00r_ch (id int primary key, pa int references x00r_pa (id) deferrable initially deferred);  -- PostgreSQL: CREATE TABLE

\echo N7 lock-and-cancel-surface
select pg_try_advisory_lock(4242);   -- PostgreSQL: t
select pg_advisory_unlock(4242);     -- PostgreSQL: t
set statement_timeout = '200ms';
select pg_sleep(1);                  -- PostgreSQL: ERROR 57014
reset statement_timeout;
begin;
lock table x00r_base in access share mode nowait;  -- PostgreSQL: LOCK TABLE
commit;

\echo N8 array-results
select '{1,2,3}'::int4[] as v, pg_typeof('{1,2,3}'::int4[]);  -- PostgreSQL: {1,2,3} | integer[] (array OID on the wire)

\echo N9 update-from-delete-using
create table x00r_u (id int primary key, name text);
create table x00r_p (id int primary key, u_id int, title text);
insert into x00r_u values (1, 'ann');
insert into x00r_p values (10, 1, 'a');
update x00r_p p set title = u.name from x00r_u u where u.id = p.u_id;  -- PostgreSQL: UPDATE 1
delete from x00r_p p using x00r_u u where u.id = p.u_id;              -- PostgreSQL: DELETE 1

\echo N10 jsonb_agg-missing
create table x00r_j (v int);
insert into x00r_j values (1), (2);
select jsonb_agg(v order by v desc)::text from x00r_j;  -- PostgreSQL: [2, 1]
select jsonb_build_object('a', 1)::text;             -- PostgreSQL: {"a": 1} (present on Nucleus too)

\echo N11 isolation-and-read-only
begin isolation level serializable;
select current_setting('transaction_isolation');  -- PostgreSQL: serializable
commit;
begin read only;
insert into x00r_u values (2, 'nope');  -- PostgreSQL: ERROR 25006
rollback;

\echo N13 derived-table-column-alias-list
select v from (values (1)) as t(v);  -- PostgreSQL: 1
select to_jsonb(1)::text as a, jsonb_build_object('k', 1)::text as b, coalesce(jsonb_agg(v), '[]'::jsonb)::text as c from (values (1)) as t(v);  -- PostgreSQL: 1 | {"k": 1} | [1] (the neutron-sql jsonb-functions capability probe, verbatim)

\echo cleanup
drop table if exists x00r_rb, x00r_base, x00r_cat, x00r_gen, x00r_ident, x00r_ch, x00r_pa, x00r_p, x00r_u, x00r_j;

-- N1: see upstream/N1-set-local-role.sql
