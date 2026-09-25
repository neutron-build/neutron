-- X02-E2 — a rolled-back CREATE TABLE leaves its catalog entry without storage
--
-- Refines N2 (DDL is not transactional; ORM_CONFORMANCE.md). Run on a
-- DISPOSABLE engine, one session, autocommit:
--   psql "postgresql://postgres@127.0.0.1:<port>/postgres" -X -At -f X02-E2-rolled-back-create-table-splits-catalog.sql
-- Oracle: the same file against PostgreSQL 17 — step 2 errors with 42P01,
-- step 3 and 4 return 0, step 5 succeeds.
-- Observed on Nucleus 1.0.2 (nucleus/ tree 3313729a): step 2 errors with
-- "table 'x02e2_t' not found in storage", steps 3 and 4 return 1, step 5
-- errors with "table 'x02e2_t' already exists". The relation is visible to
-- the catalog, unusable for reads, and cannot be re-created until DROP TABLE.
-- This explains why X00 (catalog probe: table survives ROLLBACK) and the
-- first X02 attempt (SELECT probe: table gone) recorded opposite outcomes on
-- the same build: both halves are true at once.

\echo 1 create + insert inside a rolled-back transaction
begin;
create table x02e2_t (id int);
insert into x02e2_t values (1);
rollback;

\echo 2 read the table: PostgreSQL 42P01
select count(*) from x02e2_t;

\echo 3 pg_class: PostgreSQL 0
select count(*) from pg_class c join pg_namespace n on n.oid = c.relnamespace
 where c.relname = 'x02e2_t' and n.nspname = current_schema();

\echo 4 information_schema.tables: PostgreSQL 0
select count(*) from information_schema.tables where table_name = 'x02e2_t';

\echo 5 re-create: PostgreSQL succeeds
create table x02e2_t (id int);

\echo 6 cleanup (works on both)
drop table x02e2_t;
