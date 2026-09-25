-- N1 — SET LOCAL ROLE is not transaction-scoped (security-relevant)
--
-- Run on a DISPOSABLE engine as a superuser login, one session, autocommit:
--   psql "postgresql://postgres@127.0.0.1:<port>/postgres" -X -At -f N1-set-local-role.sql
-- Oracle: the same file against PostgreSQL 17. Expected/observed output is in
-- ORM_CONFORMANCE.md (N1). Probe: rls.set_local_role_transaction_local.
--
-- The session is left in whatever role the engine ends on; on the defective
-- engine drop the role from a NEW session: drop role x00n1_app;

\echo 1 login role
select current_user;

create role x00n1_app nologin;

\echo 2 SET LOCAL ROLE, then COMMIT: role must revert to the login role
begin;
set local role x00n1_app;
select current_user;
commit;
select current_user;

\echo 3 RESET ROLE must restore the login role
reset role;
select current_user;

\echo 4 SET ROLE NONE must restore the login role
set role none;
select current_user;

\echo 5 SET LOCAL ROLE, then ROLLBACK: role must revert
begin;
set local role x00n1_app;
rollback;
select current_user;
set role none;

\echo 6 session SET ROLE inside a transaction, then ROLLBACK: role must revert
begin;
set role x00n1_app;
rollback;
select current_user;
set role none;

\echo 7 control: SET LOCAL of an ordinary setting reverts at COMMIT
show search_path;
begin;
set local search_path = x00n1_elsewhere;
commit;
show search_path;

\echo 8 cleanup
set role none;
drop role x00n1_app;
