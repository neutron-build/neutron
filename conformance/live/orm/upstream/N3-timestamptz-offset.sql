-- N3 — timestamptz input ignores the explicit UTC offset; output ignores the
--      session TimeZone
--
-- Run on a DISPOSABLE engine, one session, autocommit:
--   psql "postgresql://postgres@127.0.0.1:<port>/postgres" -X -At -f N3-timestamptz-offset.sql
-- Oracle: the same file against PostgreSQL 17. Expected/observed output is in
-- ORM_CONFORMANCE.md (N3). Probes: codec.timestamptz_utc,
-- codec.timestamptz_session_timezone.

set time zone 'UTC';

\echo 1 cast path: +02 input must normalise to 01:04:05 UTC
select '2026-01-02 03:04:05.123456+02'::timestamptz::text;

\echo 2 two spellings of one instant must compare equal
select '2026-01-02 03:04:05+02'::timestamptz = '2026-01-02 01:04:05+00'::timestamptz;

\echo 3 epoch of the +02 instant (1767315845 = 2026-01-02 01:04:05 UTC)
select extract(epoch from '2026-01-02 03:04:05+02'::timestamptz)::bigint;

\echo 4 INSERT path: +02 input into a timestamptz column
create table x00n3_t (id int primary key, v timestamptz);
insert into x00n3_t values (1, '2026-01-02 03:04:05+02');
select v::text from x00n3_t;

\echo 5 SET TIME ZONE must change the session zone
set time zone 'Asia/Tokyo';
show timezone;

\echo 6 output must follow the session TimeZone (same stored instant)
select v::text from x00n3_t;

\echo 7 same, with the zone set through SET timezone = (the form the engine stores)
set timezone = 'Asia/Tokyo';
show timezone;
select v::text from x00n3_t;

\echo 8 offset input under a non-UTC session zone: +00 must mean UTC, not local
select extract(epoch from '2026-01-02 00:00:00+00'::timestamptz)::bigint;  -- 1767312000

\echo 9 cleanup
set timezone = 'UTC';
drop table x00n3_t;
