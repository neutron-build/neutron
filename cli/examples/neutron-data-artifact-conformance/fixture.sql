CREATE SCHEMA fixture;
CREATE TABLE fixture.scalars(row_id int4 PRIMARY KEY,i2 int2,i4 int4,i8 int8,amount numeric(40,18),flag bool,label text,short_label varchar(30),fixed_label bpchar(4),ident uuid,payload bytea);
CREATE TABLE fixture.writes (LIKE fixture.scalars INCLUDING ALL);
ALTER TABLE fixture.writes ADD COLUMN revision int8 NOT NULL CHECK(revision>0);
CREATE TABLE fixture.temporal(happened timestamptz NOT NULL);
INSERT INTO fixture.scalars VALUES
(1,-32768,-2147483648,-9223372036854775808,-9999999999999999999999.123456789012345678,true,'quotes '' Unicode λ','short','ABCD','00000000-0000-0000-0000-000000000001',decode('00017f80ff','hex')),
(2,32767,2147483647,9223372036854775807,9999999999999999999999.999999999999999999,false,'','', 'WXYZ','ffffffff-ffff-ffff-ffff-ffffffffffff',decode('','hex')),
(3,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO fixture.temporal VALUES ('2024-03-01T01:59:59.123456+02:00');
