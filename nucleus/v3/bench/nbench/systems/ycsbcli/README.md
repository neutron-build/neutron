# YCSB 0.17.0 client container

Client-side load generator for row `oltp-ycsb-pg17` (card NB-2: "YCSB
A–F over pgwire" against PostgreSQL 17 through the JDBC binding — the
row C-9's OLTP row later reuses against Nucleus, because both speak
pgwire).

Built locally (`nbench-ycsbcli:local`) from the digest-pinned
**eclipse-temurin 17-jre-jammy** base. The YCSB 0.17.0 release tarball
(GitHub releases) and the PostgreSQL JDBC driver 42.7.6 jar (Maven
Central) are fetched **at image build time** and sha256-verified — see
the Dockerfile for the hash-provenance caveat: YCSB publishes no
upstream hash for the tarball and Maven Central publishes sha1, so the
recorded sha256s are pin-time pins of the fetched bits (the image-digest
precedent), not upstream attestations.

The PostgreSQL driver jar is placed in `jdbc-binding/lib/` — the exact
location the binding's own README documents for driver jars. The
launcher the row uses is `bin/ycsb.sh` (POSIX sh, same tarball): the
`bin/ycsb` launcher is Python 2 (`print >>` syntax, verified from the
0.17.0 tarball), which no current base image carries; `ycsb.sh` builds
the same classpath (`conf`, `lib/*.jar`, `jdbc-binding/conf`,
`jdbc-binding/lib/*.jar`) and the same `java` invocation.

The YCSB tarball ships every binding (~700 MB), unmodified; the row uses
the stock `workloads/workloada`–`workloadf` files unmodified (card NB-2
Open point: harness-owned copies would be a follow-up, not an edit
here).

Licence: YCSB is Apache-2.0, the PostgreSQL JDBC driver BSD-2-Clause,
the Temurin JRE GPL-2.0 with Classpath Exception — unmodified standalone
tools, not dependencies; see `../../LICENSES.md`.
