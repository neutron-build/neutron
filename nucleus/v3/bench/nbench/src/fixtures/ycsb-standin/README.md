# The offline YCSB fixtures

Four fixtures in the parent directory were recorded without a server:

| Fixture | What it is |
|---|---|
| `ycsb-run-d.txt` | stock workload D, first pass on a freshly loaded table |
| `ycsb-run-d-failed.txt` | stock workload D, a second pass on the table the first pass had grown |
| `ycsb-run-e.txt` | stock workload E, first pass |
| `ycsb-run-f.txt` | stock workload F, first pass |

All four are the real output of YCSB Client 0.17.0 from the pinned
tarball (`core-0.17.0.jar`; tarball sha256 in
`systems/ycsbcli/Dockerfile`). The `-db StandinTable` on each file's
`Command line:` line is the difference from a row run: the client talked
to `StandinTable.java` in this directory, a file-backed key set whose
`insert` returns `ERROR` on a duplicate key — the status the JDBC
binding returns on PostgreSQL's unique violation — instead of to the
JDBC binding and PostgreSQL.

So the report *shapes* are the client's own — sections, `Return=` and
`-FAILED` lines — and the latencies are the stand-in's and mean nothing.
No fixture here is a measurement. `ycsb-run.txt` and `ycsb-load.txt`,
beside these, are the pinned image against PostgreSQL 17 and stay the
fixtures for anything numeric.

## Why `ycsb-run-d-failed.txt` exists

Every `ycsb run` process restarts the core workload's run-phase insert
key sequence at `recordcount`. A second pass of an insert-bearing
workload (stock D and E carry `insertproportion=0.05`) therefore
re-inserts the keys the first pass created; on the real row each insert
fails on `usertable_pkey`, YCSB still exits 0, and the throughput line
looks healthy (verdict NB-2, R2). The fixture reproduces that with the
real client so the driver's guard is tested on the real report lines.

## Recipe

Run on the host, with a JDK and the extracted pinned tarball at
`$YCSB`; `STANDIN_FILE` is the stand-in's key file, emptied to stand
for a fresh table.

```sh
javac -cp "$YCSB/lib/core-0.17.0.jar" -d classes StandinTable.java
CP="$YCSB/conf:$YCSB/lib/core-0.17.0.jar:$YCSB/lib/HdrHistogram-2.1.4.jar"
CP="$CP:$YCSB/lib/htrace-core4-4.1.0-incubating.jar"
CP="$CP:$YCSB/lib/jackson-core-asl-1.9.4.jar:$YCSB/lib/jackson-mapper-asl-1.9.4.jar:classes"
: > keys.txt
STANDIN_FILE=keys.txt java -cp "$CP" site.ycsb.Client -load -db StandinTable \
  -P "$YCSB/workloads/workloada" -p recordcount=1000 -p threadcount=1
STANDIN_FILE=keys.txt java -cp "$CP" site.ycsb.Client -t -db StandinTable -s \
  -P "$YCSB/workloads/workloadd" -p recordcount=1000 -p operationcount=1000 -p threadcount=1
# the first command above is `ycsb-run-d.txt`; run the second again,
# without emptying keys.txt, for `ycsb-run-d-failed.txt`. For E and F,
# empty keys.txt, load, then run with `workloade` / `workloadf`.
```

The java invocation is `bin/ycsb.sh`'s own (same classpath, same
`-t -db … -s` flags); the launcher is not used because it fixes `-db`
to a binding's class.
