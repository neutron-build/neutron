# Extended-protocol prepared statements through PgBouncer transaction pooling.
#
# Two psycopg connections interleave prepared executions so PgBouncer must
# swap them across its (size-2) server pool; with max_prepared_statements set,
# PgBouncer re-prepares statements on whichever server connection a client
# lands on. Verifies results stay correct across the swaps.
import sys

import psycopg

host, port = sys.argv[1], int(sys.argv[2])
dsn = f"host={host} port={port} dbname=nucleus user=nucleus"

c1 = psycopg.connect(dsn, autocommit=True, prepare_threshold=0)
c2 = psycopg.connect(dsn, autocommit=True, prepare_threshold=0)

c1.execute("DROP TABLE IF EXISTS pool_prep")
c1.execute("CREATE TABLE pool_prep (id INT PRIMARY KEY, v TEXT)")

for i in range(20):
    conn = c1 if i % 2 == 0 else c2
    conn.execute("INSERT INTO pool_prep VALUES (%s, %s)", (i, f"v{i}"))

for i in range(20):
    conn = c2 if i % 2 == 0 else c1
    row = conn.execute("SELECT v FROM pool_prep WHERE id = %s", (i,)).fetchone()
    assert row is not None and row[0] == f"v{i}", f"row {i}: {row}"

# Interactive transactions must pin a server connection until COMMIT.
with psycopg.connect(dsn, prepare_threshold=0) as c3:
    with c3.transaction():
        c3.execute("INSERT INTO pool_prep VALUES (100, 'txn')")
        mid = c2.execute("SELECT count(*) FROM pool_prep WHERE id = 100").fetchone()
        assert mid is not None and mid[0] == 0, "uncommitted row visible to other client"
    final = c2.execute("SELECT count(*) FROM pool_prep WHERE id = 100").fetchone()
    assert final is not None and final[0] == 1, "committed row not visible"

c1.close()
c2.close()
print("prepared/txn through transaction pooling: ok")
