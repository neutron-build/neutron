"""SQLAlchemy-on-Nucleus canonical flow: metadata.create_all -> CRUD ->
transaction commit + rollback -> reflection. psycopg v3 driver.

Exits non-zero on the first failed step; prints one line per step.
"""

import os
import sys

from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    func,
    inspect,
    select,
)

url = os.environ.get("DATABASE_URL")
if not url:
    print("DATABASE_URL not set", file=sys.stderr)
    sys.exit(2)


def step(name):
    print(f"[sqlalchemy] {name}", flush=True)


def fail(msg):
    print(f"[sqlalchemy] ASSERT FAILED: {msg}", file=sys.stderr)
    sys.exit(1)


engine = create_engine(url, poolclass=None)

metadata = MetaData()

users = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("email", String, nullable=False, unique=True),
    Column("name", String, nullable=False),
    Column("age", Integer, nullable=False),
)

posts = Table(
    "posts",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("user_id", Integer, ForeignKey("users.id"), nullable=False),
    Column("title", String, nullable=False),
    Column("published", Boolean, server_default="false"),
)

step("create_all (DDL)")
metadata.create_all(engine)

step("insert")
with engine.begin() as conn:
    conn.execute(
        users.insert(),
        [
            {"id": 1, "email": "ada@example.com", "name": "Ada", "age": 36},
            {"id": 2, "email": "grace@example.com", "name": "Grace", "age": 45},
        ],
    )

step("select where")
with engine.connect() as conn:
    row = conn.execute(
        select(users).where(users.c.email == "ada@example.com")
    ).one_or_none()
    if row is None or row.name != "Ada":
        fail("select-by-email")

step("update")
with engine.begin() as conn:
    conn.execute(users.update().where(users.c.id == 1).values(age=37))
with engine.connect() as conn:
    age = conn.execute(select(users.c.age).where(users.c.id == 1)).scalar_one()
    if age != 37:
        fail(f"update visible (got {age})")

step("transaction commit")
with engine.begin() as conn:
    conn.execute(posts.insert(), {"id": 1, "user_id": 1, "title": "Hello"})
    conn.execute(posts.insert(), {"id": 2, "user_id": 2, "title": "Compat"})
with engine.connect() as conn:
    n = conn.execute(select(func.count()).select_from(posts)).scalar_one()
    if n != 2:
        fail(f"committed txn rows (got {n})")

step("transaction rollback")
try:
    with engine.begin() as conn:
        conn.execute(posts.insert(), {"id": 99, "user_id": 1, "title": "doomed"})
        raise RuntimeError("force rollback")
except RuntimeError:
    pass
with engine.connect() as conn:
    doomed = conn.execute(select(posts).where(posts.c.id == 99)).fetchall()
    if doomed:
        fail("rolled-back row visible")

step("delete")
with engine.begin() as conn:
    conn.execute(users.insert(), {"id": 3, "email": "tmp@example.com", "name": "Tmp", "age": 1})
    conn.execute(users.delete().where(users.c.id == 3))
with engine.connect() as conn:
    gone = conn.execute(select(users).where(users.c.id == 3)).fetchall()
    if gone:
        fail("deleted row visible")

step("reflection (inspector)")
insp = inspect(engine)
names = set(insp.get_table_names())
if not {"users", "posts"} <= names:
    fail(f"reflected table names missing (got {sorted(names)})")
cols = {c["name"] for c in insp.get_columns("users")}
if not {"id", "email", "name", "age"} <= cols:
    fail(f"reflected columns missing (got {sorted(cols)})")

step("reflection (Table autoload)")
reflected = Table("users", MetaData(), autoload_with=engine)
if "email" not in reflected.c:
    fail("autoload_with lost the email column")

print("[sqlalchemy] ALL STEPS PASSED")
