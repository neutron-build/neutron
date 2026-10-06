"""Authored HTTP and native scenario, run unchanged against the original and converted app.

Copied into both prepared trees as backend/tests/neutron_corpus/test_neutron_scenario.py.
It uses only the template's own fixtures (client, superuser_token_headers) and an
independent psycopg connection for database truth, never the application's data layer, so
the same bytes execute against SQLModel and against the Neutron ORM. Every HTTP exchange is
recorded with ids, timestamps, tokens and emails normalized; the two transcripts must be
identical. The database URL is read from the DATABASE_URL environment variable by name and
is never printed. NEUTRON_PY_APP_TRANSCRIPT, when set, names the file the transcript is
written to.
"""
import json
import os
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg
from fastapi.testclient import TestClient
from pwdlib.hashers.bcrypt import BcryptHasher

from app.core.config import settings

API = settings.API_V1_STR
UUID_TEXT = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
DATETIME_TEXT = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?")
USER_COLUMNS = "id, email, is_active, is_superuser, full_name, hashed_password, created_at"
ITEM_COLUMNS = "id, title, description, owner_id, created_at"


def native(sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    dsn = os.environ["DATABASE_URL"].replace("postgresql+psycopg://", "postgresql://", 1)
    with psycopg.connect(dsn) as connection:
        return connection.execute(sql, params).fetchall()


def native_write(sql: str, params: tuple[Any, ...]) -> None:
    dsn = os.environ["DATABASE_URL"].replace("postgresql+psycopg://", "postgresql://", 1)
    with psycopg.connect(dsn) as connection:
        connection.execute(sql, params)


class Recorder:
    def __init__(self) -> None:
        self.entries: list[dict[str, Any]] = []
        self.known: dict[str, str] = {}
        self.seen: dict[str, str] = {}

    def register(self, placeholder: str, value: str) -> None:
        self.known[value] = placeholder

    def normalize(self, text: str) -> str:
        for value, placeholder in sorted(self.known.items(), key=lambda pair: -len(pair[0])):
            text = text.replace(value, placeholder)
        text = DATETIME_TEXT.sub("<datetime>", text)
        return UUID_TEXT.sub(lambda match: self.seen.setdefault(match.group(0), f"<uuid:{len(self.seen)}>"), text)

    def step(self, label: str, response: Any) -> Any:
        try:
            payload: Any = response.json()
        except ValueError:
            payload = response.text
        if isinstance(payload, dict) and "access_token" in payload:
            payload = {**payload, "access_token": "<token>"}
        body = self.normalize(json.dumps(payload, sort_keys=True))
        self.entries.append({"label": label, "status": response.status_code, "body": body})
        return response

    def note(self, label: str, value: Any) -> None:
        self.entries.append({"label": label, "status": None, "body": json.dumps(value, sort_keys=True)})


def login(client: TestClient, rec: Recorder, label: str, email: str, password: str) -> Any:
    return rec.step(label, client.post(f"{API}/login/access-token", data={"username": email, "password": password}))


def headers_for(response: Any) -> dict[str, str]:
    assert response.status_code == 200
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


def user_row(email: str) -> tuple[Any, ...]:
    rows = native(f'SELECT {USER_COLUMNS} FROM "user" WHERE email = %s', (email,))
    assert len(rows) == 1
    return rows[0]


def item_rows(owner_id: Any) -> list[tuple[Any, ...]]:
    return native(f"SELECT {ITEM_COLUMNS} FROM item WHERE owner_id = %s ORDER BY id", (owner_id,))


def test_scenario(client: TestClient, superuser_token_headers: dict[str, str]) -> None:
    admin = superuser_token_headers
    run = uuid.uuid4().hex[:12]
    rec = Recorder()
    email_a, email_b, email_b2, email_c = (f"scenario-{name}-{run}@example.com" for name in ("a", "b", "b2", "c"))
    password_a, password_b, password_b2, password_c = (f"pw-{name}-{run}" for name in ("a", "b", "b2", "c"))
    for placeholder, value in (("<email:a>", email_a), ("<email:b>", email_b), ("<email:b2>", email_b2), ("<email:c>", email_c),
                               ("<pw:a>", password_a), ("<pw:b>", password_b), ("<pw:b2>", password_b2), ("<pw:c>", password_c)):
        rec.register(placeholder, value)

    # A superuser creates user A: native row, argon2 hash and exact created_at instant.
    created = rec.step("create A", client.post(f"{API}/users/", headers=admin, json={"email": email_a, "password": password_a, "full_name": "Scenario A"}))
    assert created.status_code == 200
    id_a, _, active, superuser, full_name, hashed, created_at = user_row(email_a)
    assert str(id_a) == created.json()["id"] and active is True and superuser is False and full_name == "Scenario A"
    assert hashed.startswith("$argon2") and created_at is not None
    assert created_at == datetime.fromisoformat(created.json()["created_at"])
    exists = rec.step("create A again", client.post(f"{API}/users/", headers=admin, json={"email": email_a, "password": password_a}))
    assert exists.status_code == 400
    assert len(native('SELECT id FROM "user" WHERE email = %s', (email_a,))) == 1

    headers_a = headers_for(login(client, rec, "login A", email_a, password_a))
    assert rec.step("me A", client.get(f"{API}/users/me", headers=headers_a)).status_code == 200
    rec.step("login wrong password", client.post(f"{API}/login/access-token", data={"username": email_a, "password": "wrong-password"}))
    rec.step("login unknown email", client.post(f"{API}/login/access-token", data={"username": f"nobody-{run}@example.com", "password": "wrong-password"}))

    # Three items for A: native ownership, ids and strictly increasing created_at.
    ids = []
    for title, description in (("scenario-1", "first"), ("scenario-2", None), ("scenario-3", "third")):
        body = {"title": title} if description is None else {"title": title, "description": description}
        item = rec.step(f"create {title}", client.post(f"{API}/items/", headers=headers_a, json=body))
        assert item.status_code == 200
        ids.append(item.json()["id"])
    rows = native(f"SELECT {ITEM_COLUMNS} FROM item WHERE owner_id = %s ORDER BY created_at", (id_a,))
    assert [row[1] for row in rows] == ["scenario-1", "scenario-2", "scenario-3"]
    assert [str(row[0]) for row in rows] == ids and [row[2] for row in rows] == ["first", None, "third"]
    assert all(row[3] == id_a for row in rows) and rows[0][4] < rows[1][4] < rows[2][4]

    # Owner-scoped listing: newest first, pagination, exact total count.
    for label, query in (("page 1", "skip=0&limit=2"), ("page 2", "skip=2&limit=2"), ("page past end", "skip=3&limit=2")):
        listing = rec.step(f"items {label}", client.get(f"{API}/items/?{query}", headers=headers_a))
        assert listing.status_code == 200 and listing.json()["count"] == 3
    everything = client.get(f"{API}/items/?limit=1000", headers=admin).json()
    assert everything["count"] == native("SELECT count(*) FROM item")[0][0] and len(everything["data"]) == min(everything["count"], 1000)
    rec.note("superuser listing count equals native count", True)

    # User B signs up; B cannot read, change or delete A's items, and nothing changes.
    signup = rec.step("signup B", client.post(f"{API}/users/signup", json={"email": email_b, "password": password_b, "full_name": "Scenario B"}))
    assert signup.status_code == 200
    rec.step("signup B again", client.post(f"{API}/users/signup", json={"email": email_b, "password": password_b}))
    headers_b = headers_for(login(client, rec, "login B", email_b, password_b))
    id_b = user_row(email_b)[0]
    before = item_rows(id_a)
    assert rec.step("B items", client.get(f"{API}/items/", headers=headers_b)).json()["count"] == 0
    for label, call in (("get", client.get), ("put", lambda url, **kw: client.put(url, json={"title": "stolen"}, **kw)), ("delete", client.delete)):
        denied = rec.step(f"B {label} A item", call(f"{API}/items/{ids[1]}", headers=headers_b))
        assert denied.status_code == 403
    assert item_rows(id_a) == before

    # Partial updates: omitted fields stay, explicit null clears, empty body is a no-op.
    for label, body in (("description", {"description": "added"}), ("null description", {"description": None}), ("empty", {}), ("title", {"title": "renamed"})):
        updated = rec.step(f"update {label}", client.put(f"{API}/items/{ids[1]}", headers=headers_a, json=body))
        assert updated.status_code == 200
        row = native(f"SELECT {ITEM_COLUMNS} FROM item WHERE id = %s", (ids[1],))[0]
        assert [row[1], row[2]] == [updated.json()["title"], updated.json()["description"]]
    assert native("SELECT title, description FROM item WHERE id = %s", (ids[1],)) == [("renamed", None)]
    missing = str(uuid.uuid4())
    for label, call in (("get", client.get), ("put", lambda url, **kw: client.put(url, json={"title": "x"}, **kw)), ("delete", client.delete)):
        assert rec.step(f"{label} missing item", call(f"{API}/items/{missing}", headers=headers_a)).status_code == 404
    rec.step("get malformed item id", client.get(f"{API}/items/not-a-uuid", headers=headers_a))
    deleted = rec.step("delete item", client.delete(f"{API}/items/{ids[0]}", headers=headers_a))
    assert deleted.status_code == 200 and native("SELECT count(*) FROM item WHERE id = %s", (ids[0],)) == [(0,)]
    assert rec.step("get deleted item", client.get(f"{API}/items/{ids[0]}", headers=headers_a)).status_code == 404

    # Own profile: explicit null, email conflict and change, old email stops working.
    assert rec.step("B null name", client.patch(f"{API}/users/me", headers=headers_b, json={"full_name": None})).status_code == 200
    assert native('SELECT full_name FROM "user" WHERE id = %s', (id_b,)) == [(None,)]
    assert rec.step("B email conflict", client.patch(f"{API}/users/me", headers=headers_b, json={"email": email_a})).status_code == 409
    assert rec.step("B new email", client.patch(f"{API}/users/me", headers=headers_b, json={"email": email_b2, "full_name": "Scenario B2"})).status_code == 200
    assert native('SELECT email, full_name FROM "user" WHERE id = %s', (id_b,)) == [(email_b2, "Scenario B2")]
    assert login(client, rec, "login old B email", email_b, password_b).status_code == 400
    assert login(client, rec, "login new B email", email_b2, password_b).status_code == 200

    # Password changes: wrong, identical, success; the stored hash changes and verifies by login.
    hash_before = user_row(email_b2)[5]
    for label, body, status in (("wrong current", {"current_password": "not-the-password", "new_password": password_b2}, 400),
                                ("same password", {"current_password": password_b, "new_password": password_b}, 400),
                                ("success", {"current_password": password_b, "new_password": password_b2}, 200)):
        assert rec.step(f"B password {label}", client.patch(f"{API}/users/me/password", headers=headers_b, json=body)).status_code == status
    hash_after = user_row(email_b2)[5]
    assert hash_after != hash_before and hash_after.startswith("$argon2")
    assert login(client, rec, "login B old password", email_b2, password_b).status_code == 400
    assert login(client, rec, "login B new password", email_b2, password_b2).status_code == 200

    # Superuser management: listing, privileges, deactivation, conflicts, password set.
    assert rec.step("B lists users", client.get(f"{API}/users/", headers=headers_b)).status_code == 403
    listing = client.get(f"{API}/users/?limit=1000", headers=admin).json()
    assert listing["count"] == native('SELECT count(*) FROM "user"')[0][0]
    stamps = [datetime.fromisoformat(entry["created_at"]) for entry in listing["data"]]
    assert stamps == sorted(stamps, reverse=True)
    rec.note("superuser user listing count equals native count and is newest first", True)
    for label, headers, target, status in (("B reads A", headers_b, id_a, 403), ("admin reads A", admin, id_a, 200), ("A reads A", headers_a, id_a, 200),
                                           ("admin reads unknown", admin, uuid.uuid4(), 404), ("B reads unknown", headers_b, uuid.uuid4(), 403)):
        assert rec.step(label, client.get(f"{API}/users/{target}", headers=headers)).status_code == status
    assert rec.step("admin deactivates B", client.patch(f"{API}/users/{id_b}", headers=admin, json={"is_active": False})).status_code == 200
    assert login(client, rec, "login inactive B", email_b2, password_b2).status_code == 400
    assert rec.step("inactive B token", client.get(f"{API}/users/me", headers=headers_b)).status_code == 400
    assert rec.step("admin reactivates B", client.patch(f"{API}/users/{id_b}", headers=admin, json={"is_active": True})).status_code == 200
    assert rec.step("admin email conflict", client.patch(f"{API}/users/{id_b}", headers=admin, json={"email": email_a})).status_code == 409
    assert rec.step("admin update unknown", client.patch(f"{API}/users/{uuid.uuid4()}", headers=admin, json={"full_name": "x"})).status_code == 404
    assert rec.step("admin sets B password", client.patch(f"{API}/users/{id_b}", headers=admin, json={"password": password_b})).status_code == 200
    assert login(client, rec, "login B admin-set password", email_b2, password_b).status_code == 200
    assert rec.step("B cannot create user", client.post(f"{API}/users/", headers=headers_b, json={"email": email_c, "password": password_c})).status_code == 403

    # A bcrypt hash written natively is upgraded to argon2 on first login and then kept.
    legacy = BcryptHasher().hash(password_c)
    id_c = uuid.uuid4()
    native_write('INSERT INTO "user" (id, email, is_active, is_superuser, full_name, hashed_password, created_at) VALUES (%s, %s, true, false, NULL, %s, now())',
                 (id_c, email_c, legacy))
    assert login(client, rec, "login C bcrypt", email_c, password_c).status_code == 200
    upgraded = user_row(email_c)[5]
    assert upgraded.startswith("$argon2") and upgraded != legacy
    assert login(client, rec, "login C argon2", email_c, password_c).status_code == 200
    assert user_row(email_c)[5] == upgraded

    # Credentials: malformed token, and tokens of deleted users.
    assert rec.step("malformed token", client.get(f"{API}/users/me", headers={"Authorization": "Bearer not.a.token"})).status_code == 403
    headers_c = headers_for(login(client, rec, "login C", email_c, password_c))

    # Deleting a user removes their items; superusers cannot delete themselves.
    extra = client.post(f"{API}/items/", headers=headers_b, json={"title": "b-item"})
    assert extra.status_code == 200 and len(item_rows(id_b)) == 1
    assert rec.step("admin deletes A", client.delete(f"{API}/users/{id_a}", headers=admin)).status_code == 200
    assert native('SELECT count(*) FROM "user" WHERE id = %s', (id_a,)) == [(0,)] and item_rows(id_a) == []
    assert rec.step("deleted A token", client.get(f"{API}/users/me", headers=headers_a)).status_code == 404
    assert rec.step("B deletes self", client.delete(f"{API}/users/me", headers=headers_b)).status_code == 200
    assert native('SELECT count(*) FROM "user" WHERE id = %s', (id_b,)) == [(0,)] and item_rows(id_b) == []
    assert rec.step("C deletes self", client.delete(f"{API}/users/me", headers=headers_c)).status_code == 200
    admin_id = native('SELECT id FROM "user" WHERE email = %s', (settings.FIRST_SUPERUSER,))[0][0]
    assert rec.step("admin deletes self by id", client.delete(f"{API}/users/{admin_id}", headers=admin)).status_code == 403
    assert rec.step("admin deletes me", client.delete(f"{API}/users/me", headers=admin)).status_code == 403
    assert rec.step("admin deletes unknown", client.delete(f"{API}/users/{uuid.uuid4()}", headers=admin)).status_code == 404
    assert native('SELECT count(*) FROM "user" WHERE id = %s', (admin_id,)) == [(1,)]

    target = os.environ.get("NEUTRON_PY_APP_TRANSCRIPT")
    if target:
        Path(target).write_text(json.dumps(rec.entries, indent=1, sort_keys=True) + "\n")
