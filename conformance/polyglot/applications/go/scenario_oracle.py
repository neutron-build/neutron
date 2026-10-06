#!/usr/bin/env python3
"""Independent reference evaluation of the shared data-scope scenario.

This reads shared/neutron_corpus_scenario.json and computes, in pure Python and
without a database, what the original go-admin SQL semantics select. The
semantics are transcribed from the frozen upstream sources (see README.md), not
from the converted implementation:

* go-admin-core sdk/contract/actions/permission.go Permission() raw SQL
* go-admin-core tools/search/query.go (postgres branch) for contains/exact/order
* go-admin-core sdk/contract/dto/pagination.go and dto.Paginate
* GORM zero-live soft delete (deleted_at = 0) on the outer model only

Its output is a reference expectation for the scope-matrix transcript. A
mismatch with the original native transcript means the oracle or the original
needs review; it is never an automatic claim about either side.
"""
import argparse
import functools
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SCENARIO = ROOT / "shared" / "neutron_corpus_scenario.json"


def like(pattern, text):
    if text is None:
        return False
    out = []
    i = 0
    while i < len(pattern):
        c = pattern[i]
        if c == "\\" and i + 1 < len(pattern):
            out.append(re.escape(pattern[i + 1]))
            i += 2
            continue
        out.append(".*" if c == "%" else "." if c == "_" else re.escape(c))
        i += 1
    return re.fullmatch("".join(out), text, re.DOTALL) is not None


def rows(scenario, table):
    for block in scenario["seed"]:
        if block["table"] == table:
            return [dict(zip(block["columns"], row)) for row in block["rows"]]
    raise ValueError("missing seed table " + table)


def scope_predicate(users, depts, role_depts, enabled, permission):
    """Return a function(user) -> bool for the permission scope."""
    if not enabled:
        return lambda user: True
    scope = permission["scope"]
    if scope == "1":
        return lambda user: True
    if scope == "2":
        dept_ids = {rd["dept_id"] for rd in role_depts if rd["role_id"] == permission["roleId"]}
        creators = {u["user_id"] for u in users if u.get("dept_id") in dept_ids}
    elif scope == "3":
        if permission["deptId"] <= 0:
            return lambda user: False
        creators = {u["user_id"] for u in users if u.get("dept_id") == permission["deptId"]}
    elif scope == "4":
        if permission["deptId"] <= 0:
            return lambda user: False
        pattern = "%/" + str(permission["deptId"]) + "/%"
        dept_ids = {d["dept_id"] for d in depts if like(pattern, d.get("dept_path"))}
        creators = {u["user_id"] for u in users if u.get("dept_id") in dept_ids}
    elif scope == "5":
        return lambda user: user.get("create_by") == permission["userId"]
    else:
        return lambda user: False
    return lambda user: user.get("create_by") in creators


def search_predicates(search, depts):
    checks = []
    if search.get("userId"):
        checks.append(lambda u, v=search["userId"]: u["user_id"] == v)
    for key, column in (("username", "username"), ("nickName", "nick_name"), ("phone", "phone"), ("email", "email")):
        if search.get(key):
            checks.append(lambda u, c=column, v=search[key]: like("%" + v + "%", u.get(c)))
    for key, column in (("sex", "sex"), ("status", "status")):
        if search.get(key):
            checks.append(lambda u, c=column, v=search[key]: u.get(c) == v)
    for key, column in (("roleId", "role_id"), ("postId", "post_id")):
        if search.get(key):
            checks.append(lambda u, c=column, v=int(search[key]): u.get(c) == v)
    if search.get("deptId"):
        by_id = {d["dept_id"]: d for d in depts}

        def in_matching_dept(user, value=search["deptId"]):
            dept = by_id.get(user.get("dept_id"))
            return dept is not None and like("%" + value + "%", dept.get("dept_path"))

        checks.append(in_matching_dept)
    return checks


def order_keys(search):
    keys = []
    for key, column in (("userIdOrder", "user_id"), ("usernameOrder", "username"), ("statusOrder", "status"), ("createdAtOrder", "created_at")):
        direction = str(search.get(key, "")).lower()
        if direction in ("asc", "desc"):
            keys.append((column, direction == "desc"))
    return keys


def sort_users(page, keys):
    # PostgreSQL: ASC puts NULL last, DESC puts NULL first.
    def compare(left, right):
        for column, descending in keys:
            a, b = left.get(column), right.get(column)
            if a == b:
                continue
            if a is None:
                return -1 if descending else 1
            if b is None:
                return 1 if descending else -1
            result = -1 if a < b else 1
            return -result if descending else result
        return 0

    return sorted(page, key=functools.cmp_to_key(compare))


def evaluate(scenario):
    users = rows(scenario, "sys_user")
    depts = rows(scenario, "sys_dept")
    role_depts = rows(scenario, "sys_role_dept")
    dept_by_id = {d["dept_id"]: d for d in depts}
    observations = []
    for query in scenario["queries"]:
        search = query["search"]
        keys = order_keys(search)
        if not any(column in ("user_id", "username") for column, _ in keys):
            raise ValueError("scenario query %s has no unique order key" % query["name"])
        scope = scope_predicate(users, depts, role_depts, query["enableDataPermission"], query["permission"])
        checks = search_predicates(search, depts)
        selected = [u for u in users if u.get("deleted_at") == 0 and scope(u) and all(check(u) for check in checks)]
        selected = sort_users(selected, keys)
        size = search.get("pageSize", 0)
        index = search.get("pageIndex", 0)
        size = 10 if size <= 0 else size
        index = 1 if index <= 0 else index
        offset = max((index - 1) * size, 0)
        page = selected[offset:offset + size]
        dept_ids = []
        for user in page:
            dept = dept_by_id.get(user.get("dept_id"))
            dept_ids.append(dept["dept_id"] if dept is not None and dept.get("deleted_at") == 0 else 0)
        observations.append({"name": query["name"], "count": len(selected), "ids": [u["user_id"] for u in page], "deptIds": dept_ids})
    return {"observations": observations}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", type=Path, default=SCENARIO)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = evaluate(json.loads(args.scenario.read_text()))
    text = json.dumps(result, indent=1) + "\n"
    if args.output:
        args.output.write_text(text)
    else:
        print(text, end="")


if __name__ == "__main__":
    main()
