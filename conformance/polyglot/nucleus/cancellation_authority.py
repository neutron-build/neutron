#!/usr/bin/env python3
"""Bounded native authority facts; no workloads, timing, or parity certification.

Run against an isolated PostgreSQL 17 or candidate Nucleus endpoint. Credentials
are read from an environment variable and are never included in the report.
Nucleus deliberately lacks pg_signal_backend membership authority in this finite
profile; PostgreSQL membership controls expose that restriction explicitly.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import secrets
import sys

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def plain_role(name: str) -> sql.SQL:
    # Nucleus keeps identifier quotes in a role name created with a quoted
    # identifier, so a login by the bare name cannot find it. The generated
    # names are plain lowercase identifiers, so emit them unquoted (valid and
    # identical on PostgreSQL) and refuse anything else.
    if re.fullmatch(r"[a-z_][a-z0-9_]*", name) is None:
        raise ValueError("generated role name is not a plain identifier")
    return sql.SQL(name)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=("postgres", "nucleus"), required=True)
    parser.add_argument("--admin-url-env", required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--binary-sha256")
    args = parser.parse_args()
    if args.engine == "nucleus" and (
        not args.binary_sha256 or len(args.binary_sha256) != 64
        or any(c not in "0123456789abcdef" for c in args.binary_sha256)
    ):
        parser.error("Nucleus requires the exact executed binary SHA256")
    config = conninfo_to_dict(os.environ[args.admin_url_env])
    config["connect_timeout"] = "10"
    prefix = "np02_" + secrets.token_hex(6)
    roles = {key: prefix + "_" + key for key in ("owner", "other", "super")}
    password = secrets.token_urlsafe(32)
    connections: list[psycopg.Connection] = []
    created: list[str] = []
    facts: dict[str, object] = {}
    report: dict[str, object] = {
        "status": "fail", "engine": args.engine, "profileEnabled": False,
        "scope": "native backend identity and finite cancellation authority only",
        "binarySha256": args.binary_sha256,
        "probeSha256": digest(Path(__file__)),
        "pythonSha256": digest(Path(sys.executable).resolve()),
        "psycopgVersion": psycopg.__version__,
        "endpoint": {key: config[key] for key in ("host", "port", "dbname") if key in config},
        "facts": facts,
        "restriction": "Nucleus does not implement pg_signal_backend or inherited-role cancellation authority",
    }
    admin = psycopg.connect(make_conninfo(**config), autocommit=True)

    def connect(role: str) -> psycopg.Connection:
        params = {key: value for key, value in config.items() if key not in ("service", "passfile")}
        params.update(user=roles[role], password=password)
        connection = psycopg.connect(make_conninfo(**params), autocommit=True)
        connections.append(connection)
        return connection

    def cancel(connection: psycopg.Connection, pid: int) -> bool:
        return connection.execute("SELECT pg_cancel_backend(%s)", (pid,)).fetchone()[0]

    def idle_reuse(connection: psycopg.Connection) -> None:
        # PostgreSQL signals asynchronously: a signal sent while idle can land
        # just as the next query begins. Record that finite delivery race and
        # require the immediately following command to succeed. No timing claim.
        try:
            row = connection.execute("SELECT 1").fetchone()
        except psycopg.Error as error:
            assert error.sqlstate == "57014", "unexpected idle delivery error"
            facts["idleDeliveryRaceCount"] = int(facts.get("idleDeliveryRaceCount", 0)) + 1
            row = connection.execute("SELECT 1").fetchone()
        assert row[0] == 1

    def denied(connection: psycopg.Connection, pid: int) -> None:
        try:
            cancel(connection, pid)
        except psycopg.Error as error:
            assert error.sqlstate == "42501", "authority denial must be SQLSTATE 42501"
        else:
            raise AssertionError("cross-role cancellation unexpectedly succeeded")

    try:
        facts["serverVersion"] = admin.execute("SELECT version()").fetchone()[0]
        for key, role in roles.items():
            attributes = sql.SQL(" SUPERUSER") if key == "super" else (
                sql.SQL(" BYPASSRLS") if key == "other" else sql.SQL("")
            )
            admin.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}{}").format(
                plain_role(role), sql.Literal(password), attributes
            ))
            created.append(role)
        target, peer, other, privileged = connect("owner"), connect("owner"), connect("other"), connect("super")
        pid = target.info.backend_pid
        assert pid > 0 and pid != peer.info.backend_pid
        assert target.execute("SELECT pg_backend_pid()").fetchone()[0] == pid
        assert peer.execute("SELECT pg_backend_pid()").fetchone()[0] == peer.info.backend_pid
        facts["sqlMatchesBackendKeyDataForSameRolePeers"] = True
        assert cancel(peer, pid) is True
        idle_reuse(target)
        assert cancel(peer, pid) is True
        idle_reuse(target)
        facts["sameRoleAndRepeatedSignal"] = True
        denied(other, pid)
        idle_reuse(target)
        facts["bypassRlsDoesNotAuthorizeOtherRole"] = True
        denied(peer, privileged.info.backend_pid)
        facts["nonSuperCannotCancelSuperTarget"] = True
        assert cancel(privileged, pid) is True
        idle_reuse(target)
        facts["superCanCancelOtherRole"] = True
        # Self-cancellation must fence the actual wire command, not return rows.
        try:
            peer.execute("SELECT pg_cancel_backend(pg_backend_pid())").fetchone()
        except psycopg.Error as error:
            assert error.sqlstate == "57014", "self-cancellation must report query_canceled"
        else:
            raise AssertionError("self-cancellation returned a success row")
        assert peer.execute("SELECT 1").fetchone()[0] == 1
        facts["selfCancellationAndReuse"] = True
        if args.engine == "postgres":
            admin.execute(sql.SQL("GRANT pg_signal_backend TO {}").format(plain_role(roles["other"])))
            assert cancel(other, pid) is True
            idle_reuse(target)
            denied(other, privileged.info.backend_pid)
            facts["pgSignalBackendCanCancelOtherRoleButNotSuperTarget"] = True
        target.close()
        assert cancel(peer, pid) is False
        facts["disconnectedTargetIsAbsent"] = True
        assert cancel(peer, 0) is False
        assert cancel(peer, -1) is False
        facts["nonpositiveTargetIsAbsent"] = True
        report["status"] = "pass"
    except Exception as error:
        # Exception strings can contain connection URLs or role passwords, so
        # keep only the first line with the password and any URL removed.
        report["failureType"] = type(error).__name__
        report["sqlstate"] = getattr(error, "sqlstate", None)
        first_line = (str(error).splitlines() or [""])[0].replace(password, "[redacted]")
        report["failureMessage"] = re.sub(r"\w+://\S+", "[url]", first_line)[:300]
        report["factsBeforeFailure"] = sorted(facts)
        try:
            rows = admin.execute("SELECT rolname, rolcanlogin, rolsuper FROM pg_roles ORDER BY rolname LIMIT 40").fetchall()
            report["rolesAtFailure"] = [[str(row[0]), bool(row[1]), bool(row[2])] for row in rows]
            report["createdRoleNames"] = list(created)
        except Exception as inspect_error:
            report["rolesAtFailureError"] = type(inspect_error).__name__
        raise
    finally:
        cleanup_failed = False
        for connection in connections:
            try:
                connection.close()
            except Exception:
                cleanup_failed = True
        for role in reversed(created):
            try:
                admin.execute(sql.SQL("DROP ROLE {}").format(plain_role(role)))
            except Exception:
                cleanup_failed = True
        try:
            admin.close()
        except Exception:
            cleanup_failed = True
        if cleanup_failed:
            report["status"] = "fail"
            report["cleanupFailed"] = True
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n")
        if cleanup_failed:
            raise RuntimeError("owned fixture cleanup failed")


if __name__ == "__main__":
    try:
        main()
    except Exception as failure:
        # Redact diagnostics; the structured report retains type/SQLSTATE.
        print(f"cancellation authority qualification failed: {type(failure).__name__}", file=sys.stderr)
        raise SystemExit(1)
