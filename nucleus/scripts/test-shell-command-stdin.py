#!/usr/bin/env python3
"""Process/pgwire/live regressions. Pass the built Nucleus binary as argv[1].
Uses public fixture data only; never puts SQL in process arguments.
"""
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time

BINARY = str(Path(sys.argv[1]).resolve())
MARKER = b"public-fixture-error-marker"


def read(sock, count):
    data = b""
    while len(data) < count:
        chunk = sock.recv(count - len(data))
        if not chunk:
            raise EOFError("fixture disconnected")
        data += chunk
    return data


def frame(tag, body):
    return tag + struct.pack("!I", len(body) + 4) + body


def message(sock):
    tag = read(sock, 1)
    return tag, read(sock, struct.unpack("!I", read(sock, 4))[0] - 4)


def shell(port, sql, extra=()):
    argv = [BINARY, "shell", "--command-stdin", "--json", "--port", str(port), *extra]
    assert MARKER.decode() not in " ".join(argv)
    return subprocess.run(argv, input=sql, capture_output=True, timeout=15)


def failed(result, diagnostic):
    assert result.returncode != 0 and result.stdout == b""
    assert diagnostic in result.stderr and MARKER not in result.stderr


def fixture(sql, mode="select"):
    errors = []
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.settimeout(15)
        port = listener.getsockname()[1]

        def serve():
            try:
                with listener.accept()[0] as conn:
                    conn.settimeout(10)
                    read(conn, struct.unpack("!I", read(conn, 4))[0] - 4)
                    if mode == "startup-error":
                        conn.sendall(frame(b"E", b"SERROR\0M" + MARKER + b"\0\0"))
                        return
                    conn.sendall(frame(b"R", struct.pack("!I", 0)) + frame(b"Z", b"I"))
                    if mode == "repl":
                        assert message(conn) == (b"X", b"")
                        return
                    assert message(conn) == (b"Q", sql + b"\0"), "SQL bytes changed on pgwire"
                    if mode == "disconnect":
                        return
                    if mode == "error":
                        response = frame(b"E", b"SERROR\0M" + MARKER + b"\0\0")
                    elif mode == "command":
                        response = frame(b"C", b"UPDATE 1\0")
                    else:
                        column = b"value\0" + struct.pack("!IhIhih", 0, 0, 25, -1, -1, 0)
                        value = "雪 ; \n".encode()
                        response = frame(b"T", struct.pack("!h", 1) + column)
                        response += frame(b"D", struct.pack("!hi", 1, len(value)) + value)
                        response += frame(b"C", b"SELECT 1\0")
                    conn.sendall(response + frame(b"Z", b"I"))
                    assert message(conn) == (b"X", b"")
            except BaseException as error:
                errors.append(error)

        worker = threading.Thread(target=serve, daemon=True)
        worker.start()
        if mode == "repl":
            with tempfile.TemporaryDirectory(prefix="nucleus-repl-") as home:
                result = subprocess.run([BINARY, "shell", "--json", "--port", str(port)],
                                        input=b"\\q\n", capture_output=True, timeout=15,
                                        env={**os.environ, "HOME": home, "USERPROFILE": home})
        else:
            result = shell(port, sql)
        worker.join(15)
        assert not worker.is_alive()
        if errors:
            raise errors[0]
        return result


help_text = subprocess.run([BINARY, "shell", "--help"], capture_output=True, check=True).stdout
assert b"--command-stdin" in help_text and b"With -c or --command-stdin" in help_text
for sql in [b"", b"SELECT 1", " \tSELECT '雪 ; \n';;\r\n ".encode(), b"SELECT 1; -- no newline"]:
    result = fixture(sql)
    assert result.returncode == 0 and result.stderr == b""
    assert json.loads(result.stdout) == [{"value": "雪 ; \n"}]
result = fixture(b"UPDATE public_fixture SET value=1;", "command")
assert result.returncode == 0 and json.loads(result.stdout) == {"tag": "UPDATE 1"}
for mode, diagnostic in [("error", b"SQL command failed"), ("startup-error", b"Failed to connect"),
                         ("disconnect", b"SQL command transport failed")]:
    failed(fixture(MARKER, mode), diagnostic)
result = fixture(b"", "repl")
assert result.returncode == 0 and b"Connected to Nucleus." in result.stdout
with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    listener.settimeout(0.2)
    port = listener.getsockname()[1]
    for sql, diagnostic in [(b"SELECT '\xff'", b"valid UTF-8"), (b"SELECT 1\0SELECT 2", b"NUL bytes")]:
        failed(shell(port, sql), diagnostic)
    for option in ("-c", "--command"):
        result = shell(port, b"SELECT 1", [option, "SELECT 2"])
        assert result.returncode == 2 and b"cannot be used with" in result.stderr
    try:
        listener.accept()
        raise AssertionError("invalid input connected")
    except socket.timeout:
        pass
if sys.platform != "win32":
    descriptor = os.open(tempfile.gettempdir(), os.O_RDONLY)
    try:
        result = subprocess.run([BINARY, "shell", "--command-stdin", "--json"],
                                stdin=descriptor, capture_output=True, timeout=15)
        failed(result, b"Failed to read SQL command from stdin")
    finally:
        os.close(descriptor)
print("pgwire/process: exact bytes, JSON, errors, validation, conflicts, REPL passed")

# In-memory engine avoids the unrelated host disk-watermark write refusal.
with tempfile.TemporaryDirectory(prefix="nucleus-stdin-") as data:
    with socket.socket() as reserve:
        reserve.bind(("127.0.0.1", 0))
        port = reserve.getsockname()[1]
    with open(Path(data) / "server.log", "wb") as log:
        server = subprocess.Popen([BINARY, "start", "--host", "127.0.0.1", "--port", str(port),
                                   "--data", data, "--memory", "--no-tls", "--resp-port", "0", "--s3-port", "0"],
                                  stdout=log, stderr=log,
                                  env={k: v for k, v in os.environ.items() if not k.startswith("NUCLEUS_")})
        try:
            deadline = time.monotonic() + 30
            while True:
                assert server.poll() is None, "live Nucleus exited before listening"
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                        break
                except OSError:
                    assert time.monotonic() < deadline
                    time.sleep(0.1)
            value = "  雪;\ntrailing space  "
            result = shell(port, (" \tSELECT KV_SET('stdin-public-fixture', '" + value + "');\r\n ").encode())
            assert result.returncode == 0, "live KV_SET failed"
            json.loads(result.stdout)
            result = shell(port, b"SELECT KV_GET('stdin-public-fixture'); -- no newline")
            assert result.returncode == 0, "live KV_GET failed"
            rows = json.loads(result.stdout)
            assert len(rows) == 1 and list(rows[0].values()) == [value], "literal bytes changed"
            failed(shell(port, b"SELECT " + MARKER + b";"), b"SQL command failed")
        finally:
            server.terminate()
            try:
                server.wait(timeout=15)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
print("live Nucleus: JSON KV round trip preserved whitespace/Unicode/semicolon; error nonzero")
