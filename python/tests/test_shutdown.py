"""Contract §8 graceful shutdown, against a real server process.

Each test starts the app in a subprocess, signals it, and asserts on what an
orchestrator sees: in-flight responses, the exit status, and whether the
OnStop hook ran.
"""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path

import httpx
import pytest

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="POSIX signal semantics"
)

APP_SOURCE = textwrap.dedent(
    """\
    import asyncio
    import os
    from pathlib import Path

    from neutron import App, Router

    MARKERS = Path(os.environ["MARKERS"])
    app = App(title="shutdown", drain_timeout=float(os.environ["DRAIN"]))
    router = Router()

    @router.get("/slow")
    async def slow() -> dict:
        (MARKERS / "slow-started").touch()
        await asyncio.sleep(float(os.environ["SLOW"]))
        return {"ok": True}

    app.include_router(router)

    @app.on_stop
    async def first_registered() -> None:
        with open(MARKERS / "hooks", "a") as f:
            f.write("first\\n")

    @app.on_stop
    async def second_registered() -> None:
        with open(MARKERS / "hooks", "a") as f:
            f.write("second\\n")

    if __name__ == "__main__":
        app.run(host="127.0.0.1", port=int(os.environ["PORT"]))
    """
)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    def __init__(self, tmp_path: Path, launcher: str, *, drain: float, slow: float):
        (tmp_path / "shutdown_app.py").write_text(APP_SOURCE)
        self.markers = tmp_path / "markers"
        self.markers.mkdir()
        self.port = _free_port()
        env = {
            **os.environ,
            "MARKERS": str(self.markers),
            "DRAIN": str(drain),
            "SLOW": str(slow),
            "PORT": str(self.port),
            "PYTHONPATH": str(tmp_path),
        }
        if launcher == "app.run":
            cmd = [sys.executable, "shutdown_app.py"]
        elif launcher == "uvicorn":
            cmd = [
                sys.executable, "-m", "uvicorn", "shutdown_app:app",
                "--host", "127.0.0.1", "--port", str(self.port),
            ]
        elif launcher == "neutron dev":
            cmd = [
                sys.executable, "-m", "neutron", "dev", "shutdown_app:app",
                "--host", "127.0.0.1", "--port", str(self.port),
            ]
        else:
            raise ValueError(launcher)
        # A log file, not a pipe: a surviving child would hold a pipe open
        # and block the read. Own session, so kill() reaches every process.
        self.log = tmp_path / "server.log"
        with open(self.log, "wb") as log:
            self.proc = subprocess.Popen(
                cmd, cwd=tmp_path, env=env, stdout=log, stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        self.url = f"http://127.0.0.1:{self.port}"
        self._wait_ready()

    def _wait_ready(self) -> None:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                pytest.fail(f"server exited early:\n{self.output()}")
            try:
                if httpx.get(self.url + "/health", timeout=1).status_code == 200:
                    return
            except httpx.TransportError:
                time.sleep(0.05)
        self.kill()
        pytest.fail("server did not become ready")

    def start_slow_request(self) -> dict:
        result: dict = {}

        def go() -> None:
            try:
                result["status"] = httpx.get(self.url + "/slow", timeout=60).status_code
            except Exception as exc:  # pragma: no cover - reported by the assert
                result["error"] = repr(exc)

        thread = threading.Thread(target=go, daemon=True)
        thread.start()
        result["thread"] = thread
        deadline = time.monotonic() + 10
        while not (self.markers / "slow-started").exists():
            assert time.monotonic() < deadline, "slow request never reached the handler"
            time.sleep(0.01)
        return result

    def hooks(self) -> list[str]:
        path = self.markers / "hooks"
        return path.read_text().split() if path.exists() else []

    def wait(self, timeout: float) -> int:
        try:
            return self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.kill()
            pytest.fail(f"server still running {timeout}s after signal:\n{self.output()}")

    def output(self) -> str:
        return self.log.read_text(errors="replace")

    def kill(self) -> None:
        try:
            os.killpg(self.proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        self.proc.wait()

    def port_closed(self) -> bool:
        with socket.socket() as s:
            return s.connect_ex(("127.0.0.1", self.port)) != 0


@pytest.fixture
def servers():
    started: list[Server] = []
    yield started
    for server in started:
        server.kill()


@pytest.mark.parametrize("launcher", ["app.run", "uvicorn"])
def test_sigterm_drains_inflight_runs_hooks_and_exits_zero(tmp_path, servers, launcher):
    server = Server(tmp_path, launcher, drain=30, slow=1.5)
    servers.append(server)
    slow = server.start_slow_request()

    server.proc.send_signal(signal.SIGTERM)
    time.sleep(0.2)
    # New work is refused: the listener is closed (connect error), or a
    # request that raced in on an open connection gets 503. Never served.
    try:
        late = httpx.get(server.url + "/health", timeout=2).status_code
    except httpx.TransportError:
        late = None
    assert late in (None, 503)

    code = server.wait(timeout=1.5 + 5)
    slow["thread"].join(timeout=5)
    assert slow.get("status") == 200, slow
    assert code == 0, server.output()
    assert server.hooks() == ["second", "first"]
    assert server.port_closed()


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT])
def test_idle_signal_exits_promptly(tmp_path, servers, sig):
    server = Server(tmp_path, "app.run", drain=30, slow=0)
    servers.append(server)

    started = time.monotonic()
    server.proc.send_signal(sig)
    code = server.wait(timeout=5)

    assert code == 0, server.output()
    assert time.monotonic() - started < 3
    assert server.hooks() == ["second", "first"]


def test_second_signal_forces_exit(tmp_path, servers):
    server = Server(tmp_path, "app.run", drain=30, slow=30)
    servers.append(server)
    server.start_slow_request()

    server.proc.send_signal(signal.SIGTERM)
    time.sleep(1.3)  # past the duplicate-signal window
    assert server.proc.poll() is None, "first signal must drain, not exit"
    forced_at = time.monotonic()
    server.proc.send_signal(signal.SIGTERM)
    code = server.wait(timeout=3)

    assert code != 0
    assert time.monotonic() - forced_at < 2


def test_drain_timeout_bounds_shutdown(tmp_path, servers):
    server = Server(tmp_path, "app.run", drain=1, slow=30)
    servers.append(server)
    server.start_slow_request()

    started = time.monotonic()
    server.proc.send_signal(signal.SIGTERM)
    code = server.wait(timeout=1 + 5)

    assert code == 0, server.output()
    assert time.monotonic() - started < 1 + 3
    assert server.hooks() == ["second", "first"]


def test_neutron_dev_stops_on_sigterm(tmp_path, servers):
    """Reloader parent and server child both exit; the port is released."""
    server = Server(tmp_path, "neutron dev", drain=30, slow=0)
    servers.append(server)

    server.proc.send_signal(signal.SIGTERM)
    code = server.wait(timeout=10)

    assert code == 0, server.output()
    assert server.hooks() == ["second", "first"]
    assert server.port_closed()


def test_neutron_dev_ctrl_c_to_process_group_is_graceful(tmp_path, servers):
    """Terminal Ctrl-C: SIGINT reaches reloader and worker, then the reloader
    sends the worker SIGTERM. That pair is one stop request, not a force."""
    server = Server(tmp_path, "neutron dev", drain=30, slow=0)
    servers.append(server)

    os.killpg(server.proc.pid, signal.SIGINT)
    code = server.wait(timeout=10)

    assert code == 0, server.output()
    assert server.hooks() == ["second", "first"]
    assert server.port_closed()
