"""Contract §7: /health reports dependency health, not feature detection."""

import pytest

from neutron import App
from neutron.app import _dependency_reachable


class _Pool:
    def __init__(self, ok=True):
        self._ok = ok

    async def fetchval(self, *a, **k):
        if not self._ok:
            raise ConnectionError("database is unreachable")
        return 1


class _Db:
    def __init__(self, ok=True, is_nucleus=False):
        self.pool = _Pool(ok)
        self.features = type("F", (), {"is_nucleus": is_nucleus})()


class TestDependencyReachable:
    async def test_a_healthy_plain_postgres_is_reachable(self):
        """The bug: this reported disconnected because it was not Nucleus.
        Contract §7 says feature detection is §1, not /health."""
        assert await _dependency_reachable(_Db(ok=True, is_nucleus=False)) is True

    async def test_a_healthy_nucleus_is_reachable(self):
        assert await _dependency_reachable(_Db(ok=True, is_nucleus=True)) is True

    async def test_an_unreachable_database_is_not(self):
        """Previously unreachable code: nothing ever probed the connection, so
        a genuinely down database still reported the same as a healthy one."""
        assert await _dependency_reachable(_Db(ok=False)) is False

    async def test_a_dependency_without_a_pool_is_taken_at_face_value(self):
        assert await _dependency_reachable(object()) is True
