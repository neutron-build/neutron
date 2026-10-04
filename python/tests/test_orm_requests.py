import pytest
from neutron.orm import AsyncDatabase,AsyncSessionRequests,Database,OrmError,SessionRequests
from neutron.orm.endpoint import NUCLEUS_CANDIDATE_PROFILE
from .test_orm_clients import AsyncConnection,Connection


class Recorder:
    def __init__(self): self.calls: list[tuple[str,dict[str,object]]]=[]
    def sync(self,url,**kwargs):
        self.calls.append((url,kwargs));return Database(Connection([]))
    async def asynchronous(self,url,**kwargs):
        self.calls.append((url,kwargs));return AsyncDatabase(AsyncConnection([]))


def test_sync_requests_forward_default_and_named_profile_unchanged(monkeypatch):
    recorder=Recorder();monkeypatch.setattr(Database,'connect',recorder.sync)
    with SessionRequests('unused').session(): pass
    with SessionRequests('unused',profile=NUCLEUS_CANDIDATE_PROFILE).session(): pass
    assert [kwargs['profile'] for _,kwargs in recorder.calls]==['postgres-direct',NUCLEUS_CANDIDATE_PROFILE]
    assert all(kwargs['observer'] is None for _,kwargs in recorder.calls)


def test_sync_requests_refuse_unknown_profile_at_construction_before_any_connect(monkeypatch):
    recorder=Recorder();monkeypatch.setattr(Database,'connect',recorder.sync)
    for profile in ('unknown','',None,5,b'postgres-direct','Postgres-Direct'):
        with pytest.raises(OrmError): SessionRequests('unused',profile=profile)  # type: ignore[arg-type]
    with pytest.raises(OrmError): SessionRequests('do-not-connect',profile='unknown')
    with pytest.raises(ValueError): SessionRequests('unused',max_active=0)
    assert recorder.calls==[]


@pytest.mark.asyncio
async def test_async_requests_forward_default_and_named_profile_unchanged(monkeypatch):
    recorder=Recorder();monkeypatch.setattr(AsyncDatabase,'connect',recorder.asynchronous)
    async with AsyncSessionRequests('unused').session(): pass
    async with AsyncSessionRequests('unused',profile=NUCLEUS_CANDIDATE_PROFILE).session(): pass
    assert [kwargs['profile'] for _,kwargs in recorder.calls]==['postgres-direct',NUCLEUS_CANDIDATE_PROFILE]
    assert all(kwargs['observer'] is None for _,kwargs in recorder.calls)


@pytest.mark.asyncio
async def test_async_requests_refuse_unknown_profile_at_construction_before_any_connect(monkeypatch):
    recorder=Recorder();monkeypatch.setattr(AsyncDatabase,'connect',recorder.asynchronous)
    for profile in ('unknown','',None,5,b'postgres-direct','Postgres-Direct'):
        with pytest.raises(OrmError): AsyncSessionRequests('unused',profile=profile)  # type: ignore[arg-type]
    with pytest.raises(OrmError): AsyncSessionRequests('do-not-connect',profile='unknown')
    assert recorder.calls==[]
