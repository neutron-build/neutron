"""Shipped-artifact Starlette examples: fresh Session per sync/async request.

The server owns graceful request-worker draining before lifespan shutdown.
Observers are shared bounded metadata buffers; Sessions and model caches are not.
"""
from contextlib import asynccontextmanager
from dataclasses import asdict,dataclass
import asyncio
from typing import Any
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from neutron.orm import AsyncSessionRequests,ModelMapping,OrmError,QueryObserver,SessionRequests,Table

@dataclass
class Record:
    id: int
    label: str


def export_events(observer: QueryObserver,logger: Any,*,tracer: Any=None,meter: Any=None) -> tuple[int,int]:
    """Call from an exporter worker outside request/native commit boundaries.

    Tracer/meter are optional OpenTelemetry API objects; import their SDK only in
    the consuming application. Telemetry failures cannot change a DB outcome.
    """
    counter=None if meter is None else meter.create_counter('neutron.orm.operations')
    duration=None if meter is None else meter.create_histogram('neutron.orm.duration',unit='s')
    events=observer.drain();failures=0
    for event in events:
        attributes=asdict(event)
        try:
            logger.info('postgres.operation',**attributes)
            labels={'operation':event.operation,'outcome':event.outcome}
            if counter is not None: counter.add(1,labels)
            if duration is not None: duration.record(event.elapsed_ns/1_000_000_000,labels)
            if tracer is not None:
                with tracer.start_as_current_span('postgres.operation') as span:
                    span.set_attributes({key:value for key,value in attributes.items() if value is not None})
        except Exception: failures+=1 # Report export loss without changing a DB outcome.
    return len(events),failures


def _mapping(table: Table) -> ModelMapping[Record]:
    return ModelMapping(Record,table,{'id':table.columns['id'],'label':table.columns['label']},primary_key=('id',))


def create_async_app(url: str,table: Table,observer: QueryObserver,*,grace_seconds: float=5.0) -> Starlette:
    mapping=_mapping(table)
    @asynccontextmanager
    async def lifespan(app: Starlette):
        app.state.requests=AsyncSessionRequests(url,observer=observer)
        try: yield
        finally: await app.state.requests.shutdown(grace_seconds=grace_seconds)
    async def create(request: Request):
        body=await request.json()
        try:
            async with request.app.state.requests.session() as session:
                obj=Record(body['id'],body['label']);session.add(mapping,obj)
        except OrmError as error: return JSONResponse({'sqlstate':error.sqlstate},status_code=409)
        return JSONResponse({'id':obj.id},status_code=201)
    async def get(request: Request):
        async with request.app.state.requests.session() as session: obj=await session.get(mapping,request.path_params['id'])
        return JSONResponse({'id':obj.id,'label':obj.label}) if obj is not None else JSONResponse({},status_code=404)
    return Starlette(routes=[Route('/records',create,methods=['POST']),Route('/records/{id:int}',get)],lifespan=lifespan)


def create_sync_app(url: str,table: Table,observer: QueryObserver) -> Starlette:
    mapping=_mapping(table);requests=SessionRequests(url,observer=observer)
    @asynccontextmanager
    async def lifespan(app: Starlette):
        app.state.requests=requests
        try: yield
        finally: await asyncio.to_thread(requests.shutdown)
    def write_record(body: dict[str,Any]):
        try:
            with requests.session() as session:
                obj=Record(body['id'],body['label']);session.add(mapping,obj)
        except OrmError as error: return JSONResponse({'sqlstate':error.sqlstate},status_code=409)
        return JSONResponse({'id':obj.id},status_code=201)
    async def create(request: Request):
        # Parse the JSON body on the event loop; keep every sync Session call in
        # one worker thread. Bound secrets never become URL query parameters.
        return await run_in_threadpool(write_record,await request.json())
    def get(request: Request):
        with requests.session() as session: obj=session.get(mapping,request.path_params['id'])
        return JSONResponse({'id':obj.id,'label':obj.label}) if obj is not None else JSONResponse({},status_code=404)
    return Starlette(routes=[Route('/records',create,methods=['POST']),Route('/records/{id:int}',get)],lifespan=lifespan)
