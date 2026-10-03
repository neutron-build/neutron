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
        labels={'operation':event.operation,'outcome':event.outcome}
        # Each sink is isolated: one failing exporter must not suppress the others.
        sinks=[lambda: logger.info('postgres.operation',extra={'neutron_orm':attributes})]
        if counter is not None: sinks.append(lambda: counter.add(1,labels))
        if duration is not None: sinks.append(lambda: duration.record(event.elapsed_ns/1_000_000_000,labels))
        if tracer is not None:
            def trace() -> None:
                with tracer.start_as_current_span('postgres.operation') as span:
                    span.set_attributes({key:value for key,value in attributes.items() if value is not None})
            sinks.append(trace)
        for sink in sinks:
            try: sink()
            except Exception: failures+=1 # Report export loss without changing a DB outcome.
    return len(events),failures


def _refusal(error: OrmError) -> JSONResponse:
    # No SQLSTATE means admission, shutdown, connect or cleanup refusal: retryable 503.
    return JSONResponse({'sqlstate':error.sqlstate},status_code=409 if error.sqlstate else 503)


def _record(body: Any) -> Record | None:
    if not isinstance(body,dict) or type(body.get('id')) is not int or type(body.get('label')) is not str: return None
    return Record(body['id'],body['label'])


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
        obj=_record(await request.json())
        if obj is None: return JSONResponse({},status_code=400)
        try:
            async with request.app.state.requests.session() as session: session.add(mapping,obj)
        except OrmError as error: return _refusal(error)
        return JSONResponse({'id':obj.id},status_code=201)
    async def get(request: Request):
        try:
            async with request.app.state.requests.session() as session: obj=await session.get(mapping,request.path_params['id'])
        except OrmError as error: return _refusal(error)
        return JSONResponse({'id':obj.id,'label':obj.label}) if obj is not None else JSONResponse({},status_code=404)
    return Starlette(routes=[Route('/records',create,methods=['POST']),Route('/records/{id:int}',get)],lifespan=lifespan)


def create_sync_app(url: str,table: Table,observer: QueryObserver) -> Starlette:
    mapping=_mapping(table);requests=SessionRequests(url,observer=observer)
    @asynccontextmanager
    async def lifespan(app: Starlette):
        app.state.requests=requests
        try: yield
        finally: await asyncio.to_thread(requests.shutdown)
    def write_record(body: Any):
        obj=_record(body)
        if obj is None: return JSONResponse({},status_code=400)
        try:
            with requests.session() as session: session.add(mapping,obj)
        except OrmError as error: return _refusal(error)
        return JSONResponse({'id':obj.id},status_code=201)
    async def create(request: Request):
        # Parse the JSON body on the event loop; keep every sync Session call in
        # one worker thread. Bound secrets never become URL query parameters.
        return await run_in_threadpool(write_record,await request.json())
    def get(request: Request):
        try:
            with requests.session() as session: obj=session.get(mapping,request.path_params['id'])
        except OrmError as error: return _refusal(error)
        return JSONResponse({'id':obj.id,'label':obj.label}) if obj is not None else JSONResponse({},status_code=404)
    return Starlette(routes=[Route('/records',create,methods=['POST']),Route('/records/{id:int}',get)],lifespan=lifespan)
