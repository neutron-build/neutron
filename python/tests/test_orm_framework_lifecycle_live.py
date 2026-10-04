"""Native HTTP/ASGI qualification of the authored Starlette lifecycle example; PostgreSQL is the oracle."""
from urllib.parse import urlsplit
import pytest
from neutron.orm import QueryObserver
from .test_orm_framework_lifecycle import Counter,Histogram,Logger,Meter,Tracer,load_framework_lifecycle
from .test_orm_live import live_table

pytest.importorskip('httpx')
testclient=pytest.importorskip('starlette.testclient')

LABEL='first-private-label'

@pytest.fixture
def lifecycle(): return load_framework_lifecycle()

def rows(native,table): return native.execute(f'SELECT id,label FROM {table.sql} ORDER BY id').fetchall()

def exercise(client,native,table,observer,lifecycle):
    # Malformed bodies refuse with 400 before any database work.
    for body in ({'id':'1','label':'x'},{'id':True,'label':'x'},{'id':1},{'id':1,'label':2},[],'text',{}):
        assert client.post('/records',json=body).status_code==400
    assert observer.metrics.queries==observer.metrics.executions==observer.metrics.streams==0 and rows(native,table)==[]
    created=client.post('/records',json={'id':1,'label':LABEL})
    assert created.status_code==201 and created.json()=={'id':1}
    assert rows(native,table)==[(1,LABEL)]
    fetched=client.get('/records/1');assert fetched.status_code==200 and fetched.json()=={'id':1,'label':LABEL}
    assert client.get('/records/2').status_code==404
    # Unique violation is the database's SQLSTATE answer: 409, the original row is untouched and the slot is reusable.
    duplicate=client.post('/records',json={'id':1,'label':'second-private-label'})
    assert duplicate.status_code==409 and duplicate.json()=={'sqlstate':'23505'}
    assert rows(native,table)==[(1,LABEL)]
    assert client.post('/records',json={'id':2,'label':'after-conflict'}).status_code==201
    assert rows(native,table)==[(1,LABEL),(2,'after-conflict')]
    # Telemetry export happens outside the request path; failures there cannot change database outcomes.
    logger,counter,histogram,tracer=Logger(),Counter(),Histogram(),Tracer()
    exported,failures=lifecycle.export_events(observer,logger,tracer=tracer,meter=Meter(counter,histogram))
    assert exported>=4 and failures==0
    assert any(extra['neutron_orm']['sqlstate']=='23505' and extra['neutron_orm']['outcome']=='error' for _,extra in logger.records)
    return logger,counter,histogram,tracer

def assert_redacted(url,logger,counter,histogram,tracer):
    parts=urlsplit(url)
    private=[url,LABEL,'second-private-label','after-conflict','SELECT','INSERT','FROM','VALUES']
    private+=[value for value in (parts.netloc,parts.password) if value]
    exported=repr(logger.records)+repr(counter.adds)+repr(histogram.records)+repr([span.attributes for span in tracer.spans])
    for text in private: assert text not in exported
    for _,extra in logger.records: assert all(type(value) in {int,str,bool,type(None)} for value in extra['neutron_orm'].values())

def assert_stopped(client,native,table):
    before=rows(native,table)
    for response in (client.post('/records',json={'id':99,'label':'after-shutdown'}),client.get('/records/1')):
        assert response.status_code==503 and response.json()=={'sqlstate':None}
    assert rows(native,table)==before

def test_sync_app_http_lifecycle_conflict_shutdown_and_redacted_telemetry(live_table,lifecycle):
    url,table,native=live_table;observer=QueryObserver()
    app=lifecycle.create_sync_app(url,table,observer)
    with testclient.TestClient(app) as client:
        exported=exercise(client,native,table,observer,lifecycle)
        app.state.requests.shutdown()  # the server drains workers before lifespan shutdown
        assert_stopped(client,native,table)
    assert_redacted(url,*exported)

def test_async_app_http_lifecycle_conflict_shutdown_and_redacted_telemetry(live_table,lifecycle):
    url,table,native=live_table;observer=QueryObserver()
    app=lifecycle.create_async_app(url,table,observer)
    with testclient.TestClient(app) as client:
        exported=exercise(client,native,table,observer,lifecycle)
        client.portal.call(app.state.requests.shutdown)  # same loop that owns the request manager
        assert_stopped(client,native,table)
    assert_redacted(url,*exported)

def test_sync_and_async_apps_survive_repeated_lifespans_with_fresh_state(live_table,lifecycle):
    url,table,native=live_table
    for factory,identifier in ((lifecycle.create_sync_app,1),(lifecycle.create_async_app,2)):
        with testclient.TestClient(factory(url,table,QueryObserver())) as client:
            assert client.post('/records',json={'id':identifier,'label':'lifespan'}).status_code==201
            assert client.get(f'/records/{identifier}').json()=={'id':identifier,'label':'lifespan'}
    assert rows(native,table)==[(1,'lifespan'),(2,'lifespan')]
