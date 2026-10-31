"""export_events isolation and redaction contracts; fake sinks only, no database."""
from contextlib import contextmanager
from dataclasses import fields
import importlib.util
from pathlib import Path
import sys
import pytest
from neutron.orm import QueryObserver
from neutron.orm.observability import QueryEvent

pytest.importorskip('starlette')
APP=Path(__file__).resolve().parents[2]/'conformance'/'polyglot'/'applications'/'python'/'framework_lifecycle.py'

def load_framework_lifecycle():
    if not APP.is_file(): pytest.skip('conformance application source is not part of this checkout')
    spec=importlib.util.spec_from_file_location('neutron_conformance_framework_lifecycle',APP)
    assert spec is not None and spec.loader is not None
    module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module;spec.loader.exec_module(module)
    return module

@pytest.fixture
def lifecycle(): return load_framework_lifecycle()

class Logger:
    def __init__(self,fail=False): self.fail=fail;self.records=[]
    def info(self,message,*,extra=None):
        if self.fail: raise RuntimeError('logger unavailable')
        self.records.append((message,extra))

class Counter:
    def __init__(self,fail=False): self.fail=fail;self.adds=[]
    def add(self,amount,labels):
        if self.fail: raise RuntimeError('counter unavailable')
        self.adds.append((amount,dict(labels)))

class Histogram:
    def __init__(self,fail=False): self.fail=fail;self.records=[]
    def record(self,value,labels):
        if self.fail: raise RuntimeError('histogram unavailable')
        self.records.append((value,dict(labels)))

class Meter:
    def __init__(self,counter,histogram): self.counter=counter;self.histogram=histogram;self.created=[]
    def create_counter(self,name,**kwargs): self.created.append(name);return self.counter
    def create_histogram(self,name,**kwargs): self.created.append(name);return self.histogram

class Span:
    def __init__(self): self.attributes={}
    def set_attributes(self,values): self.attributes.update(values)

class Tracer:
    def __init__(self,fail_start=False,fail_inside=False): self.fail_start=fail_start;self.fail_inside=fail_inside;self.spans=[]
    @contextmanager
    def start_as_current_span(self,name):
        if self.fail_start: raise RuntimeError('tracer unavailable')
        span=Span();span.attributes['name']=name
        yield span
        if self.fail_inside: raise RuntimeError('span export failed')
        self.spans.append(span)

def observer_with(*events):
    observer=QueryObserver()
    for event in events: observer._record(event)
    return observer

EVENTS=(QueryEvent('query',1_500_000_000,1,None,'ok',False),QueryEvent('execute',2_000,None,'23505','error',True))

def test_all_sinks_receive_every_event_with_only_fixed_metadata_fields(lifecycle):
    logger,counter,histogram,tracer=Logger(),Counter(),Histogram(),Tracer();observer=observer_with(*EVENTS)
    assert lifecycle.export_events(observer,logger,tracer=tracer,meter=Meter(counter,histogram))==(2,0)
    allowed={field.name for field in fields(QueryEvent)}
    assert len(logger.records)==2 and len(tracer.spans)==2 and len(counter.adds)==2 and len(histogram.records)==2
    for message,extra in logger.records: assert message=='postgres.operation' and set(extra['neutron_orm'])==allowed
    assert logger.records[1][1]['neutron_orm']['sqlstate']=='23505'
    assert [labels for _,labels in counter.adds]==[{'operation':'query','outcome':'ok'},{'operation':'execute','outcome':'error'}]
    assert histogram.records[0][0]==1.5 and histogram.records[1][0]==2e-06
    # Span attributes carry the same fixed fields minus unset (None) values and the span name only.
    assert set(tracer.spans[0].attributes)=={'name','operation','elapsed_ns','outcome','owned_transaction','row_count'}
    assert set(tracer.spans[1].attributes)=={'name','operation','elapsed_ns','outcome','owned_transaction','sqlstate'}
    assert observer.drain()==()

@pytest.mark.parametrize('failing',['logger','counter','histogram','tracer-start','tracer-inside'])
def test_failing_sink_never_suppresses_the_others_and_is_counted(lifecycle,failing):
    logger=Logger(fail=failing=='logger');counter=Counter(fail=failing=='counter');histogram=Histogram(fail=failing=='histogram')
    tracer=Tracer(fail_start=failing=='tracer-start',fail_inside=failing=='tracer-inside')
    exported,failures=lifecycle.export_events(observer_with(*EVENTS),logger,tracer=tracer,meter=Meter(counter,histogram))
    assert (exported,failures)==(2,2)
    assert (len(logger.records),len(counter.adds),len(histogram.records))==tuple(0 if failing==name else 2 for name in ('logger','counter','histogram'))
    assert len(tracer.spans)==(0 if failing in {'tracer-start','tracer-inside'} else 2)

def test_all_sinks_failing_reports_every_loss_and_still_drains(lifecycle):
    observer=observer_with(*EVENTS)
    result=lifecycle.export_events(observer,Logger(True),tracer=Tracer(fail_start=True),meter=Meter(Counter(True),Histogram(True)))
    assert result==(2,8) and observer.drain()==()

def test_logger_only_export_and_empty_queue(lifecycle):
    logger=Logger()
    assert lifecycle.export_events(observer_with(),logger)==(0,0) and logger.records==[]
    assert lifecycle.export_events(observer_with(EVENTS[0]),logger)==(1,0) and len(logger.records)==1


UNUSED_URL='postgresql://user:secret@127.0.0.1:1/none?connect_timeout=1'  # never dialled: these paths refuse before any connection

def _table():
    from neutron.orm import ColumnSpec,Table
    return Table('records',{'id':ColumnSpec(int,'int4'),'label':ColumnSpec(str,'text')},schema='app')

@pytest.mark.parametrize('kind',['sync','async'])
def test_apps_refuse_malformed_bodies_and_stopped_factories_without_database_work(lifecycle,kind):
    testclient=pytest.importorskip('starlette.testclient')
    observer=QueryObserver();app=(lifecycle.create_sync_app if kind=='sync' else lifecycle.create_async_app)(UNUSED_URL,_table(),observer)
    with testclient.TestClient(app) as client:
        for body in ({'id':'1','label':'x'},{'id':True,'label':'x'},{'id':1},{'id':1,'label':2},[],'text',{}):
            assert client.post('/records',json=body).status_code==400
        if kind=='sync': app.state.requests.shutdown()
        else: client.portal.call(app.state.requests.shutdown)
        for response in (client.post('/records',json={'id':1,'label':'x'}),client.get('/records/1')):
            assert response.status_code==503 and response.json()=={'sqlstate':None}
    assert observer.metrics.queries==observer.metrics.executions==observer.metrics.streams==0 and observer.drain()==()
