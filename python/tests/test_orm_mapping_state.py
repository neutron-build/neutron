from dataclasses import dataclass
import pytest
from neutron.orm import ColumnSpec, OrmError, Table
from neutron.orm.mapping import ModelMapping
from neutron.orm.state import ObjectState, StateStore

@dataclass
class User:
    id: int | None=None
    name: str='new'


def mapping(schema='public'):
    t=Table('users',{'id':ColumnSpec(int,'int4',generated=True),'name':ColumnSpec(str,'text')},schema=schema)
    return ModelMapping(User,t,{'id':t.column('id',int),'name':t.column('name',str)},primary_key=('id',))


def test_generated_identity_rollback_restores_transient_values():
    m=mapping();user=User();store=StateStore();record=store.attach(m,user,new=True)
    store.flushed(record,{'id':10,'name':'new'});assert user.id==10
    store.rollback();assert user.id is None and record.state is ObjectState.TRANSIENT
    assert store.find(m,(10,)) is None


def test_identity_qualified_and_snapshot_rollback():
    one=mapping('one');two=mapping('two');store=StateStore()
    a=User(1,'a');b=User(1,'b');r=store.attach(one,a,new=False);store.attach(two,b,new=False)
    assert store.find(one,(1,)) is a and store.find(two,(1,)) is b
    a.name='dirty';assert store.dirty(r)=={'name':'dirty'}
    store.rollback();assert a.name=='a'
    with pytest.raises(OrmError): store.attach(one,User(1,'collision'),new=False)


def test_pk_change_and_unknown_commit_state():
    m=mapping();store=StateStore();user=User(1,'x');r=store.attach(m,user,new=False)
    user.id=2
    with pytest.raises(OrmError): store.dirty(r)
    user.id=1;store.uncertain()
    with pytest.raises(OrmError): store.find(m,(1,))


def test_mapping_metadata_immutable_and_init_false_refused():
    from dataclasses import FrozenInstanceError, field
    m=mapping()
    with pytest.raises(FrozenInstanceError): m.table=mapping('other').table
    @dataclass
    class Bad:
        id: int=field(init=False,default=1)
    t=Table('t',{'id':ColumnSpec(int,'int4')})
    with pytest.raises(ValueError): ModelMapping(Bad,t,{'id':t.column('id',int)},primary_key=('id',))

def test_same_object_cannot_join_two_stores_and_detach_releases():
    m=mapping();obj=User(1,'x');one=StateStore();two=StateStore()
    one.attach(m,obj,new=False)
    with pytest.raises(OrmError): two.attach(m,obj,new=False)
    one.detach_all();assert one.object_state(obj) is ObjectState.DETACHED
    two.attach(m,obj,new=False)

def test_timestamptz_pk_fold_normalization_matches_equivalent_utc():
    import datetime as dt
    from zoneinfo import ZoneInfo
    @dataclass
    class Moment:
        id: dt.datetime
    t=Table('moments',{'id':ColumnSpec(dt.datetime,'timestamptz')})
    m=ModelMapping(Moment,t,{'id':t.column('id',dt.datetime)},primary_key=('id',))
    store=StateStore()
    for fold in [0,1]:
        value=dt.datetime(2024,11,3,1,30,tzinfo=ZoneInfo('America/New_York'),fold=fold)
        obj=Moment(value);store.attach(m,obj,new=False)
        assert store.find(m,(value.astimezone(dt.timezone.utc),)) is obj
        assert store.find(m,(value,)) is obj

def test_nonfinite_numeric_pk_refused():
    from decimal import Decimal
    @dataclass
    class Numeric:
        id: Decimal
    t=Table('numeric_keys',{'id':ColumnSpec(Decimal,'numeric')})
    m=ModelMapping(Numeric,t,{'id':t.column('id',Decimal)},primary_key=('id',))
    with pytest.raises(ValueError): StateStore().attach(m,Numeric(Decimal('NaN')),new=False)
