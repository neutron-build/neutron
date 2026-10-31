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


def test_refresh_retains_identity_validates_before_mutation_and_restores_original():
    m=mapping();obj=User(1,'original');store=StateStore();record=store.attach(m,obj,new=False)
    store.refreshed(record,{'id':1,'name':'external'})
    assert obj.name=='external' and store.find(m,(1,)) is obj
    assert record.original['name']=='original'
    with pytest.raises(ValueError): store.refreshed(record,{'id':1,'name':17})
    with pytest.raises(OrmError): store.refreshed(record,{'id':2,'name':'changed identity'})
    assert obj.id==1 and obj.name=='external'
    store.rollback();assert obj.name=='original'


def test_detach_removes_identity_and_releases_object_owner_without_reconciliation():
    m=mapping();obj=User(1,'original');store=StateStore();record=store.attach(m,obj,new=False)
    store.detach(record)
    assert store.find(m,(1,)) is None and store.object_state(obj) is ObjectState.DETACHED
    other=StateStore();other.attach(m,obj,new=False)
    obj.name='second session';store.rollback()
    assert obj.name=='second session'
    other.detach_all()


def test_existing_attach_rechecks_ownership_before_mutation_and_uses_native_undo():
    m=mapping();obj=User(1,'caller');first=StateStore();second=StateStore()
    first.check_existing_attach(m,obj,m.snapshot(obj))
    second.attach(m,obj,new=False)
    with pytest.raises(OrmError): first.attach_existing(m,obj,{'id':1,'name':'native'})
    assert obj.name=='caller' and first.object_state(obj) is ObjectState.TRANSIENT
    second.detach_all();first.attach_existing(m,obj,{'id':1,'name':'native'})
    obj.name='local';first.rollback()
    assert obj.name=='native' and first.object_state(obj) is ObjectState.PERSISTENT
    first.detach_all()


def test_unknown_attach_baseline_semantics_preserve_json_types_and_instants():
    import datetime as dt
    from zoneinfo import ZoneInfo
    from neutron.orm import JsonDocument
    from neutron.orm.mapping import same_column_value
    json_spec=ColumnSpec(JsonDocument,'jsonb')
    assert same_column_value(json_spec,JsonDocument('{"n":1.0,"v":null}'),JsonDocument('{"v":null,"n":1}'))
    assert not same_column_value(json_spec,JsonDocument('true'),JsonDocument('1'))
    assert not same_column_value(json_spec,JsonDocument('[false,0]'),JsonDocument('[0,false]'))
    spec=ColumnSpec(dt.datetime,'timestamptz')
    folded=dt.datetime(2024,11,3,1,30,tzinfo=ZoneInfo('America/New_York'),fold=1)
    assert same_column_value(spec,folded,folded.astimezone(dt.timezone.utc))
    assert not same_column_value(spec,folded,folded.replace(fold=0))


def test_mapper_refuses_raising_normalizing_and_access_hooks_before_io():
    touched=[]
    @dataclass
    class Raising:
        id: int
        name: str
        def __setattr__(self,name,value):
            touched.append('raising setter')
            raise RuntimeError('custom setter')
    @dataclass
    class Normalizing:
        id: int
        name: str
        def __setattr__(self,name,value):
            touched.append('normalizing setter')
            object.__setattr__(self,name,value.upper() if isinstance(value,str) else value)
    @dataclass
    class Reading:
        id: int
        name: str
        def __getattribute__(self,name):
            touched.append('getter')
            return object.__getattribute__(self,name)
    @dataclass
    class Fallback:
        id: int
        name: str
        def __getattr__(self,name):
            touched.append('fallback getter')
            return 'invented'
    table=Table('users',{'id':ColumnSpec(int,'int4'),'name':ColumnSpec(str,'text')})
    columns={'id':table.column('id',int),'name':table.column('name',str)}
    for model in (Raising,Normalizing,Reading,Fallback):
        with pytest.raises(ValueError,match='attribute'): ModelMapping(model,table,columns,primary_key=('id',))
    assert touched==[]


def test_mapper_refuses_property_and_custom_descriptor_without_executing_them():
    touched=[]
    @dataclass
    class PropertyModel:
        id: int
        name: str
    def getter(obj): touched.append('property read');return 'normalized'
    def setter(obj,value): touched.append('property write');raise RuntimeError('no restore')
    PropertyModel.name=property(getter,setter)
    @dataclass
    class DescriptorModel:
        id: int
        name: str
    class NormalizingDescriptor:
        def __get__(self,obj,owner=None): touched.append('descriptor read');return 'normalized'
        def __set__(self,obj,value): touched.append('descriptor write');obj.__dict__['name']=value.upper()
    DescriptorModel.name=NormalizingDescriptor()
    table=Table('users',{'id':ColumnSpec(int,'int4'),'name':ColumnSpec(str,'text')})
    columns={'id':table.column('id',int),'name':table.column('name',str)}
    for model in (PropertyModel,DescriptorModel):
        with pytest.raises(ValueError,match='descriptors'): ModelMapping(model,table,columns,primary_key=('id',))
    assert touched==[]


def test_standard_slots_supported_and_late_attribute_profile_changes_refused():
    @dataclass(slots=True,weakref_slot=True)
    class Slotted:
        id: int
        name: str
    table=Table('users',{'id':ColumnSpec(int,'int4'),'name':ColumnSpec(str,'text')})
    columns={'id':table.column('id',int),'name':table.column('name',str)}
    mapper=ModelMapping(Slotted,table,columns,primary_key=('id',))
    obj=mapper.construct({'id':1,'name':'original'})
    mapper.restore(obj,{'id':1,'name':'restored'})
    assert mapper.snapshot(obj)=={'id':1,'name':'restored'}
    def changed_setter(obj,name,value): raise RuntimeError('late model mutation')
    Slotted.__setattr__=changed_setter
    with pytest.raises(ValueError,match='attribute'): mapper.restore(obj,{'id':1,'name':'forbidden'})
    assert object.__getattribute__(obj,'name')=='restored'
