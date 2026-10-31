from dataclasses import dataclass
import pytest
from neutron.orm import ColumnSpec,ExpiredAttributeError,ModelMapping,OrmError,Table
from neutron.orm.instrumentation import expire_attributes

@dataclass(slots=True,weakref_slot=True)
class Slotted:
    id: int
    value: str='default'


def test_finite_slot_instrumentation_preserves_native_storage_and_expired_flags():
    table=Table('slotted',{'id':ColumnSpec(int,'int4'),'value':ColumnSpec(str,'text')})
    mapping=ModelMapping(Slotted,table,dict(table.columns),primary_key=('id',),instrumented=True)
    obj=Slotted(1);assert obj.value=='default'
    expire_attributes(obj,frozenset({'value'}))
    with pytest.raises(ExpiredAttributeError): _=obj.value
    assert mapping._snapshot(obj)=={'id':1,'value':'default'}
    mapping.restore(obj,{'id':1,'value':'restored'});assert obj.value=='restored'
    with pytest.raises(OrmError): del obj.value
