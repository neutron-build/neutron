from dataclasses import dataclass
import pytest
from neutron.orm import ColumnSpec,ModelMapping,PolymorphicMapping,PolymorphicView,Table

@dataclass
class Animal:
    id: int|None=None
    kind: str='animal'
    name: str='animal'

@dataclass
class Cat(Animal):
    kind: str='cat'
    lives: int=9

@dataclass
class Dog(Animal):
    kind: str='dog'
    breed: str='unknown'


def family(table=None,*,instrumented=False):
    table=table or Table('animals',{'id':ColumnSpec(int,'int4',generated=True),'kind':ColumnSpec(str,'text'),'name':ColumnSpec(str,'text'),'lives':ColumnSpec(int,'int4',nullable=True),'breed':ColumnSpec(str,'text',nullable=True)})
    return PolymorphicMapping(Animal,table,dict(table.columns),primary_key=('id',),discriminator='kind',variants={'animal':Animal,'cat':Cat,'dog':Dog},instrumented=instrumented)


def test_registry_shapes_and_immutable_discriminator_containment():
    mapping=family();cat=Cat(name='milo')
    assert mapping.snapshot(cat)=={'id':None,'kind':'cat','name':'milo','lives':9,'breed':None}
    assert mapping.construct({'id':1,'kind':'dog','name':'rex','lives':None,'breed':'collie'})==Dog(id=1,name='rex',breed='collie')
    assert mapping.subtype(Cat).query().predicate.params==('cat',)
    cat.kind='dog'
    with pytest.raises(ValueError,match='discriminator'): mapping.snapshot(cat)
    mapping.restore(cat,{'id':None,'kind':'cat','name':'milo','lives':9,'breed':None})
    with pytest.raises(ValueError,match='unknown'): mapping.construct({'id':1,'kind':'unknown','name':'x','lives':None,'breed':None})
    with pytest.raises(ValueError,match='inactive'): mapping.construct({'id':1,'kind':'cat','name':'x','lives':9,'breed':'dog'})
    with pytest.raises(ValueError,match='required'): mapping.construct({'id':1,'kind':'cat','name':'x','lives':None,'breed':None})
    with pytest.raises(ValueError): PolymorphicView(mapping,Dog,'cat')
    with pytest.raises(ValueError): ModelMapping(Cat,mapping.table,{name:column for name,column in mapping.field_columns.items() if name!='breed'},primary_key=('id',))


def test_inherited_library_instrumentation_owns_each_registered_class():
    mapping=family(instrumented=True)
    cat=Cat(name='milo');dog=Dog(name='rex');base=Animal(name='base')
    assert mapping.snapshot(cat)['lives']==9 and mapping.snapshot(dog)['breed']=='unknown'
    assert mapping.active_fields(base)==('id','kind','name')
    mapping.restore(cat,{'id':1,'kind':'cat','name':'milo','lives':8,'breed':None})
    assert cat.lives==8
    # Registering the same family again does not inherit a foreign descriptor.
    again=family(instrumented=True);assert again.snapshot(cat)['lives']==8
