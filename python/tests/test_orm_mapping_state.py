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
