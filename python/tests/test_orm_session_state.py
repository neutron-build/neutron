from dataclasses import dataclass
import pytest
from neutron.orm import ColumnSpec, Database, ModelMapping, ObjectState, OrmError, Session, Table
from .test_orm_clients import Connection

@dataclass
class User:
    id: int | None=None
    name: str='new'


def mapping():
    t=Table('t',{'id':ColumnSpec(int,'int4',generated=True),'name':ColumnSpec(str,'text')})
    return ModelMapping(User,t,{'id':t.column('id',int),'name':t.column('name',str)},primary_key=('id',))


def test_real_session_object_ownership_releases_without_io():
    mapper=mapping();user=User();one=Session(Database(Connection([])));two=Session(Database(Connection([])))
    one.add(mapper,user)
    with pytest.raises(OrmError,match='another Session'): two.add(mapper,user)
    one.rollback();assert one.object_state(user) is ObjectState.TRANSIENT
    two.add(mapper,user);two.close();one.close()


def test_fenced_database_blocks_identity_reads_and_mapping_conflicts():
    mapper=mapping();db=Database(Connection([]));session=Session(db)
    session.add(mapper,User())
    with pytest.raises(OrmError,match='different metadata'): session.add(mapping(),User())
    db.close()
    with pytest.raises(OrmError,match='fenced'): session.get(mapper,1)
    session.close()
