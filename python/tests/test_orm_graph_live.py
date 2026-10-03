from dataclasses import dataclass
import pytest
from neutron.orm import AsyncSession,ColumnSpec,LoadBudget,ModelMapping,ObjectState,OrmError,Relation,Session,Table
from .test_orm_live import live_table

@dataclass
class Parent:
    id: int|None=None
    name: str='parent'

@dataclass
class Child:
    id: int|None=None
    parent_id: int|None=None
    label: str='child'

@pytest.fixture
def graph(live_table):
    url,base,native=live_table
    p=Table('graph_parents',{'id':ColumnSpec(int,'int4',generated=True),'name':ColumnSpec(str,'text')},schema=base.schema)
    c=Table('graph_children',{'id':ColumnSpec(int,'int4',generated=True),'parent_id':ColumnSpec(int,'int4'),'label':ColumnSpec(str,'text')},schema=base.schema)
    native.execute(f'CREATE TABLE {p.sql}(id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,name text NOT NULL)')
    native.execute(f'CREATE TABLE {c.sql}(id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,parent_id int NOT NULL REFERENCES {p.sql}(id),label text NOT NULL UNIQUE)')
    pm=ModelMapping(Parent,p,dict(p.columns),primary_key=('id',))
    cm=ModelMapping(Child,c,dict(c.columns),primary_key=('id',))
    return url,Relation(pm,cm,('id',),('parent_id',)),native


def test_native_graph_generated_dependencies_and_complete_rollback(graph):
    url,relation,native=graph
    with Session.connect(url) as session:
        parent=Parent();children=[Child(label='a'),Child(label='b')]
        session.add_graph(relation,parent,children);session.flush()
        assert parent.id is not None and all(child.parent_id==parent.id and child.id is not None for child in children)
        assert native.execute(f'SELECT count(*) FROM {relation.child.table.sql}').fetchone()==(0,)
        session.rollback();assert parent.id is None
        assert all(child.id is None and child.parent_id is None for child in children)
        assert all(session.object_state(child) is ObjectState.TRANSIENT for child in children)
        session.add_graph(relation,parent,children);session.commit()
        assert native.execute(f'SELECT p.name,c.label FROM {relation.parent.table.sql} p JOIN {relation.child.table.sql} c ON c.parent_id=p.id ORDER BY c.label').fetchall()==[('parent','a'),('parent','b')]
        loaded=session.load_relation(relation,[parent,parent],budget=LoadBudget(2,4,2))
        assert loaded[0].children[0] is loaded[1].children[0]
        assert loaded[0].children[0] is children[0]


def test_native_graph_late_constraint_failure_leaves_no_parent_or_child(graph):
    url,relation,native=graph
    with Session.connect(url) as session:
        parent=Parent();first=Child(label='same');last=Child(label='same')
        session.add_graph(relation,parent,[first,last])
        with pytest.raises(OrmError) as caught: session.flush()
        assert caught.value.sqlstate=='23505'
        session.rollback()
        assert parent.id is None and first.id is None and first.parent_id is None
        assert native.execute(f'SELECT count(*) FROM {relation.parent.table.sql}').fetchone()==(0,)
        assert native.execute(f'SELECT count(*) FROM {relation.child.table.sql}').fetchone()==(0,)

@pytest.mark.asyncio
async def test_native_async_graph_dependency_and_identity_loader(graph):
    url,relation,native=graph
    async with await AsyncSession.connect(url) as session:
        parent=Parent();child=Child(label='async')
        session.add_graph(relation,parent,[child]);await session.commit()
        loaded=await session.load_relation(relation,[parent,parent],budget=LoadBudget(2,2,2))
        assert loaded[0].children[0] is loaded[1].children[0] is child
        assert native.execute(f'SELECT parent_id FROM {relation.child.table.sql}').fetchone()==(parent.id,)


def test_graph_cycle_refuses_before_first_native_write(graph):
    url,relation,native=graph
    with Session.connect(url) as session:
        # Dependency graph validation operates before opening a transaction.
        parent=Parent();child=Child(label='child');session.add_graph(relation,parent,[child])
        reverse=Relation(relation.child,relation.parent,('id',),('id',))
        # Generated child targets are refused even before cycle planning.
        with pytest.raises(OrmError,match='generated'): session.link(reverse,child,parent)
        session.rollback()
        assert native.execute(f'SELECT count(*) FROM {relation.parent.table.sql}').fetchone()==(0,)

@dataclass
class Node:
    id: int|None=None
    parent_id: int=0


def test_native_required_fk_cycle_refuses_before_opening_transaction(live_table):
    url,base,native=live_table
    t=Table('graph_nodes',{'id':ColumnSpec(int,'int4',generated=True),'parent_id':ColumnSpec(int,'int4')},schema=base.schema)
    native.execute(f'CREATE TABLE {t.sql}(id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,parent_id int NOT NULL REFERENCES {t.sql}(id))')
    mapping=ModelMapping(Node,t,dict(t.columns),primary_key=('id',))
    relation=Relation(mapping,mapping,('id',),('parent_id',))
    with Session.connect(url) as session:
        a=Node();b=Node();session.add(mapping,a);session.add(mapping,b)
        session.link(relation,a,b);session.link(relation,b,a)
        with pytest.raises(OrmError,match='cyclic'): session.flush()
        assert session._transaction is None
        session.rollback();assert a.id is None and b.id is None
    assert native.execute(f'SELECT count(*) FROM {t.sql}').fetchone()==(0,)
