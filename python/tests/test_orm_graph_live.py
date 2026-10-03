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


def test_native_owned_delete_cascade_orphan_and_rollback(graph):
    from neutron.orm import OwnedRelation
    url,read_relation,native=graph
    relation=OwnedRelation(read_relation.parent,read_relation.child,read_relation.parent_fields,read_relation.child_fields,on_delete='delete',orphan_delete=True)
    with Session.connect(url) as session:
        parent=Parent();a=Child(label='a');b=Child(label='b');session.add_graph(relation,parent,[a,b]);session.commit()
        session.remove_related(relation,parent,a);session.flush();session.rollback()
        assert session.object_state(a) is ObjectState.PERSISTENT
        assert native.execute(f'SELECT count(*) FROM {relation.child.table.sql}').fetchone()==(2,)
        session.delete_graph(relation,parent,budget=LoadBudget(1,10,10));session.flush();session.rollback()
        assert session.object_state(parent) is ObjectState.PERSISTENT
        assert native.execute(f'SELECT count(*) FROM {relation.child.table.sql}').fetchone()==(2,)
        session.delete_graph(relation,parent,budget=LoadBudget(1,10,10));session.commit()
        assert native.execute(f'SELECT count(*) FROM {relation.parent.table.sql}').fetchone()==(0,)
        assert native.execute(f'SELECT count(*) FROM {relation.child.table.sql}').fetchone()==(0,)

@dataclass
class NullableChild:
    id: int|None=None
    parent_id: int|None=None
    label: str='child'

@pytest.mark.asyncio
async def test_native_async_nullable_disconnect_and_nullify_ownership(live_table):
    from neutron.orm import OwnedRelation
    url,base,native=live_table
    p=Table('owned_parent',{'id':ColumnSpec(int,'int4',generated=True),'name':ColumnSpec(str,'text')},schema=base.schema)
    c=Table('owned_nullable',{'id':ColumnSpec(int,'int4',generated=True),'parent_id':ColumnSpec(int,'int4',nullable=True),'label':ColumnSpec(str,'text')},schema=base.schema)
    native.execute(f'CREATE TABLE {p.sql}(id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,name text NOT NULL)')
    native.execute(f'CREATE TABLE {c.sql}(id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,parent_id int REFERENCES {p.sql}(id),label text NOT NULL)')
    pm=ModelMapping(Parent,p,dict(p.columns),primary_key=('id',));cm=ModelMapping(NullableChild,c,dict(c.columns),primary_key=('id',))
    rel=OwnedRelation(pm,cm,('id',),('parent_id',),on_delete='nullify')
    async with await AsyncSession.connect(url) as session:
        parent=Parent();child=NullableChild();session.add_graph(rel,parent,[child]);await session.commit()
        session.disconnect(rel,parent,child);await session.flush();await session.rollback()
        assert child.parent_id==parent.id
        await session.delete_graph(rel,parent,budget=LoadBudget(1,10,10));await session.commit()
        assert native.execute(f'SELECT parent_id FROM {c.sql}').fetchone()==(None,)
        assert native.execute(f'SELECT count(*) FROM {p.sql}').fetchone()==(0,)

@dataclass
class Account:
    tenant: str
    id: int|None=None
    name: str='account'

@dataclass
class Group:
    tenant: str
    id: int|None=None
    name: str='group'

@dataclass
class Membership:
    tenant: str|None=None
    account_id: int|None=None
    group_id: int|None=None
    role: str='member'

@pytest.fixture
def many_graph(live_table):
    from neutron.orm import ManyToMany
    url,base,native=live_table
    common={'tenant':ColumnSpec(str,'text'),'id':ColumnSpec(int,'int4',generated=True),'name':ColumnSpec(str,'text')}
    a=Table('accounts',common,schema=base.schema);g=Table('groups',common,schema=base.schema)
    t=Table('memberships',{'tenant':ColumnSpec(str,'text'),'account_id':ColumnSpec(int,'int4'),'group_id':ColumnSpec(int,'int4'),'role':ColumnSpec(str,'text')},schema=base.schema)
    for table in (a,g): native.execute(f'CREATE TABLE {table.sql}(tenant text NOT NULL,id int GENERATED ALWAYS AS IDENTITY,name text NOT NULL,PRIMARY KEY(tenant,id))')
    native.execute(f'CREATE TABLE {t.sql}(tenant text NOT NULL,account_id int NOT NULL,group_id int NOT NULL,role text NOT NULL,PRIMARY KEY(tenant,account_id,group_id),FOREIGN KEY(tenant,account_id) REFERENCES {a.sql}(tenant,id),FOREIGN KEY(tenant,group_id) REFERENCES {g.sql}(tenant,id))')
    am=ModelMapping(Account,a,dict(a.columns),primary_key=('tenant','id'))
    gm=ModelMapping(Group,g,dict(g.columns),primary_key=('tenant','id'))
    tm=ModelMapping(Membership,t,dict(t.columns),primary_key=('tenant','account_id','group_id'))
    meta=ManyToMany(Relation(am,tm,('tenant','id'),('tenant','account_id')),Relation(gm,tm,('tenant','id'),('tenant','group_id')))
    return url,meta,native


def test_native_many_to_many_generated_composite_identity_and_disconnect(many_graph):
    from neutron.orm import ConflictError
    url,meta,native=many_graph
    with Session.connect(url) as session:
        account=Account('a');group=Group('a');through=Membership()
        session.add(meta.parent.parent,account);session.add(meta.target.parent,group)
        session.connect_many_to_many(meta,account,group,through);session.flush();session.rollback()
        assert account.id is None and group.id is None and through.tenant is None
        session.add(meta.parent.parent,account);session.add(meta.target.parent,group)
        session.connect_many_to_many(meta,account,group,through);session.commit()
        assert (through.tenant,through.account_id,through.group_id)==('a',account.id,group.id)
        assert native.execute(f'SELECT tenant,account_id,group_id,role FROM {meta.parent.child.table.sql}').fetchone()==('a',account.id,group.id,'member')
        loaded=session.load_relation(meta.parent,[account],budget=LoadBudget(1,10,10))
        assert loaded[0].children[0] is through
        duplicate=Membership()
        session.connect_many_to_many(meta,account,group,duplicate)
        with pytest.raises(OrmError) as conflict: session.flush()
        assert conflict.value.sqlstate=='23505'
        session.rollback();assert duplicate.tenant is None
        foreign=Group('b');session.add(meta.target.parent,foreign);session.commit()
        with pytest.raises(ConflictError): session.connect_many_to_many(meta,account,foreign,Membership())
        session.rollback()
        assert native.execute(f'SELECT count(*) FROM {meta.parent.child.table.sql}').fetchone()==(1,)
        session.disconnect_many_to_many(meta,account,group,through);session.flush();session.rollback()
        assert session.object_state(through) is ObjectState.PERSISTENT
        session.disconnect_many_to_many(meta,account,group,through);session.commit()
        assert native.execute(f'SELECT count(*) FROM {meta.parent.child.table.sql}').fetchone()==(0,)
        assert native.execute(f'SELECT count(*) FROM {meta.target.parent.table.sql}').fetchone()==(2,)

@pytest.mark.asyncio
async def test_native_async_many_to_many_new_targets_and_through_rows(many_graph):
    url,meta,native=many_graph
    async with await AsyncSession.connect(url) as session:
        account=Account('a');group=Group('a');through=Membership()
        session.add(meta.parent.parent,account);session.add(meta.target.parent,group)
        session.connect_many_to_many(meta,account,group,through);await session.commit()
        assert native.execute(f'SELECT tenant,account_id,group_id FROM {meta.parent.child.table.sql}').fetchone()==('a',account.id,group.id)
        session.disconnect_many_to_many(meta,account,group,through);await session.commit()
        assert native.execute(f'SELECT count(*) FROM {meta.parent.child.table.sql}').fetchone()==(0,)

@dataclass
class Leaf:
    id: int|None=None
    child_id: int|None=None
    label: str='leaf'

@pytest.fixture
def deep_graph(graph):
    from neutron.orm import OwnedRelation
    url,relation,native=graph
    table=Table('graph_leaves',{'id':ColumnSpec(int,'int4',generated=True),'child_id':ColumnSpec(int,'int4'),'label':ColumnSpec(str,'text')},schema=relation.parent.table.schema)
    native.execute(f'CREATE TABLE {table.sql}(id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,child_id int NOT NULL REFERENCES {relation.child.table.sql}(id),label text NOT NULL)')
    mapping=ModelMapping(Leaf,table,dict(table.columns),primary_key=('id',))
    root=OwnedRelation(relation.parent,relation.child,('id',),('parent_id',),on_delete='delete')
    descendant=OwnedRelation(relation.child,mapping,('id',),('child_id',),on_delete='delete')
    return url,root,descendant,native


def test_native_registered_multilevel_cascade_budgets_and_rollback(deep_graph):
    from neutron.orm import RelationBudgetError,OwnedRelation
    url,root,descendant,native=deep_graph
    with Session.connect(url) as session:
        parent=Parent();child=Child();leaf=Leaf()
        session.add_graph(root,parent,[child]);session.add_graph(descendant,child,[leaf]);session.commit()
        with pytest.raises(RelationBudgetError):
            session.delete_graph(root,parent,budget=LoadBudget(2,10,10),descendants=(descendant,))
        assert all(session.object_state(obj) is ObjectState.PERSISTENT for obj in (parent,child,leaf))
        session.rollback()
        with pytest.raises(RelationBudgetError):
            session.delete_graph(root,parent,budget=LoadBudget(3,1,10),descendants=(descendant,))
        assert all(session.object_state(obj) is ObjectState.PERSISTENT for obj in (parent,child,leaf))
        session.rollback()
        restricted=OwnedRelation(descendant.parent,descendant.child,descendant.parent_fields,descendant.child_fields)
        with pytest.raises(OrmError,match='restricts'):
            session.delete_graph(root,parent,budget=LoadBudget(3,10,10),descendants=(restricted,))
        assert all(session.object_state(obj) is ObjectState.PERSISTENT for obj in (parent,child,leaf))
        session.rollback()
        session.delete_graph(root,parent,budget=LoadBudget(3,10,10),descendants=(descendant,));session.flush();session.rollback()
        assert all(session.object_state(obj) is ObjectState.PERSISTENT for obj in (parent,child,leaf))
        session.delete_graph(root,parent,budget=LoadBudget(3,10,10),descendants=(descendant,));session.commit()
        assert all(native.execute(f'SELECT count(*) FROM {mapping.table.sql}').fetchone()==(0,) for mapping in (root.parent,root.child,descendant.child))

@pytest.mark.asyncio
async def test_native_async_registered_multilevel_cascade(deep_graph):
    from neutron.orm import RelationBudgetError
    url,root,descendant,native=deep_graph
    async with await AsyncSession.connect(url) as session:
        parent=Parent();child=Child();leaf=Leaf()
        session.add_graph(root,parent,[child]);session.add_graph(descendant,child,[leaf]);await session.commit()
        with pytest.raises(RelationBudgetError):
            await session.delete_graph(root,parent,budget=LoadBudget(3,10,10),descendants=(descendant,),max_depth=1)
        assert all(session.object_state(obj) is ObjectState.PERSISTENT for obj in (parent,child,leaf))
        await session.rollback()
        await session.delete_graph(root,parent,budget=LoadBudget(3,10,10),descendants=(descendant,));await session.commit()
        assert all(native.execute(f'SELECT count(*) FROM {mapping.table.sql}').fetchone()==(0,) for mapping in (root.parent,root.child,descendant.child))

@dataclass
class CycleNode:
    id: int
    parent_id: int


def test_native_owned_cycle_refuses_before_delete_marking(live_table):
    from neutron.orm import OwnedRelation
    url,base,native=live_table
    table=Table('graph_cycle',{'id':ColumnSpec(int,'int4'),'parent_id':ColumnSpec(int,'int4')},schema=base.schema)
    native.execute(f'CREATE TABLE {table.sql}(id int PRIMARY KEY,parent_id int NOT NULL REFERENCES {table.sql}(id) DEFERRABLE INITIALLY DEFERRED)')
    with native.transaction():
        native.execute(f'INSERT INTO {table.sql} VALUES (1,2),(2,1)')
    mapping=ModelMapping(CycleNode,table,dict(table.columns),primary_key=('id',))
    relation=OwnedRelation(mapping,mapping,('id',),('parent_id',),on_delete='delete')
    with Session.connect(url) as session:
        root=session.get(mapping,1);other=session.get(mapping,2)
        assert root is not None and other is not None
        with pytest.raises(OrmError,match='cyclic ownership'):
            session.delete_graph(relation,root,budget=LoadBudget(10,10,10))
        assert session.object_state(root) is ObjectState.PERSISTENT and session.object_state(other) is ObjectState.PERSISTENT
        session.rollback()
        assert native.execute(f'SELECT count(*) FROM {table.sql}').fetchone()==(2,)
