import pytest
from neutron.orm import AsyncDatabase, Database, CardinalityError, LoadBudget, Order, Relation, RelationBudgetError, async_load_many, async_load_one, load_many, load_one
from .test_orm_live import live_table
from .test_orm_relations import Parent,mappings


def setup_relations(parent,native):
    pm,cm=mappings(parent.schema)
    native.execute(f'CREATE TABLE {pm.table.sql} (id int NOT NULL, tenant text NOT NULL, PRIMARY KEY(id,tenant))')
    native.execute(f'CREATE TABLE {cm.table.sql} (id int PRIMARY KEY,parent_id int NOT NULL,tenant text NOT NULL,label text NOT NULL)')
    native.execute(f"INSERT INTO {cm.table.sql} VALUES (10,1,'a','one'),(11,1,'a','two'),(12,1,'b','other')")
    return Relation(pm,cm,('id','tenant'),('parent_id','tenant'))


def test_native_composite_relations_oracle_and_expanded_budget(live_table):
    url,parent,native=live_table;rel=setup_relations(parent,native)
    parents=[Parent(1,'a'),Parent(1,'b'),Parent(1,'a'),Parent(99,'missing')]
    query=rel.query().order_by(Order(rel.child.table.column('id',int),True))
    with Database.connect(url) as db:
        result=load_many(db,rel,parents,budget=LoadBudget(4,5,1),query=query)
        expected=[]
        for p in parents:
            expected.append(native.execute(f'SELECT id FROM {rel.child.table.sql} WHERE parent_id=%s AND tenant=%s ORDER BY id DESC',(p.id,p.tenant)).fetchall())
        assert [[(child.id,) for child in item.children] for item in result]==expected
        assert result[0].children[0] is not result[2].children[0]
        with pytest.raises(RelationBudgetError): load_many(db,rel,parents,budget=LoadBudget(4,4,1),query=query)
        with pytest.raises(CardinalityError): load_one(db,rel,[parents[0]],budget=LoadBudget(1,5,1))
        singular=load_one(db,rel,[parents[1]],budget=LoadBudget(1,1,1))
        assert singular[0].children[0].id==12
        filtered=load_many(db,rel,parents,budget=LoadBudget(4,2,2),query=rel.query().where(rel.child.table.column('label',str).eq('one')))
        assert [[child.id for child in item.children] for item in filtered]==[[10],[],[10],[]]

@pytest.mark.asyncio
async def test_native_async_relation_batching(live_table):
    url,parent,native=live_table;rel=setup_relations(parent,native)
    async with await AsyncDatabase.connect(url) as db:
        result=await async_load_many(db,rel,[Parent(1,'b'),Parent(99,'missing')],budget=LoadBudget(2,1,1))
        assert [[child.id for child in item.children] for item in result]==[[12],[]]
        with pytest.raises(CardinalityError): await async_load_one(db,rel,[Parent(1,'a')],budget=LoadBudget(1,3,1))


def test_native_uuid_bool_empty_key_relations(live_table):
    from uuid import UUID
    from .test_orm_relations import ScalarParent,scalar_mappings
    url,parent,native=live_table;pm,cm=scalar_mappings(parent.schema)
    native.execute(f'CREATE TABLE {cm.table.sql} (id int PRIMARY KEY,token uuid NOT NULL,flag bool NOT NULL,label text NOT NULL)')
    uid=UUID('00000000-0000-0000-0000-000000000001')
    native.execute(f'INSERT INTO {cm.table.sql} VALUES (%s,%s,%s,%s)',(10,uid,False,''))
    rel=Relation(pm,cm,('token','flag','label'),('token','flag','label'))
    with Database.connect(url) as db:
        result=load_many(db,rel,[ScalarParent(1,uid,False,''),ScalarParent(2,uid,True,'')],budget=LoadBudget(2,1,1))
        assert [[child.id for child in item.children] for item in result]==[[10],[]]
        assert native.execute(f'SELECT id,token,flag,label FROM {cm.table.sql}').fetchall()==[(10,uid,False,'')]
