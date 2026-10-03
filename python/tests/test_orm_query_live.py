import pytest
from neutron.orm import AsyncDatabase, Database, ColumnSpec, Order, Table, field, outer_field, query_from
from .test_orm_live import live_table


def setup_join(parent,native):
    other=Table('children',{'id':ColumnSpec(int,'int4'),'owner':ColumnSpec(int,'int4')},schema=parent.schema)
    native.execute(f'CREATE TABLE {other.sql} (id int NOT NULL, owner int NOT NULL)')
    native.execute(f'INSERT INTO {parent.sql} (id) VALUES (1),(2),(3)')
    native.execute(f'INSERT INTO {other.sql} VALUES (11,1),(12,1),(21,2)')
    aid=parent.column('id',int); bid=other.column('id',int)
    scope=query_from(parent).left_join(other,on=aid.eq(other.column('owner',int)))
    q=scope.select_pair(field(aid),outer_field(bid)).order_by(Order(aid),Order(bid))
    expected=native.execute(f'SELECT a.id,b.id FROM {parent.sql} a LEFT JOIN {other.sql} b ON a.id=b.owner ORDER BY a.id,b.id NULLS LAST').fetchall()
    return q,expected


def test_native_join_oracle_and_exact_one(live_table):
    url,parent,native=live_table; q,expected=setup_join(parent,native)
    with Database.connect(url) as db:
        assert db.all(q)==expected==[(1,11),(1,12),(2,21),(3,None)]
        assert db.all(q.limit(2).offset(1))==expected[1:3]
        for paginated in (q.limit(1),q.offset(3)):
            with pytest.raises(ValueError): db.one(paginated)
            with pytest.raises(ValueError): db.one_or_none(paginated)

@pytest.mark.asyncio
async def test_native_async_join_oracle(live_table):
    url,parent,native=live_table; q,expected=setup_join(parent,native)
    async with await AsyncDatabase.connect(url) as db:
        assert await db.all(q)==expected
        with pytest.raises(ValueError): await db.one(q.limit(1))


def test_forged_join_predicate_refused_before_dispatch(live_table):
    from neutron.orm import Column
    url,parent,native=live_table
    forged=Column(parent,'id',ColumnSpec(int,'int4'))
    with Database.connect(url) as db:
        with pytest.raises(ValueError): forged.eq(1)
        with pytest.raises(ValueError): parent.column('id',int).eq(forged)
        assert native.execute(f'SELECT count(*) FROM {parent.sql}').fetchone()==(0,)
