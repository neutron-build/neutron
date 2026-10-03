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


def test_native_self_alias_correlated_exists_and_in_oracle(live_table):
    from neutron.orm import alias,exists,in_query
    url,parent,native=live_table
    native.execute(f'INSERT INTO {parent.sql}(id) VALUES (1),(2),(3)')
    a=alias(parent,'a%s');b=alias(parent,'b')
    aid=a.column('id',int);bid=b.column('id',int)
    sub=query_from(b).correlate(a).select(field(bid)).where(bid.eq(aid)).where(bid.in_([1,3]))
    q=query_from(a).select(field(aid)).where(exists(sub)).order_by(Order(aid))
    with Database.connect(url) as db:
        assert db.all(q)==[row[0] for row in native.execute(f'SELECT a.id FROM {parent.sql} a WHERE EXISTS (SELECT 1 FROM {parent.sql} b WHERE b.id=a.id AND b.id IN (1,3)) ORDER BY a.id').fetchall()]==[1,3]
        assert db.all(query_from(a).select(field(aid)).where(in_query(aid,sub)).order_by(Order(aid)))==[1,3]
        joined=query_from(a).inner_join(b,on=aid.eq(bid)).select_pair(field(aid),field(bid)).order_by(Order(aid))
        assert db.all(joined)==[(1,1),(2,2),(3,3)]


def test_native_promoted_aggregate_group_window_and_set_oracle(live_table):
    from decimal import Decimal
    from neutron.orm import avg,count,max_value,min_value,row_number,sum_value
    url,parent,native=live_table
    native.execute(f'CREATE TABLE "{parent.schema}".metrics(group_id int NOT NULL,value int NOT NULL,wide bigint NOT NULL,exact numeric NOT NULL)')
    t=Table('metrics',{'group_id':ColumnSpec(int,'int4'),'value':ColumnSpec(int,'int4'),'wide':ColumnSpec(int,'int8'),'exact':ColumnSpec(Decimal,'numeric')},schema=parent.schema)
    native.execute(f'INSERT INTO {t.sql} VALUES (1,2147483647,9223372036854775807,0.1),(1,2147483647,9223372036854775807,0.2),(2,1,1,0.3)')
    group=t.column('group_id',int);value=t.column('value',int);wide=t.column('wide',int);exact=t.column('exact',Decimal)
    with Database.connect(url) as db:
        assert db.one(query_from(t).select(sum_value(value)))==4294967295
        assert db.one(query_from(t).select(sum_value(wide)))==Decimal(18446744073709551615)
        assert db.one(query_from(t).select(sum_value(exact)))==Decimal('0.6')
        assert db.one(query_from(t).select(avg(value)))==native.execute(f'SELECT avg(value) FROM {t.sql}').fetchone()[0]
        for projection in (sum_value(value),min_value(value),max_value(value)):
            assert db.one(query_from(t).select(projection).where(group.eq(999))) is None
        assert db.one(query_from(t).select(count(value)).where(group.eq(999)))==0
        total=sum_value(value)
        grouped=query_from(t).select_pair(field(group),total).group_by(group).having(total.gt(2)).order_by(Order(group))
        assert db.all(grouped)==native.execute(f'SELECT group_id,sum(value) FROM {t.sql} GROUP BY group_id HAVING sum(value)>2 ORDER BY group_id').fetchall()==[(1,4294967294)]
        window=query_from(t).select_pair(field(group),row_number(t,partition_by=(group,),order_by=(Order(value),))).order_by(Order(group),Order(value))
        assert db.all(window)==native.execute(f'SELECT group_id,row_number() OVER (PARTITION BY group_id ORDER BY value) FROM {t.sql} ORDER BY group_id,value').fetchall()
        first=query_from(t).select(field(group)).where(group.eq(1));second=query_from(t).select(field(group)).where(group.in_([1,2]))
        assert sorted(db.all(first.union(second)))==[1,2]
        assert sorted(db.all(first.union(second,all=True)))==[1,1,1,1,2]
        assert db.all(first.intersect(second))==[1]
        assert db.all(second.except_(first))==[2]
        assert sorted(db.all(query_from(t).select(field(group)).distinct()))==[1,2]


def test_native_derived_and_cte_projection_bind_oracle(live_table):
    from neutron.orm import cte,derived
    url,parent,native=live_table
    native.execute(f'INSERT INTO {parent.sql}(id) VALUES (1),(2),(3)')
    col=parent.column('id',int)
    source=query_from(parent).select(field(col)).where(col.in_([1,3]))
    with Database.connect(url) as db:
        for factory in (derived,cte):
            projected=factory(source,'result%s',labels=('identity',))
            key=projected.column('identity',int)
            q=query_from(projected).select(field(key)).where(key.eq(3))
            assert db.all(q)==[row[0] for row in native.execute(f'SELECT id FROM {parent.sql} WHERE id IN (%s,%s) AND id=%s',(1,3,3)).fetchall()]==[3]
