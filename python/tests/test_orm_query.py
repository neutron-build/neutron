import pytest
from neutron.orm import ColumnSpec, Order, Table, field, outer_field, query_from


def tables():
    a=Table('same',{'id':ColumnSpec(int,'int4')},schema='one%s')
    b=Table('same',{'id':ColumnSpec(int,'int4'),'owner':ColumnSpec(int,'int4')},schema='two')
    return a,b


def test_join_projection_owned_aliases_and_bound_order():
    a,b=tables(); aid=a.column('id',int); bid=b.column('id',int)
    q=query_from(a).left_join(b,on=aid.eq(b.column('owner',int))).select_pair(field(aid),outer_field(bid)).where(aid.eq(7)).order_by(Order(bid,True,True)).limit(3).offset(0)
    compiled=q.compile()
    assert 'AS "p0"' in compiled.sql and 'AS "p1"' in compiled.sql
    assert '"one%%s"."same"."id"' in compiled.sql
    assert compiled.params==(7,3,0)
    assert compiled.decode({'p0':7,'p1':None})==(7,None)
    assert compiled.decode({'p0':7,'p1':9})==(7,9)
    with pytest.raises(ValueError): compiled.decode({'p0':None,'p1':None})


def test_invalid_scope_and_nullability():
    a,b=tables(); aid=a.column('id',int); bid=b.column('id',int)
    scope=query_from(a).left_join(b,on=aid.eq(bid))
    with pytest.raises(ValueError): scope.select(field(bid)).compile()
    with pytest.raises(ValueError): query_from(a).select(outer_field(aid)).compile()
    with pytest.raises(ValueError): query_from(a).inner_join(b,on=bid.eq(1))
    with pytest.raises(ValueError): query_from(a).inner_join(a,on=aid.eq(aid))
    with pytest.raises(ValueError): query_from(a).select(field(bid)).compile()
    with pytest.raises(ValueError): query_from(a).select(field(aid)).order_by(Order(bid)).compile()
    with pytest.raises(ValueError): query_from(a).select(field(aid)).where(bid.eq(2))
    for value in (True,-1,1.5):
        with pytest.raises(ValueError): query_from(a).select(field(aid)).limit(value)


def test_forged_predicate_columns_refused():
    from neutron.orm import Column
    a,b=tables(); aid=a.column('id',int)
    forged=Column(a,'id',ColumnSpec(str,'text'))
    with pytest.raises(ValueError): forged.eq('wrong')
    with pytest.raises(ValueError): forged.in_(['wrong'])
    with pytest.raises(ValueError): aid.eq(Column(a,'id',ColumnSpec(int,'int4')))


def test_self_alias_and_explicit_correlated_exists_parameter_order():
    from neutron.orm import alias,exists,in_query,insert
    a,_=tables();outer=alias(a,'outer%s');inner=alias(a,'inner')
    outer_id=outer.column('id',int);inner_id=inner.column('id',int)
    sub=query_from(inner).correlate(outer).select(field(inner_id)).where(inner_id.eq(outer_id)).where(inner_id.eq(7))
    compiled=query_from(outer).select(field(outer_id)).where(exists(sub)).where(outer_id.eq(8)).compile()
    assert 'AS "outer%%s"' in compiled.sql and '"outer%%s"."id"' in compiled.sql
    assert compiled.params==(7,8)
    assert query_from(outer).select(field(outer_id)).where(in_query(outer_id,sub)).compile().params==(7,)
    with pytest.raises(ValueError): query_from(inner).select(field(inner_id)).where(inner_id.eq(outer_id))
    with pytest.raises(ValueError): query_from(a).select(field(a.column('id',int))).where(exists(sub))
    with pytest.raises(ValueError): query_from(outer).inner_join(alias(a,'outer%s'),on=outer_id.eq(outer_id))
    with pytest.raises(ValueError): insert(outer,{'id':1})


def test_promoted_aggregate_empty_window_group_and_set_contracts():
    from decimal import Decimal
    from neutron.orm import avg,count,sum_value,min_value,max_value,row_number
    a,b=tables();aid=a.column('id',int)
    total=sum_value(aid)
    assert total.decode(2**32)==2**32 and total.decode(None) is None
    assert avg(aid).decode(Decimal('0.5'))==Decimal('0.5')
    assert count(aid).decode(0)==0 and min_value(aid).decode(None) is None
    with pytest.raises(ValueError): count(aid).decode(None)
    q=query_from(a).select_pair(field(aid),total).group_by(aid).having(total.gt(5)).where(aid.in_([1,2]))
    assert q.compile().params==(1,2,5)
    assert ' GROUP BY ' in q.compile().sql and ' HAVING SUM(' in q.compile().sql
    with pytest.raises(ValueError): query_from(a).select_pair(field(aid),total).compile()
    with pytest.raises(ValueError): query_from(a).select(field(aid)).having(aid.eq(1)).compile()
    window=query_from(a).select(row_number(a,order_by=(Order(aid),)))
    assert 'ROW_NUMBER() OVER (ORDER BY' in window.compile().sql
    assert window.compile().decode({'p0':2**32})==2**32
    with pytest.raises(ValueError): query_from(a).select(row_number(a,partition_by=(b.column('id',int),))).compile()
    first=query_from(a).select(field(aid)).where(aid.eq(1))
    second=query_from(a).select(field(aid)).where(aid.eq(2))
    assert first.union(second,all=True).limit(5).compile().params==(1,2,5)
    assert 'UNION ALL' in first.union(second,all=True).compile().sql
    assert 'SELECT DISTINCT' in first.distinct().compile().sql
    with pytest.raises(ValueError): first.union(query_from(a).select_pair(field(aid),field(aid)))
    with pytest.raises(ValueError): first.union(query_from(a).select(max_value(aid)))
