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
