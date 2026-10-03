import pytest
from neutron.orm import ColumnSpec, Table, insert


def test_returning_owns_projection_and_params():
    t=Table('t',{'id':ColumnSpec(int,'int4')},schema='qualified')
    query=insert(t,{'id':0}).returning(t.column('id',int))
    compiled=query.compile()
    assert compiled.sql=='INSERT INTO "qualified"."t" ("id") VALUES (%s) RETURNING "id"'
    assert compiled.params==(0,)
    assert compiled.decode({'id':0})==0
    other=Table('t',{'id':ColumnSpec(int,'int4')},schema='other')
    with pytest.raises(ValueError): insert(t,{'id':0}).returning(other.column('id',int))
