from dataclasses import FrozenInstanceError
from decimal import Decimal
import datetime as dt
import pytest
from neutron.orm import DEFAULT, OMIT, Column, ColumnSpec, Table, delete, insert, select, select_row, update


def table():
    return Table('values',{'id':ColumnSpec(int,'int4'),'n':ColumnSpec(int,'int4',nullable=True),'active':ColumnSpec(bool,'bool'),'label':ColumnSpec(str,'text')},schema='Odd.Schema')


def test_identifiers_and_parameters():
    t=table(); c=t.column('label',str)
    compiled=select(c).where(c.eq("' OR 1=1 --")).compile()
    assert '"Odd.Schema"."values"."label"' in compiled.sql
    assert "OR 1=1" not in compiled.sql
    assert compiled.params == ("' OR 1=1 --",)
    with pytest.raises(FrozenInstanceError): t.name='changed'


def test_four_write_states_and_zero_values():
    t=table()
    command=insert(t,{'id':0,'n':None,'active':False,'label':''})
    assert command.params==(0,None,False,'')
    assert insert(t,{'n':OMIT}).sql.endswith('DEFAULT VALUES')
    assert insert(t,{'n':DEFAULT}).params==()
    assert 'DEFAULT' in update(t,{'n':DEFAULT},where=t.column('id',int).eq(1)).sql
    assert update(t,{'n':OMIT,'label':'kept'},where=t.column('id',int).eq(1)).params==('kept',1)
    with pytest.raises(ValueError): update(t,{'n':OMIT},where=t.column('id',int).eq(1))


def test_owned_projection_and_predicates():
    t=table(); other=table()
    with pytest.raises(ValueError): select_row(t,other.column('id',int))
    with pytest.raises(ValueError): select(t.column('id',int)).where(other.column('id',int).eq(1))
    with pytest.raises(ValueError): select(Column(t,'id',ColumnSpec(int,'int4')))
    with pytest.raises(TypeError): bool(t.column('id',int).eq(1))
    assert 'FALSE' in select(t.column('id',int)).where(t.column('id',int).in_([])).compile().sql


def test_value_types_nullability_generated():
    t=table()
    with pytest.raises(ValueError): t.column('n',int)
    assert t.nullable_column('n',int).spec.nullable
    with pytest.raises(ValueError): insert(t,{'id':True})
    with pytest.raises(ValueError): insert(t,{'id':2**31})
    with pytest.raises(ValueError): insert(t,{'id':None})
    with pytest.raises(ValueError): ColumnSpec(str,'domain')
    generated=Table('t',{'id':ColumnSpec(int,'int4',generated=True)})
    with pytest.raises(ValueError): insert(generated,{'id':DEFAULT})
    temporal=Table('t',{'moment':ColumnSpec(dt.datetime,'timestamptz')})
    with pytest.raises(ValueError): insert(temporal,{'moment':dt.datetime(2024,1,1)})
    exact=Table('t',{'n':ColumnSpec(Decimal,'numeric')})
    assert insert(exact,{'n':Decimal('12345678901234567890.123456789')}).params==(Decimal('12345678901234567890.123456789'),)


def test_decode_rejects_missing_or_wrong_projection():
    t=table(); compiled=select_row(t,t.column('id',int)).compile()
    with pytest.raises(KeyError): compiled.decode({})
    with pytest.raises(ValueError): compiled.decode({'id':'1'})
    assert compiled.decode({'id':0})=={'id':0}


def test_percent_identifiers_cannot_be_driver_placeholders():
    t=Table('odd%s',{'value%s':ColumnSpec(str,'text')},schema='scope%')
    compiled=select(t.column('value%s',str)).where(t.column('value%s',str).eq('bound')).compile()
    assert '"scope%%"."odd%%s"."value%%s"' in compiled.sql
    assert compiled.params==('bound',)
    assert t.sql=='"scope%"."odd%s"'
