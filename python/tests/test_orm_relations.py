from dataclasses import dataclass, replace
from decimal import Decimal
import pytest
from neutron.orm import ColumnSpec, LoadBudget, ModelMapping, Relation, RelationBudgetError, Table, UnsupportedRelationError
from neutron.orm.relations import _prepare

@dataclass
class Parent:
    id: int
    tenant: str

@dataclass
class Child:
    id: int
    parent_id: int
    tenant: str
    label: str


def mappings(schema='public'):
    p=Table('parents',{'id':ColumnSpec(int,'int4'),'tenant':ColumnSpec(str,'text')},schema=schema)
    c=Table('children',{'id':ColumnSpec(int,'int4'),'parent_id':ColumnSpec(int,'int4'),'tenant':ColumnSpec(str,'text'),'label':ColumnSpec(str,'text')},schema=schema)
    pm=ModelMapping(Parent,p,{'id':p.column('id',int),'tenant':p.column('tenant',str)},primary_key=('id','tenant'))
    cm=ModelMapping(Child,c,{'id':c.column('id',int),'parent_id':c.column('parent_id',int),'tenant':c.column('tenant',str),'label':c.column('label',str)},primary_key=('id',))
    return pm,cm


def test_composite_dedup_budget_order_and_copy_before_attachment():
    pm,cm=mappings();rel=Relation(pm,cm,('id','tenant'),('parent_id','tenant'))
    parents=[Parent(1,'a'),Parent(1,'b'),Parent(1,'a'),Parent(9,'missing')]
    state=_prepare(rel,parents,None,LoadBudget(4,5,1),False)
    assert len(state.distinct)==3
    query,requested=state.batch(0)
    assert query.compile().params==(1,'a',6)
    state.consume([Child(11,1,'a','first'),Child(10,1,'a','second')],requested)
    query,requested=state.batch(1);state.consume([Child(12,1,'b','other')],requested)
    result=state.finish()
    assert [item.parent for item in result]==parents
    assert [[c.id for c in item.children] for item in result]==[[11,10],[12],[11,10],[]]
    assert result[0].children[0] is not result[2].children[0]
    assert rel.inverse().parent is cm
    state=_prepare(rel,parents,None,LoadBudget(4,3,1),False)
    _,requested=state.batch(0)
    with pytest.raises(RelationBudgetError): state.consume([Child(11,1,'a','x'),Child(10,1,'a','y')],requested)


def test_validation_even_empty_and_no_projection_escape():
    pm,cm=mappings();rel=Relation(pm,cm,('id','tenant'),('parent_id','tenant'))
    budget=LoadBudget(10,10,10)
    with pytest.raises(UnsupportedRelationError): _prepare(rel,[],rel.query().limit(1),budget,False)
    with pytest.raises(UnsupportedRelationError): _prepare(rel,[],rel.query().offset(0),budget,False)
    with pytest.raises(UnsupportedRelationError): _prepare(rel,[],replace(rel.query(),decoder=lambda row:None),budget,False)
    with pytest.raises(ValueError): rel.query().where(pm.table.column('id',int).eq(1))
    with pytest.raises(ValueError): Relation(pm,cm,('id','id'),('id','parent_id'))
    with pytest.raises(ValueError): _prepare(rel,[Parent(True,'a')],None,budget,False)
    for invalid in (LoadBudget(True,10,1),LoadBudget(1,0,1),LoadBudget(1,2**63-1,1)):
        with pytest.raises(ValueError): _prepare(rel,[],None,invalid,False)


def test_parameter_budget_and_unrequested_rows():
    pm,cm=mappings();rel=Relation(pm,cm,('id','tenant'),('parent_id','tenant'))
    huge=cm.table.column('id',int).in_(range(65535))
    with pytest.raises(UnsupportedRelationError): _prepare(rel,[Parent(1,'a')],rel.query().where(huge),LoadBudget(1,10,1),False)
    state=_prepare(rel,[Parent(1,'a')],None,LoadBudget(1,10,1),False)
    _,requested=state.batch(0)
    from neutron.orm import OrmError,CardinalityError
    with pytest.raises(OrmError): state.consume([Child(10,99,'wrong','x')],requested)
    state=_prepare(rel,[Parent(1,'a')],None,LoadBudget(1,10,1),True)
    with pytest.raises(CardinalityError): state.consume([Child(10,1,'a','x'),Child(11,1,'a','y')],requested)


@dataclass
class ScalarParent:
    id: int
    token: 'UUID'
    flag: bool
    label: str

@dataclass
class ScalarChild:
    id: int
    token: 'UUID'
    flag: bool
    label: str

from uuid import UUID

def scalar_mappings(schema='public'):
    specs={'id':ColumnSpec(int,'int4'),'token':ColumnSpec(UUID,'uuid'),'flag':ColumnSpec(bool,'bool'),'label':ColumnSpec(str,'text')}
    a=Table('scalar_parents',specs,schema=schema);b=Table('scalar_children',specs,schema=schema)
    def columns(table):
        return {'id':table.column('id',int),'token':table.column('token',UUID),'flag':table.column('flag',bool),'label':table.column('label',str)}
    return ModelMapping(ScalarParent,a,columns(a),primary_key=('id',)),ModelMapping(ScalarChild,b,columns(b),primary_key=('id',))


def test_exact_uuid_bool_empty_string_and_composite_boundaries():
    pm,cm=scalar_mappings();rel=Relation(pm,cm,('token','flag','label'),('token','flag','label'))
    uid=UUID('00000000-0000-0000-0000-000000000001')
    state=_prepare(rel,[ScalarParent(1,uid,False,''),ScalarParent(2,uid,True,'')],None,LoadBudget(2,2,2),False)
    assert len(state.distinct)==2
    query,requested=state.batch(0)
    assert query.compile().params==(uid,False,'',uid,True,'',3)
    state.consume([ScalarChild(10,uid,False,'')],requested)
    assert [[child.id for child in item.children] for item in state.finish()]==[[10],[]]

@dataclass
class NullableParent:
    id: int
    key: str | None

@dataclass
class NumericParent:
    id: int
    key: Decimal


def test_nullable_and_custom_key_profiles_refused():
    for model,spec,typ in ((NullableParent,ColumnSpec(str,'text',nullable=True),str),(NumericParent,ColumnSpec(Decimal,'numeric'),Decimal)):
        a=Table('a',{'id':ColumnSpec(int,'int4'),'key':spec})
        b=Table('b',{'id':ColumnSpec(int,'int4'),'key':spec})
        def mapped(table):
            key=table.nullable_column('key',typ) if spec.nullable else table.column('key',typ)
            return ModelMapping(model,table,{'id':table.column('id',int),'key':key},primary_key=('id',))
        with pytest.raises(UnsupportedRelationError): Relation(mapped(a),mapped(b),('key',),('key',))


def test_richer_algebra_cannot_escape_mapped_relation_budget():
    from neutron.orm import count
    pm,cm=mappings();rel=Relation(pm,cm,('id','tenant'),('parent_id','tenant'));q=rel.query()
    budget=LoadBudget(10,10,10)
    for changed in (q.distinct(),q.union(q),q.group_by(cm.table.column('id',int)),replace(q,fields=(count(cm.table.column('id',int)),*q.fields[1:]))):
        with pytest.raises(UnsupportedRelationError): _prepare(rel,[],changed,budget,False)
