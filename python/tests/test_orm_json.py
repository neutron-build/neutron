from dataclasses import dataclass
from decimal import Decimal
import pytest
from neutron.orm import JSON_NULL, ColumnSpec, JsonDocument, ModelMapping, Table, insert
from neutron.orm.json_value import BoundJson


def test_exact_document_immutable_null_and_binding():
    doc=JsonDocument('{"n":12345678901234567890.123456789,"a":[null,false,0,""]}')
    assert doc.parsed()['n']==Decimal('12345678901234567890.123456789')
    parsed=doc.parsed();parsed['a'].append('changed')
    assert doc.parsed()['a']==[None,False,0,'']
    assert JSON_NULL.parsed() is None and JSON_NULL is not None
    for text in ('NaN','Infinity','{"x":1,"x":2}','broken'):
        with pytest.raises(ValueError): JsonDocument(text)
    t=Table('docs',{'value':ColumnSpec(JsonDocument,'jsonb',nullable=True)})
    assert insert(t,{'value':None}).params==(None,)
    assert insert(t,{'value':JSON_NULL}).params==(BoundJson(JSON_NULL,True),)
    with pytest.raises(ValueError): insert(t,{'value':{'x':1}})


def test_json_identity_and_mapping_refusal():
    @dataclass
    class Doc:
        id: JsonDocument
    for kind in ('json','jsonb'):
        t=Table('docs',{'id':ColumnSpec(JsonDocument,kind)})
        with pytest.raises(ValueError): ModelMapping(Doc,t,{'id':t.column('id',JsonDocument)},primary_key=('id',))
    t=Table('docs',{'id':ColumnSpec(JsonDocument,'json')})
    with pytest.raises(ValueError): t.column('id',JsonDocument).eq(JSON_NULL)
