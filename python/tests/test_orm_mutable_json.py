from decimal import Decimal
import pytest
from neutron.orm import ColumnSpec,MutableJson,Table,insert,select


def test_mutable_codec_exact_validation_and_compiled_bind_snapshot():
    t=Table('documents',{'body':ColumnSpec(MutableJson,'jsonb')});value=MutableJson({'items':[Decimal('0.00000000000000001')]})
    compiled=insert(t,{'body':value});value.value['items'].append(None)
    assert compiled.params[0].document.parsed()=={'items':[Decimal('0.00000000000000001')]}
    from neutron.orm import JsonDocument
    decoded=select(t.column('body',MutableJson)).compile().decode({'body':JsonDocument('null')})
    assert isinstance(decoded,MutableJson) and decoded.value is None
    assert MutableJson({'flag':True})!=MutableJson({'flag':1})
    for bad in (float('nan'),{1:'bad'},Decimal('Infinity')):
        with pytest.raises(ValueError): MutableJson(bad)
    cycle=[];cycle.append(cycle)
    with pytest.raises(ValueError): MutableJson(cycle)
