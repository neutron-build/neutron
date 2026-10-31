from dataclasses import FrozenInstanceError
import struct
import pytest
from neutron.orm import CatalogType,ColumnSpec,PgComposite
from neutron.orm.composite_value import BoundComposite,admitted_components,register_composite


def test_composite_admission_field_identity_null_and_binary_validation():
    from psycopg import adapters
    from psycopg.adapt import AdaptersMap,PyFormat
    from psycopg.pq import Format
    owner=object();identity=CatalogType('app','pair',12345,'c',12345,False,owner)
    declared={'n':ColumnSpec(int,'int8',nullable=True),'label':ColumnSpec(str,'text',nullable=True)}
    fields=admitted_components([{'name':'n','oid':20},{'name':'label','oid':25}],declared)
    spec=ColumnSpec(PgComposite,'composite',native_type=identity,composite_fields=fields)
    value=PgComposite((None,''),identity);spec.check(value)
    with pytest.raises(FrozenInstanceError): value.fields=()  # type: ignore[misc]
    with pytest.raises(ValueError): admitted_components([{'name':'label','oid':25},{'name':'n','oid':20}],declared)
    with pytest.raises(ValueError): admitted_components([{'name':'n','oid':23},{'name':'label','oid':25}],declared)
    with pytest.raises(ValueError): spec.check(PgComposite((1,),identity))
    with pytest.raises(ValueError): spec.check(PgComposite((1,'x'),CatalogType('shadow','pair',54321,'c',54321,False,owner)))
    class Context:
        def __init__(self): self.adapters=AdaptersMap(adapters);self.connection=None
    context=Context();register_composite(context,owner,identity,fields)
    dumper=context.adapters.get_dumper(BoundComposite,PyFormat.BINARY)(BoundComposite,context)
    raw=dumper.dump(BoundComposite(value,fields));assert raw is not None
    loader=context.adapters.get_loader(identity.oid,Format.BINARY)(identity.oid,context)
    assert loader.load(raw)==value
    for malformed in (b'',struct.pack('!i',1),bytes(raw)+b'extra',struct.pack('!iIi',2,23,-1)+bytes(raw)[12:]):
        with pytest.raises(ValueError): loader.load(malformed)
    with pytest.raises(ValueError): dumper.dump(BoundComposite(PgComposite((None,''),CatalogType('app','pair',12345,'c',12345,False,object())),fields))
