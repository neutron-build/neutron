import math
import struct
import pytest
from neutron.orm import CatalogType,ColumnSpec,PgVector,Table,insert
from neutron.orm.vector_value import BoundVector,admitted_vector,register_vector


def test_vector_extension_identity_explicit_float32_precision_and_binary_contract():
    from psycopg import adapters
    from psycopg.adapt import AdaptersMap,PyFormat
    from psycopg.pq import Format
    owner=object();identity=CatalogType('private_vectors','vector',12345,'b',12345,False,owner)
    admitted_vector([{'name':'vector'}],identity)
    for rows in ([],[{'name':'other'}],[{'name':'vector'},{'name':'vector'}]):
        with pytest.raises(ValueError): admitted_vector(rows,identity)
    rounded=PgVector.from_values((0.1,1.0),identity)
    assert rounded.elements==(struct.unpack('!f',struct.pack('!f',0.1))[0],1.0)
    for values in ((),(0.1,),(float('nan'),),(float('inf'),),(1.0e100,),(1,)):
        with pytest.raises(ValueError): PgVector(values,identity)
    with pytest.raises(ValueError): PgVector.from_values((math.inf,),identity)
    spec=ColumnSpec(PgVector,'vector',native_type=identity)
    table=Table('vectors',{'value':spec},_catalog_owner=owner)
    compiled=insert(table,{'value':rounded}).returning(table.column('value',PgVector)).compile()
    assert compiled.result_oids==(('value',identity.oid),)
    predicate=table.column('value',PgVector).eq(rounded)
    assert 'OPERATOR("private_vectors".=)' in predicate.sql
    assert 'ANY(ARRAY[' in table.column('value',PgVector).in_((rounded,)).sql
    class Context:
        def __init__(self): self.adapters=AdaptersMap(adapters);self.connection=None
    context=Context();register_vector(context,owner,identity)
    dumper=context.adapters.get_dumper(BoundVector,PyFormat.BINARY)(BoundVector,context)
    raw=dumper.dump(BoundVector(rounded));assert raw is not None
    loader=context.adapters.get_loader(identity.oid,Format.BINARY)(identity.oid,context)
    assert loader.load(raw)==rounded
    for malformed in (b'',struct.pack('!HH',0,0),struct.pack('!HH',2,1)+bytes(raw)[4:],bytes(raw)+b'extra',struct.pack('!HHf',1,0,math.nan)):
        with pytest.raises(ValueError): loader.load(malformed)
    with pytest.raises(ValueError): dumper.dump(BoundVector(PgVector((1.0,),CatalogType('private_vectors','vector',12345,'b',12345,False,object()))))
