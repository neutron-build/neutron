from dataclasses import FrozenInstanceError
from decimal import Decimal
import pytest
from neutron.orm import ArrayDimension,ColumnSpec,Interval,PgArray,Table,TimeOfDay,array_spec,insert,select


def test_immutable_array_dimensions_and_containment():
    value=PgArray((ArrayDimension(2,-3),ArrayDimension(2,5)),(1,None,2,3))
    assert value.dimensions[0].lower_bound==-3
    with pytest.raises(FrozenInstanceError): value.elements=()  # type: ignore[misc]
    with pytest.raises(ValueError): PgArray((ArrayDimension(2),),(1,))
    with pytest.raises(ValueError): PgArray((ArrayDimension(1),),([1],))
    with pytest.raises(ValueError): ArrayDimension(2,2**31-1)
    with pytest.raises(ValueError): PgArray((ArrayDimension(1_000_001),),())
    spec=array_spec(int,'int4',nullable=True);spec.check(value);spec.check(None)
    with pytest.raises(ValueError): spec.check(PgArray((ArrayDimension(1),),(True,)))
    with pytest.raises(ValueError): ColumnSpec(PgArray,'unknown[]')
    with pytest.raises(ValueError): array_spec(str,'int4')
    table=Table('array_values',{'data':spec})
    assert table.nullable_column('data',PgArray[int]).spec is spec
    with pytest.raises(ValueError): table.nullable_column('data',PgArray[str])
    compiled=insert(table,{'data':value}).returning(table.nullable_column('data',PgArray[int])).compile()
    assert compiled.result_oids==(('data',1007),)
    assert compiled.params[0].value is value


def test_native_temporal_component_range_refusal():
    assert TimeOfDay(86_400_000_000).microseconds==86_400_000_000
    assert Interval(2,-3,4).months==2
    for value in (-1,86_400_000_001,True):
        with pytest.raises(ValueError): TimeOfDay(value)
    with pytest.raises(ValueError): Interval(months=2**31)
    with pytest.raises(ValueError): Interval(microseconds=2**63)
    ColumnSpec(TimeOfDay,'time').check(TimeOfDay(1))
    ColumnSpec(Interval,'interval').check(Interval(12,3,1))


def test_native_binary_array_decoder_refuses_element_oid_and_trailing_bytes():
    import struct
    from neutron.orm.pg_adapters import _adapter_classes
    from psycopg import adapters
    class Context:
        def __init__(self): self.adapters=adapters
    cls=_adapter_classes()[6]  # int4[]
    loader=cls(1007,Context())
    with pytest.raises(ValueError,match='OID'): loader.load(struct.pack('!iii',0,0,20))
    with pytest.raises(ValueError,match='trailing'): loader.load(struct.pack('!iii',0,0,23)+b'extra')
