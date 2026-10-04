from dataclasses import FrozenInstanceError
import datetime as dt
from decimal import Decimal
import struct
import pytest
from neutron.orm import CatalogType,ColumnSpec,PgMultirange,PgRange,Table,insert
from neutron.orm.multirange_value import BoundMultirange,MAX_MULTIRANGE_BYTES,MAX_MULTIRANGE_RANGES,MULTIRANGE_OIDS,admitted_multirange,register_multirange
from neutron.orm.pg_adapters import RANGE_OIDS


def ident(name='int4multirange',owner=None):
    return CatalogType('pg_catalog',name,MULTIRANGE_OIDS[name][0],'m',MULTIRANGE_OIDS[name][0],False,owner if owner is not None else object())

def r(lower,upper,li=True,ui=False): return PgRange(lower,upper,li and lower is not None,ui and upper is not None)

def test_builtin_multirange_oids_agree_with_native_range_profile():
    for name,(oid,range_name,range_oid,subtype) in MULTIRANGE_OIDS.items():
        assert name==range_name[:-5]+'multirange' and RANGE_OIDS[range_name]==(range_oid,subtype) and oid not in {range_oid,subtype}


def test_identity_admission_refuses_unproven_and_user_defined_multiranges():
    owner=object();admitted=ident(owner=owner)
    assert PgMultirange((),admitted).ranges==()
    for bad in (CatalogType('public','int4multirange',4451,'m',4451,False,owner),CatalogType('pg_catalog','int4multirange',4451,'r',4451,False,owner),CatalogType('pg_catalog','int4multirange',4452,'m',4452,False,owner),CatalogType('pg_catalog','int4multirange',4451,'m',23,False,owner),CatalogType('pg_catalog','app_multirange',99999,'m',99999,False,owner),CatalogType('app','int4multirange',4451,'m',4451,False,owner)):
        with pytest.raises(ValueError): PgMultirange((),bad)
    with pytest.raises(ValueError): PgMultirange((),None)  # type: ignore[arg-type]
    rows=[{'oid':4451,'name':'int4multirange','schema':'pg_catalog','kind':'m','range_oid':3904,'subtype':23}]
    assert admitted_multirange(rows,'int4multirange',owner)==admitted
    for broken in ([],rows+rows,[{**rows[0],'kind':'r'}],[{**rows[0],'schema':'public'}],[{**rows[0],'name':'other'}],[{**rows[0],'oid':4452}],[{**rows[0],'range_oid':3926}],[{**rows[0],'subtype':20}]):
        with pytest.raises(ValueError): admitted_multirange(broken,'int4multirange',owner)
    with pytest.raises(ValueError): admitted_multirange(rows,'app_multirange',owner)
    with pytest.raises(ValueError): admitted_multirange(rows,'int8multirange',owner)


def test_strict_constructor_requires_canonical_disjoint_ordered_members():
    identity=ident()
    value=PgMultirange((r(1,3),r(5,8),r(10,None)),identity)
    assert PgMultirange((r(None,0),r(5,8)),identity).ranges[0].lower is None
    with pytest.raises(FrozenInstanceError): value.ranges=()  # type: ignore[misc]
    with pytest.raises(ValueError): PgMultirange([r(1,3)],identity)  # type: ignore[arg-type]
    for members in ((r(5,8),r(1,3)),(r(1,4),r(3,6)),(r(1,3),r(3,6)),(r(1,3),r(None,0)),(r(None,None),r(1,2)),(r(1,None),r(5,8)),(r(1,3),r(1,3))):
        with pytest.raises(ValueError): PgMultirange(members,identity)
    for members in ((PgRange(empty=True),),(r(3,1),),(r(1,1),),(PgRange(1,3,True,True),),(PgRange(1,3,False,False),),(object(),)):
        with pytest.raises(ValueError): PgMultirange(members,identity)  # type: ignore[arg-type]
    assert PgMultirange((),identity)!=PgMultirange((r(1,2),),identity) and PgMultirange((),identity)==PgMultirange((),identity)
    assert hash(value)==hash(PgMultirange((r(1,3),r(5,8),r(10,None)),identity))
    with pytest.raises(ValueError): PgMultirange(tuple(r(2*i,2*i+1) for i in range(MAX_MULTIRANGE_RANGES+1)),identity)


def test_continuous_adjacency_shared_exclusive_point_and_incomparable_bounds():
    identity=ident('nummultirange');d=Decimal
    gap=PgMultirange((PgRange(d(1),d(2),True,False),PgRange(d(2),d(3),False,True)),identity)
    assert len(gap.ranges)==2
    for members in ((PgRange(d(1),d(2),True,True),PgRange(d(2),d(3),False,True)),(PgRange(d(1),d(2),True,False),PgRange(d(2),d(3),True,True)),(PgRange(d(1),d('NaN'),True,False),)):
        with pytest.raises(ValueError): PgMultirange(members,identity)
    ts=ident('tsmultirange')
    with pytest.raises(ValueError): PgMultirange((PgRange(dt.datetime(2020,1,1),dt.datetime(2020,1,2,tzinfo=dt.timezone.utc),True,False),),ts)


def test_from_ranges_normalizes_explicitly_and_strict_constructor_stays_strict():
    identity=ident()
    assert PgMultirange.from_ranges((),identity)==PgMultirange((),identity)
    assert PgMultirange.from_ranges((PgRange(empty=True),PgRange(1,1,False,False)),identity).ranges==()
    value=PgMultirange.from_ranges((PgRange(10,12,True,True),PgRange(1,3,True,True),PgRange(3,5,False,False),PgRange(20,None,True,False),PgRange(11,15,True,False)),identity)
    assert value.ranges==(PgRange(1,5,True,False),PgRange(10,15,True,False),PgRange(20,None,True,False))
    assert PgMultirange.from_ranges((PgRange(1,3,True,True),PgRange(4,6,True,True)),identity).ranges==(PgRange(1,7,True,False),)
    assert PgMultirange.from_ranges((PgRange(None,3,False,True),PgRange(2,None,True,False)),identity).ranges==(PgRange(None,None),)
    with pytest.raises(ValueError): PgMultirange.from_ranges((PgRange(3,1,True,False),),identity)
    with pytest.raises(ValueError): PgMultirange.from_ranges(iter([object()]),identity)  # type: ignore[list-item]
    with pytest.raises(ValueError): PgMultirange.from_ranges((r(i,i+1) for i in range(MAX_MULTIRANGE_RANGES+1)),identity)
    with pytest.raises(ValueError): PgMultirange((PgRange(1,3,True,True),),identity)
    dates=PgMultirange.from_ranges((PgRange(dt.date(2020,1,5),dt.date(2020,1,9),False,True),PgRange(dt.date(2020,1,10),dt.date(2020,1,12),True,False)),ident('datemultirange'))
    assert dates.ranges==(PgRange(dt.date(2020,1,6),dt.date(2020,1,12),True,False),)
    with pytest.raises(ValueError): PgMultirange.from_ranges((PgRange(1.5,3.5),),identity)
    d=Decimal
    continuous=PgMultirange.from_ranges((PgRange(d(2),d(3),False,True),PgRange(d(1),d(2),True,False),PgRange(d(5),d(5),True,True),PgRange(d(3),d(4),False,False)),ident('nummultirange'))
    assert continuous.ranges==(PgRange(d(1),d(2),True,False),PgRange(d(2),d(4),False,False),PgRange(d(5),d(5),True,True))
    merged=PgMultirange.from_ranges((PgRange(d(1),d(2),True,False),PgRange(d(2),d(3),True,False)),ident('nummultirange'))
    assert merged.ranges==(PgRange(d(1),d(3),True,False),)


def test_column_spec_binding_comparison_and_owner_contracts():
    owner=object();identity=ident(owner=owner);spec=ColumnSpec(PgMultirange,'int4multirange',native_type=identity)
    for sql_type,native in (('int4multirange',None),('int8multirange',identity),('nummultirange',identity),('int4range',identity)):
        with pytest.raises(ValueError): ColumnSpec(PgMultirange,sql_type,native_type=native)
    with pytest.raises(ValueError): ColumnSpec(PgMultirange,'int4multirange',native_type=identity,composite_fields=(('a',ColumnSpec(int,'int4')),))
    value=PgMultirange((r(1,3),r(5,8)),identity);empty=PgMultirange((),identity)
    spec.check(value);spec.check(empty)
    with pytest.raises(ValueError): spec.check(None)
    with pytest.raises(ValueError): spec.check(PgMultirange((r(1,3),),ident(owner=owner)).ranges)
    with pytest.raises(ValueError): spec.check(PgMultirange((r(1,3),),ident(owner=object())))
    with pytest.raises(ValueError): spec.check(PgMultirange((r(1,2**31),),identity))
    with pytest.raises(ValueError): ColumnSpec(PgMultirange,'nummultirange',native_type=ident('nummultirange',owner)).check(PgMultirange((PgRange(Decimal('-Infinity'),Decimal(1)),),ident('nummultirange',owner)))
    with pytest.raises(ValueError): Table('bad',{'value':spec})
    nullable=ColumnSpec(PgMultirange,'int4multirange',nullable=True,native_type=identity)
    table=Table('multiranges',{'value':nullable},_catalog_owner=owner)
    column=table.nullable_column('value',PgMultirange)
    with pytest.raises(ValueError): table.column('value',PgMultirange)
    compiled=insert(table,{'value':value}).returning(column).compile()
    assert compiled.result_oids==(('value',4451),) and isinstance(compiled.params[0],BoundMultirange) and compiled.params[0].value is value
    assert 'OPERATOR(pg_catalog.=)' in column.eq(value).sql and 'ANY(ARRAY[' in column.in_((value,empty)).sql
    assert column.eq(None).sql.endswith('IS NULL') and column.in_(()).sql=='FALSE'


def _context(owner,identity):
    from psycopg import adapters
    from psycopg.adapt import AdaptersMap
    class Context:
        def __init__(self): self.adapters=AdaptersMap(adapters);self.connection=None
    context=Context()
    register_multirange(context,owner,identity)
    return context


def _range(flags,*bounds):
    return bytes((flags,))+b''.join(struct.pack('!i',len(b))+b for b in bounds)

def _multirange(*members): return struct.pack('!i',len(members))+b''.join(struct.pack('!i',len(m))+m for m in members)


def test_binary_codec_exact_wire_format_empty_unbounded_and_refusals():
    from psycopg.adapt import PyFormat
    from psycopg.pq import Format
    owner=object();identity=ident(owner=owner);context=_context(owner,identity)
    dumper=context.adapters.get_dumper(BoundMultirange,PyFormat.BINARY)(BoundMultirange,context)
    loader=context.adapters.get_loader(identity.oid,Format.BINARY)(identity.oid,context)
    four=lambda n: struct.pack('!i',n)
    cases=[
        (PgMultirange((),identity),struct.pack('!i',0)),
        (PgMultirange((r(1,3),r(5,8)),identity),_multirange(_range(2,four(1),four(3)),_range(2,four(5),four(8)))),
        (PgMultirange((r(None,3),r(5,None)),identity),_multirange(_range(8,four(3)),_range(18,four(5)))),
        (PgMultirange((PgRange(None,None),),identity),_multirange(bytes((24,)))),
        (PgMultirange((r(-2**31,2**31-1),),identity),_multirange(_range(2,four(-2**31),four(2**31-1)))),
    ]
    for value,wire in cases:
        assert dumper.dump(BoundMultirange(value))==wire
        assert loader.load(wire)==value and loader.load(memoryview(wire))==value
    good=cases[1][1]
    for malformed in (b'',b'\x00',struct.pack('!i',-1),struct.pack('!i',MAX_MULTIRANGE_RANGES+1),struct.pack('!i',1),struct.pack('!ii',1,-1),struct.pack('!ii',1,0),good+b'x',good[:-1],struct.pack('!i',3)+good[4:],struct.pack('!i',1)+good[4:],
        _multirange(bytes((1,))),_multirange(_range(2,four(1),four(3)),_range(2,four(2),four(4))),_multirange(_range(2,four(5),four(8)),_range(2,four(1),four(3))),_multirange(_range(2,four(1),four(3)),_range(2,four(3),four(5))),_multirange(_range(2,four(1))),_multirange(_range(0xE2,four(1),four(2))),_multirange(_range(0,four(1),four(2))),_multirange(_range(2,four(3),four(1))),
        b'\x00'*(MAX_MULTIRANGE_BYTES+1)):
        with pytest.raises(ValueError): loader.load(malformed)
    foreign=PgMultirange((r(1,2),),ident(owner=object()))
    with pytest.raises(ValueError,match='another connection'): dumper.dump(BoundMultirange(foreign))
    with pytest.raises(ValueError): dumper.dump(BoundMultirange(object()))  # type: ignore[arg-type]
    assert dumper.upgrade(BoundMultirange(cases[1][0]),PyFormat.BINARY).oid==4451
    assert dumper.get_key(BoundMultirange(cases[1][0]),PyFormat.BINARY)==(BoundMultirange,4451)
    with pytest.raises(ValueError): loader.__class__(identity.oid,None)


def test_binary_codec_other_builtin_multiranges_and_byte_budget():
    from psycopg.adapt import PyFormat
    from psycopg.pq import Format
    owner=object()
    big=ident('int8multirange',owner);context=_context(owner,big)
    dumper=context.adapters.get_dumper(BoundMultirange,PyFormat.BINARY)(BoundMultirange,context)
    loader=context.adapters.get_loader(big.oid,Format.BINARY)(big.oid,context)
    value=PgMultirange((r(2**40,2**41),),big);wire=_multirange(_range(2,struct.pack('!q',2**40),struct.pack('!q',2**41)))
    assert dumper.dump(BoundMultirange(value))==wire and loader.load(wire)==value
    day=ident('datemultirange',owner);context=_context(owner,day)
    dumper=context.adapters.get_dumper(BoundMultirange,PyFormat.BINARY)(BoundMultirange,context)
    loader=context.adapters.get_loader(day.oid,Format.BINARY)(day.oid,context)
    dates=PgMultirange((r(dt.date(2000,1,2),dt.date(2000,1,5)),),day);wire=_multirange(_range(2,struct.pack('!i',1),struct.pack('!i',4)))
    assert dumper.dump(BoundMultirange(dates))==wire and loader.load(wire)==dates
    # Dumping is bounded independently of decoding: members that encode past the byte budget refuse.
    import neutron.orm.multirange_value as module
    original=module.MAX_MULTIRANGE_BYTES
    module.MAX_MULTIRANGE_BYTES=20
    try:
        with pytest.raises(ValueError,match='byte budget'): dumper.dump(BoundMultirange(PgMultirange((r(dt.date(2000,1,2),dt.date(2000,1,5)),r(dt.date(2000,2,2),dt.date(2000,2,5))),day)))
    finally: module.MAX_MULTIRANGE_BYTES=original


def test_mapping_identity_refusal_snapshot_equality_and_composite_domain_refusal():
    from dataclasses import dataclass
    from neutron.orm import ModelMapping
    from neutron.orm.mapping import same_value
    owner=object();identity=ident(owner=owner)
    @dataclass
    class Row:
        id: int
        value: PgMultirange
    @dataclass
    class Keyed:
        value: PgMultirange
    spec=ColumnSpec(PgMultirange,'int4multirange',native_type=identity)
    table=Table('rows',{'id':ColumnSpec(int,'int4'),'value':spec},_catalog_owner=owner)
    ModelMapping(Row,table,dict(table.columns),primary_key=('id',))
    keyed=Table('keyed',{'value':spec},_catalog_owner=owner)
    with pytest.raises(ValueError,match='identity profile'): ModelMapping(Keyed,keyed,dict(keyed.columns),primary_key=('value',))
    one=PgMultirange((r(1,3),),identity)
    assert same_value(one,PgMultirange((r(1,3),),identity)) and not same_value(one,PgMultirange((r(1,4),),identity)) and not same_value(one,PgMultirange((r(1,3),),ident(owner=owner)))
    from neutron.orm.composite_value import admitted_components
    with pytest.raises(ValueError): admitted_components([{'name':'m','oid':4451}],{'m':spec})
