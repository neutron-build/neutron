"""Qualified immutable builtin PostgreSQL 14+ multirange values with exact binary codecs."""
from __future__ import annotations
from dataclasses import dataclass
import datetime as dt
from decimal import InvalidOperation
from functools import cmp_to_key
import struct
from typing import Any,Iterable
from .catalog_value import CatalogType
from .pg_value import BoundRange,PgRange

MAX_MULTIRANGE_RANGES=4096
MAX_MULTIRANGE_BYTES=1<<20
# multirange name -> (multirange OID, range name, range OID, subtype OID); builtin OIDs are stable catalog identities.
MULTIRANGE_OIDS={'int4multirange':(4451,'int4range',3904,23),'int8multirange':(4536,'int8range',3926,20),'nummultirange':(4532,'numrange',3906,1700),'tsmultirange':(4533,'tsrange',3908,1114),'tstzmultirange':(4534,'tstzrange',3910,1184),'datemultirange':(4535,'daterange',3912,1082)}
_DISCRETE=frozenset({'int4multirange','int8multirange','datemultirange'})

def _cmp(left: Any,right: Any) -> int:
    try: return (left>right)-(left<right)
    except (TypeError,InvalidOperation) as exc: raise ValueError('multirange bounds require comparable finite values') from exc

def _separated(previous: PgRange[Any],following: PgRange[Any]) -> bool:
    # A shared exclusive point is a real gap; any touching or overlapping pair is one native member.
    if previous.upper is None or following.lower is None: return False
    order=_cmp(previous.upper,following.lower)
    return order<0 or order==0 and not previous.upper_inclusive and not following.lower_inclusive

def _member(item: object,discrete: bool) -> PgRange[Any]:
    if type(item) is not PgRange or item.empty: raise ValueError('non-empty PgRange members required')
    if discrete and (item.lower is not None and not item.lower_inclusive or item.upper is not None and item.upper_inclusive):
        raise ValueError('discrete multirange members require canonical [lower,upper) bounds')
    if item.lower is not None and item.upper is not None:
        order=_cmp(item.lower,item.upper)
        if order>0 or order==0 and not (item.lower_inclusive and item.upper_inclusive): raise ValueError('multirange member must be nonempty with lower<=upper')
    return item

@dataclass(frozen=True)
class PgMultirange:
    ranges: tuple[PgRange[Any],...]
    identity: CatalogType
    def __post_init__(self) -> None:
        entry=MULTIRANGE_OIDS.get(self.identity.name) if isinstance(self.identity,CatalogType) else None
        if entry is None or self.identity.kind!='m' or self.identity.schema!='pg_catalog' or self.identity.oid!=entry[0] or self.identity.base_oid!=entry[0]:
            raise ValueError('qualified builtin multirange identity required')
        if type(self.ranges) is not tuple or len(self.ranges)>MAX_MULTIRANGE_RANGES: raise ValueError('immutable multirange within the range budget required')
        previous: PgRange[Any]|None=None
        for item in self.ranges:
            _member(item,self.identity.name in _DISCRETE)
            if previous is not None and not _separated(previous,item): raise ValueError('multirange members must be ordered, disjoint and non-adjacent; use from_ranges to normalize')
            previous=item
    @classmethod
    def from_ranges(cls,ranges: Iterable[PgRange[Any]],identity: CatalogType) -> PgMultirange:
        """Explicit normalizer: drops empty members, canonicalizes discrete bounds, sorts and merges touching members."""
        discrete=isinstance(identity,CatalogType) and identity.name in _DISCRETE
        members: list[PgRange[Any]]=[]
        for item in ranges:
            if len(members)>=MAX_MULTIRANGE_RANGES: raise ValueError('multirange input range budget exceeded')
            if type(item) is not PgRange: raise ValueError('PgRange members required')
            if item.empty: continue
            if item.lower is not None and item.upper is not None:
                order=_cmp(item.lower,item.upper)
                if order>0: raise ValueError('range lower bound exceeds upper bound')
                if order==0 and not (item.lower_inclusive and item.upper_inclusive): continue
            if discrete:
                lower,upper=item.lower,item.upper
                if lower is not None and not item.lower_inclusive: lower=_step(lower)
                if upper is not None and item.upper_inclusive: upper=_step(upper)
                if lower is not None and upper is not None and _cmp(lower,upper)>=0: continue
                item=PgRange(lower,upper,lower is not None,False)
            members.append(item)
        def by_lower(left: PgRange[Any],right: PgRange[Any]) -> int:
            if left.lower is None or right.lower is None: return (left.lower is not None)-(right.lower is not None)
            return _cmp(left.lower,right.lower) or int(right.lower_inclusive)-int(left.lower_inclusive)
        merged: list[PgRange[Any]]=[]
        for item in sorted(members,key=cmp_to_key(by_lower)):
            if merged and not _separated(merged[-1],item):
                current=merged[-1];top: Any=None;top_inclusive=False
                if current.upper is not None and item.upper is not None:
                    larger=_cmp(item.upper,current.upper)
                    top,top_inclusive=(item.upper,item.upper_inclusive) if larger>0 or larger==0 and item.upper_inclusive else (current.upper,current.upper_inclusive)
                merged[-1]=PgRange(current.lower,top,current.lower_inclusive,top_inclusive)
            else: merged.append(item)
        return cls(tuple(merged),identity)

def _step(bound: Any) -> Any:
    if type(bound) is int: return bound+1
    if type(bound) is dt.date:
        try: return bound+dt.timedelta(days=1)
        except OverflowError as exc: raise ValueError('date bound exceeds native range') from exc
    raise ValueError('discrete multirange bounds require int or date values')

@dataclass(frozen=True)
class BoundMultirange:
    value: PgMultirange

MULTIRANGE_SQL='''SELECT t.oid AS oid,t.typname AS name,n.nspname AS schema,t.typtype::pg_catalog.text AS kind,r.rngtypid AS range_oid,r.rngsubtype AS subtype FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) t.typnamespace JOIN pg_catalog.pg_range r ON r.rngmultitypid OPERATOR(pg_catalog.=) t.oid WHERE t.oid OPERATOR(pg_catalog.=) %s::pg_catalog.oid'''


def admitted_multirange(rows: list[dict[str,Any]],name: str,owner: object) -> CatalogType:
    entry=MULTIRANGE_OIDS.get(name)
    if entry is None: raise ValueError('only builtin PostgreSQL multirange types are admitted')
    if len(rows)!=1: raise ValueError('native multirange type absent; PostgreSQL 14+ required')
    row=rows[0]
    if row['kind']!='m' or row['schema']!='pg_catalog' or row['name']!=name or int(row['oid'])!=entry[0] or int(row['range_oid'])!=entry[2] or int(row['subtype'])!=entry[3]:
        raise ValueError('native multirange catalog identity mismatch')
    return CatalogType('pg_catalog',name,entry[0],'m',entry[0],False,owner)


def register_multirange(connection: Any,owner: object,identity: CatalogType) -> None:
    from psycopg.adapt import Dumper,Loader,PyFormat
    from psycopg.pq import Format
    from .pg_adapters import ARRAY_OIDS,RANGE_OIDS,_adapter_classes
    entry=MULTIRANGE_OIDS.get(identity.name)
    if entry is None or identity.kind!='m' or identity.oid!=entry[0]: raise ValueError('qualified builtin multirange identity required')
    classes=_adapter_classes();range_start=5+len(ARRAY_OIDS)
    range_dumper=classes[range_start];range_loader=classes[range_start+1+list(RANGE_OIDS).index(entry[1])]
    class MultirangeLoader(Loader):
        format=Format.BINARY
        def __init__(self,oid: int,context: Any=None) -> None:
            super().__init__(oid,context)
            if context is None: raise ValueError('multirange codec requires connection context')
            self.range_loader=range_loader(entry[2],context)
        def load(self,data: Any) -> PgMultirange:
            raw=bytes(data)
            if len(raw)<4 or len(raw)>MAX_MULTIRANGE_BYTES: raise ValueError('native multirange header/byte budget mismatch')
            count=struct.unpack_from('!i',raw)[0]
            if not 0<=count<=MAX_MULTIRANGE_RANGES or len(raw)<4+5*count: raise ValueError('native multirange count/budget mismatch')
            members: list[Any]=[];offset=4
            for _ in range(count):
                if len(raw)<offset+4: raise ValueError('truncated multirange member size')
                size=struct.unpack_from('!i',raw,offset)[0];offset+=4
                if size<1 or len(raw)<offset+size: raise ValueError('invalid multirange member size')
                members.append(self.range_loader.load(raw[offset:offset+size]));offset+=size
            if offset!=len(raw): raise ValueError('trailing native multirange bytes')
            return PgMultirange(tuple(members),identity)
    class MultirangeDumper(Dumper):
        format=Format.BINARY
        def __init__(self,cls: type[Any],context: Any=None) -> None:
            super().__init__(cls,context);self.context=context
        def get_key(self,obj: BoundMultirange,format: PyFormat) -> Any: return (type(obj),obj.value.identity.oid)
        def upgrade(self,obj: BoundMultirange,format: PyFormat) -> MultirangeDumper:
            upgraded=MultirangeDumper(self.cls,self.context);upgraded.oid=obj.value.identity.oid;return upgraded
        def dump(self,obj: BoundMultirange) -> bytes:
            value=obj.value
            if not isinstance(value,PgMultirange) or value.identity._owner is not owner: raise ValueError('multirange binding belongs to another connection')
            member_dumper=range_dumper(BoundRange,self.context);range_name=MULTIRANGE_OIDS[value.identity.name][1]
            parts=[struct.pack('!i',len(value.ranges))];total=4
            for member in value.ranges:
                encoded=member_dumper.dump(BoundRange(member,range_name))
                if encoded is None: raise ValueError('non-empty multirange member encoded NULL')
                raw=bytes(encoded);total+=4+len(raw)
                if total>MAX_MULTIRANGE_BYTES: raise ValueError('multirange native byte budget exceeded')
                parts.extend((struct.pack('!i',len(raw)),raw))
            return b''.join(parts)
    connection.adapters.register_loader(identity.oid,MultirangeLoader)
    connection.adapters.register_dumper(BoundMultirange,MultirangeDumper)
