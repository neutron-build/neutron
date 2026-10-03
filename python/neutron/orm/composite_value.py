"""Qualified immutable records with declared native binary component codecs."""
from __future__ import annotations
from dataclasses import dataclass
import struct
from typing import Any,Mapping
from .catalog_value import CatalogType

MAX_COMPOSITE_FIELDS=256
MAX_COMPOSITE_BYTES=1<<20

@dataclass(frozen=True)
class PgComposite:
    fields: tuple[object|None,...]
    identity: CatalogType
    def __post_init__(self) -> None:
        if not isinstance(self.identity,CatalogType) or self.identity.kind!='c' or type(self.fields) is not tuple or not 0<len(self.fields)<=MAX_COMPOSITE_FIELDS:
            raise ValueError('qualified composite identity and finite immutable fields required')
        if any(isinstance(value,(dict,list,set,bytearray)) for value in self.fields): raise ValueError('composite members require immutable native values')

@dataclass(frozen=True)
class BoundComposite:
    value: PgComposite
    specs: tuple[tuple[str,object],...]

COMPOSITE_SQL='''SELECT a.attname AS name,a.atttypid AS oid FROM pg_catalog.pg_type t JOIN pg_catalog.pg_attribute a ON a.attrelid OPERATOR(pg_catalog.=) t.typrelid WHERE t.oid OPERATOR(pg_catalog.=) %s AND a.attnum OPERATOR(pg_catalog.>) 0 AND NOT a.attisdropped ORDER BY a.attnum'''


def admitted_components(rows: list[dict[str,Any]],declared: Mapping[str,object]) -> tuple[tuple[str,object],...]:
    from .core import ColumnSpec
    from .json_value import MutableJson
    if not 0<len(rows)<=MAX_COMPOSITE_FIELDS or tuple(row['name'] for row in rows)!=tuple(declared):
        raise ValueError('ordered composite fields must match native catalog')
    for row,(name,spec) in zip(rows,declared.items()):
        if not isinstance(spec,ColumnSpec) or spec.native_type is not None or spec.generated or spec.python_type is MutableJson or row['oid']!=spec.type_oid:
            raise ValueError('composite requires declared immutable builtin component SQL type/OIDs')
    return tuple(declared.items())


def register_composite(connection: Any,owner: object,identity: CatalogType,specs: tuple[tuple[str,object],...]) -> None:
    from psycopg.adapt import Dumper,Loader,PyFormat
    from psycopg.pq import Format
    from .core import ColumnSpec
    from .pg_value import BoundArray,BoundRange,PgArray,PgRange
    from .pg_adapters import _adapter_classes,ARRAY_OIDS
    from .json_value import JsonDocument
    from psycopg.types.json import Json,Jsonb
    typed: list[ColumnSpec[Any]]=[]
    for _,spec in specs:
        if not isinstance(spec,ColumnSpec): raise ValueError('composite component metadata required')
        typed.append(spec)
    class CompositeLoader(Loader):
        format=Format.BINARY
        def __init__(self,oid: int,context: Any=None) -> None:
            super().__init__(oid,context)
            if context is None: raise ValueError('composite requires native codec context')
            self.loaders=[context.adapters.get_loader(spec.type_oid,Format.BINARY)(spec.type_oid,context) for spec in typed]
        def load(self,data: Any) -> PgComposite:
            raw=bytes(data)
            if len(raw)<4 or len(raw)>MAX_COMPOSITE_BYTES or struct.unpack_from('!i',raw)[0]!=len(typed): raise ValueError('native composite field count/budget mismatch')
            values=[];offset=4
            for spec,loader in zip(typed,self.loaders):
                if len(raw)<offset+8: raise ValueError('truncated composite field header')
                oid,size=struct.unpack_from('!Ii',raw,offset);offset+=8
                if oid!=spec.type_oid: raise ValueError('composite component OID mismatch')
                if size==-1: value=None
                else:
                    if size<0 or len(raw)<offset+size: raise ValueError('invalid composite component length')
                    value=loader.load(raw[offset:offset+size]);offset+=size
                values.append(spec.decode(value))
            if offset!=len(raw): raise ValueError('trailing composite component bytes')
            return PgComposite(tuple(values),identity)
    class CompositeDumper(Dumper):
        format=Format.BINARY
        def __init__(self,cls: type[Any],context: Any=None) -> None:
            super().__init__(cls,context);self.context=context
        def get_key(self,obj: BoundComposite,format: PyFormat) -> Any: return (type(obj),obj.value.identity.oid)
        def upgrade(self,obj: BoundComposite,format: PyFormat) -> CompositeDumper:
            upgraded=CompositeDumper(self.cls,self.context);upgraded.oid=obj.value.identity.oid;return upgraded
        def dump(self,obj: BoundComposite) -> bytes:
            if obj.value.identity._owner is not owner: raise ValueError('composite binding belongs to another connection')
            # A shared dumper serves all composites on this connection; use the
            # admitted binding's component specs rather than a captured registry.
            if len(obj.value.fields)!=len(obj.specs): raise ValueError('composite binding field count mismatch')
            parts=[struct.pack('!i',len(obj.specs))];total=4
            for (_,spec),value in zip(obj.specs,obj.value.fields):
                if not isinstance(spec,ColumnSpec): raise ValueError('composite component metadata required')
                spec.check(value)
                if value is None: raw=None
                elif isinstance(value,PgArray): raw=_adapter_classes()[4](type(value),self.context).dump(BoundArray(value,spec.sql_type))
                elif isinstance(value,PgRange): raw=_adapter_classes()[5+len(ARRAY_OIDS)](type(value),self.context).dump(BoundRange(value,spec.sql_type))
                else:
                    if isinstance(value,JsonDocument): value=(Jsonb if spec.sql_type=='jsonb' else Json)(value.text,dumps=lambda text:text)
                    raw=self.context.adapters.get_dumper_by_oid(spec.type_oid,Format.BINARY)(type(value),self.context).dump(value)
                    if raw is None: raise ValueError('non-NULL composite component encoded NULL')
                if value is not None and raw is None: raise ValueError('non-NULL composite component encoded NULL')
                encoded=None if raw is None else bytes(raw)
                total+=8+(0 if encoded is None else len(encoded))
                if total>MAX_COMPOSITE_BYTES: raise ValueError('composite native byte budget exceeded')
                parts.append(struct.pack('!Ii',spec.type_oid,-1 if encoded is None else len(encoded)))
                if encoded is not None: parts.append(encoded)
            return b''.join(parts)
    connection.adapters.register_loader(identity.oid,CompositeLoader)
    connection.adapters.register_dumper(BoundComposite,CompositeDumper)
