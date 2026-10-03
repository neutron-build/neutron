"""Qualified, connection-owned PostgreSQL enum/domain identity."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Generic, TypeVar
T=TypeVar('T')

@dataclass(frozen=True)
class CatalogType:
    schema: str
    name: str
    oid: int
    kind: str
    base_oid: int
    required: bool
    _owner: object
    def __deepcopy__(self,memo: dict[int,object]) -> CatalogType: return self

@dataclass(frozen=True)
class PgEnum:
    label: str
    identity: CatalogType
    def __post_init__(self) -> None:
        if type(self.label) is not str or not isinstance(self.identity,CatalogType) or self.identity.kind!='e':
            raise ValueError('qualified enum identity and native label required')

@dataclass(frozen=True)
class PgDomain(Generic[T]):
    value: T
    identity: CatalogType
    def __post_init__(self) -> None:
        if self.value is None or not isinstance(self.identity,CatalogType) or self.identity.kind!='d':
            raise ValueError('non-NULL domain value and qualified identity required')

@dataclass(frozen=True)
class BoundCatalog:
    value: object
    identity: CatalogType
    base_spec: object|None=None

TYPE_SQL='''WITH RECURSIVE chain AS (
 SELECT t.oid,t.typtype::text AS kind,t.typbasetype AS base,t.typnotnull AS required,n.nspname AS schema,t.typname AS name,0 AS depth
 FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname=%s AND t.typname=%s
 UNION ALL
 SELECT t.oid,t.typtype::text,t.typbasetype,t.typnotnull,n.nspname,t.typname,c.depth+1
 FROM chain c JOIN pg_catalog.pg_type t ON t.oid=c.base JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace WHERE c.kind='d' AND c.depth<16)
 SELECT * FROM chain ORDER BY depth'''
TABLE_SQL='''SELECT a.attname AS name,a.atttypid AS oid,a.attnotnull AS required FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid=a.attrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relname=%s AND c.relkind IN ('r','p','v','m','f') AND a.attnum>0 AND NOT a.attisdropped'''


def admitted_type(rows: list[dict[str,Any]],schema: str,name: str,kind: str,owner: object) -> CatalogType:
    if not rows or rows[0]['kind']!=kind or rows[-1]['kind']=='d': raise ValueError('catalog type absent, incompatible or domain depth exceeded')
    return CatalogType(schema,name,int(rows[0]['oid']),kind,int(rows[-1]['oid']),any(bool(row['required']) for row in rows),owner)


def register_catalog_values(connection: object,owner: object,identity: CatalogType) -> None:
    from typing import Any
    from psycopg.adapt import Dumper, Loader, PyFormat
    from psycopg.pq import Format
    from .pg_value import BoundArray, BoundRange, PgArray, PgRange
    from .pg_adapters import _adapter_classes, ARRAY_OIDS
    from .json_value import JsonDocument, MutableJson
    from psycopg.types.json import Json,Jsonb
    context: Any=connection
    encoding=context.info.encoding
    class EnumLoader(Loader):
        format=Format.BINARY
        def load(self,data: Any) -> PgEnum: return PgEnum(bytes(data).decode(encoding),identity)
    class CatalogDumper(Dumper):
        format=Format.BINARY
        def __init__(self,cls: type[Any],context: Any=None) -> None:
            super().__init__(cls,context);self.context=context
        def get_key(self,obj: BoundCatalog,format: PyFormat) -> Any: return (type(obj),obj.identity.oid)
        def upgrade(self,obj: BoundCatalog,format: PyFormat) -> CatalogDumper:
            upgraded=CatalogDumper(self.cls,self.context);upgraded.oid=obj.identity.oid;return upgraded
        def dump(self,obj: BoundCatalog) -> bytes:
            if obj.identity._owner is not owner: raise ValueError('catalog binding belongs to another connection')
            if obj.identity.kind=='e':
                if type(obj.value) is not str: raise ValueError('native enum label required')
                return obj.value.encode(encoding)
            from .core import ColumnSpec
            spec=obj.base_spec
            if not isinstance(spec,ColumnSpec) or spec.type_oid!=obj.identity.base_oid: raise ValueError('domain native base mismatch')
            value=obj.value
            classes=_adapter_classes()
            if isinstance(value,PgArray): return classes[4](type(value),self.context).dump(BoundArray(value,spec.sql_type))
            if isinstance(value,PgRange): return classes[5+len(ARRAY_OIDS)](type(value),self.context).dump(BoundRange(value,spec.sql_type))
            if isinstance(value,MutableJson): value=JsonDocument(value.text)
            if isinstance(value,JsonDocument): value=(Jsonb if spec.sql_type=='jsonb' else Json)(value.text,dumps=lambda text:text)
            cls=self.context.adapters.get_dumper_by_oid(spec.type_oid,Format.BINARY)
            data=cls(type(value),self.context).dump(value)
            if data is None: raise ValueError('non-NULL domain encoded NULL')
            return bytes(data)
    if identity.kind=='e': context.adapters.register_loader(identity.oid,EnumLoader)
    context.adapters.register_dumper(BoundCatalog,CatalogDumper)
