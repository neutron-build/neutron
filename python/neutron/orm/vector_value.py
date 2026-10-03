"""Extension-qualified immutable finite pgvector float32 components."""
from __future__ import annotations
from dataclasses import dataclass
import math
import struct
from typing import Any,Iterable
from .catalog_value import CatalogType

MAX_VECTOR_DIMENSIONS=16000

def _float32(value: float) -> float:
    if type(value) is not float or not math.isfinite(value): raise ValueError('finite float32 vector elements required')
    try: rounded=struct.unpack('!f',struct.pack('!f',value))[0]
    except (OverflowError,struct.error) as exc: raise ValueError('vector element exceeds float32 range') from exc
    if not math.isfinite(rounded): raise ValueError('vector element exceeds finite float32 range')
    return rounded

@dataclass(frozen=True)
class PgVector:
    elements: tuple[float,...]
    identity: CatalogType
    def __post_init__(self) -> None:
        if not isinstance(self.identity,CatalogType) or self.identity.kind!='b' or self.identity.name!='vector' or type(self.elements) is not tuple or not 0<len(self.elements)<=MAX_VECTOR_DIMENSIONS:
            raise ValueError('qualified vector identity and immutable finite dimensions required')
        if any(_float32(value)!=value for value in self.elements): raise ValueError('exact float32 components required; use from_values for explicit rounding')
    @classmethod
    def from_values(cls,values: Iterable[float],identity: CatalogType) -> PgVector:
        result=[]
        for value in values:
            if len(result)>=MAX_VECTOR_DIMENSIONS: raise ValueError('vector dimension budget exceeded')
            result.append(_float32(value))
        return cls(tuple(result),identity)

@dataclass(frozen=True)
class BoundVector:
    value: PgVector

VECTOR_SQL='''SELECT e.extname AS name FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_extension e ON e.oid OPERATOR(pg_catalog.=) d.refobjid WHERE d.classid OPERATOR(pg_catalog.=) 'pg_catalog.pg_type'::pg_catalog.regclass AND d.objid OPERATOR(pg_catalog.=) %s AND d.refclassid OPERATOR(pg_catalog.=) 'pg_catalog.pg_extension'::pg_catalog.regclass AND d.deptype OPERATOR(pg_catalog.=) 'e' '''


def admitted_vector(rows: list[dict[str,Any]],identity: CatalogType) -> None:
    if identity.kind!='b' or identity.name!='vector' or len(rows)!=1 or rows[0]['name']!='vector': raise ValueError('native vector extension membership required')


def register_vector(connection: Any,owner: object,identity: CatalogType) -> None:
    from psycopg.adapt import Dumper,Loader,PyFormat
    from psycopg.pq import Format
    class VectorLoader(Loader):
        format=Format.BINARY
        def load(self,data: Any) -> PgVector:
            raw=bytes(data)
            if len(raw)<4: raise ValueError('truncated vector native header')
            count,reserved=struct.unpack_from('!HH',raw)
            if reserved!=0 or not 0<count<=MAX_VECTOR_DIMENSIONS or len(raw)!=4+count*4: raise ValueError('vector native dimensions/header mismatch')
            return PgVector(struct.unpack_from('!'+str(count)+'f',raw,4),identity)
    class VectorDumper(Dumper):
        format=Format.BINARY
        def __init__(self,cls: type[Any],context: Any=None) -> None:
            super().__init__(cls,context);self.context=context
        def get_key(self,obj: BoundVector,format: PyFormat) -> Any: return (type(obj),obj.value.identity.oid)
        def upgrade(self,obj: BoundVector,format: PyFormat) -> VectorDumper:
            upgraded=VectorDumper(self.cls,self.context);upgraded.oid=obj.value.identity.oid;return upgraded
        def dump(self,obj: BoundVector) -> bytes:
            if obj.value.identity._owner is not owner: raise ValueError('vector binding belongs to another connection')
            values=obj.value.elements
            return struct.pack('!HH',len(values),0)+struct.pack('!'+str(len(values))+'f',*values)
    connection.adapters.register_loader(identity.oid,VectorLoader)
    connection.adapters.register_dumper(BoundVector,VectorDumper)
