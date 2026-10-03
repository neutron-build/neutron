"""Connection-local binary component codecs with explicit builtin OID identity."""
from __future__ import annotations
from functools import cache
import struct
from typing import Any
from .pg_value import ArrayDimension, BoundArray, Interval, MAX_ARRAY_ELEMENTS, PgArray, TimeOfDay

# PostgreSQL catalog builtin identities are stable, unlike user-defined OIDs.
BUILTIN_OIDS={'int2':21,'int4':23,'int8':20,'text':25,'varchar':1043,'bool':16,'numeric':1700,'uuid':2950,'bytea':17,'date':1082,'timestamp':1114,'timestamptz':1184,'time':1083,'interval':1186,'json':114,'jsonb':3802}
ARRAY_OIDS={'int2[]':1005,'int4[]':1007,'int8[]':1016,'text[]':1009,'varchar[]':1015,'bool[]':1000,'numeric[]':1231,'uuid[]':2951,'bytea[]':1001,'date[]':1182,'timestamp[]':1115,'timestamptz[]':1185,'time[]':1183,'interval[]':1187}

@cache
def _adapter_classes() -> tuple[type[Any],...]:
    from psycopg.adapt import Dumper, Loader
    from psycopg.pq import Format
    from psycopg.adapt import PyFormat

    class TimeLoader(Loader):
        format=Format.BINARY
        def load(self,data: Any) -> TimeOfDay:
            return TimeOfDay(struct.unpack('!q',bytes(data))[0])
    class TimeDumper(Dumper):
        format=Format.BINARY;oid=1083
        def dump(self,obj: TimeOfDay) -> bytes: return struct.pack('!q',obj.microseconds)
    class IntervalLoader(Loader):
        format=Format.BINARY
        def load(self,data: Any) -> Interval:
            micros,days,months=struct.unpack('!qii',bytes(data));return Interval(months,days,micros)
    class IntervalDumper(Dumper):
        format=Format.BINARY;oid=1186
        def dump(self,obj: Interval) -> bytes: return struct.pack('!qii',obj.microseconds,obj.days,obj.months)
    class ArrayLoader(Loader):
        format=Format.BINARY
        element_oid: int
        def __init__(self,oid: int,context: Any=None) -> None:
            super().__init__(oid,context)
            if context is None: raise ValueError('array codec requires connection context')
            cls=context.adapters.get_loader(self.element_oid,Format.BINARY)
            self.element_loader=cls(self.element_oid,context)
        def load(self,data: Any) -> PgArray[Any]:
            raw=bytes(data)
            if len(raw)<12: raise ValueError('truncated binary array')
            dims,has_null,oid=struct.unpack_from('!iii',raw)
            if not 0<=dims<=6 or has_null not in {0,1} or oid!=self.element_oid or len(raw)<12+8*dims:
                raise ValueError('array element OID/header mismatch')
            dimensions=tuple(ArrayDimension(*struct.unpack_from('!ii',raw,12+8*i)) for i in range(dims))
            count=1 if dims else 0
            for dim in dimensions:
                count*=dim.length
                if count>MAX_ARRAY_ELEMENTS: raise ValueError('array element budget exceeded')
            values: list[Any]=[];offset=12+8*dims
            for _ in range(count):
                if len(raw)<offset+4: raise ValueError('truncated array member size')
                size=struct.unpack_from('!i',raw,offset)[0];offset+=4
                if size==-1:
                    if not has_null: raise ValueError('array NULL member/header mismatch')
                    values.append(None)
                else:
                    if size<0 or len(raw)<offset+size: raise ValueError('invalid array member size')
                    values.append(self.element_loader.load(raw[offset:offset+size]));offset+=size
            if offset!=len(raw): raise ValueError('trailing binary array bytes')
            return PgArray(dimensions,tuple(values))
    class ArrayDumper(Dumper):
        format=Format.BINARY
        def __init__(self,cls: type[Any],context: Any=None) -> None:
            super().__init__(cls,context);self.context=context
        def get_key(self,obj: BoundArray,format: PyFormat) -> Any: return (type(obj),obj.sql_type)
        def upgrade(self,obj: BoundArray,format: PyFormat) -> ArrayDumper:
            upgraded=ArrayDumper(self.cls,self.context);upgraded.oid=ARRAY_OIDS[obj.sql_type];return upgraded
        def dump(self,obj: BoundArray) -> bytes:
            oid=BUILTIN_OIDS[obj.sql_type[:-2]]
            parts=[struct.pack('!iii',len(obj.value.dimensions),int(None in obj.value.elements),oid)]
            parts.extend(struct.pack('!ii',dim.length,dim.lower_bound) for dim in obj.value.dimensions)
            for item in obj.value.elements:
                if item is None: parts.append(struct.pack('!i',-1));continue
                cls=self.context.adapters.get_dumper_by_oid(oid,Format.BINARY)
                value=cls(type(item),self.context).dump(item)
                if value is None: raise ValueError('non-NULL array scalar encoded NULL')
                encoded=bytes(value);parts.extend((struct.pack('!i',len(encoded)),encoded))
            return b''.join(parts)
    arrays=tuple(type('Native'+name.replace('[]','Array'),(ArrayLoader,),{'element_oid':BUILTIN_OIDS[name[:-2]]}) for name in ARRAY_OIDS)
    return TimeLoader,TimeDumper,IntervalLoader,IntervalDumper,ArrayDumper,*arrays


def register_native_values(connection: Any) -> None:
    classes=_adapter_classes();time_load,time_dump,interval_load,interval_dump,array_dump=classes[:5]
    for name,oid in BUILTIN_OIDS.items():
        info=connection.adapters.types.get(name)
        if info is None or info.oid!=oid: raise ValueError('native builtin SQL type identity mismatch')
    connection.adapters.register_loader(1083,time_load);connection.adapters.register_dumper(TimeOfDay,time_dump)
    connection.adapters.register_loader(1186,interval_load);connection.adapters.register_dumper(Interval,interval_dump)
    connection.adapters.register_dumper(BoundArray,array_dump)
    for (name,oid),cls in zip(ARRAY_OIDS.items(),classes[5:]):
        info=connection.adapters.types.get(name[:-2])
        if info is None or info.array_oid!=oid: raise ValueError('native array SQL type identity mismatch')
        connection.adapters.register_loader(oid,cls)
