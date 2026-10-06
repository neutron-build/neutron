"""Connection-local binary component codecs with explicit builtin OID identity."""
from __future__ import annotations
from functools import cache
import struct
from typing import Any
from .network_value import Inet,CIDR
from ipaddress import ip_address
from .pg_value import ArrayDimension, BoundArray, BoundRange, Interval, MAX_ARRAY_ELEMENTS, PgArray, PgRange, TimeOfDay

# PostgreSQL catalog builtin identities are stable, unlike user-defined OIDs.
BUILTIN_OIDS={'int2':21,'int4':23,'int8':20,'text':25,'varchar':1043,'bool':16,'numeric':1700,'uuid':2950,'bytea':17,'date':1082,'timestamp':1114,'timestamptz':1184,'time':1083,'interval':1186,'json':114,'jsonb':3802,'inet':869,'cidr':650}
ARRAY_OIDS={'int2[]':1005,'int4[]':1007,'int8[]':1016,'text[]':1009,'varchar[]':1015,'bool[]':1000,'numeric[]':1231,'uuid[]':2951,'bytea[]':1001,'date[]':1182,'timestamp[]':1115,'timestamptz[]':1185,'time[]':1183,'interval[]':1187}

RANGE_OIDS={'int4range':(3904,23),'int8range':(3926,20),'numrange':(3906,1700),'daterange':(3912,1082),'tsrange':(3908,1114),'tstzrange':(3910,1184)}
BUILTIN_OIDS.update({name:oids[0] for name,oids in RANGE_OIDS.items()})

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
    class RangeLoader(Loader):
        format=Format.BINARY
        element_oid: int
        def __init__(self,oid: int,context: Any=None) -> None:
            super().__init__(oid,context)
            if context is None: raise ValueError('range codec requires connection context')
            cls=context.adapters.get_loader(self.element_oid,Format.BINARY)
            self.element_loader=cls(self.element_oid,context)
        def load(self,data: Any) -> PgRange[Any]:
            raw=bytes(data)
            if not raw or raw[0]&~31: raise ValueError('invalid native range flags')
            flags=raw[0]
            if flags&1:
                if flags!=1 or len(raw)!=1: raise ValueError('invalid empty native range')
                return PgRange(empty=True)
            bounds: list[Any]=[];offset=1
            for unbounded in (8,16):
                if flags&unbounded: bounds.append(None);continue
                if len(raw)<offset+4: raise ValueError('truncated range bound')
                size=struct.unpack_from('!i',raw,offset)[0];offset+=4
                if size<0 or len(raw)<offset+size: raise ValueError('invalid range bound size')
                bounds.append(self.element_loader.load(raw[offset:offset+size]));offset+=size
            if offset!=len(raw): raise ValueError('trailing native range bytes')
            return PgRange(bounds[0],bounds[1],bool(flags&2),bool(flags&4))
    class RangeDumper(Dumper):
        format=Format.BINARY
        def __init__(self,cls: type[Any],context: Any=None) -> None:
            super().__init__(cls,context);self.context=context
        def get_key(self,obj: BoundRange,format: PyFormat) -> Any: return (type(obj),obj.sql_type)
        def upgrade(self,obj: BoundRange,format: PyFormat) -> RangeDumper:
            upgraded=RangeDumper(self.cls,self.context);upgraded.oid=RANGE_OIDS[obj.sql_type][0];return upgraded
        def dump(self,obj: BoundRange) -> bytes:
            value=obj.value
            if value.empty: return bytes((1,))
            flags=int(value.lower_inclusive)*2+int(value.upper_inclusive)*4+int(value.lower is None)*8+int(value.upper is None)*16
            parts=[bytes((flags,))];oid=RANGE_OIDS[obj.sql_type][1]
            for bound in (value.lower,value.upper):
                if bound is None: continue
                cls=self.context.adapters.get_dumper_by_oid(oid,Format.BINARY)
                encoded=cls(type(bound),self.context).dump(bound)
                if encoded is None: raise ValueError('finite range bound encoded NULL')
                raw=bytes(encoded);parts.extend((struct.pack('!i',len(raw)),raw))
            return b''.join(parts)
    arrays=tuple(type('Native'+name.replace('[]','Array'),(ArrayLoader,),{'element_oid':BUILTIN_OIDS[name[:-2]]}) for name in ARRAY_OIDS)
    ranges=tuple(type('Native'+name.title(),(RangeLoader,),{'element_oid':oids[1]}) for name,oids in RANGE_OIDS.items())
    return TimeLoader,TimeDumper,IntervalLoader,IntervalDumper,ArrayDumper,*arrays,RangeDumper,*ranges


def register_native_values(connection: Any) -> None:
    inet_load,cidr_load,inet_dump,cidr_dump=_network_adapter_classes()
    connection.adapters.register_loader(869,inet_load);connection.adapters.register_loader(650,cidr_load)
    connection.adapters.register_dumper(Inet,inet_dump);connection.adapters.register_dumper(CIDR,cidr_dump)
    classes=_adapter_classes();time_load,time_dump,interval_load,interval_dump,array_dump=classes[:5]
    for name,oid in BUILTIN_OIDS.items():
        info=connection.adapters.types.get(name)
        if info is None or info.oid!=oid: raise ValueError('native builtin SQL type identity mismatch')
    connection.adapters.register_loader(1083,time_load);connection.adapters.register_dumper(TimeOfDay,time_dump)
    connection.adapters.register_loader(1186,interval_load);connection.adapters.register_dumper(Interval,interval_dump)
    connection.adapters.register_dumper(BoundArray,array_dump)
    range_start=5+len(ARRAY_OIDS)
    connection.adapters.register_dumper(BoundRange,classes[range_start])
    for (name,(oid,_)),cls in zip(RANGE_OIDS.items(),classes[range_start+1:]):
        connection.adapters.register_loader(oid,cls)
    for (name,oid),cls in zip(ARRAY_OIDS.items(),classes[5:range_start]):
        info=connection.adapters.types.get(name[:-2])
        if info is None or info.array_oid!=oid: raise ValueError('native array SQL type identity mismatch')
        connection.adapters.register_loader(oid,cls)


@cache
def _network_adapter_classes() -> tuple[type[Any],...]:
    from psycopg.adapt import Dumper,Loader
    from psycopg.pq import Format
    class NetworkLoader(Loader):
        format=Format.BINARY
        model: type[Inet]|type[CIDR]
        def load(self,data: Any) -> Inet|CIDR:
            raw=bytes(data)
            if len(raw)<4: raise ValueError('truncated native network header')
            family,bits,is_cidr,size=raw[:4]
            expected=4 if family==2 else 16 if family==3 else 0
            if expected==0 or size!=expected or len(raw)!=4+expected or is_cidr!=int(self.model is CIDR):
                raise ValueError('native network family/header mismatch')
            return self.model(ip_address(raw[4:]),bits)
    class InetLoader(NetworkLoader): model=Inet
    class CIDRLoader(NetworkLoader): model=CIDR
    class NetworkDumper(Dumper):
        format=Format.BINARY
        def dump(self,obj: Inet|CIDR) -> bytes:
            # Value constructors reject scopes, host bits for CIDR and bad prefixes.
            if type(obj) not in {Inet,CIDR}: raise ValueError('exact native network family required')
            return bytes((2 if obj.address.version==4 else 3,obj.prefix_length,int(type(obj) is CIDR),len(obj.address.packed)))+obj.address.packed
    class InetDumper(NetworkDumper): oid=869
    class CIDRDumper(NetworkDumper): oid=650
    return InetLoader,CIDRLoader,InetDumper,CIDRDumper
