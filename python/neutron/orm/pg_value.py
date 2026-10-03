"""Immutable native PostgreSQL components; SQL NULL remains None."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Generic, TypeVar

T=TypeVar('T')
MAX_ARRAY_ELEMENTS=1_000_000

@dataclass(frozen=True)
class ArrayDimension:
    length: int
    lower_bound: int=1
    def __post_init__(self) -> None:
        if type(self.length) is not int or type(self.lower_bound) is not int or not 0<self.length<2**31 or not -2**31<=self.lower_bound<2**31 or self.lower_bound+self.length-1>=2**31:
            raise ValueError('invalid PostgreSQL array dimension')

@dataclass(frozen=True)
class PgArray(Generic[T]):
    dimensions: tuple[ArrayDimension,...]
    elements: tuple[T|None,...]
    def __post_init__(self) -> None:
        if not isinstance(self.dimensions,tuple) or not isinstance(self.elements,tuple) or len(self.dimensions)>6:
            raise ValueError('immutable array tuples and at most six dimensions required')
        count=1 if self.dimensions else 0
        for dim in self.dimensions:
            if not isinstance(dim,ArrayDimension): raise ValueError('array dimension metadata required')
            count*=dim.length
            if count>MAX_ARRAY_ELEMENTS: raise ValueError('array element budget exceeded')
        if count!=len(self.elements): raise ValueError('array cardinality does not match dimensions')
        # Element profiles are validated by ColumnSpec before binding/decoding.
        if any(isinstance(item,(dict,list,set,bytearray,PgArray)) for item in self.elements):
            raise ValueError('array elements require immutable scalar values')

@dataclass(frozen=True)
class TimeOfDay:
    microseconds: int
    def __post_init__(self) -> None:
        if type(self.microseconds) is not int or not 0<=self.microseconds<=86_400_000_000:
            raise ValueError('finite microsecond time including 24:00 required')

@dataclass(frozen=True)
class Interval:
    months: int=0
    days: int=0
    microseconds: int=0
    def __post_init__(self) -> None:
        if any(type(value) is not int or not -2**31<=value<2**31 for value in (self.months,self.days)) or type(self.microseconds) is not int or not -2**63<=self.microseconds<2**63:
            raise ValueError('interval native component range exceeded')

@dataclass(frozen=True)
class BoundArray:
    value: PgArray[object]
    sql_type: str

@dataclass(frozen=True)
class PgRange(Generic[T]):
    lower: T|None=None
    upper: T|None=None
    lower_inclusive: bool=False
    upper_inclusive: bool=False
    empty: bool=False
    def __post_init__(self) -> None:
        if any(type(flag) is not bool for flag in (self.lower_inclusive,self.upper_inclusive,self.empty)):
            raise ValueError('range bound flags require booleans')
        if self.empty and (self.lower is not None or self.upper is not None or self.lower_inclusive or self.upper_inclusive):
            raise ValueError('empty range cannot carry bounds')
        if self.lower is None and self.lower_inclusive or self.upper is None and self.upper_inclusive:
            raise ValueError('unbounded range cannot be inclusive')
        if any(isinstance(value,(dict,list,set,bytearray,PgArray,PgRange)) for value in (self.lower,self.upper)):
            raise ValueError('range bounds require immutable scalars')

@dataclass(frozen=True)
class BoundRange:
    value: PgRange[object]
    sql_type: str
