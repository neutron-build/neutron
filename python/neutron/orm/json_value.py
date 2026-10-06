"""Immutable JSON documents; SQL NULL remains Python None."""
from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal
import json
from typing import Any


def _constant(value: str) -> Any:
    raise ValueError('nonfinite JSON numbers are not supported')


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result: raise ValueError('duplicate JSON object keys are not supported')
        result[key] = value
    return result

@dataclass(frozen=True)
class JsonDocument:
    """Validated JSON text, retaining numeric precision and lexical source.

    parsed() returns a fresh tree: changing it cannot mutate this document.
    PostgreSQL jsonb may normalize lexical formatting/order on round trips.
    """
    text: str

    def __post_init__(self) -> None:
        if not isinstance(self.text, str): raise ValueError('JSON document requires text')
        self.parsed()

    def parsed(self) -> Any:
        return json.loads(self.text, parse_float=Decimal, parse_int=int,
                          parse_constant=_constant, object_pairs_hook=_pairs)

JSON_NULL = JsonDocument('null')

@dataclass(frozen=True)
class BoundJson:
    document: JsonDocument
    binary: bool


def load_document(value: str | bytes) -> JsonDocument:
    return JsonDocument(value.decode('utf-8') if isinstance(value, bytes) else value)


def native_params(values: tuple[object, ...]) -> tuple[object, ...]:
    # Keep optional native dependencies out of pure compilation/import paths.
    result: list[object] = []
    for value in values:
        if isinstance(value, BoundJson):
            from psycopg.types.json import Json, Jsonb
            wrapper = Jsonb if value.binary else Json
            result.append(wrapper(value.document.text, dumps=lambda text: text))
        else: result.append(value)
    return tuple(result)


def _tree_text(value: Any,seen: set[int]) -> str:
    if value is None: return 'null'
    if type(value) is bool: return 'true' if value else 'false'
    if type(value) is int: return str(value)
    if isinstance(value,Decimal):
        if not value.is_finite(): raise ValueError('nonfinite mutable JSON number')
        return str(value)
    if type(value) is str: return json.dumps(value,ensure_ascii=True)
    if type(value) not in {dict,list}: raise ValueError('mutable JSON requires dict/list/native JSON scalar values')
    identity=id(value)
    if identity in seen: raise ValueError('cyclic mutable JSON collection')
    seen.add(identity)
    try:
        if type(value) is list: return '['+','.join(_tree_text(item,seen) for item in value)+']'
        if any(type(key) is not str for key in value): raise ValueError('mutable JSON object keys must be strings')
        return '{'+','.join(json.dumps(key)+':'+_tree_text(item,seen) for key,item in value.items())+'}'
    finally: seen.remove(identity)


def _tree_equal(left: Any,right: Any) -> bool:
    if isinstance(left,bool) or isinstance(right,bool): return type(left) is bool and type(right) is bool and left is right
    if type(left) in {int,Decimal} and type(right) in {int,Decimal}: return Decimal(left)==Decimal(right)
    if type(left) is not type(right): return False
    if type(left) is dict: return left.keys()==right.keys() and all(_tree_equal(left[key],right[key]) for key in left)
    if type(left) is list: return len(left)==len(right) and all(_tree_equal(a,b) for a,b in zip(left,right))
    return bool(left==right)


class MutableJson:
    """Explicit mutable JSONB tree. None inside value is JSON null, not SQL NULL."""
    def __init__(self,value: Any) -> None:
        from copy import deepcopy
        _tree_text(value,set())
        self.value=deepcopy(value)

    @property
    def text(self) -> str: return _tree_text(self.value,set())

    def parsed(self) -> Any:
        from copy import deepcopy
        _tree_text(self.value,set())
        return deepcopy(self.value)

    def __eq__(self,other: object) -> bool:
        if not isinstance(other,MutableJson): return False
        # Refuse invalid edited trees before recursively comparing snapshots.
        self.text;other.text
        return _tree_equal(self.value,other.value)
