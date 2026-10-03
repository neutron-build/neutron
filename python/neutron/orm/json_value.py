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
