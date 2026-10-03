"""Ordered scalar Session events; callbacks never imply graph instrumentation."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Literal

EventName = Literal['before_flush', 'before_insert', 'before_update', 'before_delete',
                    'after_flush', 'after_commit', 'after_rollback']
EVENT_NAMES = frozenset(('before_flush', 'before_insert', 'before_update', 'before_delete',
                         'after_flush', 'after_commit', 'after_rollback'))

@dataclass(frozen=True)
class SessionEvent:
    name: EventName
    obj: object | None = None
