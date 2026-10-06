"""Ordered scalar Session events; callbacks never imply graph instrumentation."""
from __future__ import annotations
import asyncio
from dataclasses import dataclass
from typing import Literal
from .core import OrmError

EventName = Literal['before_flush', 'before_insert', 'before_update', 'before_delete',
                    'after_flush', 'after_commit', 'after_rollback']
EVENT_NAMES = frozenset(('before_flush', 'before_insert', 'before_update', 'before_delete',
                         'after_flush', 'after_commit', 'after_rollback'))

@dataclass(frozen=True)
class SessionEvent:
    name: EventName
    obj: object | None = None


class PostCommitError(OrmError):
    """Postcommit state/notification failure; the database commit is known."""
    def __init__(self) -> None:
        super().__init__('postcommit state or notification failed after known commit',outcome='committed')

class PostCommitCancelledError(asyncio.CancelledError):
    outcome='committed'
    sqlstate=None

class PostCommitInterruptedError(KeyboardInterrupt):
    outcome='committed'
    sqlstate=None
