"""Bounded callback-free query metrics: no SQL, values, URLs or error messages."""
from __future__ import annotations
import asyncio
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
import threading
import time
from typing import Iterator,Literal

Operation=Literal['query','execute','stream']
Outcome=Literal['ok','error','cancelled']
_MAX_COUNTER=2**63-1

@dataclass(frozen=True)
class QueryEvent:
    operation: Operation
    elapsed_ns: int
    row_count: int|None
    sqlstate: str|None
    outcome: Outcome
    owned_transaction: bool

@dataclass(frozen=True)
class QueryMetrics:
    queries: int
    executions: int
    streams: int
    failures: int
    cancellations: int
    elapsed_ns: int
    dropped_events: int

class QueryObserver:
    """Pull immutable redacted events; sharing this buffer never shares a Session."""
    def __init__(self,*,capacity: int=256) -> None:
        if type(capacity) is not int or not 0<capacity<=4096: raise ValueError('observer capacity must be from 1 through 4096')
        self._events: deque[QueryEvent]=deque(maxlen=capacity)
        self._lock=threading.Lock();self._counts=[0]*7
    def drain(self) -> tuple[QueryEvent,...]:
        with self._lock:
            result=tuple(self._events);self._events.clear();return result
    @property
    def metrics(self) -> QueryMetrics:
        with self._lock: return QueryMetrics(*self._counts)
    def _record(self,event: QueryEvent) -> None:
        with self._lock:
            if len(self._events)==self._events.maxlen: self._counts[6]=min(_MAX_COUNTER,self._counts[6]+1)
            self._events.append(event)
            index={'query':0,'execute':1,'stream':2}[event.operation]
            self._counts[index]=min(_MAX_COUNTER,self._counts[index]+1)
            if event.outcome!='ok': self._counts[3]=min(_MAX_COUNTER,self._counts[3]+1)
            if event.outcome=='cancelled': self._counts[4]=min(_MAX_COUNTER,self._counts[4]+1)
            self._counts[5]=min(_MAX_COUNTER,self._counts[5]+event.elapsed_ns)

class _Measurement:
    def __init__(self,observer: QueryObserver|None,operation: Operation) -> None:
        self.observer=observer;self.operation=operation;self.started: int|None=None;self.rows: int|None=None;self.owned=False;self.outcome: Outcome='ok';self.sqlstate: str|None=None
    def dispatch(self,*,owned: bool) -> None:
        if self.observer is not None and self.started is None: self.started=time.perf_counter_ns();self.owned=owned
    def note_error(self,error: BaseException) -> None:
        from .core import OrmError
        # Read only owned error fields or a native class's fixed SQLSTATE. Never
        # invoke arbitrary exception property access, __str__ or repr callbacks.
        state=vars(error).get('sqlstate') if type(error) is OrmError else vars(type(error)).get('sqlstate')
        self.sqlstate=state if type(state) is str and len(state)==5 and all(c in '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ' for c in state) else None
        self.outcome='cancelled' if isinstance(error,(asyncio.CancelledError,KeyboardInterrupt,SystemExit)) else 'error'
    def finish(self,error: BaseException|None) -> None:
        if self.observer is None or self.started is None: return
        start=self.started;self.started=None
        if error is not None: self.note_error(error)
        rows=self.rows if self.rows is not None and self.rows>=0 else None
        self.observer._record(QueryEvent(self.operation,min(_MAX_COUNTER,max(0,time.perf_counter_ns()-start)),rows,self.sqlstate,self.outcome,self.owned))

@contextmanager
def _measure(observer: QueryObserver|None,operation: Operation) -> Iterator[_Measurement]:
    measurement=_Measurement(observer,operation)
    try: yield measurement
    except BaseException as exc:
        measurement.finish(exc);raise
    else: measurement.finish(None)
