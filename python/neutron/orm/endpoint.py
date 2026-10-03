"""Engine admission evidence, separate from caller-declared network topology."""
from __future__ import annotations
from dataclasses import dataclass
import re
from .core import OrmError

_STARTUP = re.compile(r'^([0-9]+(?:\.[0-9]+){1,2})(?:\s+\([^\r\n]*\))?$')
_VERSION = re.compile(r'^PostgreSQL ([0-9]+(?:\.[0-9]+){1,2})(?:\s|$)')
_UNSUPPORTED = ('nucleus','cockroach','yugabyte','redshift','greenplum','materialize','questdb','cratedb')

@dataclass(frozen=True)
class EndpointIdentity:
    """Reported engine/version admission; not TLS or intermediary attestation."""
    engine: str
    version: str
    profile: str = 'postgres-direct'
    topology: str = 'caller-declared-direct'


def startup_version(value: object) -> str:
    if not isinstance(value,str) or any(marker in value.casefold() for marker in _UNSUPPORTED):
        raise OrmError('unsupported or unknown endpoint engine identity')
    match=_STARTUP.fullmatch(value)
    if match is None: raise OrmError('unsupported or unknown endpoint engine identity')
    return match.group(1)


def admit(startup: object, reported: object) -> EndpointIdentity:
    version=startup_version(startup)
    if not isinstance(reported,str) or any(marker in reported.casefold() for marker in _UNSUPPORTED):
        raise OrmError('unsupported or unknown endpoint engine identity')
    match=_VERSION.match(reported)
    if match is None or match.group(1)!=version:
        raise OrmError('contradictory or unknown endpoint engine identity')
    return EndpointIdentity('postgresql',version)
