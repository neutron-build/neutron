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
    capabilities: frozenset[str] = frozenset()
    package_enabled: bool = True
    qualification: str = 'reported-identity-only'


def startup_version(value: object, *, profile: str='postgres-direct') -> str:
    validate_profile(profile)
    if profile == NUCLEUS_CANDIDATE_PROFILE:
        if value != '16.0 (Nucleus)': raise OrmError('unknown Nucleus candidate startup identity')
        return '16.0'
    if not isinstance(value,str) or any(marker in value.casefold() for marker in _UNSUPPORTED):
        raise OrmError('unsupported or unknown endpoint engine identity')
    match=_STARTUP.fullmatch(value)
    if match is None: raise OrmError('unsupported or unknown endpoint engine identity')
    return match.group(1)


def admit(startup: object, reported: object, *, profile: str='postgres-direct') -> EndpointIdentity:
    version=startup_version(startup,profile=profile)
    if profile == NUCLEUS_CANDIDATE_PROFILE:
        if reported != 'PostgreSQL 16.0 (Nucleus '+NUCLEUS_CANDIDATE_VERSION+' — The Definitive Database)':
            raise OrmError('unknown or contradictory Nucleus candidate reported identity')
        return EndpointIdentity('nucleus',NUCLEUS_CANDIDATE_VERSION,profile=profile,
            capabilities=NUCLEUS_CAPABILITIES,package_enabled=False,qualification='uncertified-finite-candidate')
    if not isinstance(reported,str) or any(marker in reported.casefold() for marker in _UNSUPPORTED):
        raise OrmError('unsupported or unknown endpoint engine identity')
    match=_VERSION.match(reported)
    if match is None or match.group(1)!=version:
        raise OrmError('contradictory or unknown endpoint engine identity')
    return EndpointIdentity('postgresql',version)

# An explicit qualification profile, never inferred from pgwire compatibility.
# This does not attest a binary or enable a certified package support matrix.
NUCLEUS_CANDIDATE_PROFILE = 'nucleus-relational-rc-v1-candidate'
NUCLEUS_CANDIDATE_VERSION = '1.2.2'
NUCLEUS_CAPABILITIES = frozenset({'point-crud','read-committed-transaction','savepoint'})


def validate_profile(profile: str) -> None:
    if not isinstance(profile,str) or profile not in {'postgres-direct', NUCLEUS_CANDIDATE_PROFILE}:
        raise OrmError('unsupported/unknown execution profile; operation refused')


def require_profile_capability(identity: EndpointIdentity | None, capability: str) -> None:
    if identity is not None and identity.engine == 'nucleus' and capability not in identity.capabilities:
        raise OrmError('operation outside uncertified Nucleus finite profile; refused before dispatch')


def guard_finite_operation(identity: EndpointIdentity | None, operation: object) -> None:
    if identity is None or identity.engine != 'nucleus': return
    from .core import Mutation, Returning, Select, Table
    from .json_value import BoundJson
    import datetime as dt
    if isinstance(operation,Returning) and type(operation) is Returning:
        mutation=operation.mutation
        table=mutation.table
        compiled=operation.compile()
        sql,params=compiled.sql,compiled.params
    elif isinstance(operation,Select) and type(operation) is Select:
        table=operation.table
        if operation.predicate is None:
            raise OrmError('finite profile SELECT requires a bound point predicate')
        compiled=operation.compile()
        sql,params=compiled.sql,compiled.params
    elif isinstance(operation,Mutation) and type(operation) is Mutation:
        table=operation.table
        sql,params=operation.sql,operation.params
    else:
        raise OrmError('query algebra outside uncertified Nucleus finite profile')
    if not isinstance(table,Table) or type(table) is not Table or table._catalog_owner is not None:
        raise OrmError('finite profile requires ordinary qualified physical table metadata')
    allowed_types={'int4','int8','bool','text','jsonb','timestamptz'}
    if any(c.spec.native_type is not None or c.spec.sql_type not in allowed_types for c in table.columns.values()):
        raise OrmError('column type outside uncertified Nucleus finite profile')
    for value in params:
        if value is None or type(value) in {int,bool,str}: continue
        if isinstance(value,BoundJson) and value.binary: continue
        if isinstance(value,dt.datetime) and value.tzinfo is not None and value.utcoffset()==dt.timedelta(0): continue
        raise OrmError('parameter type outside uncertified Nucleus finite profile')
    # Finite generated grammar: no raw literals, comments, casts, functions,
    # subqueries, joins, graph/aggregate operations, or lifecycle/DDL text.
    from .core import _bound_quote
    source=table._bound_sql
    column_names='(?:'+'|'.join(re.escape(_bound_quote(c.name)) for c in table.columns.values())+')'
    qualified=re.escape(source)+r'\.'+column_names
    point=qualified+r' = %s'
    fields=qualified+r'(?:, '+qualified+r')*'
    returning=r'(?: RETURNING '+column_names+r'(?:, '+column_names+r')*)?'
    atom=r'(?:%s|DEFAULT)'
    insert=re.escape('INSERT INTO '+source)+r'(?: DEFAULT VALUES| \('+column_names+r'(?:, '+column_names+r')*\) VALUES \('+atom+r'(?:, '+atom+r')*\))'+returning
    update=re.escape('UPDATE '+source)+r' SET '+column_names+r' = '+atom+r'(?:, '+column_names+r' = '+atom+r')* WHERE '+point+returning
    delete=re.escape('DELETE FROM '+source)+r' WHERE '+point+returning
    select=r'SELECT '+fields+re.escape(' FROM '+source)+r' WHERE '+point
    pattern=select if type(operation) is Select else '(?:'+insert+'|'+update+'|'+delete+')'
    if re.fullmatch(pattern,sql) is None or sql.count('%s') != len(params):
        raise OrmError('SQL shape outside uncertified Nucleus finite point profile')
