"""Conservative transaction statement admission, not a SQL security sandbox."""
from __future__ import annotations
import re
from .core import OrmError

_WORD = re.compile(r'[A-Za-z0-9_]')

def validate_scope_sql(sql: str,*,owned: bool=True) -> None:
    first='';terminated=False;i=0
    while i<len(sql):
        char=sql[i]
        if char in ' \t\r\n': i+=1;continue
        if sql.startswith('--',i):
            end=sql.find('\n',i+2);i=len(sql) if end<0 else end+1;continue
        if sql.startswith('/*',i):
            i+=2;depth=1
            while i<len(sql) and depth:
                if sql.startswith('/*',i): depth+=1;i+=2
                elif sql.startswith('*/',i): depth-=1;i+=2
                else: i+=1
            if depth: raise OrmError('unterminated SQL comment')
            continue
        if terminated: raise OrmError('multiple SQL statements forbidden in owned transaction')
        if char==';': terminated=True;i+=1;continue
        if char in "'\"":
            delimiter=char
            escaped=char=="'" and i>0 and sql[i-1] in 'eE' and (i<2 or not _WORD.fullmatch(sql[i-2]))
            i+=1;closed=False
            while i<len(sql):
                if sql[i]=='\\' and delimiter=="'":
                    if not escaped: raise OrmError('ambiguous ordinary SQL string backslash forbidden')
                    i+=2;continue
                if sql[i]==delimiter:
                    if i+1<len(sql) and sql[i+1]==delimiter: i+=2;continue
                    i+=1;closed=True;break
                i+=1
            if not closed: raise OrmError('unterminated SQL quote')
            continue
        if char=='$':
            match=re.match(r'\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$',sql[i:])
            if match:
                delimiter=match.group();end=sql.find(delimiter,i+len(delimiter))
                if end<0: raise OrmError('unterminated SQL dollar quote')
                i=end+len(delimiter);continue
        if not first:
            # Generated set algebra wraps SELECT operands in parentheses.
            if char=='(': i+=1;continue
            start=i
            while i<len(sql) and _WORD.fullmatch(sql[i]): i+=1
            if i==start: raise OrmError('data statement keyword required')
            first=sql[start:i].upper();continue
        i+=1
    allowed={'SELECT','INSERT','UPDATE','DELETE','WITH','VALUES','EXPLAIN'}
    if not owned: allowed|={'CREATE','ALTER','DROP','TRUNCATE','COMMENT','GRANT','REVOKE','ANALYZE','VACUUM','REINDEX','REFRESH'}
    if first not in allowed:
        raise OrmError('SQL lifecycle/session control refused; use owned ORM transaction APIs')
