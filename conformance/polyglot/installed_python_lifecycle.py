"""Outside-origin installed Python lifecycle/graph corpus with native SQL oracle.

No source ORM imports, test framework dependency, skips, or timing assertions.
The qualifier owns the schema; this consumer creates only tables within it.
"""
from contextlib import asynccontextmanager
from dataclasses import dataclass
import argparse
import asyncio
import inspect
import json
import os
from pathlib import Path
import re
import sys

@dataclass
class Parent:
    id: int|None=None
    name: str='parent'

@dataclass
class Child:
    id: int|None=None
    parent_id: int|None=None
    label: str='child'

async def call(function,*args,**kw):
    result=function(*args,**kw)
    return await result if inspect.isawaitable(result) else result

@asynccontextmanager
async def context(value):
    if hasattr(value,'__aenter__'):
        async with value as entered: yield entered
    else:
        with value as entered: yield entered

async def execute(args,request):
    import neutron.orm as orm
    import psycopg
    from psycopg import sql
    prefix=args.prefix.resolve()
    if Path(sys.prefix).resolve()!=prefix or any(not Path(m.__file__).resolve().is_relative_to(prefix) for m in (orm,psycopg)):
        raise ValueError('fresh installed consumer required')
    scope=request['schema_scope']
    if request['protocol']!='polyglot-installed-lifecycle-v1' or not re.fullmatch('neutron_polyglot_[0-9a-f]{32}',scope): raise ValueError('owned scope required')
    p=orm.Table('parents',{'id':orm.ColumnSpec(int,'int4',generated=True),'name':orm.ColumnSpec(str,'text')},schema=scope)
    c=orm.Table('children',{'id':orm.ColumnSpec(int,'int4',generated=True),'parent_id':orm.ColumnSpec(int,'int4'),'label':orm.ColumnSpec(str,'text')},schema=scope)
    pm=orm.ModelMapping(Parent,p,dict(p.columns),primary_key=('id',))
    cm=orm.ModelMapping(Child,c,dict(c.columns),primary_key=('id',))
    relation=orm.Relation(pm,cm,('id',),('parent_id',))
    url=os.environ['NEUTRON_TEST_DATABASE_URL']
    Session=orm.AsyncSession if args.mode=='async' else orm.Session
    Database=orm.AsyncDatabase if args.mode=='async' else orm.Database
    cases=[]
    with psycopg.connect(url,autocommit=True) as native:
        marker=native.execute('SELECT pg_catalog.obj_description(oid,%s) FROM pg_catalog.pg_namespace WHERE nspname=%s',('pg_namespace',scope)).fetchone()
        if marker!=('polyglot-installed-lifecycle-v1:'+request['ownership_token'],): raise ValueError('fixture ownership mismatch')
        native.execute(sql.SQL('CREATE TABLE {}.parents(id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,name text NOT NULL)').format(sql.Identifier(scope)))
        native.execute(sql.SQL('CREATE TABLE {}.children(id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,parent_id integer NOT NULL REFERENCES {}.parents(id),label text NOT NULL UNIQUE)').format(sql.Identifier(scope),sql.Identifier(scope)))
        def counts():
            return native.execute(sql.SQL('SELECT (SELECT count(*) FROM {}.parents),(SELECT count(*) FROM {}.children)').format(sql.Identifier(scope),sql.Identifier(scope))).fetchone()
        async with context(await call(Session.connect,url)) as session:
            events=[]
            def listener(event): events.append(event.name)
            for name in ('before_flush','before_insert','after_flush','after_commit','after_rollback'): session.listen(name,listener)
            def bind_hook(event):
                if isinstance(event.obj,Parent): event.obj.name="hook:'; SELECT secret"
            session.listen('before_insert',bind_hook)
            parent=Parent();children=[Child(label='a'),Child(label='b')]
            session.add_graph(relation,parent,children)
            await call(session.flush)
            if parent.id is None or any(child.id is None or child.parent_id!=parent.id for child in children) or counts()!=(0,0): raise ValueError('graph flush visibility/identity')
            await call(session.rollback)
            if parent.id is not None or any(child.id is not None or child.parent_id is not None for child in children) or 'after_commit' in events or 'after_rollback' not in events: raise ValueError('graph rollback reconciliation')
            cases.append('graph-generated-identity-rollback')
            session.add_graph(relation,parent,children);await call(session.commit)
            actual=native.execute(sql.SQL('SELECT p.name,c.label FROM {}.parents p JOIN {}.children c ON c.parent_id=p.id ORDER BY c.label').format(sql.Identifier(scope),sql.Identifier(scope))).fetchall()
            if actual!=[("hook:'; SELECT secret",'a'),("hook:'; SELECT secret",'b')] or events.count('after_commit')!=1: raise ValueError('hook binding/commit oracle')
            cases.append('prewrite-hooks-bound-values-known-commit')
            loaded=await call(session.load_relation,relation,[parent,parent],budget=orm.LoadBudget(2,4,2))
            again=await call(session.load_relation,relation,[parent],budget=orm.LoadBudget(1,2,2))
            if loaded[0].children[0] is not loaded[1].children[0] or loaded[0].children[0] is not again[0].children[0] or not all(any(x is child for x in loaded[0].children) for child in children): raise ValueError('identity loader duplicated objects')
            cases.append('graph-selectin-identity-reuse')
            def fail_after(event): raise RuntimeError('intentional-after-commit')
            session.listen('after_commit',fail_after)
            parent.name='committed-after-callback-failure'
            try: await call(session.commit)
            except orm.PostCommitError as error:
                if error.outcome!='committed' or error.sqlstate is not None: raise
            else: raise ValueError('after_commit failure swallowed')
            actual=native.execute(sql.SQL('SELECT name FROM {}.parents WHERE id=%s').format(sql.Identifier(scope)),(parent.id,)).fetchone()
            if actual!=('committed-after-callback-failure',): raise ValueError('after_commit failure undid known commit')
            await call(session.rollback)
            if parent.name!='committed-after-callback-failure': raise ValueError('known commit baseline not adopted')
            cases.append('aftercommit-failure-retains-native-commit')
        async with context(await call(Session.connect,url)) as session:
            parent=Parent();children=[Child(label='duplicate'),Child(label='duplicate')]
            commits=[];session.listen('after_commit',lambda event:commits.append(event.name))
            session.add_graph(relation,parent,children)
            try: await call(session.flush)
            except orm.OrmError as error:
                if error.sqlstate!='23505': raise
            else: raise ValueError('late unique constraint not enforced')
            await call(session.rollback)
            if counts()!=(1,2) or commits or parent.id is not None or any(child.id is not None or child.parent_id is not None for child in children): raise ValueError('late graph failure partial write or state')
            cases.append('late-graph-constraint-full-rollback')
        async with context(await call(Database.connect,url)) as db:
            query=orm.select_row(c)
            async with context(db.stream(query,batch_size=1)) as rows:
                first=await call(rows.__anext__ if args.mode=='async' else rows.__next__)
                if first['label'] not in ('a','b'): raise ValueError('stream first native row')
            try: await call(rows.__anext__ if args.mode=='async' else rows.__next__)
            except orm.OrmError: pass
            else: raise ValueError('escaped stream remained usable')
            if len(await call(db.all,query))!=2: raise ValueError('early stream cleanup prevented reuse')
            cases.append('early-stream-close-and-escaped-lifetime')
            handle=await call(db.begin)
            try: await call(db.execute,orm.Mutation('COMMIT',()))
            except orm.OrmError: pass
            else: raise ValueError('raw transaction control bypassed ownership')
            await call(handle.rollback)
            try: await call(handle.commit)
            except orm.OrmError: pass
            else: raise ValueError('terminal handle committed')
            if len(await call(db.all,query))!=2: raise ValueError('transaction fence affected native fixture')
            cases.append('manual-transaction-terminal-and-raw-control-fence')
        final=native.execute(sql.SQL('SELECT p.name,c.label FROM {}.parents p JOIN {}.children c ON c.parent_id=p.id ORDER BY c.label').format(sql.Identifier(scope),sql.Identifier(scope))).fetchall()
    return {**request,'status':'pass','mode':args.mode,'cases':cases,'native_rows':final}

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--prefix',type=Path,required=True);parser.add_argument('--mode',choices=('sync','async'),required=True)
    try: print(json.dumps(asyncio.run(execute(parser.parse_args(),json.load(sys.stdin)))))
    except BaseException:
        print(json.dumps({'status':'fail','diagnostics':'installed Python lifecycle corpus failed'}));raise SystemExit(1)
