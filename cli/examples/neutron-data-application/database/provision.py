#!/usr/bin/env python3
"""Provision an isolated PostgreSQL reference database; secrets go to a 0600 file."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import shutil
import tempfile
from urllib.parse import quote, urlsplit, urlunsplit

import asyncpg

TABLES = ('projects', 'documents', 'processing_requests', 'jobs', 'results')


def identifier(value):
    return '"' + value.replace('"', '""') + '"'


def connection_url(admin_url, database, user, password):
    parts = urlsplit(admin_url)
    host = parts.hostname
    if ':' in host:
        host = '[' + host + ']'
    authority = quote(user, safe='') + ':' + quote(password, safe='') + '@' + host
    if parts.port:
        authority += ':' + str(parts.port)
    return urlunsplit((parts.scheme, authority, '/' + database, parts.query, ''))


async def run(args):
    if not re.fullmatch(r'v10_data_[a-z0-9_]{1,24}', args.database):
        raise ValueError('database must use the disposable v10_data_ prefix, max 33 characters')
    admin_url = os.environ['ADMIN_DATABASE_URL']
    owner = args.database + '_owner'
    roles = [args.database + '_' + kind + '_' + tenant
             for kind in ('api', 'worker', 'studio') for tenant in ('a', 'b')]
    admin = await asyncpg.connect(admin_url)
    try:
        if args.drop:
            # Require the recorded owner, not merely a similarly named database.
            actual_owner = await admin.fetchval('SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname=$1', args.database)
            if actual_owner is None:
                print('Reference database is already absent; no roles removed.')
                return
            if actual_owner != owner:
                raise ValueError('refusing database with unexpected owner')
            await admin.execute('DROP DATABASE IF EXISTS ' + identifier(args.database) + ' WITH (FORCE)')
            for role in roles + [owner]:
                await admin.execute('DROP ROLE IF EXISTS ' + identifier(role))
            print('Removed owned reference database and roles.')
            return
        if await admin.fetchval('SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)', args.database):
            raise ValueError('database already exists; refusing to overwrite')
        for role in roles + [owner]:
            if await admin.fetchval('SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)', role):
                raise ValueError('role already exists; refusing to overwrite')
        if args.out.exists():
            raise ValueError('credential file already exists; refusing to overwrite')
        passwords = {role: secrets.token_urlsafe(32) for role in roles + [owner]}
        made_roles = []
        made_database = False
        try:
            for role in roles + [owner]:
                # Generated passwords contain only token_urlsafe alphabet.
                await admin.execute('CREATE ROLE ' + identifier(role) + " LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOINHERIT PASSWORD '" + passwords[role] + "'")
                made_roles.append(role)
            await admin.execute('CREATE DATABASE ' + identifier(args.database) + ' OWNER ' + identifier(owner))
            made_database = True
            owner_url = connection_url(admin_url, args.database, owner, passwords[owner])
            env = dict(os.environ, DATABASE_URL=owner_url)
            # CLI errors can contain connection strings; never print captured diagnostics here.
            # One immutable source set; revision1 is an explicit old-consumer fixture.
            with tempfile.TemporaryDirectory(prefix='neutron-reference-migrations-') as temporary:
                migration_dir = Path(temporary)
                for file in (Path(__file__).parent / 'migrations').glob('*.sql'):
                    if int(file.name.split('_', 1)[0]) <= args.schema_revision:
                        shutil.copyfile(file, migration_dir / file.name)
                result = await asyncio.to_thread(subprocess.run,
                    [str(args.cli.resolve()), 'migrate', '--dir', str(migration_dir)],
                    env=env, capture_output=True, text=True, timeout=120)
            if result.returncode:
                raise RuntimeError('CLI migration failed (captured diagnostics withheld because they may contain credentials)')
            database_admin_url = urlunsplit(urlsplit(admin_url)._replace(path='/' + args.database))
            db = await asyncpg.connect(database_admin_url)
            try:
                async with db.transaction():
                    await db.execute('REVOKE ALL ON DATABASE ' + identifier(args.database) + ' FROM PUBLIC')
                    await db.execute('REVOKE CREATE ON SCHEMA public FROM PUBLIC')
                    for role in roles:
                        r = identifier(role)
                        await db.execute('GRANT CONNECT ON DATABASE ' + identifier(args.database) + ' TO ' + r)
                        await db.execute('GRANT USAGE ON SCHEMA public TO ' + r)
                        await db.execute('GRANT SELECT ON public.app_schema_revision TO ' + r)
                        kind = role.rsplit('_', 2)[-2]
                        tenant = 'tenant-' + role[-1]
                        for table in TABLES:
                            privileges = 'SELECT'
                            if kind == 'api' and table in ('projects', 'documents', 'processing_requests', 'jobs'):
                                privileges += ', INSERT'
                                if table in ('projects', 'documents'):
                                    privileges += ', UPDATE'
                            if kind == 'worker' and table == 'jobs':
                                privileges += ', UPDATE'
                            if kind == 'worker' and table == 'results':
                                privileges += ', INSERT, UPDATE'
                            await db.execute('GRANT ' + privileges + ' ON public.' + identifier(table) + ' TO ' + r)
                            await db.execute('CREATE POLICY ' + identifier(role) + ' ON public.' + identifier(table) +
                                ' TO ' + r + " USING (tenant_id = '" + tenant + "') WITH CHECK (tenant_id = '" + tenant + "')")
            finally:
                await db.close()
            manifest = {'database': args.database, 'schema_revision': args.schema_revision, 'migration_owner_url': owner_url,
                        'roles': {role: connection_url(admin_url, args.database, role, passwords[role]) for role in roles}}
            args.out.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(args.out, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, 'w') as stream:
                json.dump(manifest, stream, indent=2)
                stream.write('\n')
            print('Provisioned reference database; credentials written to a private file.')
        except BaseException:
            if made_database:
                await admin.execute('DROP DATABASE ' + identifier(args.database) + ' WITH (FORCE)')
            for role in reversed(made_roles):
                await admin.execute('DROP ROLE ' + identifier(role))
            raise
    finally:
        await admin.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', required=True)
    parser.add_argument('--schema-revision', type=int, choices=(1, 2), default=2)
    parser.add_argument('--cli', type=Path, default=Path('neutron'))
    parser.add_argument('--out', type=Path, default=Path('.reference-credentials.local.json'))
    parser.add_argument('--drop', action='store_true')
    try:
        asyncio.run(run(parser.parse_args()))
    except Exception as error:
        # Driver exception messages may contain secrets. Report safe category only.
        print('Provisioning failed: ' + type(error).__name__)
        raise SystemExit(1)
