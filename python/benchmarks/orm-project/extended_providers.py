"""Transactional adoption workflow using each provider's public APIs."""
from providers import *

async def workflow(provider, row, rollback=False):
    """Create, guarded update, read own write, delete; commit or explicit rollback."""
    async def native(sql):
        await sql.execute(CREATE, *params(row))
        changed = await sql.execute(CAS, row['tenant'], row['id'], 0, 'updated')
        value = await sql.query_one_or_none(Document, POINT, row['tenant'], row['id'])
        removed = await sql.execute('DELETE FROM documents WHERE tenant=$1 AND id=$2', row['tenant'], row['id'])
        return changed, value, removed
    async def raw(conn):
        await conn.execute(CREATE, *params(row))
        changed = int((await conn.execute(CAS, row['tenant'], row['id'], 0, 'updated')).split()[-1])
        value = await conn.fetchrow(POINT, row['tenant'], row['id'])
        removed = int((await conn.execute('DELETE FROM documents WHERE tenant=$1 AND id=$2', row['tenant'], row['id'])).split()[-1])
        return changed, value, removed
    async def sa(conn):
        if provider.orm:
            conn.add(SADocument(**row))
            await conn.flush()
        else:
            await conn.execute(insert(SADocument.__table__).values(**row))
        changed = (await conn.execute(update(SADocument).where(SADocument.tenant == row['tenant'], SADocument.id == row['id'], SADocument.version == 0).values(version=1, note='updated'))).rowcount
        result = await conn.execute(select(provider.doc()).where(SADocument.tenant == row['tenant'], SADocument.id == row['id']))
        value = result.scalar_one() if provider.orm else result.mappings().one()
        removed = (await conn.execute(delete(SADocument).where(SADocument.tenant == row['tenant'], SADocument.id == row['id']))).rowcount
        return changed, value, removed
    if isinstance(provider, Native):
        async with provider.db.transaction() as tx:
            result = await native(tx.sql)
            if rollback: raise RollbackProbe()
            return result
    if isinstance(provider, Raw):
        async with provider.pool.acquire() as conn:
            async with conn.transaction():
                result = await raw(conn)
                if rollback: raise RollbackProbe()
                return result
    manager = provider.sessions.begin() if provider.orm else provider.engine.begin()
    async with manager as conn:
        result = await sa(conn)
        if rollback: raise RollbackProbe()
        return result
