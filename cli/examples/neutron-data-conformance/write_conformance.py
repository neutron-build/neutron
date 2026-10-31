"""Native parameter binding and transactions; generated Writes is a read model."""
import os
from decimal import Decimal
from uuid import UUID
from writes import Writes

INSERT = 'INSERT INTO fixture.writes VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)'
LOW = [-32768,-2147483648,-9223372036854775808,Decimal('-9999999999999999999999.123456789012345678'),True,"quotes ' Unicode λ",'short','ABCD',UUID('00000000-0000-0000-0000-000000000001'),bytes(range(256))]
HIGH = [32767,2147483647,9223372036854775807,Decimal('9999999999999999999999.999999999999999999'),False,'','','WXYZ',UUID('ffffffff-ffff-ffff-ffff-ffffffffffff'),b'']

def norm(row):
    return {k: (v.hex() if isinstance(v,bytes) else str(v) if isinstance(v,(Decimal,UUID)) or k in ('i8','revision') and v is not None else v) for k,v in row.model_dump().items()}

async def writes(db):
    phase=os.environ.get('CONFORMANCE_WRITE_PHASE')
    if not phase:
        return None
    base,version=100,9007199254740993
    if phase=='write':
        assert await db.sql.execute(INSERT,base,*LOW,version)==1
        before=await db.sql.query_one(Writes,'SELECT * FROM fixture.writes WHERE row_id=$1',base)
        assert [getattr(before,k) for k in ('i2','i4','i8','amount','flag','label','short_label','fixed_label','ident','payload')]==LOW
        assert before.revision==version
        assert await db.sql.execute(INSERT,base+1,*([None]*10),version)==1
        update='UPDATE fixture.writes SET i2=$2,i4=$3,i8=$4,amount=$5,flag=$6,label=$7,short_label=$8,fixed_label=$9,ident=$10,payload=$11,revision=revision+1 WHERE row_id=$1 AND revision=$12'
        assert await db.sql.execute(update,base,*HIGH,version)==1
        assert await db.sql.execute(update,base,*LOW,version)==0
        try:
            async with db.transaction() as tx:
                assert await tx.sql.execute('UPDATE fixture.writes SET label=$2 WHERE row_id=$1',base,'must rollback')==1
                assert await tx.sql.execute('DELETE FROM fixture.writes WHERE row_id=$1',base+1)==1
                assert await tx.sql.execute(INSERT,base+2,*LOW,version)==1
                raise RuntimeError('intentional conformance rollback')
        except RuntimeError as exc:
            assert str(exc)=='intentional conformance rollback'
        else:
            raise AssertionError('rollback exception lost')
    elif phase=='update':
        others=await db.sql.query(Writes,'SELECT * FROM fixture.writes WHERE row_id<$1 OR row_id>$2 ORDER BY row_id',base,base+1)
        assert len(others)==4
        for row in others:
            assert await db.sql.execute('UPDATE fixture.writes SET revision=revision+1 WHERE row_id=$1 AND revision=$2',row.row_id,row.revision)==1
            assert await db.sql.execute('UPDATE fixture.writes SET revision=revision+1 WHERE row_id=$1 AND revision=$2',row.row_id,row.revision)==0
    elif phase!='read':
        raise ValueError('unknown conformance write phase')
    rows=await db.sql.query(Writes,'SELECT * FROM fixture.writes ORDER BY row_id')
    assert all(isinstance(r.revision,int) and (r.ident is None or isinstance(r.ident,UUID)) and (r.payload is None or isinstance(r.payload,bytes)) for r in rows)
    return [norm(r) for r in rows]
