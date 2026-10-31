import assert from 'node:assert/strict';
import type { SQLModel } from './sdk/sql/index.js';
import type { Writes } from './writes.js';

const insert = 'INSERT INTO fixture.writes VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)';
const low = [-32768,-2147483648,'-9223372036854775808','-9999999999999999999999.123456789012345678',true,"quotes ' Unicode λ",'short','ABCD','00000000-0000-0000-0000-000000000001',Buffer.from(Array.from({length:256},(_,i)=>i))] as const;
const high = [32767,2147483647,'9223372036854775807','9999999999999999999999.999999999999999999',false,'','','WXYZ','ffffffff-ffff-ffff-ffff-ffffffffffff',Buffer.alloc(0)] as const;
const norm = (r: Writes) => ({...r,payload:r.payload===null?null:Buffer.from(r.payload).toString('hex')});
export async function writes(db: SQLModel) {
  const phase=process.env.CONFORMANCE_WRITE_PHASE;
  if (!phase) return undefined;
  const base=300, version='9007199254740993';
  if(phase==='write') {
    assert.equal(await db.execute(insert,base,...low,version),1);
    const before=await db.queryOne<Writes>('SELECT * FROM fixture.writes WHERE row_id=$1',base);
    assert.deepEqual(norm(before),{row_id:base,i2:low[0],i4:low[1],i8:low[2],amount:low[3],flag:low[4],label:low[5],short_label:low[6],fixed_label:low[7],ident:low[8],payload:low[9].toString('hex'),revision:version});
    assert.equal(await db.execute(insert,base+1,...Array(10).fill(null),version),1);
    const update='UPDATE fixture.writes SET i2=$2,i4=$3,i8=$4,amount=$5,flag=$6,label=$7,short_label=$8,fixed_label=$9,ident=$10,payload=$11,revision=revision+1 WHERE row_id=$1 AND revision=$12';
    assert.equal(await db.execute(update,base,...high,version),1);
    assert.equal(await db.execute(update,base,...low,version),0);
    let rolledBack=false;
    try {await db.transaction(async tx=>{
      assert.equal(await tx.execute('UPDATE fixture.writes SET label=$2 WHERE row_id=$1',base,'must rollback'),1);
      assert.equal(await tx.execute('DELETE FROM fixture.writes WHERE row_id=$1',base+1),1);
      assert.equal(await tx.execute(insert,base+2,...low,version),1);
      throw Error('intentional conformance rollback');
    });}catch(e){if(!(e instanceof Error)||e.message!=='intentional conformance rollback')throw e;rolledBack=true;}
    assert.equal(rolledBack,true);
  } else if(phase==='update') {
    const others=await db.query<Writes>('SELECT * FROM fixture.writes WHERE row_id<$1 OR row_id>$2 ORDER BY row_id',base,base+1);
    assert.equal(others.length,4);
    for(const row of others){
      assert.equal(await db.execute('UPDATE fixture.writes SET revision=revision+1 WHERE row_id=$1 AND revision=$2',row.row_id,row.revision),1);
      assert.equal(await db.execute('UPDATE fixture.writes SET revision=revision+1 WHERE row_id=$1 AND revision=$2',row.row_id,row.revision),0);
    }
  } else if(phase!=='read') throw Error('unknown conformance write phase');
  const rows=await db.query<Writes>('SELECT * FROM fixture.writes ORDER BY row_id');
  assert(rows.every(r=>typeof r.revision==='string' && (r.ident===null||typeof r.ident==='string') && (r.payload===null||r.payload instanceof Uint8Array)));
  return rows.map(norm);
}
