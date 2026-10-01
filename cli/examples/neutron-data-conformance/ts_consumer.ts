import { createClient } from './sdk/client.js';
import { PgTransport } from './sdk/transport.js';
import { withSQL } from './sdk/sql/index.js';
import { writes } from './write_conformance.js';
import type { Scalars } from './scalars.js';
const db = await createClient({url: process.env.DATABASE_URL!}).use(withSQL).connect();
try {
  let refused = false;
  try { await db.sql.query('SELECT i8 FROM fixture.scalars ORDER BY row_id'); }
  catch (e) { refused = String(e).includes('INT8_PRECISION') || String(e).includes('MAX_SAFE_INTEGER'); }
  if (!refused) throw Error('expected native INT8 precision refusal');
  const exactDb = await createClient({
    url: process.env.DATABASE_URL!,
    transport: new PgTransport(process.env.DATABASE_URL!, {valueProfile: 'lossless-read-v1'}),
  }).use(withSQL).connect();
  try {
    const rows = await exactDb.sql.query<Scalars>('SELECT * FROM fixture.scalars ORDER BY row_id');
    if (typeof rows[0].i8 !== 'string' || typeof rows[0].amount !== 'string' || !(rows[0].payload instanceof Uint8Array)) throw Error('scalar runtime shape');
    if (Object.entries(rows[2]).some(([k,v]) => k !== 'row_id' && v !== null)) throw Error('NULL shape');
    const temporal = await db.sql.queryOne<{happened: Date, exact: string}>("SELECT happened,to_char(happened AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US')||'+00:00' AS exact FROM fixture.temporal");
    if (!(temporal.happened instanceof Date) || temporal.happened.toISOString() !== '2024-02-29T23:59:59.123Z') throw Error('native Date boundary');
    const writeRows = await writes(exactDb.sql);
    console.log(JSON.stringify({...(writeRows ? {writes:writeRows} : {}),rows: rows.map(r => ({...r,payload:r.payload === null ? null : Buffer.from(r.payload).toString('hex')})),temporal:temporal.exact,native_int8:'refused',native_temporal:'milliseconds; explicit SQL projection preserves microseconds'}));
  } finally { await exactDb.close(); }
} finally { await db.close(); }
