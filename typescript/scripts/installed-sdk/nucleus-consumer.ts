import * as root from '@neutron-build/nucleus';
import * as p0 from '@neutron-build/nucleus/sql';
import * as p1 from '@neutron-build/nucleus/kv';
import * as p2 from '@neutron-build/nucleus/vector';
import * as p3 from '@neutron-build/nucleus/document';
import * as p4 from '@neutron-build/nucleus/graph';
import * as p5 from '@neutron-build/nucleus/fts';
import * as p6 from '@neutron-build/nucleus/geo';
import * as p7 from '@neutron-build/nucleus/blob';
import * as p8 from '@neutron-build/nucleus/timeseries';
import * as p9 from '@neutron-build/nucleus/streams';
import * as p10 from '@neutron-build/nucleus/columnar';
import * as p11 from '@neutron-build/nucleus/datalog';
import * as p12 from '@neutron-build/nucleus/cdc';
import * as p13 from '@neutron-build/nucleus/pubsub';

const client = await root.createClient({url:'postgres://unused'})
.use(p0.withSQL).use(p1.withKV).use(p3.withDocument).use(p4.withGraph)
.use(p6.withGeo).use(p7.withBlob).use(p8.withTimeSeries).use(p10.withColumnar).connect();
const rows: {v:number}[] = await client.sql.query<{v:number}>('SELECT 1 AS v');
const value: string|null = await client.kv.get('key');
const doc: Record<string,unknown>|null = await client.document.getIn('docs',1);
const edges = await client.graph.neighbors(1);
const last: number|null = await client.timeseries.last('series');
const sum: number = await client.columnar.sum('table','value');
const distance: number = await client.geo.distance({lat:0,lon:0},{lat:0,lon:1});
const bytes = await client.blob.get('bucket','key');
void [rows,value,doc,edges,last,sum,distance,bytes,p2,p5,p9,p11,p12,p13];
