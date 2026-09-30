import * as data from '@neutron-build/data';
const cache: data.CacheClient = new data.MemoryCacheClient();
await cache.set('installed', 'value');
const value: string | null = await cache.get('installed');
const provider: data.DatabaseProfile = {provider:'postgres',connectionString:'postgres://unused'};
const loose: Promise<data.DrizzleDatabase> = data.createDrizzleDatabase({profile:provider});
void [value,loose];
