// Release gate fixture: compile against package exports after build/packing.
// Not executed or type-checked during the resource hold.
import { createJobs, createNucleusCacheClient, type CounterCapabilities, type QueueCapabilities } from '@neutron-build/data';
import { createDrizzleDatabase } from '@neutron-build/data/drizzle';
import { admitConnection, resourceLifecycle, type ConnectionCapabilities } from '@neutron-build/sql/lifecycle';
import { createClient, HttpTransport, type NucleusClientConfig } from '@neutron-build/nucleus';
import { withTimeSeriesProfile, type TimeSeriesAdmissionProfile } from '@neutron-build/nucleus/timeseries';

declare const profile: TimeSeriesAdmissionProfile;
declare const kv: Parameters<typeof createNucleusCacheClient>[0]['kv'];
const native = createNucleusCacheClient({ kv });
const strict = createNucleusCacheClient({ kv, counterMode: 'strict' });
const counters: Readonly<CounterCapabilities> = native.counterCapabilities;
const queue = createJobs({ requiredCapabilities: { durability: 'backend', acknowledgement: 'fenced-uncertain-stop' } });
const queueCaps: Readonly<QueueCapabilities> | undefined = queue.capabilities;
const config: NucleusClientConfig = { url: 'https://example.invalid', cacheEnabled: false, requiredCapabilities: { cancellation: 'response-only' } };
const builder = createClient(config).use(withTimeSeriesProfile(profile));
const connection = admitConnection(new HttpTransport(config.url), { cancellation: 'response-only' });
const caps: Readonly<ConnectionCapabilities> = connection.capabilities;
const lifecycle = resourceLifecycle('owned', async () => {});
lifecycle.assertOpen();
void createDrizzleDatabase({ requiredCapabilities: { cancellation: 'unsupported' } });
// @ts-expect-error no exactly-once queue capability
createJobs({ requiredCapabilities: { acknowledgement: 'exactly-once' } });
// @ts-expect-error a boolean does not admit time-series capabilities
withTimeSeriesProfile(true);
// @ts-expect-error no implicit checked-native mode
createNucleusCacheClient({ kv, counterMode: 'checked-native' });
void [strict, counters, queueCaps, builder, caps];
