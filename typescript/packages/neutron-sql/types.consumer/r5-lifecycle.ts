// Export/declaration consumer gate, deliberately not compiled during hold.
import { createDatabase } from '@neutron-build/sql';
import { admitConnection, resourceLifecycle, type ConnectionCapabilities } from '@neutron-build/sql/lifecycle';
declare const capabilities: Readonly<ConnectionCapabilities>;
admitConnection({ capabilities }, { cancellation: 'server-attempt' });
void createDatabase({ url: 'postgres://example.invalid', requiredCapabilities: { automaticMutationReplay: false } });
const lifecycle = resourceLifecycle('owned', async () => {});
const closing: boolean = lifecycle.closing;
void closing;
// @ts-expect-error cancellation describes attempts, never guaranteed rollback
admitConnection({ capabilities }, { cancellation: 'guaranteed-rollback' });
