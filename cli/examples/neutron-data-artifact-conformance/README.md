# Candidate artifact native conformance

This operator runner builds unreleased candidate artifacts from the checkout's
committed Git revision, consumes them in a new private directory outside the
checkout, and checks native PostgreSQL scalar outcomes. It publishes nothing,
changes no package versions and does not certify released consumers.

It builds and packs the actual TypeScript package manifest with its declarations
and exports; unpacks that tarball as the external consumer's physical SDK package;
builds the declared Hatchling Python wheel and installs it with offline,
no-dependency `uv pip --target`; and extracts an actual Git Go module source
archive used by an external consumer through a private `replace`. Go source
archives are candidates, not Go proxy releases. TypeScript third-party dependencies
and Python consumer dependencies come from existing environments. Tarball unpacking
is not an npm registry/install-client test. No SDK source symlink substitutes for
these consumed artifacts.

Use existing Node/pnpm/TypeScript/Go/uv, cached Go modules, a Python environment
with the SDK's runtime dependencies, and a separate build-tool Python environment
with `build` and the project's declared `hatchling` backend. Tested build tools
are build 1.2.2.post1 and hatchling 1.27.0. Provision build tools independently;
the runner never downloads dependencies. Keep administrator URLs private.

```sh
export ADMIN_DATABASE_URL='postgresql://...'
export NEUTRON_CLI=/private/path/to/reviewed/candidate/neutron
export ARTIFACT_RUNTIME=/private/path/to/new-runtime
export ARTIFACT_TS_DEPS=/path/to/existing/neutron-nucleus/node_modules
export ARTIFACT_TSC=/path/to/existing/tsc
export ARTIFACT_BUILD_PYTHON=/private/build-tools/bin/python
python cli/examples/neutron-data-artifact-conformance/run.py
```

Commit source changes first: package archives intentionally use Git HEAD, while
the runner and shared consumer templates are hashed in provenance. The runner
requires 6 GiB free disk space, bounds each command, builds only one Go consumer
with offline module resolution, and creates/drops only its own unique
`v10_artifact_*` PostgreSQL database. The new private directory retains artifacts,
logs, declarations, generated read models, consumer binaries and exact provenance.
No credentials are printed. Do not share private diagnostics without inspection.

Consumers reuse the reviewed native scalar conformance templates. The TypeScript
consumer imports the package root and SQL public exports; Python asserts its
client import is under the wheel installation target; Go checks the module's
resolved directory is the extracted archive. All three parameter-bind the ten
scalar kinds, create and read signed minima and exact negative numeric values,
CAS-update to maxima/positive decimal and empty values, refuse stale revisions,
and roll back update/delete/insert effects. Every client then reads and updates
the other two writers' rows, and all read the final state. Independent fixed
expected values and PostgreSQL text/hex queries check every phase. NULL remains
distinct from empty, and revisions beyond JavaScript's safe integer range remain
exact. Existing native temporal reads are separate: TS Date is milliseconds;
Python/Go and explicit timestamp wire projection preserve microseconds.

Provenance identifies archive SHA256s, full source revision/hashes, package
manifests/exports, compiler/build tools, reused dependency versions, locks,
resolved import/module paths, generated models and consumer hashes. These checks
cover named candidate packaging and scalar-native contracts, not general ORM
parity, temporal generation, Nucleus, clean network install, public auth, all
package exports, production durability or shipping release compatibility.
