# Neutron SDK services under the coordinator

A Go API built on the Neutron Go SDK and a web app built on the Neutron
TypeScript framework, declared as `contract = "neutron/v1"` services. Neither
declares a port: the coordinator assigns one to each, passes it as
`NEUTRON_HOST`/`NEUTRON_PORT`, waits on each SDK's built-in `GET /health`, and
gives the web app `NEUTRON_SERVICE_API_URL` because it `depends_on` the API.
Once each service is ready, the coordinator checks its `/health` body and
`/openapi.json` against FRAMEWORK_CONTRACT.md and reports any deviation.

The example tracks the SDKs in this repository (environment-based addressing is
newer than the published releases): `api/go.mod` replaces the Go SDK with
`../../../../go`, and `web/package.json` links the TypeScript packages by path.

## Run it from the repository

Requires Go 1.23+, Node 22+ and pnpm, on macOS or Linux.

```sh
(cd ../../../typescript && pnpm install && pnpm --filter "@neutron-build/cli..." build)
(cd web && npm install)
(cd ../.. && go build -o /tmp/neutron .)
/tmp/neutron dev
```

The log shows each assigned port. The web page lists items fetched from the API;
`POST /api/items` with `{}` returns a 422 `application/problem+json` body.
Interrupt once to drain and stop both services.

## Typed client across the language boundary

`api/openapi.json` is a committed snapshot of the Go API's OpenAPI document:
the reviewed interface, so an API change shows up as a diff in review. The web
app's types are generated from it by an ordinary task, never hand-written.

```sh
/tmp/neutron project spec --write --service api   # refresh the snapshot
/tmp/neutron project spec --check                 # fail if a service drifted
/tmp/neutron project run web-typecheck            # generate src/api.d.ts, then tsc
```

Renaming a field in the Go API makes `spec --check` fail; after `--write`, the
web typecheck fails where the old field was used.

## Smoke test

`python3 smoke.py /path/to/neutron` packs the TypeScript SDKs from this checkout,
copies the example outside the repository, installs it, and checks: the
snapshot matches the running API, an API field rename is caught as spec drift and
as a TypeScript error, data flows
web → API through the injected URL, validation errors follow RFC 7807, no
contract deviation is reported, two copies run at once on assigned ports, and an
in-flight request completes during shutdown with no process left behind. CI runs
it on Linux and macOS.
