# Experimental application coordinator

This example runs a native Go API and a native TypeScript HTTP service together.
It demonstrates process coordination, not integration with the Neutron language
SDKs. The TypeScript service calls the Go API and returns their combined response.

Requires Go 1.23+, Node 22.6+, and macOS or Linux. No package installation is
needed. From the repository's `cli` directory:

```sh
go build -o /tmp/neutron-coordinator .
cd examples/application
/tmp/neutron-coordinator project check
/tmp/neutron-coordinator project plan --json --service web
/tmp/neutron-coordinator dev --service web
```

Open `http://127.0.0.1:4300`. Edit `web/server.ts` to exercise Node's native watch
mode. Interrupt the CLI to stop both services: dependents stop first, each gets
SIGTERM and then SIGKILL after a grace period; a second interrupt skips the
remaining grace. Services also stop if the terminal closes or the CLI itself is
killed. Starting from the `web` directory
also discovers the application's manifest.

## Manifest behavior

The optional `[application]` table uses `version = 1`, an application name, and
named `services`. Each service declares a directory relative to the manifest
and a command as an argument array. Commands execute directly without a shell.
Environment entries override inherited values; ports declare expected listeners
for preflight checks. Directories must remain inside the application root.

`depends_on` requires each dependency to declare readiness: either an HTTP URL
returning a 2xx response (without redirects) or a TCP `host:port`, plus a positive
timeout up to one hour. The coordinator starts services in deterministic
dependency order. Selecting one service includes its dependencies.

`project check` and `project plan` validate without running commands, contacting
services, or writing files. JSON plans omit environment values, but command
arguments and readiness URLs remain visible. Application discovery uses the
nearest `neutron.toml` or explicit `--config`, never a home configuration.
Projects without an application table keep the existing `dev` delegation.

## Runtime boundaries

- Foreground development on macOS and Linux only; other platforms can plan but
  cannot run the coordinator yet.
- One session per application root, enforced by `.neutron/application.lock`.
- Startup is sequential; readiness is checked at startup, not continuously.
- Native tools own reload behavior. Any supervised command exiting, including
  with status zero, fails the session and stops its siblings.
- Shutdown signals process groups in reverse startup order, then escalates to
  killing them. Programs that deliberately detach into another session are not
  supported. Interrupting the CLI currently returns a nonzero exit status.
- Port checks detect existing conflicts but do not reserve ports. Environment
  values are omitted from plans; child logs and command arguments are not redacted.
- No automatic installation, daemon, restart policy, task graph, build pipeline,
  or deployment integration is provided by this prototype.

## Reproduce the smoke test

With Python 3.11+ and the compiled CLI:

```sh
python3 smoke.py /tmp/neutron-coordinator
```

The test copies this example to a temporary directory, chooses available ports,
checks planning from a subdirectory, verifies communication and native reload,
and checks that interruption or an API crash closes both service ports.

## Service discovery and Neutron services

A service receives `NEUTRON_SERVICE_<NAME>_URL` (`http://127.0.0.1:<port>`) for
each direct dependency with exactly one port, so it never hard-codes another
service's port. The web service here reads `NEUTRON_SERVICE_API_URL`. Explicit
`env` entries always win over injected values.

`contract = "neutron/v1"` marks a service built on a Neutron SDK
(FRAMEWORK_CONTRACT.md). Such a service:

- may omit `ports`; the coordinator then assigns a free loopback port at start,
- receives `NEUTRON_HOST` and `NEUTRON_PORT`,
- is ready when `GET /health` succeeds (overridable with `ready`),
- gets the contract's 30-second shutdown drain before SIGKILL,
- is checked once ready: a degraded `/health`, a missing `/openapi.json` or a
  non-3.1 spec is reported as a `contract:` line. These are warnings; the
  session keeps running.

`ready = { path = "/health", timeout = "30s" }` probes a path on any service with
exactly one port.

## Tasks

`[application.tasks.<name>]` declares finite commands such as builds, tests or
checks. A task succeeds when it exits 0, unlike a service. Each task needs a
`timeout`. `depends_on` may name only other tasks. `outputs` lists paths the task
is expected to produce; they are reported but not cached or verified.

```sh
/tmp/neutron-coordinator project run api-vet          # runs api-build first
/tmp/neutron-coordinator project run api-vet --json   # versioned result on stdout
```

`--jobs N` bounds how many tasks run at once (default: CPU count). The run is
fail-fast: after a failure or timeout, nothing new starts, running tasks are
stopped, and remaining tasks are reported as `skipped`. The command exits nonzero
unless every selected task succeeded. Task and service names share one namespace.
