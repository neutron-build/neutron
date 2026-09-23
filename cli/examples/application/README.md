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
/tmp/neutron-coordinator project plan --json --component web
/tmp/neutron-coordinator dev --component web
```

Open `http://127.0.0.1:4300`. Edit `web/server.ts` to exercise Node's native watch
mode. Interrupt the CLI to stop both services. Starting from the `web` directory
also discovers the application's manifest.

## Manifest behavior

The optional `[application]` table uses `version = 1`, an application name, and
named `components`. Each component declares a directory relative to the manifest
and a command as an argument array. Commands execute directly without a shell.
Environment entries override inherited values; ports declare expected listeners
for preflight checks. Directories must remain inside the application root.

`depends_on` requires each dependency to declare readiness: either an HTTP URL
returning a 2xx response (without redirects) or a TCP `host:port`, plus a positive
timeout up to one hour. The coordinator starts components in deterministic
dependency order. Selecting one component includes its dependencies.

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
