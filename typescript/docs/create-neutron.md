# @neutron-build/create

> **Terminology note:** This page documents **Neutron TypeScript**. In broader ecosystem docs, **Neutron** refers to the umbrella framework/platform across implementations.


Scaffold a new Neutron TypeScript app from templates.

## Usage

```bash
pnpm create @neutron-build@latest my-app
```

Direct binary usage:

```bash
create-neutron my-app --template basic --runtime preact
```

## Options

- `--template basic`: Starter app template.
- `--template basic|marketing|app|full|docs|blog`: Starter shape.
- `--runtime preact|react-compat`: Runtime mode written into `neutron.config.ts`.

## Generated Project

Templates:

- `basic`: minimal mixed static+app starter.
- `marketing`: static-first site with optional islands and static blog routes.
- `app`: SaaS-style app routes (`loader`/`action`/auth-ready examples).
- `full`: mixed static + app + islands + route groups example.
- `docs`: documentation site with content collections, sidebar and search.
- `blog`: server-rendered blog with `@neutron-build/cms` at `/n-admin`.

Every template includes:

- Vite + Preact setup
- `neutron dev/build/start/preview`
- `release:check` one-command project validation (`neutron release-check --preset ...`)
