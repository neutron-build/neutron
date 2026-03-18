# create-neutron

> **Terminology note:** This page documents **Neutron TypeScript**. In broader ecosystem docs, **Neutron** refers to the umbrella framework/platform across implementations.


Scaffold a new Neutron TypeScript app from templates.

## Usage

```bash
pnpm create neutron my-app
```

Direct binary usage:

```bash
create-neutron my-app --template basic --runtime preact
```

## Options

- `--template basic`: Starter app template.
- `--template basic|marketing|app|full`: Starter shape.
- `--runtime preact|react-compat`: Runtime mode written into `neutron.config.ts`.

## Generated Project

Templates:

- `basic`: minimal mixed static+app starter.
- `marketing`: static-first site with optional islands and static blog routes.
- `app`: SaaS-style app routes (`loader`/`action`/auth-ready examples).
- `full`: mixed static + app + islands + route groups example.

Every template includes:

- Vite + Preact setup
- `neutron dev/build/start/preview`
- `release:check` one-command project validation (`neutron release-check --preset ...`)
