# @neutron-build/create

Scaffold a new Neutron project.

Previously published as `create-neutron`; that package is deprecated. Use
`@neutron-build/create` for new projects.

Project scaffolding with templates for static sites, marketing pages, SaaS apps, documentation, and a blog with a CMS.

## Installation

## Usage

```bash
npm create @neutron-build@latest
# or
pnpm create @neutron-build@latest
# or
bun create @neutron-build@latest
```

## Templates

Pass `--template <name>` (default `basic`):

```bash
npm create @neutron-build@latest my-blog -- --template blog
```

- `basic`: minimal mixed static and app starter.
- `marketing`: static-first site with islands and static blog routes.
- `app`: app routes with loaders, actions and a login example.
- `full`: static, app, islands and route groups together.
- `docs`: documentation site with content collections, sidebar and search.
- `blog`: server-rendered blog with [`@neutron-build/cms`](https://neutron.build/docs/content/cms) at `/n-admin`.

## Documentation

[neutron.build](https://neutron.build)

## License

MIT
