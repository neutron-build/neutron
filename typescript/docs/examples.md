# Reference Examples

> **Terminology note:** This page documents **Neutron TypeScript**. In broader ecosystem docs, **Neutron** refers to the umbrella framework/platform across implementations.


Current reference apps in this repo:

- `apps/playground`: mixed static + app routes (general integration baseline, now includes `neutron-data` runtime profile + worker + DB scripts).
- `apps/playground-react-compat`: same runtime flow in `react-compat` mode.
- `examples/marketing-reference`: static-first marketing site (content collections + MDX).
- `examples/saas-reference`: app-route SaaS skeleton (loaders/actions/forms/cache).

Recommended usage:

- Marketing/static pattern: prioritize static routes + content collections.
- SaaS/app pattern: use app routes with loaders/actions/forms, caching, and sessions.

Planned expansion:

- SaaS auth integration variant.
- Internationalized marketing variant.
- Enterprise hardening variant (`@neutron/cache-redis` + `@neutron/security` + `@neutron/ops`).
