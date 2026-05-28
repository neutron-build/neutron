# Support Policy

## Release Lines

- `MAJOR`: support window of 12 months from first release.
- `MINOR`: bug-fix support until next minor release.
- `PATCH`: only latest patch in a supported line is maintained.

## Breaking Changes

- Breaking removals occur only in major releases.
- Deprecated APIs are documented at least one minor before removal.

## Enterprise Guidance

- Use optional packages (`@neutron-build/cache-redis`, `@neutron-build/otel`, `@neutron-build/auth`, `@neutron-build/security`, `@neutron-build/ops`) for production hardening.
- Keep core framework usage lean and provider-agnostic.
