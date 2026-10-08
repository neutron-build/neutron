# Security policy

## Support status

Security response support is not yet provisioned for a released version. A
maintainer-approved release support matrix, monitored private reporting channel,
primary/backup triage owners and response window are required before this policy
can promise supported versions or acknowledgments. Package versions alone do
not establish security support.

## Private reporting

GitHub private vulnerability reporting for this repository is currently disabled.
A usable monitored private destination has not been approved or verified.
Do not publish vulnerability details in a public issue. Maintainers must enable
and verify a private reporting destination and delivery before advertising one.
This policy currently makes no acknowledgment-time or monitoring guarantee.

## Scope

All repository components are in scope, including `rust/` (web framework),
`typescript/` (UI framework), `nucleus/` (database engine), `native/`, `desktop/`,
and language SDKs. Report dependency vulnerabilities to their upstream projects.

## Maintainer activation procedure

The route remains disabled until a maintainer completes these steps. In repository
Settings → Code security → Private vulnerability reporting, enable reporting (or
use `gh api --method PUT repos/neutron-build/neutron/private-vulnerability-reporting`
with an authorized repository administrator). Verify the state with
`gh api repos/neutron-build/neutron/private-vulnerability-reporting` and the private
report form at `https://github.com/neutron-build/neutron/security/advisories/new`.

Before replacing this disabled-route status, designate actual primary and backup
responders, confirm their private advisory access and notification delivery, and
agree on a response target and a released-version support matrix. Submit an
authorized non-sensitive private test report and retain private evidence of receipt
and acknowledgment from the designated people. Publish the route, supported
versions and response target only after that test succeeds. This procedure does
not name responders, promise an acknowledgment, or establish that reporting is
currently operational.
