# Security policy

## Support status

Security response support is not yet provisioned for a released version. A
maintainer-approved release support matrix, monitored private reporting channel,
primary/backup triage owners and response window are required before this policy
can promise supported versions or acknowledgments. Package versions alone do
not establish security support.

## Private reporting

Report vulnerabilities through GitHub private vulnerability reporting: the
"Report a vulnerability" button on this repository's Security tab
(`https://github.com/neutron-build/neutron/security/advisories/new`). It was
enabled on 2026-10-08. Do not publish vulnerability details in a public issue.

Response commitments are currently disabled: no primary/backup responders,
acknowledgment target or released-version support matrix have been verified,
so this policy makes no acknowledgment-time or monitoring guarantee.

## Scope

All repository components are in scope, including `rust/` (web framework),
`typescript/` (UI framework), `nucleus/` (database engine), `native/`, `desktop/`,
and language SDKs. Report dependency vulnerabilities to their upstream projects.

## Maintainer activation procedure

The reporting route is enabled (verify with
`gh api repos/neutron-build/neutron/private-vulnerability-reporting`). Before
replacing the disabled response-commitment status above, designate actual primary and backup
responders, confirm their private advisory access and notification delivery, and
agree on a response target and a released-version support matrix. Submit an
authorized non-sensitive private test report and retain private evidence of receipt
and acknowledgment from the designated people. Publish the route, supported
versions and response target only after that test succeeds. This procedure does
not name responders, promise an acknowledgment, or establish that reporting is
currently operational.
