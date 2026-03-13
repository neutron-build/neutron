# Security Policy

## Supported Versions

| Component | Version | Supported |
|-----------|---------|-----------|
| Neutron RS | 0.1.x | Yes |
| Neutron TS | 0.1.x | Yes |
| Neutron Mojo | 0.1.x | Yes |
| Nucleus | 0.1.x | Yes |

## Reporting a Vulnerability

If you discover a security vulnerability in any Neutron or Nucleus component, please report it responsibly.

**Do not open a public issue.** Instead:

1. Email **SOON** with a description of the vulnerability
2. Include steps to reproduce, affected versions, and any potential impact
3. Allow up to 72 hours for an initial response

## What to Expect

- **Acknowledgment** within 72 hours of your report
- **Assessment** of severity and affected components within 1 week
- **Fix and disclosure** coordinated with you before public release
- **Credit** in the release notes (unless you prefer to remain anonymous)

## Scope

This policy covers all code in this repository:

- `rs/` — Rust web framework
- `ts/` — TypeScript UI framework
- `nucleus/` — Database engine (encryption, authentication, wire protocol)
- `mojo/` — ML inference library

Security issues in dependencies should be reported to the respective upstream projects.
