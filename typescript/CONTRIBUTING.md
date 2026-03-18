# Contributing to Neutron

Thanks for your interest in contributing to Neutron. This guide covers everything you need to get started.

## Development Setup

### Prerequisites

- Node.js 20+
- pnpm 9.15+

### Getting Started

```bash
# Clone the repo
git clone https://github.com/neutron-build/neutron.git
cd neutron

# Install dependencies
pnpm install

# Build all packages
pnpm build

# Run tests
pnpm test
```

### Monorepo Structure

```
packages/
  neutron/              Core framework
  neutron-cli/          CLI (dev, build, start, preview)
  create-neutron/       Project scaffolding
  neutron-data/         Data layer (DB, cache, sessions, queues)
  neutron-auth/         Auth middleware
  neutron-security/     Security middleware
  neutron-cache-redis/  Redis cache stores
  neutron-ops/          Observability middleware
  neutron-otel/         OpenTelemetry integration

apps/
  playground/           Test app for development

benchmarks/             Performance comparison suite

docs/                   Documentation
```

### Development Workflow

```bash
# Work on core framework
cd packages/neutron
pnpm dev              # Watch mode

# Run the playground app
cd apps/playground
pnpm dev

# Run benchmarks
pnpm ci:bench:smoke
```

## Making Changes

### Before You Start

1. Check existing issues and PRs to avoid duplicate work.
2. For significant changes, open an issue first to discuss the approach.
3. For bug fixes, include a description of the bug and how to reproduce it.

### Code Style

- TypeScript throughout. No `any` unless absolutely necessary.
- Web standard APIs (Request, Response, Headers) over custom abstractions.
- Keep dependencies minimal. Justify any new dependency.
- No unnecessary abstractions. Three similar lines is better than a premature helper.

### Tests

- Add tests for new functionality.
- Run the full test suite before submitting: `pnpm test`
- For performance-sensitive changes, run benchmarks: `pnpm ci:bench:smoke`

### Commit Messages

Use clear, descriptive commit messages:

```
fix: handle empty FormData in action execution
feat: add client:media island directive
docs: add deployment adapter guide
```

## Pull Requests

1. Fork the repo and create your branch from `main`.
2. Make your changes with tests.
3. Ensure `pnpm test` passes.
4. Ensure `pnpm build` succeeds.
5. Submit a PR with a clear description of what changed and why.

## Reporting Bugs

Open an issue with:

- A clear description of the bug
- Steps to reproduce
- Expected vs actual behavior
- Node.js version and OS

## Requesting Features

Open an issue describing:

- The problem you're trying to solve
- Your proposed solution
- Any alternatives you considered

## Security

For security vulnerabilities, see [SECURITY.md](./SECURITY.md). Do not open public issues for security concerns.
