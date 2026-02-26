# Contributing to Neutron

## Repository Structure

Each subdirectory is an independent project with its own language, toolchain, and tests.

| Directory | Language | How to contribute |
|-----------|----------|-------------------|
| `rs/` | Rust | `cargo test` |
| `ts/` | TypeScript | `pnpm test` |
| `mojo/` | Mojo | `pixi run mojo build` |
| `nucleus/` | Rust | `cargo test --lib` |
| `studio/` | TypeScript | `pnpm dev` |
| `go/` | Go | `go test ./...` |
| `zig/` | Zig | `zig build test` |
| `python/` | Python | `pytest` |
| `native/` | TypeScript | `pnpm test` |
| `desktop/` | Rust + TypeScript | `cargo tauri dev` |
| `mobile-preview/` | Go | `go test ./...` |

## Getting Started

1. Fork the repo and clone your fork
2. Navigate to the subdirectory you want to work on
3. Follow the README in that directory for setup instructions
4. Make your changes and ensure tests pass
5. Open a pull request

## Pull Requests

- Keep PRs focused on one project/directory
- Include tests for new functionality
- Update the relevant README if behavior changes
- Reference any related issues

## Issues

- Use the issue tracker for bugs and feature requests
- Tag issues with the relevant project (`rs`, `ts`, `nucleus`, etc.)

## Code of Conduct

Be respectful and constructive. See [`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md) for details.

## License

By contributing, you agree your contributions will be licensed under the MIT License.
