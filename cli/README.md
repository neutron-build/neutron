# Neutron CLI

The universal command-line tool for the Neutron ecosystem.

One binary for every language Neutron supports. Scaffold projects, manage a local
Nucleus database, run migrations, launch Studio, generate typed clients from your
schema, build mobile and desktop apps, and expose Nucleus to AI agents over the
Model Context Protocol -- all from `neutron`.

Written in Go on top of [Cobra](https://github.com/spf13/cobra) and
[Viper](https://github.com/spf13/viper). It talks to Nucleus (or any PostgreSQL
server) over the pgwire protocol via [pgx](https://github.com/jackc/pgx). It
compiles to a single self-contained binary (~25 MB from a local `go build` on
this platform) with no runtime dependencies.

## Install

From source (Go 1.23+):

```bash
cd cli && go build -o bin/neutron . && ./bin/neutron --help
```

Release binaries are published on GitHub under `cli/v*` tags as
`neutron_<version>_<os>_<arch>` archives with a `checksums.txt`. Once installed,
the CLI updates itself:

```bash
neutron upgrade        # self-update from the latest cli/ release
brew upgrade neutron   # delegated automatically for Homebrew installs
```

## Commands

| Command | What it does |
|---------|--------------|
| `new <name>` | Scaffold a new project in a chosen language (python, typescript, go, rust, zig, julia) |
| `init` | Add a `neutron.toml` (and `migrations/`) to an existing project |
| `dev` | Detect the project language and start its dev server |
| `project check` / `plan` / `run` | Validate, inspect and run tasks for an experimental multi-service application |
| `db` | Manage a local Nucleus instance -- subcommands `start`, `stop`, `status`, `reset` |
| `migrate` | Apply SQL migrations -- subcommands `status`, `create <name>`, `down [N]` |
| `seed` | Run a SQL seed file against the database |
| `generate` | Generate typed code from a table schema (go, ts, rust, python, elixir, zig) |
| `studio` | Launch the embedded Studio web UI in the browser |
| `repl` | Interactive SQL shell (delegates to `nucleus shell` when the binary is present) |
| `mcp` | Start a Model Context Protocol server that exposes Nucleus as tools |
| `native` | Build and run mobile apps (iOS & Android) -- subcommands `init`, `dev`, `run`, `build` |
| `desktop` | Build and run Tauri 2.0 desktop apps -- subcommands `dev`, `build`, `preview` |
| `doctor` | Diagnose the local environment: runtimes, database connectivity, configuration |
| `upgrade` | Self-update the CLI to the latest release |
| `version` | Print the CLI version and, if reachable, the connected Nucleus version |
| `completion` | Generate a shell completion script (bash, zsh, fish, powershell) |

### Global flags

| Flag | Purpose |
|------|---------|
| `--url` | Database URL, overrides config and `DATABASE_URL` |
| `--config` | Explicit config file (default: nearest `neutron.toml`) |
| `--verbose` | Enable debug logging |
| `--no-color` | Disable colored output |

## Quick Start

```bash
neutron new my-api --lang go   # scaffold a Go project (omit --lang to pick interactively)
cd my-api
neutron db start               # download + start a local Nucleus instance
neutron migrate                # apply pending migrations
neutron dev                    # start the language-appropriate dev server
```

Generate a typed client for a table, or every table in a schema:

```bash
neutron generate --table users --lang ts --out ./src/db/
neutron generate --all --lang go --out ./gen
```

For an opt-in multi-language development session, see the
[experimental application coordinator example](examples/application/README.md).
An `[application]` table lets `dev` start native commands with dependency readiness
and coordinated shutdown on macOS and Linux. Projects without that table retain
the existing language delegation.

## neutron mcp

The `mcp` command turns Neutron into an agent-native tool surface over the Model
Context Protocol. By default it offers 24 read-only tools: 17 over the SQL and
Nucleus data models, 5 inspection and planning tools, and `search_docs` and
`get_doc` for the Neutron documentation. `--allow-writes` adds one write tool,
`execute_sql`.

```bash
# stdio transport (default) -- for Claude Desktop, Cursor, Windsurf, Zed, Continue
neutron mcp --db postgres://localhost:5432/mydb

# HTTP transport -- MCP + OpenAI-compatible + plain REST surfaces on one port
neutron mcp --transport http --port 7700 --db postgres://localhost:5432/mydb

# Print tool schemas without starting a server
neutron mcp --dump-schema openai     # OpenAI function-calling JSON
neutron mcp --dump-schema mcp        # MCP tools/list JSON
neutron mcp --dump-schema markdown   # human-readable, paste into a system prompt
```

**Read-only by default.** On PostgreSQL every read tool runs inside a
`READ ONLY` transaction that is rolled back, so a data-modifying CTE,
`SELECT INTO`, `nextval()` or `EXPLAIN ANALYZE` of a write fails in the server.
Nucleus does not apply `READ ONLY`, so there a lexical guard refuses
data-modifying keywords, row locks, `EXPLAIN ANALYZE` and every function the
engine classifies as mutating (`KV_SET`, `DOC_INSERT`, `GRAPH_ADD_NODE`, ...).
Both engines refuse multiple statements and functions whose effects escape a
rollback (`pg_advisory_lock`, `pg_terminate_backend`, `dblink_exec`, ...).
`cypher_query` refuses clauses that change the graph.

**Writes are an explicit tool.** `execute_sql` exists only when the server is
started with `--allow-writes`; it runs one statement in its own transaction,
commits it, and reports the touched models' actual limits. `query_sql` stays
read-only either way. Over HTTP, `--allow-writes` also requires
`NEUTRON_MCP_TOKEN`, and the HTTP transport binds `127.0.0.1` unless `--host`
says otherwise.

**Structured, redacted results.** Each result carries the engine, the access
class, how read-only was enforced, the touched models' transaction and
durability limits, and the names of any redacted fields, as MCP
`structuredContent` and as the JSON text content. Values under secret-looking
names (`password`, `password_hash`, `token`, `api_key`, `apiKey`, `secret`, ...)
are replaced with `[redacted]`; matching is by name, so a secret stored under
an innocuous name is not detected. `--no-redact` turns it off. Connection
passwords are masked in logs and errors. The REST surfaces answer
`{"result": <data as JSON text>, "structured": <envelope>}`.

Over the HTTP transport the server answers on several surfaces for maximum client
compatibility: `POST /mcp` (JSON-RPC 2.0), `GET /openai/tools`,
`POST /openai/tools/call`, `GET /tools`, and `POST /tools/{name}`.

### MCP flags

| Flag | Default | Purpose |
|------|---------|---------|
| `--db` | -- | Database URL (overrides `DATABASE_URL` and config) |
| `--transport` | `stdio` | Transport: `stdio` or `http` |
| `--host` | `127.0.0.1` | HTTP bind address (only used with `--transport http`) |
| `--port` | `7700` | HTTP port (only used with `--transport http`) |
| `--dump-schema` | -- | Print schema and exit: `openai`, `mcp`, or `markdown` |
| `--allow-writes` | `false` | Offer `execute_sql`; over HTTP also requires `NEUTRON_MCP_TOKEN` |
| `--no-redact` | `false` | Return values under secret-looking names |
| `--migrations` | `migrations` | Migrations directory for `migration_status` and `inspect_table` |
| `--log` | `false` | Write debug logs to stderr |

### Tools

| Tool | Model | Purpose |
|------|-------|---------|
| `list_tables` | SQL | List SQL tables with column counts and row estimates |
| `describe_table` | SQL | Describe a table's columns, types, nullability, and primary key |
| `list_nucleus_models` | All | Counts the engine exposes per Nucleus model |
| `query_sql` | SQL | Run one read-only statement |
| `kv_get` | KV | Get a single key's value |
| `kv_scan` | KV | List keys by prefix |
| `fts_search` | FTS | Full-text search with BM25 ranking and optional fuzzy matching |
| `vector_search` | Vector | Nearest-neighbor search (cosine, l2, or inner) |
| `cypher_query` | Graph | Run a read-only Cypher query |
| `doc_find` | Document | Query documents with a JSON filter |
| `ts_range` | TimeSeries | Range average or count over an epoch-millisecond window |
| `geo_distance` | Geo | Haversine distance (metres) between two lat/lon points |
| `blob_list` | Blob | List blob keys by prefix |
| `stream_range` | Streams | Read entries from a stream over a time window |
| `datalog_query` | Datalog | Evaluate a Datalog query |
| `cdc_changes` | CDC | Read change events after a sequence number |
| `pubsub_list` | PubSub | List active pub/sub channels |
| `engine_limits` | All | The engine and every model's availability, transaction, durability and hazards, with evidence |
| `inspect_table` | All | One table's schema, migrations, plan, sample rows, bound graph nodes and change events |
| `migration_status` | SQL | Applied and pending migrations with checksum status (PostgreSQL) |
| `explain_sql` | SQL | `EXPLAIN (FORMAT JSON)` of a read, never executed (PostgreSQL) |
| `plan_schema_changes` | SQL | The CLI planner's statements and risk for schema edits, not applied (PostgreSQL) |
| `execute_sql` | All | With `--allow-writes` only: run one statement and commit it |

Example Claude Desktop entry (`~/.config/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "nucleus": {
      "command": "neutron",
      "args": ["mcp"],
      "env": { "DATABASE_URL": "postgres://localhost:5432/mydb" }
    }
  }
}
```

## Configuration

The CLI reads `neutron.toml`, walking up from the current directory, and falls
back to `~/.neutron/config.toml`. Environment variables `DATABASE_URL` /
`NUCLEUS_URL`, `NEUTRON_LANG`, and `NO_COLOR` are honored.

```toml
[database]
url = "postgres://localhost:5432/neutron"

[studio]
port = 4983

[project]
lang = "go"

[nucleus]
version = "latest"
port = 5432
data_dir = "nucleus_data"
```

## Testing

Unit tests cover the command layer and the internal packages:

```bash
go test ./...
```

Some tests exercise database-dependent paths and expect a reachable Nucleus or
PostgreSQL instance at `DATABASE_URL`.

## License

MIT
