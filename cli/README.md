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

## neutron mcp

The `mcp` command turns Neutron into an agent-native tool surface over the Model
Context Protocol. It exposes 19 tools: 17 spanning all of the Nucleus data models
(so an LLM can inspect and query your database directly), plus `search_docs` and
`get_doc` for querying the Neutron framework documentation.

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

Writes are off by default. `query_sql` only accepts `SELECT`, `EXPLAIN`, `SHOW`,
and `WITH` unless you pass `--allow-writes`, which permits `INSERT`/`UPDATE`/
`DELETE`/DDL.

Over the HTTP transport the server answers on several surfaces for maximum client
compatibility: `POST /mcp` (JSON-RPC 2.0), `GET /openai/tools`,
`POST /openai/tools/call`, `GET /tools`, and `POST /tools/{name}`.

### MCP flags

| Flag | Default | Purpose |
|------|---------|---------|
| `--db` | -- | Database URL (overrides `DATABASE_URL` and config) |
| `--transport` | `stdio` | Transport: `stdio` or `http` |
| `--port` | `7700` | HTTP port (only used with `--transport http`) |
| `--dump-schema` | -- | Print schema and exit: `openai`, `mcp`, or `markdown` |
| `--allow-writes` | `false` | Allow `query_sql` to execute mutations and DDL |
| `--log` | `false` | Write debug logs to stderr |

### Tools

| Tool | Model | Purpose |
|------|-------|---------|
| `list_tables` | SQL | List SQL tables with column counts and row estimates |
| `describe_table` | SQL | Describe a table's columns, types, nullability, and primary key |
| `list_nucleus_models` | All | List non-SQL collections (KV, vector, FTS, doc, graph, ts, blob, geo, streams) |
| `query_sql` | SQL | Run a SQL query (read-only unless `--allow-writes`) |
| `kv_get` | KV | Get a single key's value |
| `kv_scan` | KV | Scan keys by prefix, with value and TTL |
| `fts_search` | FTS | Full-text search with BM25 ranking and optional fuzzy matching |
| `vector_search` | Vector | Nearest-neighbor search (cosine, l2, or dot) |
| `cypher_query` | Graph | Run a Cypher query over a graph store |
| `doc_find` | Document | Query a document collection with a JSON filter |
| `ts_range` | TimeSeries | Range query with optional bucketing and aggregation |
| `geo_distance` | Geo | Haversine distance (metres) between two lat/lon points |
| `blob_list` | Blob | List blobs with size, content type, and hash |
| `stream_range` | Streams | Read entries from an append-only stream between two IDs |
| `datalog_query` | Datalog | Evaluate a Datalog query against asserted facts and rules |
| `cdc_changes` | CDC | Read recent change events from the WAL, filtered by table/operation |
| `pubsub_list` | PubSub | List active pub/sub channels |

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
