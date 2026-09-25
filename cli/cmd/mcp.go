package cmd

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/neutron-build/neutron/cli/internal/inspect"
	"github.com/neutron-build/neutron/cli/internal/mcp"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {
	mcpCmd.Flags().String("db", "", "Database URL (overrides DATABASE_URL and config)")
	mcpCmd.Flags().Bool("log", false, "Write debug logs to stderr (default: silent)")
	mcpCmd.Flags().String("transport", "stdio", "Transport: stdio or http")
	mcpCmd.Flags().Int("port", 7700, "HTTP port (only used with --transport http)")
	mcpCmd.Flags().String("dump-schema", "", "Print tool schema and exit: openai, mcp, or markdown")
	mcpCmd.Flags().Bool("allow-writes", false, "Offer the execute_sql write tool (default: read-only; over HTTP also requires NEUTRON_MCP_TOKEN)")
	mcpCmd.Flags().String("host", "127.0.0.1", "HTTP bind address (only used with --transport http)")
	mcpCmd.Flags().Bool("no-redact", false, "Return values under secret-looking names (password, token, api_key, ...) instead of redacting them")
	mcpCmd.Flags().String("migrations", "migrations", "Application migrations directory for migration_status and inspect_table")
	rootCmd.AddCommand(mcpCmd)
}

var mcpCmd = &cobra.Command{
	Use:   "mcp",
	Short: "Start a Model Context Protocol (MCP) server for Nucleus",
	Long: `Start an MCP server that exposes the database (PostgreSQL or Nucleus,
all 14 Nucleus data models) as inspection and planning tools.

Every tool is read-only by default: on PostgreSQL statements run inside a
READ ONLY transaction that is rolled back, on a connection that is closed
afterwards (advisory locks released); on Nucleus, which does not apply
READ ONLY, a lexical guard refuses writes, the engine's mutating functions
and write Cypher. The guard checks names and cannot see through a
user-defined function or view that wraps a refused one: on PostgreSQL connect
as a low-privilege role; on Nucleus the read-only default is best-effort (a
wrapped write persists), so expose it only to agents you would trust with
writes. Values under secret-looking names are redacted (--no-redact to
disable). Results carry the engine identity and the touched models' actual
transaction/durability limits (engine_limits reports all of them).
--allow-writes adds one explicit write tool, execute_sql; over HTTP it also
requires NEUTRON_MCP_TOKEN. The HTTP transport binds 127.0.0.1 unless --host
says otherwise and answers only requests addressed to localhost, an IP
address or the --host name.

Supports three transports:

  stdio (default) — MCP over stdin/stdout. Use with Claude Desktop, Cursor,
  Windsurf, Zed, and Continue.dev.

    neutron mcp --db postgres://localhost:5432/mydb

  http — HTTP server with multiple API surfaces for maximum compatibility:
    POST /mcp               MCP over HTTP (JSON-RPC 2.0)
    GET  /openai/tools      OpenAI function definitions
    POST /openai/tools/call OpenAI-compatible tool execution
    GET  /tools             Plain JSON tool list
    POST /tools/{name}      Plain REST tool call

    neutron mcp --transport http --port 7700 --db postgres://localhost:5432/mydb

Dump schema without starting a server:

    neutron mcp --dump-schema openai    # OpenAI function definitions JSON
    neutron mcp --dump-schema mcp       # MCP tools/list JSON
    neutron mcp --dump-schema markdown  # Human-readable for system prompts

Claude Desktop (~/.config/Claude/claude_desktop_config.json):

  {
    "mcpServers": {
      "nucleus": {
        "command": "neutron",
        "args": ["mcp"],
        "env": { "DATABASE_URL": "postgres://localhost:5432/mydb" }
      }
    }
  }
`,
	RunE: func(cmd *cobra.Command, args []string) error { return reportRunE(runMCP(cmd, args)) },
}

func runMCP(cmd *cobra.Command, _ []string) error {
	// --dump-schema needs no DB connection
	dumpFormat, _ := cmd.Flags().GetString("dump-schema")
	if dumpFormat != "" {
		allowWrites, _ := cmd.Flags().GetBool("allow-writes")
		out, err := mcp.DumpSchema(dumpFormat, allowWrites)
		if err != nil {
			return err
		}
		fmt.Println(out)
		return nil
	}

	dbURL, _ := cmd.Flags().GetString("db")
	if dbURL == "" {
		dbURL = viper.GetString("database.url")
	}
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
	if dbURL == "" {
		return fmt.Errorf("database URL required: set DATABASE_URL, use --db, or add database.url to neutron.toml")
	}

	enableLog, _ := cmd.Flags().GetBool("log")
	if !enableLog {
		log.SetOutput(os.Stderr)
		log.SetFlags(0)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("mcp: connecting to %s", inspect.RedactURL(dbURL))

	srv, err := mcp.NewServer(ctx, dbURL, version)
	if err != nil {
		return fmt.Errorf("mcp server: %w", err)
	}
	defer srv.Close()

	allowWrites, _ := cmd.Flags().GetBool("allow-writes")
	noRedact, _ := cmd.Flags().GetBool("no-redact")
	migrationsDir, _ := cmd.Flags().GetString("migrations")
	srv.Configure(mcp.Options{AllowWrites: allowWrites, NoRedact: noRedact, MigrationsDir: migrationsDir})

	transport, _ := cmd.Flags().GetString("transport")
	switch transport {
	case "http":
		if allowWrites && os.Getenv("NEUTRON_MCP_TOKEN") == "" {
			return fmt.Errorf("--allow-writes over the HTTP transport requires NEUTRON_MCP_TOKEN (bearer authentication)")
		}
		port, _ := cmd.Flags().GetInt("port")
		host, _ := cmd.Flags().GetString("host")
		addr := net.JoinHostPort(host, strconv.Itoa(port))
		fmt.Fprintf(os.Stderr, "Nucleus MCP HTTP server on http://%s\n", addr)
		fmt.Fprintf(os.Stderr, "  POST /mcp               — MCP over HTTP\n")
		fmt.Fprintf(os.Stderr, "  GET  /openai/tools      — OpenAI function definitions\n")
		fmt.Fprintf(os.Stderr, "  POST /openai/tools/call — OpenAI-compatible tool call\n")
		fmt.Fprintf(os.Stderr, "  POST /tools/{name}      — plain REST\n")
		return srv.RunHTTP(ctx, addr)
	default: // stdio
		log.Printf("mcp: MCP server ready on %s %s — %d tools available (%s)", srv.Engine().Product, srv.Engine().Version, srv.ToolCount(), map[bool]string{true: "writes allowed", false: "read-only"}[allowWrites])
		srv.Run(ctx)
		return nil
	}
}
