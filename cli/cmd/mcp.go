package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

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
	rootCmd.AddCommand(mcpCmd)
}

var mcpCmd = &cobra.Command{
	Use:   "mcp",
	Short: "Start a Model Context Protocol (MCP) server for Nucleus",
	Long: `Start an MCP server that exposes all 14 Nucleus data models as tools.

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
	RunE: runMCP,
}

func runMCP(cmd *cobra.Command, _ []string) error {
	// --dump-schema needs no DB connection
	dumpFormat, _ := cmd.Flags().GetString("dump-schema")
	if dumpFormat != "" {
		out, err := mcp.DumpSchema(dumpFormat)
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

	log.Printf("mcp: connecting to %s", dbURL)

	srv, err := mcp.NewServer(ctx, dbURL, version)
	if err != nil {
		return fmt.Errorf("mcp server: %w", err)
	}
	defer srv.Close()

	transport, _ := cmd.Flags().GetString("transport")
	switch transport {
	case "http":
		port, _ := cmd.Flags().GetInt("port")
		addr := fmt.Sprintf(":%d", port)
		fmt.Fprintf(os.Stderr, "Nucleus MCP HTTP server on http://localhost%s\n", addr)
		fmt.Fprintf(os.Stderr, "  POST /mcp               — MCP over HTTP\n")
		fmt.Fprintf(os.Stderr, "  GET  /openai/tools      — OpenAI function definitions\n")
		fmt.Fprintf(os.Stderr, "  POST /openai/tools/call — OpenAI-compatible tool call\n")
		fmt.Fprintf(os.Stderr, "  POST /tools/{name}      — plain REST\n")
		return srv.RunHTTP(ctx, addr)
	default: // stdio
		log.Printf("mcp: nucleus MCP server ready — %d tools available", 17)
		srv.Run(ctx)
		return nil
	}
}
