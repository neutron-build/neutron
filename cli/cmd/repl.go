package cmd

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/neutron-build/neutron/cli/internal/config"
	"github.com/neutron-build/neutron/cli/internal/ui"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(replCmd)
}

var replCmd = &cobra.Command{
	Use:   "repl",
	Short: "Interactive SQL shell",
	Long:  "Open an interactive SQL REPL connected to the database.",
	RunE:  runRepl,
}

func runRepl(cmd *cobra.Command, args []string) error {
	url := config.DatabaseURL()

	// Try to delegate to nucleus shell if available
	nucleusBin, err := exec.LookPath("nucleus")
	if err == nil {
		return delegateToNucleusShell(nucleusBin, url)
	}

	// Built-in REPL
	return builtinRepl(url)
}

func delegateToNucleusShell(binary, url string) error {
	// Parse host and port from URL
	// postgres://host:port/db -> host, port
	host := "127.0.0.1"
	port := "5432"

	if strings.Contains(url, "@") {
		parts := strings.SplitN(url, "@", 2)
		url = parts[1]
	}
	if strings.Contains(url, "://") {
		url = strings.SplitN(url, "://", 2)[1]
	}
	if strings.Contains(url, ":") {
		parts := strings.SplitN(url, ":", 2)
		host = parts[0]
		portAndDB := parts[1]
		if strings.Contains(portAndDB, "/") {
			port = strings.SplitN(portAndDB, "/", 2)[0]
		} else {
			port = portAndDB
		}
	}

	cmd := exec.Command(binary, "shell", "-H", host, "-p", port)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	return cmd.Run()
}

func builtinRepl(url string) error {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)

	ui.Successf("Connected to %s", url)
	fmt.Println("Type SQL queries ending with ';'. Use \\q to quit, \\dt for tables.")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	var buffer strings.Builder
	prompt := "neutron> "

	for {
		fmt.Print(prompt)
		if !scanner.Scan() {
			break
		}

		line := scanner.Text()

		// Meta-commands
		trimmed := strings.TrimSpace(line)
		if buffer.Len() == 0 {
			switch {
			case trimmed == "\\q" || trimmed == "\\quit" || trimmed == "exit":
				fmt.Println("Bye!")
				return nil
			case trimmed == "\\dt":
				executeAndPrint(ctx, conn, "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename")
				continue
			case trimmed == "\\?" || trimmed == "\\help":
				fmt.Println("  \\dt       List tables")
				fmt.Println("  \\q        Quit")
				fmt.Println("  \\?        Show help")
				continue
			case trimmed == "":
				continue
			}
		}

		buffer.WriteString(line)
		buffer.WriteString("\n")

		// Check if query is complete (ends with semicolon)
		if strings.HasSuffix(trimmed, ";") {
			query := strings.TrimSpace(buffer.String())
			buffer.Reset()
			prompt = "neutron> "

			executeAndPrint(ctx, conn, query)
		} else {
			prompt = "      -> "
		}
	}

	return nil
}

func executeAndPrint(ctx context.Context, conn *pgx.Conn, query string) {
	start := time.Now()

	rows, err := conn.Query(ctx, query)
	if err != nil {
		ui.Errorf("%v", err)
		return
	}
	defer rows.Close()

	descs := rows.FieldDescriptions()
	if len(descs) == 0 {
		// Non-SELECT statement
		elapsed := time.Since(start)
		ui.Successf("OK (%s)", elapsed.Round(time.Millisecond))
		return
	}

	// Build header
	headers := make([]string, len(descs))
	for i, d := range descs {
		headers[i] = string(d.Name)
	}

	tbl := ui.NewTable(headers...)

	var rowCount int
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			ui.Errorf("scan: %v", err)
			return
		}

		cells := make([]string, len(values))
		for i, v := range values {
			if v == nil {
				cells[i] = "NULL"
			} else {
				cells[i] = fmt.Sprintf("%v", v)
			}
		}
		tbl.AddRow(cells...)
		rowCount++
	}

	if err := rows.Err(); err != nil {
		ui.Errorf("%v", err)
		return
	}

	tbl.Render()
	elapsed := time.Since(start)
	fmt.Printf("(%d rows, %s)\n\n", rowCount, elapsed.Round(time.Millisecond))
}
