package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/neutron-build/neutron/cli/internal/config"
	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/neutron-build/neutron/cli/internal/studio"
	"github.com/neutron-build/neutron/cli/internal/ui"
	"github.com/spf13/cobra"
)

func init() {
	generateCmd.Flags().String("table", "", "table name to generate code for")
	generateCmd.Flags().String("schema", "public", "database schema")
	generateCmd.Flags().String("lang", "", "target language (go, ts, rust, python, elixir, zig)")
	generateCmd.Flags().String("profile", "legacy", "read profile (legacy, lossless-read-v1; PostgreSQL scalar TS/Python/Go only)")
	generateCmd.Flags().String("out", "-", "output file or directory (- for stdout)")
	generateCmd.Flags().Bool("all", false, "generate code for all tables in schema")

	rootCmd.AddCommand(generateCmd)
}

var generateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generate typed code from database schema",
	Long:  "Generate type-safe code (Go, TypeScript, Rust, Python, Elixir, Zig) from database tables.",
	RunE:  runGenerate,
}

func runGenerate(cmd *cobra.Command, args []string) error {
	table, _ := cmd.Flags().GetString("table")
	schema, _ := cmd.Flags().GetString("schema")
	lang, _ := cmd.Flags().GetString("lang")
	out, _ := cmd.Flags().GetString("out")
	all, _ := cmd.Flags().GetBool("all")
	profile, _ := cmd.Flags().GetString("profile")

	// Validate flags
	if !all && table == "" {
		return fmt.Errorf("either --table or --all is required")
	}

	// Resolve language
	if lang == "" {
		cfg, err := config.Load()
		if err == nil && cfg.Project.Lang != "" {
			lang = cfg.Project.Lang
		} else {
			lang = "go"
		}
	}

	// Validate language
	validLangs := map[string]bool{"go": true, "ts": true, "rust": true, "python": true, "elixir": true, "zig": true}
	if !validLangs[lang] {
		return fmt.Errorf("unsupported language: %s", lang)
	}

	if err := studio.ValidateCodegenProfile(profile, lang); err != nil {
		return err
	}

	url := config.DatabaseURL()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client, err := db.Connect(ctx, url)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer client.Close()

	// Determine tables to generate
	var tables []string
	if all {
		// Query all tables in schema
		rows, err := client.Query(ctx, `
			SELECT table_name FROM information_schema.tables
			WHERE table_schema = $1 AND table_type = 'BASE TABLE'
			ORDER BY table_name
		`, schema)
		if err != nil {
			return fmt.Errorf("query tables: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var t string
			if err := rows.Scan(&t); err != nil {
				if profile == studio.LosslessReadProfile {
					return fmt.Errorf("scan tables: %w", err)
				}
				continue
			}
			tables = append(tables, t)
		}

		if err := rows.Err(); err != nil && profile == studio.LosslessReadProfile {
			return fmt.Errorf("query table rows: %w", err)
		}
		if len(tables) == 0 {
			ui.Warnf("No tables found in schema %s", schema)
			return nil
		}
	} else {
		tables = []string{table}
	}

	if profile == studio.LosslessReadProfile {
		// Prevalidate batch names and every table before touching any output destination.
		if err := studio.ValidateCodegenBatch(profile, lang, tables); err != nil {
			return err
		}
		codes := make([]string, len(tables))
		for i, t := range tables {
			cols, err := studio.FetchColsForProfile(ctx, client, schema, t, profile)
			if err != nil {
				return fmt.Errorf("%s %s.%s: %w", lang, schema, t, err)
			}
			codes[i], err = studio.GenerateCodeProfile(profile, lang, t, cols)
			if err != nil {
				return err
			}
		}
		for i, t := range tables {
			if err := writeGeneratedCode(t, lang, out, codes[i], len(tables)); err != nil {
				return err
			}
			ui.Successf("Generated code for %s", t)
		}
		return nil
	}

	// Generate code for each table
	var failed []string
	for _, t := range tables {
		if err := generateForTable(ctx, client, schema, t, lang, out); err != nil {
			ui.Warnf("Failed to generate for %s: %v", t, err)
			failed = append(failed, t)
		} else {
			ui.Successf("Generated code for %s", t)
		}
	}

	if len(failed) > 0 {
		return fmt.Errorf("failed to generate for %d table(s)", len(failed))
	}

	return nil
}

func generateForTable(ctx context.Context, client *db.Client, schema, table, lang, out string) error {
	// Fetch columns
	cols, err := studio.FetchColsForTable(ctx, client, schema, table)
	if err != nil {
		return fmt.Errorf("fetch columns: %w", err)
	}

	if len(cols) == 0 {
		return fmt.Errorf("no columns found")
	}

	// Generate code
	code, err := studio.GenerateCode(lang, table, cols)
	if err != nil {
		return err
	}

	return writeGeneratedCode(table, lang, out, code, len(cols))
}

func writeGeneratedCode(table, lang, out, code string, count int) error {
	// Write output
	if out == "-" {
		// Stdout
		fmt.Println(code)
	} else {
		// File or directory
		var filePath string
		if out == "" {
			filePath = table + extensionForLang(lang)
		} else {
			// Check if out is a directory
			fi, err := os.Stat(out)
			if err == nil && fi.IsDir() {
				filePath = filepath.Join(out, table+extensionForLang(lang))
			} else if strings.HasSuffix(out, string(filepath.Separator)) {
				// Path ends with /, treat as directory
				os.MkdirAll(out, 0755)
				filePath = filepath.Join(out, table+extensionForLang(lang))
			} else if count == 1 || out == table+extensionForLang(lang) {
				// Single table or exact filename match
				filePath = out
			} else {
				// Multiple tables, out is a directory path
				os.MkdirAll(out, 0755)
				filePath = filepath.Join(out, table+extensionForLang(lang))
			}
		}

		// Create parent directory if needed
		if dir := filepath.Dir(filePath); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0755); err != nil {
				return fmt.Errorf("create directory: %w", err)
			}
		}

		// Write file
		if err := os.WriteFile(filePath, []byte(code), 0644); err != nil {
			return fmt.Errorf("write file: %w", err)
		}
	}

	return nil
}

func extensionForLang(lang string) string {
	switch lang {
	case "go":
		return ".go"
	case "ts":
		return ".ts"
	case "rust":
		return ".rs"
	case "python":
		return ".py"
	case "elixir":
		return ".ex"
	case "zig":
		return ".zig"
	default:
		return ".txt"
	}
}
