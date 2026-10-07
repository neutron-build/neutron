package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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
	// Derive the timeout from the command context so cancellation (Ctrl-C,
	// a caller's deadline) propagates into the catalog queries. Cobra does
	// not install a context when a command is invoked directly in a test, so
	// fall back to a background one there (NA-14).
	baseCtx := cmd.Context()
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	ctx, cancel := context.WithTimeout(baseCtx, 60*time.Second)
	defer cancel()

	client, err := db.Connect(ctx, url)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer client.Close()

	// Determine tables to generate
	var tables []string
	if all {
		var err error
		tables, err = enumerateTables(ctx, func(ctx context.Context, sql string, args ...any) (rowIterator, error) {
			return client.Query(ctx, sql, args...)
		}, schema)
		if err != nil {
			return err
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
		codes, err := generateBatch(ctx, client, schema, tables, lang, profile)
		if err != nil {
			return err
		}
		return publishBatch(tables, codes, lang, out)
	}

	// Legacy profile: same order — stage every table's code first, then plan
	// destinations for the whole batch, then write. A later table's failure
	// must not leave a mixed set of new and old files behind (NA-09/NA-14);
	// the error names every table that failed.
	codes, err := generateBatch(ctx, client, schema, tables, lang, profile)
	if err != nil {
		return err
	}
	return publishBatch(tables, codes, lang, out)
}

// rowIterator is the slice of the rows surface enumeration needs, so the
// fail-closed behavior (NA-14) can be tested with an injected cursor.
type rowIterator interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close()
}

// enumerateTables lists the schema's base tables. It fails closed: a scan
// error on any row, or an iteration error after valid rows, is an error —
// never a silently truncated batch presented as success. Profile choice may
// change how columns are read, never whether failure is failure.
func enumerateTables(ctx context.Context, query func(ctx context.Context, sql string, args ...any) (rowIterator, error), schema string) ([]string, error) {
	rows, err := query(ctx, `
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = $1 AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`, schema)
	if err != nil {
		return nil, fmt.Errorf("query tables: %w", err)
	}

	var tables []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan tables: %w", err)
		}
		tables = append(tables, t)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("query table rows: %w", err)
	}
	rows.Close()
	return tables, nil
}

// generateBatch fetches columns and generates code for every table, writing
// nothing: on any error the caller has published nothing, which is the only
// honest outcome for a batch that could not be completed.
func generateBatch(ctx context.Context, client *db.Client, schema string, tables []string, lang, profile string) (map[string]string, error) {
	codes := make(map[string]string, len(tables))
	var errs []string
	for _, t := range tables {
		cols, err := studio.FetchColsForProfile(ctx, client, schema, t, profile)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s %s.%s: %v", lang, schema, t, err))
			continue
		}
		code, err := studio.GenerateCodeProfile(profile, lang, t, cols)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s %s.%s: %v", lang, schema, t, err))
			continue
		}
		codes[t] = code
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("failed to generate for %d table(s):\n%s", len(errs), strings.Join(errs, "\n"))
	}
	return codes, nil
}

// publishBatch resolves every table's destination against the true batch
// table count (NA-09: the legacy path used to pass the column count, so one
// three-column table created a *directory* named after a file argument, and
// several one-column tables all selected the same file and overwrote each
// other), then writes each file through a temporary sibling + rename so a
// failed write cannot truncate a previously valid generated file.
func publishBatch(tables []string, codes map[string]string, lang, out string) error {
	if out == "-" {
		names := append([]string(nil), tables...)
		sort.Strings(names)
		for _, t := range names {
			fmt.Println(codes[t])
		}
		return nil
	}

	dests, err := planOutputDestinations(tables, lang, out)
	if err != nil {
		return err
	}
	for _, t := range tables {
		if err := writeFileAtomically(dests[t], codes[t]); err != nil {
			return err
		}
		ui.Successf("Generated code for %s", t)
	}
	return nil
}

// planOutputDestinations decides where each table's generated file lands, for
// the whole batch, before anything is written. Duplicate destinations are
// rejected rather than raced; a multi-table batch pointed at what looks like a
// single file fails with an explanation instead of silently creating a
// directory named `models.go`.
func planOutputDestinations(tables []string, lang, out string) (map[string]string, error) {
	dests := make(map[string]string, len(tables))
	ext := extensionForLang(lang)

	multi := len(tables) > 1
	if out == "" {
		out = "."
	}

	fi, statErr := os.Stat(out)
	isDir := statErr == nil && fi.IsDir()

	base := out
	if !isDir && strings.HasSuffix(out, string(filepath.Separator)) {
		// An explicit trailing separator is an explicit directory, even one
		// that does not exist yet.
		if err := os.MkdirAll(out, 0o755); err != nil {
			return nil, fmt.Errorf("create output directory: %w", err)
		}
		isDir = true
	}
	if !isDir && multi && statErr == nil {
		return nil, fmt.Errorf("output %s is an existing file; generating %d tables requires a directory", out, len(tables))
	}
	if !isDir && multi && strings.HasSuffix(out, ext) {
		return nil, fmt.Errorf("output %s looks like a single %s file; generating %d tables requires a directory (pass a directory, or a trailing %s)",
			out, lang, len(tables), string(filepath.Separator))
	}
	if !isDir && multi {
		if err := os.MkdirAll(base, 0o755); err != nil {
			return nil, fmt.Errorf("create output directory: %w", err)
		}
		isDir = true
	}

	seen := map[string]string{}
	for _, t := range tables {
		dest := out
		if isDir {
			dest = filepath.Join(out, t+ext)
		}
		if prev, dup := seen[dest]; dup {
			return nil, fmt.Errorf("tables %s and %s both generate to %s; rename one of them", prev, t, dest)
		}
		seen[dest] = t
		dests[t] = dest
	}
	return dests, nil
}

// dirLike is gone: whether a path names a directory is decided by Stat above.
// writeFileAtomically writes to a temporary sibling and renames over the
// destination, so an interrupted write leaves the previous file intact
// instead of a truncated hybrid.
func writeFileAtomically(dest, code string) error {
	if dir := filepath.Dir(dest); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create directory: %w", err)
		}
	}
	tmp, err := os.CreateTemp(filepath.Dir(dest), "."+filepath.Base(dest)+".*")
	if err != nil {
		return fmt.Errorf("stage write: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.WriteString(code); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("stage write: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("stage write: %w", err)
	}
	if err := os.Chmod(tmpName, 0o644); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("stage write: %w", err)
	}
	if err := os.Rename(tmpName, dest); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("write file: %w", err)
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
