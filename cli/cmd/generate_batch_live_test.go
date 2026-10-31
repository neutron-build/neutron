package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/spf13/cobra"
)

// Execute the public command with real catalog discovery and existing outputs.
func TestGenerateLosslessBatchSentinelsPostgres(t *testing.T) {
	dsn := os.Getenv("NEUTRON_VALUES_DATABASE_URL")
	if dsn == "" {
		t.Skip("profile PostgreSQL control URL absent")
	}
	t.Setenv("DATABASE_URL", dsn)
	ctx := context.Background()
	c, err := db.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	for _, names := range [][]string{{"a_b", "a__b"}, {"alphaBeta", "alphabeta"}} {
		schema := fmt.Sprintf("v10_values_batch_%d", time.Now().UnixNano())
		if err := c.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := c.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
				t.Error(err)
			}
		})
		for _, name := range names {
			if err := c.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.%q(id bigint)`, schema, name)); err != nil {
				t.Fatal(err)
			}
		}
		for _, lang := range []string{"go", "ts", "python"} {
			out := t.TempDir()
			for _, name := range names {
				if err := os.WriteFile(filepath.Join(out, name+extensionForLang(lang)), []byte("sentinel"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			command := &cobra.Command{Use: "generate", RunE: runGenerate, SilenceErrors: true, SilenceUsage: true}
			command.Flags().String("table", "", "")
			command.Flags().String("schema", schema, "")
			command.Flags().String("lang", lang, "")
			command.Flags().String("profile", "lossless-read-v1", "")
			command.Flags().String("out", out, "")
			command.Flags().Bool("all", true, "")
			err := command.Execute()
			if err == nil {
				t.Fatal("colliding public batch succeeded")
			}
			for _, name := range names {
				if !strings.Contains(err.Error(), name) {
					t.Fatalf("missing source table %q: %v", name, err)
				}
				data, e := os.ReadFile(filepath.Join(out, name+extensionForLang(lang)))
				if e != nil || string(data) != "sentinel" {
					t.Fatalf("output mutated for %s", name)
				}
			}
		}
	}
}
