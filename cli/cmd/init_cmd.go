package cmd

import (
	"fmt"
	"os"

	"github.com/neutron-build/neutron/cli/internal/detect"
	"github.com/neutron-build/neutron/cli/internal/ui"
	"github.com/spf13/cobra"
)

func init() {
	initCmd.Flags().StringP("lang", "l", "", "project language (auto-detected if not specified)")
	rootCmd.AddCommand(initCmd)
}

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize Neutron in an existing project",
	Long:  "Add a neutron.toml configuration file to an existing project.",
	RunE:  runInit,
}

func runInit(cmd *cobra.Command, args []string) error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	// Check if neutron.toml already exists
	if _, err := os.Stat("neutron.toml"); err == nil {
		return fmt.Errorf("neutron.toml already exists in %s", cwd)
	}

	langStr, _ := cmd.Flags().GetString("lang")
	var lang detect.Language
	if langStr != "" {
		lang = detect.ParseLanguage(langStr)
	} else {
		lang = detect.DetectLanguage(cwd)
	}

	if lang == detect.Unknown {
		ui.Warnf("Could not detect project language. Specify with --lang")
		lang = detect.Go // default
	}

	// Write neutron.toml
	dirName := cwd
	if base := lastPathComponent(cwd); base != "" {
		dirName = base
	}

	content := fmt.Sprintf(`[database]
url = "postgres://localhost:5432/%s"

[studio]
port = 4983

[project]
lang = "%s"
`, dirName, lang)

	if err := os.WriteFile("neutron.toml", []byte(content), 0644); err != nil {
		return fmt.Errorf("write neutron.toml: %w", err)
	}

	// Create migrations directory
	if err := os.MkdirAll("migrations", 0755); err != nil {
		return fmt.Errorf("create migrations dir: %w", err)
	}

	ui.Successf("Initialized Neutron (%s) in %s", lang.DisplayName(), cwd)
	ui.KeyValue("config", "neutron.toml")
	ui.KeyValue("migrations", "migrations/")
	return nil
}

func lastPathComponent(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[i+1:]
		}
	}
	return path
}
