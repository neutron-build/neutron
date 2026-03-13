package cmd

import (
	"fmt"

	"github.com/neutron-build/neutron/cli/internal/detect"
	"github.com/neutron-build/neutron/cli/internal/scaffold"
	"github.com/neutron-build/neutron/cli/internal/ui"
	"github.com/spf13/cobra"
)

func init() {
	newCmd.Flags().StringP("lang", "l", "", "project language (python, typescript, go, rust, zig, julia)")
	rootCmd.AddCommand(newCmd)
}

var newCmd = &cobra.Command{
	Use:   "new <project-name>",
	Short: "Create a new Neutron project",
	Long: `Scaffold a new Neutron project in any supported language.

Examples:
  neutron new my-api --lang python
  neutron new my-app --lang typescript
  neutron new my-service --lang go`,
	Args: cobra.ExactArgs(1),
	RunE: runNew,
}

func runNew(cmd *cobra.Command, args []string) error {
	name := args[0]
	langStr, _ := cmd.Flags().GetString("lang")

	var lang detect.Language
	if langStr != "" {
		lang = detect.ParseLanguage(langStr)
		if lang == detect.Unknown {
			return fmt.Errorf("unsupported language %q — use one of: python, typescript, go, rust, zig, julia", langStr)
		}
	} else {
		// Interactive selection
		languages := detect.AllLanguages()
		options := make([]string, len(languages))
		for i, l := range languages {
			options[i] = l.DisplayName()
		}
		idx, err := ui.Select("Which language?", options)
		if err != nil {
			return err
		}
		lang = languages[idx]
	}

	spinner := ui.NewSpinner(fmt.Sprintf("Creating %s project %q...", lang.DisplayName(), name))

	if err := scaffold.ScaffoldProject(name, lang); err != nil {
		spinner.StopWithMessage(ui.CrossMark, fmt.Sprintf("Failed: %v", err))
		return err
	}

	spinner.StopWithMessage(ui.CheckMark, fmt.Sprintf("Created %s project %q", lang.DisplayName(), name))

	fmt.Println()
	ui.Infof("Next steps:")
	fmt.Printf("  cd %s\n", name)

	switch lang {
	case detect.Python:
		fmt.Println("  pip install -e .")
		fmt.Println("  neutron dev")
	case detect.TypeScript:
		fmt.Println("  npm install")
		fmt.Println("  neutron dev")
	case detect.Go:
		fmt.Println("  go mod tidy")
		fmt.Println("  neutron dev")
	case detect.Rust:
		fmt.Println("  cargo build")
		fmt.Println("  neutron dev")
	case detect.Zig:
		fmt.Println("  zig build")
		fmt.Println("  neutron dev")
	case detect.Julia:
		fmt.Println("  julia --project=. -e 'using Pkg; Pkg.instantiate()'")
		fmt.Println("  neutron dev")
	}

	return nil
}
