package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/neutron-build/neutron/cli/internal/project"
	"github.com/spf13/cobra"
)

func init() {
	command := &cobra.Command{Use: "project", Short: "Inspect an opt-in multi-service application"}
	check := &cobra.Command{Use: "check", Short: "Validate application configuration without running commands", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		plan, err := loadApplication("")
		if err != nil {
			return applicationError(cmd, err)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "Application %s is valid (%d services)\n", plan.Name, len(plan.Services))
		return nil
	}}
	planCmd := &cobra.Command{Use: "plan", Short: "Show an application plan without executing it", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		selected, _ := cmd.Flags().GetString("service")
		plan, err := loadApplication(selected)
		if err != nil {
			return applicationError(cmd, err)
		}
		jsonMode, _ := cmd.Flags().GetBool("json")
		if jsonMode {
			encoder := json.NewEncoder(cmd.OutOrStdout())
			encoder.SetIndent("", "  ")
			return encoder.Encode(plan)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "Application %s (%s)\n", plan.Name, plan.Root)
		for _, s := range plan.Services {
			fmt.Fprintf(cmd.OutOrStdout(), "  %s: %s; dependencies=%v; environment keys=%v\n", s.Name, s.Dir, s.DependsOn, s.EnvironmentKeys)
		}
		return nil
	}}
	planCmd.Flags().Bool("json", false, "output versioned JSON (environment values omitted)")
	planCmd.Flags().String("service", "", "select a service and its dependencies")
	command.AddCommand(check, planCmd)
	rootCmd.AddCommand(command)
}
func loadApplication(selected string) (*project.Plan, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	manifest, err := project.Discover(cwd, cfgFile)
	if err != nil {
		return nil, err
	}
	if manifest == nil {
		return nil, fmt.Errorf("no [application] in the project's neutron.toml")
	}
	return manifest.Build(selected)
}
func applicationError(cmd *cobra.Command, err error) error {
	fmt.Fprintf(cmd.ErrOrStderr(), "Application: %v\n", err)
	return err
}
