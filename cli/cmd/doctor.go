package cmd

import (
	"fmt"

	"github.com/neutron-build/neutron/cli/internal/doctor"
	"github.com/neutron-build/neutron/cli/internal/ui"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(doctorCmd)
}

var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Check your development environment",
	Long:  "Run diagnostic checks on your environment: installed runtimes, database connectivity, configuration.",
	RunE:  runDoctor,
}

func runDoctor(cmd *cobra.Command, args []string) error {
	ui.Header("Neutron Doctor")
	fmt.Println()

	results := doctor.RunAll()

	tbl := ui.NewTable("Check", "Status", "Version", "Detail")
	for _, r := range results {
		var status string
		switch r.Status {
		case doctor.Pass:
			status = ui.CheckMark
		case doctor.Warn:
			status = ui.WarnMark
		case doctor.Fail:
			status = ui.CrossMark
		}
		tbl.AddRow(r.Name, status, r.Version, r.Detail)
	}
	tbl.Render()

	// Summary
	var pass, warn, fail int
	for _, r := range results {
		switch r.Status {
		case doctor.Pass:
			pass++
		case doctor.Warn:
			warn++
		case doctor.Fail:
			fail++
		}
	}

	fmt.Println()
	if fail > 0 {
		ui.Errorf("%d checks failed, %d warnings, %d passed", fail, warn, pass)
	} else if warn > 0 {
		ui.Warnf("%d warnings, %d passed", warn, pass)
	} else {
		ui.Successf("All %d checks passed", pass)
	}

	return nil
}
