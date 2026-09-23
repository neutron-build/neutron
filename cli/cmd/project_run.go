package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/neutron-build/neutron/cli/internal/project"
	"github.com/neutron-build/neutron/cli/internal/supervisor"
	"github.com/spf13/cobra"
)

func newProjectRunCmd() *cobra.Command {
	run := &cobra.Command{
		Use:   "run <task>...",
		Short: "Run application tasks and their dependencies",
		Args:  cobra.MinimumNArgs(1),
		RunE:  runProjectTasks,
	}
	run.Flags().Int("jobs", runtime.NumCPU(), "maximum tasks running at once")
	run.Flags().Bool("json", false, "print a versioned JSON result on stdout (task output goes to stderr)")
	return run
}

func runProjectTasks(cmd *cobra.Command, args []string) error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	manifest, err := project.Discover(cwd, cfgFile)
	if err != nil {
		return applicationError(cmd, err)
	}
	if manifest == nil {
		return applicationError(cmd, fmt.Errorf("no [application] in the project's neutron.toml"))
	}
	tasks, err := manifest.TaskPlan(args)
	if err != nil {
		return applicationError(cmd, err)
	}
	jobs, _ := cmd.Flags().GetInt("jobs")
	if jobs < 1 {
		return applicationError(cmd, fmt.Errorf("--jobs must be at least 1"))
	}
	jsonMode, _ := cmd.Flags().GetBool("json")
	logs := cmd.OutOrStdout()
	if jsonMode {
		logs = cmd.ErrOrStderr()
	}
	ctx, force, stop := interruptContext(cmd.Context())
	defer stop()
	report, err := supervisor.RunTasks(ctx, tasks, supervisor.TaskOptions{Output: logs, Jobs: jobs, Force: force})
	if err != nil {
		return applicationError(cmd, err)
	}
	if jsonMode {
		encoder := json.NewEncoder(cmd.OutOrStdout())
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(report); err != nil {
			return err
		}
	}
	if !report.Succeeded() {
		return applicationError(cmd, fmt.Errorf("tasks did not all succeed"))
	}
	return nil
}

// interruptContext cancels on the first SIGINT/SIGTERM/SIGHUP (SIGHUP covers
// a closed terminal) and closes force on the second, which skips the
// remaining grace periods.
func interruptContext(parent context.Context) (context.Context, <-chan struct{}, func()) {
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	ctx, cancel := context.WithCancel(parent)
	force := make(chan struct{})
	go func() {
		select {
		case <-signals:
		case <-ctx.Done():
			return
		}
		cancel()
		<-signals
		close(force)
	}()
	return ctx, force, func() {
		signal.Stop(signals)
		cancel()
	}
}
