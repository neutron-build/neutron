package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/neutron-build/neutron/cli/internal/project"
	"github.com/neutron-build/neutron/cli/internal/supervisor"
	"github.com/spf13/cobra"
)

func newProjectSpecCmd() *cobra.Command {
	spec := &cobra.Command{
		Use:   "spec",
		Short: "Snapshot or check the OpenAPI documents of neutron/v1 services",
		Long: `Starts the application's neutron/v1 services, fetches each /openapi.json once
ready, and writes it canonically to <service path>/openapi.json (--write) or
fails if a committed snapshot differs (--check). The snapshot is the reviewed
interface other services generate typed clients from.`,
		Args: cobra.NoArgs,
		RunE: runProjectSpec,
	}
	spec.Flags().Bool("write", false, "write snapshots")
	spec.Flags().Bool("check", false, "fail if a snapshot is missing or differs from the running service")
	spec.Flags().String("service", "", "only this service (its dependencies still start)")
	return spec
}

func runProjectSpec(cmd *cobra.Command, args []string) error {
	write, _ := cmd.Flags().GetBool("write")
	check, _ := cmd.Flags().GetBool("check")
	if write == check {
		return applicationError(cmd, fmt.Errorf("pass exactly one of --write or --check"))
	}
	selected, _ := cmd.Flags().GetString("service")
	plan, err := loadApplication(selected)
	if err != nil {
		return applicationError(cmd, err)
	}
	var contract []project.Service
	for _, s := range plan.Services {
		if s.Contract == project.ContractNeutronV1 && (selected == "" || s.Name == selected) {
			contract = append(contract, s)
		}
	}
	if len(contract) == 0 {
		return applicationError(cmd, fmt.Errorf("no neutron/v1 services in the application"))
	}
	drift := []string{}
	onReady := func(ports map[string]int) error {
		for _, s := range contract {
			canonical, err := fetchSpec(cmd.Context(), ports[s.Name])
			if err != nil {
				return fmt.Errorf("%s: %w", s.Name, err)
			}
			file := filepath.Join(s.Dir, "openapi.json")
			if write {
				if err := os.WriteFile(file, canonical, 0o644); err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), "wrote %s\n", file)
				continue
			}
			existing, err := os.ReadFile(file)
			if err != nil {
				drift = append(drift, fmt.Sprintf("%s: no snapshot at %s (run neutron project spec --write)", s.Name, file))
			} else if !bytes.Equal(existing, canonical) {
				drift = append(drift, fmt.Sprintf("%s: running service's /openapi.json differs from %s", s.Name, file))
			} else {
				fmt.Fprintf(cmd.OutOrStdout(), "%s: snapshot matches\n", s.Name)
			}
		}
		return nil
	}
	ctx, force, stop := interruptContext(cmd.Context())
	defer stop()
	// Service output is not the point here; keep it off stdout.
	if err := supervisor.Run(ctx, plan, supervisor.Options{Output: cmd.ErrOrStderr(), Force: force, OnReady: onReady}); err != nil {
		return applicationError(cmd, err)
	}
	if len(drift) > 0 {
		for _, d := range drift {
			fmt.Fprintln(cmd.ErrOrStderr(), d)
		}
		return applicationError(cmd, fmt.Errorf("OpenAPI snapshots are out of date"))
	}
	return nil
}

// fetchSpec returns the service's OpenAPI document with sorted keys and
// two-space indentation, so snapshots diff cleanly.
func fetchSpec(ctx context.Context, port int) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/openapi.json", port), nil)
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("/openapi.json returned %s", response.Status)
	}
	var document any
	if err := json.NewDecoder(io.LimitReader(response.Body, 16<<20)).Decode(&document); err != nil {
		return nil, fmt.Errorf("/openapi.json is not JSON: %w", err)
	}
	canonical, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(canonical, '\n'), nil
}
