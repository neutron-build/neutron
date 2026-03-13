package cmd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/neutron-build/neutron/cli/internal/ui"
	"github.com/spf13/cobra"
)

var desktopCmd = &cobra.Command{
	Use:   "desktop",
	Short: "Build and run Neutron Desktop apps (Tauri 2.0)",
	Long: `Commands for developing, building, and previewing Neutron Desktop applications.

Desktop apps use Tauri 2.0 with the neutron:// protocol bridge and embedded Nucleus.

Examples:
  neutron desktop dev          Start dev server with hot reload
  neutron desktop build        Build production .dmg / .msi / .AppImage
  neutron desktop preview      Preview a production build locally`,
}

var desktopDevCmd = &cobra.Command{
	Use:   "dev",
	Short: "Start desktop development server with hot reload",
	RunE:  runDesktopDev,
}

var desktopBuildCmd = &cobra.Command{
	Use:   "build",
	Short: "Build production desktop app",
	RunE:  runDesktopBuild,
}

var desktopPreviewCmd = &cobra.Command{
	Use:   "preview",
	Short: "Preview a production build locally",
	RunE:  runDesktopPreview,
}

func init() {
	desktopDevCmd.Flags().String("port", "5173", "Vite dev server port")
	desktopBuildCmd.Flags().Bool("release", true, "Build in release mode")
	desktopBuildCmd.Flags().String("target", "", "Build target (e.g. aarch64-apple-darwin)")
	desktopCmd.AddCommand(desktopDevCmd)
	desktopCmd.AddCommand(desktopBuildCmd)
	desktopCmd.AddCommand(desktopPreviewCmd)
	rootCmd.AddCommand(desktopCmd)
}

func findDesktopRoot() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	// Walk up to find src-tauri/tauri.conf.json or desktop/Cargo.toml
	dir := cwd
	for {
		if _, err := os.Stat(filepath.Join(dir, "src-tauri", "tauri.conf.json")); err == nil {
			return dir, nil
		}
		if _, err := os.Stat(filepath.Join(dir, "desktop", "examples", "starter", "src-tauri", "tauri.conf.json")); err == nil {
			return filepath.Join(dir, "desktop", "examples", "starter"), nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf("no Tauri project found — run from a directory with src-tauri/tauri.conf.json")
}

func runDesktopDev(cmd *cobra.Command, args []string) error {
	root, err := findDesktopRoot()
	if err != nil {
		return err
	}

	port, _ := cmd.Flags().GetString("port")
	ui.Infof("Starting Neutron Desktop dev server...")
	ui.KeyValue("Project", root)
	ui.KeyValue("Frontend", fmt.Sprintf("http://localhost:%s", port))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Start Vite dev server in background
	viteCmd := exec.CommandContext(ctx, "npx", "vite", "--port", port)
	viteCmd.Dir = root
	viteCmd.Stdout = os.Stdout
	viteCmd.Stderr = os.Stderr
	if err := viteCmd.Start(); err != nil {
		ui.Warnf("Vite not found, trying without frontend dev server")
	}

	// Start Tauri dev
	tauriCmd := exec.CommandContext(ctx, "cargo", "tauri", "dev")
	tauriCmd.Dir = filepath.Join(root, "src-tauri")
	tauriCmd.Stdout = os.Stdout
	tauriCmd.Stderr = os.Stderr
	tauriCmd.Env = append(os.Environ(), fmt.Sprintf("TAURI_DEV_SERVER_URL=http://localhost:%s", port))

	if err := tauriCmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil // User cancelled
		}
		return fmt.Errorf("cargo tauri dev failed: %w", err)
	}

	return nil
}

func runDesktopBuild(cmd *cobra.Command, args []string) error {
	root, err := findDesktopRoot()
	if err != nil {
		return err
	}

	release, _ := cmd.Flags().GetBool("release")
	target, _ := cmd.Flags().GetString("target")

	spinner := ui.NewSpinner("Building Neutron Desktop app...")

	// Build frontend first
	buildCmd := exec.Command("npx", "vite", "build")
	buildCmd.Dir = root
	if out, err := buildCmd.CombinedOutput(); err != nil {
		spinner.StopWithMessage(ui.CrossMark, fmt.Sprintf("Frontend build failed: %s", string(out)))
		return err
	}

	// Build Tauri
	tauriArgs := []string{"tauri", "build"}
	if !release {
		tauriArgs = append(tauriArgs, "--debug")
	}
	if target != "" {
		tauriArgs = append(tauriArgs, "--target", target)
	}

	tauriCmd := exec.Command("cargo", tauriArgs...)
	tauriCmd.Dir = filepath.Join(root, "src-tauri")
	if out, err := tauriCmd.CombinedOutput(); err != nil {
		spinner.StopWithMessage(ui.CrossMark, fmt.Sprintf("Tauri build failed: %s", string(out)))
		return err
	}

	spinner.StopWithMessage(ui.CheckMark, "Desktop app built successfully")

	// Show output location
	bundleDir := filepath.Join(root, "src-tauri", "target", "release", "bundle")
	ui.KeyValue("Output", bundleDir)

	return nil
}

func runDesktopPreview(cmd *cobra.Command, args []string) error {
	root, err := findDesktopRoot()
	if err != nil {
		return err
	}

	ui.Infof("Previewing desktop build...")

	// Find the built app
	bundleDir := filepath.Join(root, "src-tauri", "target", "release", "bundle")
	if _, err := os.Stat(bundleDir); os.IsNotExist(err) {
		return fmt.Errorf("no build found — run 'neutron desktop build' first")
	}

	// On macOS, open the .app bundle
	macosApp := filepath.Join(bundleDir, "macos")
	if entries, err := os.ReadDir(macosApp); err == nil {
		for _, entry := range entries {
			if filepath.Ext(entry.Name()) == ".app" {
				appPath := filepath.Join(macosApp, entry.Name())
				ui.KeyValue("Opening", appPath)
				return exec.Command("open", appPath).Run()
			}
		}
	}

	ui.Warnf("Could not find built app in %s", bundleDir)
	return nil
}
