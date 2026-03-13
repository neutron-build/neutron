package cmd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/neutron-build/neutron/cli/internal/ui"
	"github.com/spf13/cobra"
)

var nativeCmd = &cobra.Command{
	Use:   "native",
	Short: "Build and run Neutron Native apps (iOS & Android)",
	Long: `Commands for developing, building, and running Neutron Native mobile applications.

Native apps use React Native's Fabric renderer with Re.Pack (Rspack) bundler.

Examples:
  neutron native init              Scaffold a new native project
  neutron native dev               Start the bundler dev server
  neutron native run ios            Run on iOS Simulator
  neutron native run android        Run on Android Emulator
  neutron native build --ios        Build iOS production IPA
  neutron native build --android    Build Android production AAB`,
}

var nativeInitCmd = &cobra.Command{
	Use:   "init [name]",
	Short: "Scaffold a new Neutron Native project",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runNativeInit,
}

var nativeRunCmd = &cobra.Command{
	Use:   "run <ios|android>",
	Short: "Run on device or simulator",
	Args:  cobra.ExactArgs(1),
	RunE:  runNativeRun,
}

var nativeBuildCmd = &cobra.Command{
	Use:   "build",
	Short: "Build production app bundle",
	RunE:  runNativeBuild,
}

var nativeDevCmd = &cobra.Command{
	Use:   "dev",
	Short: "Start the Re.Pack development server",
	RunE:  runNativeDev,
}

func init() {
	nativeDevCmd.Flags().String("port", "8081", "Bundler port")
	nativeBuildCmd.Flags().Bool("ios", false, "Build for iOS")
	nativeBuildCmd.Flags().Bool("android", false, "Build for Android")
	nativeBuildCmd.Flags().Bool("release", true, "Release mode")
	nativeCmd.AddCommand(nativeInitCmd)
	nativeCmd.AddCommand(nativeRunCmd)
	nativeCmd.AddCommand(nativeBuildCmd)
	nativeCmd.AddCommand(nativeDevCmd)
	rootCmd.AddCommand(nativeCmd)
}

func findNativeRoot() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	dir := cwd
	for {
		// Look for repack.config.ts or react-native in package.json
		if _, err := os.Stat(filepath.Join(dir, "repack.config.ts")); err == nil {
			return dir, nil
		}
		if _, err := os.Stat(filepath.Join(dir, "repack.config.js")); err == nil {
			return dir, nil
		}
		if _, err := os.Stat(filepath.Join(dir, "ios")); err == nil {
			if _, err2 := os.Stat(filepath.Join(dir, "android")); err2 == nil {
				return dir, nil
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf("no React Native project found — run from a directory with repack.config.ts")
}

func runNativeInit(cmd *cobra.Command, args []string) error {
	name := "my-neutron-app"
	if len(args) > 0 {
		name = args[0]
	}

	spinner := ui.NewSpinner(fmt.Sprintf("Creating native project '%s'...", name))

	// Check prerequisites
	if _, err := exec.LookPath("npx"); err != nil {
		spinner.StopWithMessage(ui.CrossMark, "Node.js not found — install from https://nodejs.org")
		return err
	}

	// Use @react-native-community/cli to init, then overlay Neutron template
	initCmd := exec.Command("npx", "@react-native-community/cli", "init", name, "--skip-install")
	if out, err := initCmd.CombinedOutput(); err != nil {
		spinner.StopWithMessage(ui.CrossMark, fmt.Sprintf("React Native init failed: %s", string(out)))
		return err
	}

	// Install Neutron dependencies
	installCmd := exec.Command("npm", "install", "@neutron/native", "@neutron/native-styling", "react-native-reanimated", "react-native-gesture-handler")
	installCmd.Dir = name
	if out, err := installCmd.CombinedOutput(); err != nil {
		spinner.StopWithMessage(ui.CrossMark, fmt.Sprintf("npm install failed: %s", string(out)))
		return err
	}

	// Install Re.Pack
	installRepack := exec.Command("npm", "install", "--save-dev", "@callstack/repack")
	installRepack.Dir = name
	if out, err := installRepack.CombinedOutput(); err != nil {
		ui.Warnf("Re.Pack install failed (optional): %s", string(out))
	}

	spinner.StopWithMessage(ui.CheckMark, fmt.Sprintf("Created '%s'", name))

	fmt.Println()
	ui.Header("Next steps")
	ui.Infof("cd %s", name)
	ui.Infof("neutron native dev")
	if runtime.GOOS == "darwin" {
		ui.Infof("neutron native run ios")
	}
	ui.Infof("neutron native run android")

	return nil
}

func runNativeDev(cmd *cobra.Command, args []string) error {
	root, err := findNativeRoot()
	if err != nil {
		return err
	}

	port, _ := cmd.Flags().GetString("port")
	ui.Infof("Starting Re.Pack bundler on port %s...", port)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Prefer Re.Pack, fall back to Metro
	bundlerCmd := exec.CommandContext(ctx, "npx", "react-native", "webpack-start", "--port", port)
	if _, err := exec.LookPath("repack"); err == nil {
		bundlerCmd = exec.CommandContext(ctx, "npx", "repack", "start", "--port", port)
	}

	bundlerCmd.Dir = root
	bundlerCmd.Stdout = os.Stdout
	bundlerCmd.Stderr = os.Stderr

	if err := bundlerCmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		// Fall back to Metro
		ui.Warnf("Re.Pack not found, falling back to Metro bundler")
		metroCmd := exec.CommandContext(ctx, "npx", "react-native", "start", "--port", port)
		metroCmd.Dir = root
		metroCmd.Stdout = os.Stdout
		metroCmd.Stderr = os.Stderr
		return metroCmd.Run()
	}

	return nil
}

func runNativeRun(cmd *cobra.Command, args []string) error {
	root, err := findNativeRoot()
	if err != nil {
		return err
	}

	platform := args[0]
	if platform != "ios" && platform != "android" {
		return fmt.Errorf("platform must be 'ios' or 'android', got '%s'", platform)
	}

	if platform == "ios" && runtime.GOOS != "darwin" {
		return fmt.Errorf("iOS builds require macOS")
	}

	ui.Infof("Running on %s...", platform)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	runCmd := exec.CommandContext(ctx, "npx", "react-native", "run-"+platform)
	runCmd.Dir = root
	runCmd.Stdout = os.Stdout
	runCmd.Stderr = os.Stderr

	if err := runCmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("run %s failed: %w", platform, err)
	}

	return nil
}

func runNativeBuild(cmd *cobra.Command, args []string) error {
	root, err := findNativeRoot()
	if err != nil {
		return err
	}

	buildIOS, _ := cmd.Flags().GetBool("ios")
	buildAndroid, _ := cmd.Flags().GetBool("android")

	if !buildIOS && !buildAndroid {
		return fmt.Errorf("specify --ios, --android, or both")
	}

	if buildIOS {
		if runtime.GOOS != "darwin" {
			return fmt.Errorf("iOS builds require macOS")
		}

		spinner := ui.NewSpinner("Building iOS production archive...")

		buildCmd := exec.Command("xcodebuild",
			"-workspace", filepath.Join(root, "ios", "*.xcworkspace"),
			"-scheme", "App",
			"-configuration", "Release",
			"-sdk", "iphoneos",
			"-archivePath", filepath.Join(root, "build", "App.xcarchive"),
			"archive",
		)
		if out, err := buildCmd.CombinedOutput(); err != nil {
			spinner.StopWithMessage(ui.CrossMark, fmt.Sprintf("iOS build failed: %s", string(out)))
			return err
		}

		spinner.StopWithMessage(ui.CheckMark, "iOS archive built")
		ui.KeyValue("Output", filepath.Join(root, "build", "App.xcarchive"))
	}

	if buildAndroid {
		spinner := ui.NewSpinner("Building Android production bundle...")

		gradlew := filepath.Join(root, "android", "gradlew")
		buildCmd := exec.Command(gradlew, "bundleRelease")
		buildCmd.Dir = filepath.Join(root, "android")
		if out, err := buildCmd.CombinedOutput(); err != nil {
			spinner.StopWithMessage(ui.CrossMark, fmt.Sprintf("Android build failed: %s", string(out)))
			return err
		}

		spinner.StopWithMessage(ui.CheckMark, "Android AAB built")
		ui.KeyValue("Output", filepath.Join(root, "android", "app", "build", "outputs", "bundle", "release"))
	}

	return nil
}
