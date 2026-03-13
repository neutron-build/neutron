package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "neutron",
	Short: "The universal CLI for the Neutron ecosystem",
	Long: `Neutron CLI — one tool for every language in the Neutron ecosystem.

Manage your Nucleus database, scaffold projects in any supported language,
run migrations, launch Studio, and more.`,
	SilenceUsage:  true,
	SilenceErrors: true,
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default: neutron.toml)")
	rootCmd.PersistentFlags().String("url", "", "database URL (overrides config and DATABASE_URL)")
	rootCmd.PersistentFlags().Bool("verbose", false, "enable debug logging")
	rootCmd.PersistentFlags().Bool("no-color", false, "disable colored output")

	_ = viper.BindPFlag("database.url", rootCmd.PersistentFlags().Lookup("url"))
	_ = viper.BindPFlag("verbose", rootCmd.PersistentFlags().Lookup("verbose"))
	_ = viper.BindPFlag("no_color", rootCmd.PersistentFlags().Lookup("no-color"))
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		// Walk up from cwd looking for neutron.toml
		dir, err := os.Getwd()
		if err == nil {
			for {
				candidate := filepath.Join(dir, "neutron.toml")
				if _, err := os.Stat(candidate); err == nil {
					viper.SetConfigFile(candidate)
					break
				}
				parent := filepath.Dir(dir)
				if parent == dir {
					break
				}
				dir = parent
			}
		}
	}

	// User-level config as fallback
	home, err := os.UserHomeDir()
	if err == nil {
		userCfg := filepath.Join(home, ".neutron", "config.toml")
		if _, err := os.Stat(userCfg); err == nil && cfgFile == "" {
			if viper.ConfigFileUsed() == "" {
				viper.SetConfigFile(userCfg)
			}
		}
	}

	viper.SetConfigType("toml")

	// Environment variable bindings
	viper.SetEnvPrefix("")
	_ = viper.BindEnv("database.url", "DATABASE_URL", "NUCLEUS_URL")
	_ = viper.BindEnv("project.lang", "NEUTRON_LANG")
	_ = viper.BindEnv("no_color", "NO_COLOR")

	// Defaults
	viper.SetDefault("database.url", "postgres://localhost:5432/neutron")
	viper.SetDefault("studio.port", 4983)
	viper.SetDefault("nucleus.version", "latest")
	viper.SetDefault("nucleus.port", 5432)
	viper.SetDefault("nucleus.data_dir", "nucleus_data")

	if err := viper.ReadInConfig(); err != nil {
		// Config file not found is fine
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			if viper.ConfigFileUsed() != "" {
				fmt.Fprintf(os.Stderr, "Warning: error reading config %s: %v\n", viper.ConfigFileUsed(), err)
			}
		}
	}
}
