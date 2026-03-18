// Package config defines the Neutron CLI configuration structure and loading.
package config

import "github.com/spf13/viper"

// NeutronConfig holds all CLI configuration.
type NeutronConfig struct {
	Database DatabaseConfig `mapstructure:"database"`
	Studio   StudioConfig   `mapstructure:"studio"`
	Project  ProjectConfig  `mapstructure:"project"`
	Nucleus  NucleusConfig  `mapstructure:"nucleus"`
}

// DatabaseConfig holds database connection settings.
type DatabaseConfig struct {
	URL string `mapstructure:"url"`
}

// StudioConfig holds Studio UI settings.
type StudioConfig struct {
	Port int `mapstructure:"port"`
}

// ProjectConfig holds project-level settings.
type ProjectConfig struct {
	Lang string `mapstructure:"lang"`
}

// NucleusConfig holds Nucleus binary management settings.
type NucleusConfig struct {
	Version string `mapstructure:"version"`
	Port    int    `mapstructure:"port"`
	DataDir string `mapstructure:"data_dir"`
}

// Load reads the current viper state into a NeutronConfig.
func Load() (*NeutronConfig, error) {
	var cfg NeutronConfig
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// DatabaseURL returns the configured database URL.
func DatabaseURL() string {
	return viper.GetString("database.url")
}

// StudioPort returns the configured Studio port.
func StudioPort() int {
	return viper.GetInt("studio.port")
}

// NucleusVersion returns the configured Nucleus version.
func NucleusVersion() string {
	return viper.GetString("nucleus.version")
}

// NucleusPort returns the configured Nucleus port.
func NucleusPort() int {
	return viper.GetInt("nucleus.port")
}

// NucleusDataDir returns the configured Nucleus data directory.
func NucleusDataDir() string {
	return viper.GetString("nucleus.data_dir")
}

// Verbose returns whether verbose logging is enabled.
func Verbose() bool {
	return viper.GetBool("verbose")
}

// NoColor returns whether colored output is disabled.
func NoColor() bool {
	return viper.GetBool("no_color")
}
