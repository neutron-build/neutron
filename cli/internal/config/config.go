// Package config defines the Neutron CLI configuration structure and loading.
package config

import "github.com/spf13/viper"

// NeutronConfig holds all CLI configuration.
type NeutronConfig struct {
	Database   DatabaseConfig   `mapstructure:"database"`
	Studio     StudioConfig     `mapstructure:"studio"`
	Project    ProjectConfig    `mapstructure:"project"`
	Nucleus    NucleusConfig    `mapstructure:"nucleus"`
	Migrations MigrationsConfig `mapstructure:"migrations"`
}

// DatabaseConfig holds database connection settings.
type DatabaseConfig struct {
	URL string `mapstructure:"url"`
}

// MigrationsConfig holds the migration workflow settings ([migrations] in
// neutron.toml). Snapshots opts `neutron migrate generate` into offline
// snapshot-based planning; dir/schema/module carry the project's paths so
// two checkouts of one repository behave identically.
type MigrationsConfig struct {
	Dir       string `mapstructure:"dir"`
	Snapshots bool   `mapstructure:"snapshots"`
	Schema    string `mapstructure:"schema"`
	Module    string `mapstructure:"module"`
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

// MigrationsDir returns the migrations directory (default "migrations").
func MigrationsDir() string {
	if v := viper.GetString("migrations.dir"); v != "" {
		return v
	}
	return "migrations"
}

// MigrationsSnapshotMode reports whether the manifest opted into offline
// snapshot-based generation ([migrations] snapshots = true).
func MigrationsSnapshotMode() bool {
	return viper.GetBool("migrations.snapshots")
}

// MigrationsSchemaSource returns the desired schema document path.
func MigrationsSchemaSource() string {
	if v := viper.GetString("migrations.schema"); v != "" {
		return v
	}
	return "neutron.schema.json"
}

// MigrationsExportModule returns the schema export module path.
func MigrationsExportModule() string {
	if v := viper.GetString("migrations.module"); v != "" {
		return v
	}
	return "export-schema.mjs"
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
