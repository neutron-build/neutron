package config

import (
	"testing"

	"github.com/spf13/viper"
)

func TestLoadDefaults(t *testing.T) {
	viper.Reset()
	viper.SetDefault("database.url", "postgres://localhost:5432/neutron")
	viper.SetDefault("studio.port", 4983)
	viper.SetDefault("nucleus.version", "latest")
	viper.SetDefault("nucleus.port", 5432)
	viper.SetDefault("nucleus.data_dir", "nucleus_data")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Database.URL != "postgres://localhost:5432/neutron" {
		t.Errorf("Database.URL = %q, want postgres://localhost:5432/neutron", cfg.Database.URL)
	}
	if cfg.Studio.Port != 4983 {
		t.Errorf("Studio.Port = %d, want 4983", cfg.Studio.Port)
	}
	if cfg.Nucleus.Version != "latest" {
		t.Errorf("Nucleus.Version = %q, want latest", cfg.Nucleus.Version)
	}
	if cfg.Nucleus.Port != 5432 {
		t.Errorf("Nucleus.Port = %d, want 5432", cfg.Nucleus.Port)
	}
	if cfg.Nucleus.DataDir != "nucleus_data" {
		t.Errorf("Nucleus.DataDir = %q, want nucleus_data", cfg.Nucleus.DataDir)
	}
}

func TestDatabaseURLHelper(t *testing.T) {
	viper.Reset()
	viper.Set("database.url", "postgres://remote:5432/prod")

	if got := DatabaseURL(); got != "postgres://remote:5432/prod" {
		t.Errorf("DatabaseURL() = %q, want postgres://remote:5432/prod", got)
	}
}

func TestNoColor(t *testing.T) {
	viper.Reset()
	viper.Set("no_color", true)
	if !NoColor() {
		t.Error("NoColor() = false, want true")
	}
}
