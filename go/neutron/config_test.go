package neutron

import (
	"os"
	"testing"
	"time"
)

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := LoadConfig[Config]("TEST_NEUTRON")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Server.Addr != ":8080" {
		t.Errorf("Server.Addr = %q, want :8080", cfg.Server.Addr)
	}
	if cfg.Server.ReadTimeout != 5*time.Second {
		t.Errorf("ReadTimeout = %v, want 5s", cfg.Server.ReadTimeout)
	}
	if cfg.Server.WriteTimeout != 10*time.Second {
		t.Errorf("WriteTimeout = %v, want 10s", cfg.Server.WriteTimeout)
	}
	if cfg.Server.ShutdownTimeout != 30*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 30s", cfg.Server.ShutdownTimeout)
	}
	if cfg.Database.MaxConns != 25 {
		t.Errorf("MaxConns = %d, want 25", cfg.Database.MaxConns)
	}
	if cfg.Log.Level != "info" {
		t.Errorf("Log.Level = %q, want info", cfg.Log.Level)
	}
	if cfg.Log.Format != "json" {
		t.Errorf("Log.Format = %q, want json", cfg.Log.Format)
	}
}

func TestLoadConfigFromEnv(t *testing.T) {
	os.Setenv("MYAPP_SERVER_ADDR", ":9090")
	os.Setenv("MYAPP_DATABASE_URL", "postgres://localhost/test")
	os.Setenv("MYAPP_LOG_LEVEL", "debug")
	defer func() {
		os.Unsetenv("MYAPP_SERVER_ADDR")
		os.Unsetenv("MYAPP_DATABASE_URL")
		os.Unsetenv("MYAPP_LOG_LEVEL")
	}()

	cfg, err := LoadConfig[Config]("MYAPP")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Server.Addr != ":9090" {
		t.Errorf("Server.Addr = %q, want :9090", cfg.Server.Addr)
	}
	if cfg.Database.URL != "postgres://localhost/test" {
		t.Errorf("Database.URL = %q", cfg.Database.URL)
	}
	if cfg.Log.Level != "debug" {
		t.Errorf("Log.Level = %q, want debug", cfg.Log.Level)
	}
}

func TestLoadConfigCustomStruct(t *testing.T) {
	type Custom struct {
		Port    int    `env:"PORT" default:"3000"`
		Debug   bool   `env:"DEBUG" default:"false"`
		AppName string `env:"APP_NAME" default:"myapp"`
	}

	os.Setenv("CUSTOM_PORT", "5000")
	os.Setenv("CUSTOM_DEBUG", "true")
	defer func() {
		os.Unsetenv("CUSTOM_PORT")
		os.Unsetenv("CUSTOM_DEBUG")
	}()

	cfg, err := LoadConfig[Custom]("CUSTOM")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Port != 5000 {
		t.Errorf("Port = %d, want 5000", cfg.Port)
	}
	if !cfg.Debug {
		t.Error("Debug should be true")
	}
	if cfg.AppName != "myapp" {
		t.Errorf("AppName = %q, want myapp", cfg.AppName)
	}
}
