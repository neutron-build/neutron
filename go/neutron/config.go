package neutron

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Config holds the server configuration.
type Config struct {
	Server   ServerConfig   `env:"SERVER"`
	Database DatabaseConfig `env:"DATABASE"`
	Log      LogConfig      `env:"LOG"`
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Addr            string        `env:"ADDR" default:":8080"`
	ReadTimeout     time.Duration `env:"READ_TIMEOUT" default:"5s"`
	WriteTimeout    time.Duration `env:"WRITE_TIMEOUT" default:"10s"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" default:"30s"`
}

// DatabaseConfig holds database connection settings.
type DatabaseConfig struct {
	URL      string `env:"URL"`
	MaxConns int    `env:"MAX_CONNS" default:"25"`
	MinConns int    `env:"MIN_CONNS" default:"5"`
}

// LogConfig holds logging settings.
type LogConfig struct {
	Level  string `env:"LEVEL" default:"info"`
	Format string `env:"FORMAT" default:"json"`
}

// LoadConfig loads configuration from environment variables with the given prefix.
// It reads struct tags `env` for variable names and `default` for fallback values.
func LoadConfig[T any](prefix string) (T, error) {
	var cfg T
	v := reflect.ValueOf(&cfg).Elem()
	if err := loadStructFromEnv(v, prefix); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func loadStructFromEnv(v reflect.Value, prefix string) error {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldVal := v.Field(i)

		envTag := field.Tag.Get("env")
		if envTag == "" {
			continue
		}

		envKey := prefix + "_" + envTag

		// Recurse into nested structs
		if field.Type.Kind() == reflect.Struct && field.Type != reflect.TypeOf(time.Duration(0)) {
			if err := loadStructFromEnv(fieldVal, envKey); err != nil {
				return err
			}
			continue
		}

		envVal := os.Getenv(envKey)
		if envVal == "" {
			envVal = field.Tag.Get("default")
		}
		if envVal == "" {
			if field.Tag.Get("required") == "true" {
				return fmt.Errorf("required environment variable %s is not set", envKey)
			}
			continue
		}

		if err := setFieldFromString(fieldVal, envVal); err != nil {
			return fmt.Errorf("failed to set %s from %q: %w", envKey, envVal, err)
		}
	}
	return nil
}

func setFieldFromString(v reflect.Value, s string) error {
	if s == "" {
		return nil
	}

	// Handle time.Duration specially
	if v.Type() == reflect.TypeOf(time.Duration(0)) {
		d, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		v.Set(reflect.ValueOf(d))
		return nil
	}

	switch v.Kind() {
	case reflect.String:
		v.SetString(s)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		v.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return err
		}
		v.SetUint(n)
	case reflect.Float32, reflect.Float64:
		n, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return err
		}
		v.SetFloat(n)
	case reflect.Bool:
		b, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		v.SetBool(b)
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.String {
			parts := strings.Split(s, ",")
			v.Set(reflect.ValueOf(parts))
		}
	default:
		return fmt.Errorf("unsupported field type %s", v.Kind())
	}
	return nil
}
