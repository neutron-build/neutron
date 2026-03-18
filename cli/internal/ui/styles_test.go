package ui

import (
	"os"
	"testing"

	"github.com/spf13/viper"
)

func TestColorEnabled(t *testing.T) {
	viper.Reset()
	// Default should be enabled
	got := ColorEnabled()
	if os.Getenv("NO_COLOR") != "" {
		// If NO_COLOR is set in env, it should be disabled
		if got {
			t.Error("ColorEnabled() = true, but NO_COLOR is set")
		}
	} else {
		if !got {
			t.Error("ColorEnabled() = false, want true by default")
		}
	}
}

func TestColorDisabledByViper(t *testing.T) {
	viper.Reset()
	viper.Set("no_color", true)
	if ColorEnabled() {
		t.Error("ColorEnabled() = true when no_color is set in viper")
	}
}

func TestColorDisabledByEnv(t *testing.T) {
	viper.Reset()
	t.Setenv("NO_COLOR", "1")
	if ColorEnabled() {
		t.Error("ColorEnabled() = true when NO_COLOR env is set")
	}
}

func TestStatusIndicatorsNotEmpty(t *testing.T) {
	if CheckMark == "" {
		t.Error("CheckMark is empty")
	}
	if CrossMark == "" {
		t.Error("CrossMark is empty")
	}
	if WarnMark == "" {
		t.Error("WarnMark is empty")
	}
	if Arrow == "" {
		t.Error("Arrow is empty")
	}
}
