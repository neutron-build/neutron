package neutroncli

import (
	"testing"
)

func TestVersionConst(t *testing.T) {
	if Version == "" {
		t.Fatal("Version should not be empty")
	}
}
