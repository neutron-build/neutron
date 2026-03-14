package cmd

import (
	"testing"

	"github.com/neutron-build/neutron/cli/internal/doctor"
)

func TestDoctorCommand(t *testing.T) {
	if doctorCmd.Use != "doctor" {
		t.Errorf("doctorCmd.Use = %q, want %q", doctorCmd.Use, "doctor")
	}
	if doctorCmd.Short == "" {
		t.Error("doctorCmd.Short is empty")
	}
	if doctorCmd.Long == "" {
		t.Error("doctorCmd.Long is empty")
	}
	if doctorCmd.RunE == nil {
		t.Error("doctorCmd.RunE is nil")
	}
}

func TestDoctorStatusConstants(t *testing.T) {
	// Verify status constants are distinct
	if doctor.Pass == doctor.Warn {
		t.Error("Pass == Warn, should be different")
	}
	if doctor.Pass == doctor.Fail {
		t.Error("Pass == Fail, should be different")
	}
	if doctor.Warn == doctor.Fail {
		t.Error("Warn == Fail, should be different")
	}
}

func TestDoctorStatusMapping(t *testing.T) {
	tests := []struct {
		status doctor.Status
		name   string
	}{
		{doctor.Pass, "Pass"},
		{doctor.Warn, "Warn"},
		{doctor.Fail, "Fail"},
	}

	for _, tt := range tests {
		// Verify the status values are usable in switch statements
		var label string
		switch tt.status {
		case doctor.Pass:
			label = "pass"
		case doctor.Warn:
			label = "warn"
		case doctor.Fail:
			label = "fail"
		}
		if label == "" {
			t.Errorf("status %d (%s) did not match any case", tt.status, tt.name)
		}
	}
}
