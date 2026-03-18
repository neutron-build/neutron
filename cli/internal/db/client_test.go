package db

import (
	"testing"
)

func TestParseNucleusVersion(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"PostgreSQL 16.0 (Nucleus 0.1.0 — The Definitive Database)", "0.1.0"},
		{"PostgreSQL 16.0 (Nucleus 1.2.3)", "1.2.3"},
		{"Nucleus 0.5.0", "0.5.0"},
		{"PostgreSQL 16.0", ""},
		{"", ""},
		{"Nucleus 2.0.0-beta", "2.0.0-beta"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseNucleusVersion(tt.input)
			if got != tt.want {
				t.Errorf("parseNucleusVersion(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestStatusInfoStruct(t *testing.T) {
	info := StatusInfo{
		URL:            "postgres://localhost:5432/neutron",
		Version:        "PostgreSQL 16.0 (Nucleus 0.1.0)",
		IsNucleus:      true,
		NucleusVersion: "0.1.0",
	}

	if info.URL != "postgres://localhost:5432/neutron" {
		t.Errorf("URL = %q", info.URL)
	}
	if !info.IsNucleus {
		t.Error("IsNucleus should be true")
	}
	if info.NucleusVersion != "0.1.0" {
		t.Errorf("NucleusVersion = %q", info.NucleusVersion)
	}
}

func TestCreateTrackingTableSQL(t *testing.T) {
	if createTrackingTable == "" {
		t.Error("createTrackingTable SQL is empty")
	}
	if len(createTrackingTable) < 50 {
		t.Error("createTrackingTable SQL seems too short")
	}
}
