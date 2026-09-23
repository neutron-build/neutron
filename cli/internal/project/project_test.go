package project

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const sample = `[project]
lang="go"
[unrelated]
setting=true
[application]
version=1
name="example"
[application.components.api]
path="."
command=["does-not-need-to-exist"]
env={TOKEN="never-print-this-secret"}
[application.components.api.ready]
http="http://127.0.0.1:8000/health"
timeout="1s"
[application.components.web]
path="."
command=["also-not-executed"]
depends_on=["api"]
[application.components.extra]
path="."
command=["unused"]
`

func manifestFile(t *testing.T, s string) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), "neutron.toml")
	if err := os.WriteFile(file, []byte(s), 0600); err != nil {
		t.Fatal(err)
	}
	return file
}
func TestDiscoverPlanSelectionAndNoSideEffects(t *testing.T) {
	file := manifestFile(t, sample)
	child := filepath.Join(filepath.Dir(file), "nested")
	os.Mkdir(child, 0700)
	m, err := Discover(child, "")
	if err != nil {
		t.Fatal(err)
	}
	p, err := m.Build("web")
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Components) != 2 || p.Components[0].Name != "api" || p.Components[1].Name != "web" {
		t.Fatalf("order: %+v", p.Components)
	}
	data, _ := json.Marshal(p)
	if strings.Contains(string(data), "never-print") || !strings.Contains(string(data), "TOKEN") {
		t.Fatal(string(data))
	}
	p2, err := m.Build("web")
	if err != nil {
		t.Fatal(err)
	}
	again, _ := json.Marshal(p2)
	if string(data) != string(again) {
		t.Fatal("nondeterministic plan")
	}
	if _, err := os.Stat(filepath.Join(m.Root, ".neutron")); !os.IsNotExist(err) {
		t.Fatal("planner modified filesystem")
	}
}
func TestInvalidManifests(t *testing.T) {
	cases := map[string]string{
		"unknown version":    strings.Replace(sample, "version=1", "version=2", 1),
		"unknown field":      strings.Replace(sample, "name=\"example\"", "name=\"example\"\ntypo=true", 1),
		"nested typo":        strings.Replace(sample, "timeout=\"1s\"", "timeout=\"1s\"\nextra=1", 1),
		"unknown dependency": strings.Replace(sample, "depends_on=[\"api\"]", "depends_on=[\"missing\"]", 1),
		"bad command":        strings.Replace(sample, "command=[\"unused\"]", "command=[]", 1),
		"bad duration":       strings.Replace(sample, "timeout=\"1s\"", "timeout=\"0s\"", 1),
		"path escape":        strings.Replace(sample, "path=\".\"", "path=\"..\"", 1),
		"cycle":              strings.Replace(sample, "env={TOKEN", "depends_on=[\"api\"]\nenv={TOKEN", 1),
		"duplicate ports":    strings.ReplaceAll(sample, "path=\".\"", "path=\".\"\nports=[8080]"),
		"duplicate keys":     strings.Replace(sample, "version=1", "version=1\nversion=1", 1),
		"no readiness":       strings.Replace(sample, "depends_on=[\"api\"]", "depends_on=[\"extra\"]", 1),
	}
	for name, s := range cases {
		t.Run(name, func(t *testing.T) {
			m, err := Load(manifestFile(t, s))
			if err == nil {
				_, err = m.Build("")
			}
			if err == nil {
				t.Fatal("accepted invalid manifest")
			}
		})
	}
}
func TestLegacyAndExplicitConfig(t *testing.T) {
	file := manifestFile(t, "[project]\nlang='rust'\n")
	m, err := Discover(filepath.Dir(file), "")
	if err != nil || m != nil {
		t.Fatalf("%v %v", m, err)
	}
	if _, err := Discover(filepath.Dir(file), "missing.toml"); err == nil {
		t.Fatal("missing explicit file accepted")
	}
	app := manifestFile(t, sample)
	m, err = Discover(filepath.Dir(file), app)
	if err != nil || m == nil {
		t.Fatalf("explicit: %v", err)
	}
	if _, err := m.Build("absent"); err == nil {
		t.Fatal("accepted missing selection")
	}
}
func TestSymlinkCannotEscapeRoot(t *testing.T) {
	file := manifestFile(t, strings.Replace(sample, "path=\".\"", "path=\"outside\"", 1))
	if err := os.Symlink(t.TempDir(), filepath.Join(filepath.Dir(file), "outside")); err != nil {
		t.Skip(err)
	}
	m, err := Load(file)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.Build(""); err == nil {
		t.Fatal("accepted outside symlink")
	}
}
