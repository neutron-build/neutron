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
[application.services.api]
path="."
command=["does-not-need-to-exist"]
env={TOKEN="never-print-this-secret"}
[application.services.api.ready]
http="http://127.0.0.1:8000/health"
timeout="1s"
[application.services.web]
path="."
command=["also-not-executed"]
depends_on=["api"]
[application.services.extra]
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
	if len(p.Services) != 2 || p.Services[0].Name != "api" || p.Services[1].Name != "web" {
		t.Fatalf("order: %+v", p.Services)
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
		"tcp named port":     strings.Replace(sample, "http=\"http://127.0.0.1:8000/health\"", "tcp=\"127.0.0.1:http\"", 1),
		"tcp port zero":      strings.Replace(sample, "http=\"http://127.0.0.1:8000/health\"", "tcp=\"127.0.0.1:0\"", 1),
		"tcp port range":     strings.Replace(sample, "http=\"http://127.0.0.1:8000/health\"", "tcp=\"127.0.0.1:70000\"", 1),
		"http port range":    strings.Replace(sample, "127.0.0.1:8000", "127.0.0.1:70000", 1),
		"http port zero":     strings.Replace(sample, "127.0.0.1:8000", "127.0.0.1:0", 1),
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

const taskSample = `[application]
version=1
name="tasks"
[application.services.api]
path="."
command=["api"]
[application.services.api.ready]
tcp="127.0.0.1:9000"
timeout="1s"
[application.tasks.build]
path="."
command=["go","build","./..."]
timeout="5m"
outputs=["bin/api"]
[application.tasks.test]
path="."
command=["go","test","./..."]
depends_on=["build"]
timeout="10m"
env={SECRET="never-print-this"}
[application.tasks.lint]
path="."
command=["go","vet","./..."]
timeout="1m"
`

func TestTaskPlanning(t *testing.T) {
	m, err := Load(manifestFile(t, taskSample))
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := m.TaskPlan([]string{"test"})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 2 || tasks[0].Name != "build" || tasks[1].Name != "test" {
		t.Fatalf("%+v", tasks)
	}
	if _, err := m.TaskPlan([]string{"api"}); err == nil || !strings.Contains(err.Error(), "neutron dev") {
		t.Fatalf("service as task: %v", err)
	}
	if _, err := m.TaskPlan([]string{"nope"}); err == nil {
		t.Fatal("unknown task accepted")
	}
	plan, err := m.Build("")
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(plan)
	if !strings.Contains(string(encoded), `"tasks"`) || strings.Contains(string(encoded), "never-print-this") {
		t.Fatalf("%s", encoded)
	}
}

func TestInvalidTasks(t *testing.T) {
	cases := map[string]string{
		"task depends on service": strings.Replace(taskSample, `depends_on=["build"]`, `depends_on=["api"]`, 1),
		"service depends on task": strings.Replace(taskSample, "command=[\"api\"]", "command=[\"api\"]\ndepends_on=[\"build\"]", 1),
		"missing timeout":         strings.Replace(taskSample, "timeout=\"1m\"", "", 1),
		"output escapes":          strings.Replace(taskSample, `outputs=["bin/api"]`, `outputs=["../elsewhere"]`, 1),
		"task cycle":              strings.Replace(taskSample, "timeout=\"5m\"", "timeout=\"5m\"\ndepends_on=[\"test\"]", 1),
		"name clash":              strings.Replace(taskSample, "[application.tasks.lint]", "[application.tasks.api]", 1),
		"unknown task field":      strings.Replace(taskSample, "timeout=\"1m\"", "timeout=\"1m\"\ncache=true", 1),
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

const contractSample = `[application]
version=1
name="contract"
[application.services.api]
path="."
command=["api"]
contract="neutron/v1"
[application.services.web]
path="."
command=["web"]
depends_on=["api"]
ports=[3000]
[application.services.web.ready]
path="/"
timeout="5s"
[application.services.admin]
path="."
command=["admin"]
depends_on=["api"]
env={NEUTRON_SERVICE_API_URL="http://example.test"}
`

func TestContractServices(t *testing.T) {
	m, err := Load(manifestFile(t, contractSample))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := m.Build("")
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]Service{}
	for _, s := range plan.Services {
		byName[s.Name] = s
	}
	api := byName["api"]
	if !api.AssignPort || api.GracePeriod != "30s" || api.Ready == nil || api.Ready.Path != "/health" {
		t.Fatalf("contract defaults: %+v %+v", api, api.Ready)
	}
	if strings.Join(api.EnvironmentKeys, ",") != "NEUTRON_HOST,NEUTRON_PORT" {
		t.Fatalf("api keys %v", api.EnvironmentKeys)
	}
	if strings.Join(byName["web"].EnvironmentKeys, ",") != "NEUTRON_SERVICE_API_URL" {
		t.Fatalf("web keys %v", byName["web"].EnvironmentKeys)
	}
	if keys := InjectedKeys(byName["admin"], byName); len(keys) != 0 {
		t.Fatalf("explicit env must win: %v", keys)
	}
	if ServiceURLKey("user-api") != "NEUTRON_SERVICE_USER_API_URL" {
		t.Fatal(ServiceURLKey("user-api"))
	}
}

func TestInvalidContractServices(t *testing.T) {
	cases := map[string]string{
		"unknown contract":    strings.Replace(contractSample, "neutron/v1", "neutron/v9", 1),
		"two ports":           strings.Replace(contractSample, "contract=\"neutron/v1\"", "contract=\"neutron/v1\"\nports=[1,2]", 1),
		"path without port":   strings.Replace(contractSample, "ports=[3000]\n", "", 1),
		"path not absolute":   strings.Replace(contractSample, "path=\"/\"", "path=\"health\"", 1),
		"two readiness kinds": strings.Replace(contractSample, "path=\"/\"", "path=\"/\"\ntcp=\"127.0.0.1:3000\"", 1),
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
