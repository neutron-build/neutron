// Package project loads opt-in application manifests without global config state.
package project

import (
	"bytes"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
)

type Application struct {
	Version  int                    `toml:"version"`
	Name     string                 `toml:"name"`
	Services map[string]ServiceSpec `toml:"services"`
	Tasks    map[string]TaskSpec    `toml:"tasks"`
}
type ServiceSpec struct {
	Path      string            `toml:"path"`
	Contract  string            `toml:"contract"`
	Command   []string          `toml:"command"`
	DependsOn []string          `toml:"depends_on"`
	Ports     []int             `toml:"ports"`
	Env       map[string]string `toml:"env"`
	Ready     *Readiness        `toml:"ready"`
}

// TaskSpec is a finite command: exit 0 is success, unlike a service.
type TaskSpec struct {
	Path      string            `toml:"path"`
	Command   []string          `toml:"command"`
	DependsOn []string          `toml:"depends_on"`
	Env       map[string]string `toml:"env"`
	Timeout   string            `toml:"timeout"`
	Outputs   []string          `toml:"outputs"`
}
type Readiness struct {
	HTTP string `toml:"http" json:"http,omitempty"`
	TCP  string `toml:"tcp" json:"tcp,omitempty"`
	// Path probes HTTP on the service's own single port.
	Path    string `toml:"path" json:"path,omitempty"`
	Timeout string `toml:"timeout" json:"timeout"`
}
type Manifest struct {
	File, Root  string
	Application Application
}
type Plan struct {
	Version  int       `json:"version"`
	Name     string    `json:"name"`
	Root     string    `json:"root"`
	Services []Service `json:"services"`
	Tasks    []Task    `json:"tasks,omitempty"`
}
type Service struct {
	Name      string   `json:"name"`
	Dir       string   `json:"directory"`
	Command   []string `json:"command"`
	Contract  string   `json:"contract,omitempty"`
	DependsOn []string `json:"depends_on,omitempty"`
	Ports     []int    `json:"ports,omitempty"`
	// AssignPort means the coordinator picks a free loopback port at start.
	AssignPort      bool              `json:"assign_port,omitempty"`
	GracePeriod     string            `json:"grace_period,omitempty"`
	Env             map[string]string `json:"-"`
	EnvironmentKeys []string          `json:"environment_keys,omitempty"`
	Ready           *Readiness        `json:"ready,omitempty"`
}

type Task struct {
	Name            string            `json:"name"`
	Dir             string            `json:"directory"`
	Command         []string          `json:"command"`
	DependsOn       []string          `json:"depends_on,omitempty"`
	Env             map[string]string `json:"-"`
	EnvironmentKeys []string          `json:"environment_keys,omitempty"`
	Timeout         string            `json:"timeout"`
	Outputs         []string          `json:"outputs,omitempty"`
}

// Discover never reads runnable application definitions from home config.
// A nil manifest means the nearest manifest has no application, or none exists.
func Discover(cwd, explicit string) (*Manifest, error) {
	if explicit != "" {
		if !filepath.IsAbs(explicit) {
			explicit = filepath.Join(cwd, explicit)
		}
		return Load(explicit)
	}
	dir, err := filepath.Abs(cwd)
	if err != nil {
		return nil, err
	}
	for {
		file := filepath.Join(dir, "neutron.toml")
		if _, err := os.Stat(file); err == nil {
			return Load(file)
		} else if !os.IsNotExist(err) {
			return nil, err
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return nil, nil
		}
		dir = parent
	}
}

func Load(file string) (*Manifest, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	var root map[string]any
	if err := toml.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	raw, exists := root["application"]
	if !exists {
		return nil, nil
	}
	table, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s: application must be a table", file)
	}
	encoded, err := toml.Marshal(table)
	if err != nil {
		return nil, err
	}
	var app Application
	if err := toml.NewDecoder(bytes.NewReader(encoded)).DisallowUnknownFields().Decode(&app); err != nil {
		return nil, fmt.Errorf("%s: invalid application: %w", file, err)
	}
	abs, err := filepath.Abs(file)
	if err != nil {
		return nil, err
	}
	resolved, err := filepath.EvalSymlinks(filepath.Dir(abs))
	if err != nil {
		return nil, err
	}
	return &Manifest{File: abs, Root: resolved, Application: app}, nil
}

// validPort accepts only numeric ports: a service name such as "http" would
// resolve through the system services database at probe time.
func validPort(s string) bool {
	n, err := strconv.Atoi(s)
	return err == nil && n >= 1 && n <= 65535 && strconv.Itoa(n) == s
}

func validName(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

// Build validates the whole manifest before selecting a dependency closure.
// It performs no command execution, network requests, or filesystem mutations.
func (m *Manifest) Build(selected string) (*Plan, error) {
	app := m.Application
	if app.Version != 1 {
		return nil, fmt.Errorf("unsupported application version %d (want 1)", app.Version)
	}
	if !validName(app.Name) || len(app.Services)+len(app.Tasks) == 0 {
		return nil, fmt.Errorf("application needs a name and at least one service or task")
	}
	names := make([]string, 0, len(app.Services))
	for name := range app.Services {
		names = append(names, name)
	}
	sort.Strings(names)
	services := map[string]Service{}
	ports := map[int]string{}
	for _, name := range names {
		c := app.Services[name]
		if !validName(name) {
			return nil, fmt.Errorf("invalid service name %q", name)
		}
		dir, keys, err := m.validateCommon(name, c.Path, c.Command, c.Env)
		if err != nil {
			return nil, err
		}
		for _, port := range c.Ports {
			if port < 1 || port > 65535 {
				return nil, fmt.Errorf("%s: invalid port %d", name, port)
			}
			if previous, ok := ports[port]; ok {
				return nil, fmt.Errorf("port %d declared by both %s and %s", port, previous, name)
			}
			ports[port] = name
		}
		svc := Service{Name: name, Dir: dir, Command: append([]string(nil), c.Command...), Contract: c.Contract, Ports: c.Ports, Env: c.Env, Ready: c.Ready}
		switch c.Contract {
		case "":
		case ContractNeutronV1:
			if len(c.Ports) > 1 {
				return nil, fmt.Errorf("%s: a %s service serves one port; declare at most one", name, ContractNeutronV1)
			}
			svc.AssignPort = len(c.Ports) == 0
			svc.GracePeriod = "30s" // FRAMEWORK_CONTRACT §8 drain default
			if svc.Ready == nil {
				svc.Ready = &Readiness{Path: "/health", Timeout: "60s"}
			}
		default:
			return nil, fmt.Errorf("%s: unsupported contract %q (supported: %s)", name, c.Contract, ContractNeutronV1)
		}
		if c.Ready != nil {
			r := c.Ready
			set := 0
			for _, v := range []string{r.HTTP, r.TCP, r.Path} {
				if v != "" {
					set++
				}
			}
			if set != 1 {
				return nil, fmt.Errorf("%s: readiness needs exactly one of http, tcp or path", name)
			}
			if r.Path != "" && (!strings.HasPrefix(r.Path, "/") || (len(c.Ports) != 1 && !svc.AssignPort)) {
				return nil, fmt.Errorf("%s: readiness path must start with / and the service must have exactly one port", name)
			}
			if d, err := time.ParseDuration(r.Timeout); err != nil || d <= 0 || d > time.Hour {
				return nil, fmt.Errorf("%s: readiness timeout must be positive and at most 1h", name)
			}
			if r.HTTP != "" {
				u, err := url.Parse(r.HTTP)
				if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Fragment != "" || (u.Port() != "" && !validPort(u.Port())) {
					return nil, fmt.Errorf("%s: invalid HTTP readiness URL", name)
				}
			}
			if r.TCP != "" {
				if host, port, err := net.SplitHostPort(r.TCP); err != nil || host == "" || !validPort(port) {
					return nil, fmt.Errorf("%s: invalid TCP readiness address (want host:port, port 1-65535)", name)
				}
			}
		}
		deps := append([]string(nil), c.DependsOn...)
		sort.Strings(deps)
		for i, dep := range deps {
			target, ok := app.Services[dep]
			if _, isTask := app.Tasks[dep]; !ok && isTask {
				return nil, fmt.Errorf("%s: services cannot depend on task %s", name, dep)
			}
			if !ok {
				return nil, fmt.Errorf("%s: unknown dependency %s", name, dep)
			}
			if i > 0 && deps[i-1] == dep {
				return nil, fmt.Errorf("%s: duplicate dependency %s", name, dep)
			}
			if target.Ready == nil && target.Contract != ContractNeutronV1 {
				return nil, fmt.Errorf("%s: dependency %s must declare readiness", name, dep)
			}
		}
		svc.DependsOn = deps
		svc.EnvironmentKeys = keys
		services[name] = svc
	}
	// Injected keys are part of the plan; their values are resolved at start.
	for _, name := range names {
		svc := services[name]
		keys := map[string]bool{}
		for _, k := range svc.EnvironmentKeys {
			keys[k] = true
		}
		for k := range InjectedKeys(svc, services) {
			keys[k] = true
		}
		svc.EnvironmentKeys = svc.EnvironmentKeys[:0]
		for k := range keys {
			svc.EnvironmentKeys = append(svc.EnvironmentKeys, k)
		}
		sort.Strings(svc.EnvironmentKeys)
		services[name] = svc
	}
	serviceDeps := map[string][]string{}
	for name, s := range services {
		serviceDeps[name] = s.DependsOn
	}
	order, err := topological(names, serviceDeps)
	if err != nil {
		return nil, err
	}
	tasks, taskOrder, err := m.buildTasks()
	if err != nil {
		return nil, err
	}
	var roots []string
	if selected != "" {
		if _, ok := services[selected]; !ok {
			return nil, fmt.Errorf("unknown service %s", selected)
		}
		roots = []string{selected}
	} else {
		roots = names
	}
	included := closure(roots, serviceDeps)
	plan := &Plan{Version: 1, Name: app.Name, Root: m.Root, Services: []Service{}}
	for _, name := range order {
		if included[name] {
			plan.Services = append(plan.Services, services[name])
		}
	}
	for _, name := range taskOrder {
		plan.Tasks = append(plan.Tasks, tasks[name])
	}
	return plan, nil
}

const ContractNeutronV1 = "neutron/v1"

// SinglePort reports whether a service has exactly one port, declared or assigned.
func (s Service) SinglePort() bool { return len(s.Ports) == 1 || s.AssignPort }

// ServiceURLKey names the variable through which dependents find a service.
func ServiceURLKey(name string) string {
	return "NEUTRON_SERVICE_" + strings.ToUpper(strings.ReplaceAll(name, "-", "_")) + "_URL"
}

// InjectedKeys lists the variables the coordinator adds for a service, with
// the name of the service whose port supplies each value. Explicit env wins.
func InjectedKeys(s Service, services map[string]Service) map[string]string {
	keys := map[string]string{}
	if s.Contract == ContractNeutronV1 {
		keys["NEUTRON_HOST"] = s.Name
		keys["NEUTRON_PORT"] = s.Name
	}
	for _, dep := range s.DependsOn {
		if services[dep].SinglePort() {
			keys[ServiceURLKey(dep)] = dep
		}
	}
	for k := range s.Env {
		delete(keys, k)
	}
	return keys
}

// TaskPlan validates the whole manifest and returns the selected tasks plus
// their transitive dependencies in deterministic dependency order.
func (m *Manifest) TaskPlan(selected []string) ([]Task, error) {
	plan, err := m.Build("")
	if err != nil {
		return nil, err
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("name at least one task")
	}
	byName := map[string]Task{}
	deps := map[string][]string{}
	for _, t := range plan.Tasks {
		byName[t.Name] = t
		deps[t.Name] = t.DependsOn
	}
	for _, name := range selected {
		if _, ok := byName[name]; !ok {
			if _, isService := m.Application.Services[name]; isService {
				return nil, fmt.Errorf("%s is a service; run services with neutron dev", name)
			}
			return nil, fmt.Errorf("unknown task %s", name)
		}
	}
	included := closure(selected, deps)
	result := []Task{}
	for _, t := range plan.Tasks {
		if included[t.Name] {
			result = append(result, t)
		}
	}
	return result, nil
}

func (m *Manifest) buildTasks() (map[string]Task, []string, error) {
	app := m.Application
	names := make([]string, 0, len(app.Tasks))
	for name := range app.Tasks {
		names = append(names, name)
	}
	sort.Strings(names)
	tasks := map[string]Task{}
	deps := map[string][]string{}
	for _, name := range names {
		c := app.Tasks[name]
		if !validName(name) {
			return nil, nil, fmt.Errorf("invalid task name %q", name)
		}
		if _, clash := app.Services[name]; clash {
			return nil, nil, fmt.Errorf("%s: name is used by both a service and a task", name)
		}
		dir, keys, err := m.validateCommon(name, c.Path, c.Command, c.Env)
		if err != nil {
			return nil, nil, err
		}
		if d, err := time.ParseDuration(c.Timeout); err != nil || d <= 0 || d > 24*time.Hour {
			return nil, nil, fmt.Errorf("%s: task timeout is required, positive and at most 24h", name)
		}
		for _, output := range c.Outputs {
			clean := filepath.Clean(output)
			if output == "" || filepath.IsAbs(output) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
				return nil, nil, fmt.Errorf("%s: output %q must be a relative path inside the task directory", name, output)
			}
		}
		taskDeps := append([]string(nil), c.DependsOn...)
		sort.Strings(taskDeps)
		for i, dep := range taskDeps {
			if _, isService := app.Services[dep]; isService {
				return nil, nil, fmt.Errorf("%s: tasks cannot depend on service %s", name, dep)
			}
			if _, ok := app.Tasks[dep]; !ok {
				return nil, nil, fmt.Errorf("%s: unknown dependency %s", name, dep)
			}
			if i > 0 && taskDeps[i-1] == dep {
				return nil, nil, fmt.Errorf("%s: duplicate dependency %s", name, dep)
			}
		}
		deps[name] = taskDeps
		tasks[name] = Task{Name: name, Dir: dir, Command: append([]string(nil), c.Command...), DependsOn: taskDeps, Env: c.Env, EnvironmentKeys: keys, Timeout: c.Timeout, Outputs: c.Outputs}
	}
	order, err := topological(names, deps)
	if err != nil {
		return nil, nil, err
	}
	return tasks, order, nil
}

// validateCommon checks the fields services and tasks share.
func (m *Manifest) validateCommon(name, path string, command []string, env map[string]string) (string, []string, error) {
	if path == "" || filepath.IsAbs(path) {
		return "", nil, fmt.Errorf("%s: path must be relative to the manifest", name)
	}
	dir, err := filepath.EvalSymlinks(filepath.Join(m.Root, path))
	if err != nil {
		return "", nil, fmt.Errorf("%s: %w", name, err)
	}
	rel, err := filepath.Rel(m.Root, dir)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", nil, fmt.Errorf("%s: path escapes application root", name)
	}
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return "", nil, fmt.Errorf("%s: path must be a directory", name)
	}
	if len(command) == 0 || strings.TrimSpace(command[0]) == "" {
		return "", nil, fmt.Errorf("%s: command needs an executable", name)
	}
	for _, arg := range command {
		if strings.ContainsRune(arg, 0) {
			return "", nil, fmt.Errorf("%s: command contains NUL", name)
		}
	}
	keys := []string{}
	for key, value := range env {
		if key == "" || strings.ContainsAny(key, "=\x00") || strings.ContainsRune(value, 0) {
			return "", nil, fmt.Errorf("%s: invalid environment entry", name)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return dir, keys, nil
}

// topological orders names so dependencies come first, visiting in the given
// (sorted) order for determinism, and reports a cycle with its path.
func topological(names []string, deps map[string][]string) ([]string, error) {
	state := map[string]int{}
	order := []string{}
	stack := []string{}
	var visit func(string) error
	visit = func(name string) error {
		if state[name] == 2 {
			return nil
		}
		if state[name] == 1 {
			return fmt.Errorf("dependency cycle: %s", strings.Join(append(stack, name), " -> "))
		}
		state[name] = 1
		stack = append(stack, name)
		for _, dep := range deps[name] {
			if err := visit(dep); err != nil {
				return err
			}
		}
		stack = stack[:len(stack)-1]
		state[name] = 2
		order = append(order, name)
		return nil
	}
	for _, name := range names {
		if err := visit(name); err != nil {
			return nil, err
		}
	}
	return order, nil
}

func closure(roots []string, deps map[string][]string) map[string]bool {
	included := map[string]bool{}
	var include func(string)
	include = func(name string) {
		if included[name] {
			return
		}
		included[name] = true
		for _, dep := range deps[name] {
			include(dep)
		}
	}
	for _, root := range roots {
		include(root)
	}
	return included
}
