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
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
)

type Application struct {
	Version    int                  `toml:"version"`
	Name       string               `toml:"name"`
	Components map[string]Component `toml:"components"`
}
type Component struct {
	Path      string            `toml:"path"`
	Command   []string          `toml:"command"`
	DependsOn []string          `toml:"depends_on"`
	Ports     []int             `toml:"ports"`
	Env       map[string]string `toml:"env"`
	Ready     *Readiness        `toml:"ready"`
}
type Readiness struct {
	HTTP    string `toml:"http" json:"http,omitempty"`
	TCP     string `toml:"tcp" json:"tcp,omitempty"`
	Timeout string `toml:"timeout" json:"timeout"`
}
type Manifest struct {
	File, Root  string
	Application Application
}
type Plan struct {
	Version    int       `json:"version"`
	Name       string    `json:"name"`
	Root       string    `json:"root"`
	Components []Service `json:"components"`
}
type Service struct {
	Name            string            `json:"name"`
	Dir             string            `json:"directory"`
	Command         []string          `json:"command"`
	DependsOn       []string          `json:"depends_on,omitempty"`
	Ports           []int             `json:"ports,omitempty"`
	Env             map[string]string `json:"-"`
	EnvironmentKeys []string          `json:"environment_keys,omitempty"`
	Ready           *Readiness        `json:"ready,omitempty"`
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
	if !validName(app.Name) || len(app.Components) == 0 {
		return nil, fmt.Errorf("application needs a name and at least one component")
	}
	names := make([]string, 0, len(app.Components))
	for name := range app.Components {
		names = append(names, name)
	}
	sort.Strings(names)
	services := map[string]Service{}
	ports := map[int]string{}
	for _, name := range names {
		c := app.Components[name]
		if !validName(name) {
			return nil, fmt.Errorf("invalid component name %q", name)
		}
		if c.Path == "" || filepath.IsAbs(c.Path) {
			return nil, fmt.Errorf("%s: path must be relative to the manifest", name)
		}
		dir, err := filepath.EvalSymlinks(filepath.Join(m.Root, c.Path))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		rel, err := filepath.Rel(m.Root, dir)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return nil, fmt.Errorf("%s: path escapes application root", name)
		}
		info, err := os.Stat(dir)
		if err != nil || !info.IsDir() {
			return nil, fmt.Errorf("%s: path must be a directory", name)
		}
		if len(c.Command) == 0 || strings.TrimSpace(c.Command[0]) == "" {
			return nil, fmt.Errorf("%s: command needs an executable", name)
		}
		for _, arg := range c.Command {
			if strings.ContainsRune(arg, 0) {
				return nil, fmt.Errorf("%s: command contains NUL", name)
			}
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
		keys := []string{}
		for key, value := range c.Env {
			if key == "" || strings.ContainsAny(key, "=\x00") || strings.ContainsRune(value, 0) {
				return nil, fmt.Errorf("%s: invalid environment entry", name)
			}
			keys = append(keys, key)
		}
		sort.Strings(keys)
		if c.Ready != nil {
			r := c.Ready
			if (r.HTTP == "") == (r.TCP == "") {
				return nil, fmt.Errorf("%s: readiness needs exactly one of http or tcp", name)
			}
			if d, err := time.ParseDuration(r.Timeout); err != nil || d <= 0 || d > time.Hour {
				return nil, fmt.Errorf("%s: readiness timeout must be positive and at most 1h", name)
			}
			if r.HTTP != "" {
				u, err := url.Parse(r.HTTP)
				if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Fragment != "" {
					return nil, fmt.Errorf("%s: invalid HTTP readiness URL", name)
				}
			} else if host, port, err := net.SplitHostPort(r.TCP); err != nil || host == "" || port == "" {
				return nil, fmt.Errorf("%s: invalid TCP readiness address", name)
			}
		}
		deps := append([]string(nil), c.DependsOn...)
		sort.Strings(deps)
		for i, dep := range deps {
			target, ok := app.Components[dep]
			if !ok {
				return nil, fmt.Errorf("%s: unknown dependency %s", name, dep)
			}
			if i > 0 && deps[i-1] == dep {
				return nil, fmt.Errorf("%s: duplicate dependency %s", name, dep)
			}
			if target.Ready == nil {
				return nil, fmt.Errorf("%s: dependency %s must declare readiness", name, dep)
			}
		}
		services[name] = Service{Name: name, Dir: dir, Command: append([]string(nil), c.Command...), DependsOn: deps, Ports: c.Ports, Env: c.Env, EnvironmentKeys: keys, Ready: c.Ready}
	}
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
		for _, dep := range services[name].DependsOn {
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
	included := map[string]bool{}
	var include func(string)
	include = func(name string) {
		if included[name] {
			return
		}
		included[name] = true
		for _, dep := range services[name].DependsOn {
			include(dep)
		}
	}
	if selected != "" {
		if _, ok := services[selected]; !ok {
			return nil, fmt.Errorf("unknown component %s", selected)
		}
		include(selected)
	} else {
		for _, name := range names {
			include(name)
		}
	}
	plan := &Plan{Version: 1, Name: app.Name, Root: m.Root, Components: []Service{}}
	for _, name := range order {
		if included[name] {
			plan.Components = append(plan.Components, services[name])
		}
	}
	return plan, nil
}
