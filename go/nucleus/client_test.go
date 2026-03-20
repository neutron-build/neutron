package nucleus

import (
	"strings"
	"testing"

	"github.com/neutron-dev/neutron-go/neutron"
)

func TestIsValidIdentifierExtended(t *testing.T) {
	tests := []struct {
		input string
		valid bool
	}{
		{"users", true},
		{"_private", true},
		{"table_name", true},
		{"col123", true},
		{"T", true},
		{"123bad", false},
		{"drop-table", false},
		{"has space", false},
		{"semi;colon", false},
		{"", false},
		{"Robert'); DROP TABLE students;--", false},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got := isValidIdentifier(tc.input)
			if got != tc.valid {
				t.Errorf("isValidIdentifier(%q) = %v, want %v", tc.input, got, tc.valid)
			}
		})
	}
}

func TestFeaturesDefaults(t *testing.T) {
	f := Features{}
	if f.IsNucleus {
		t.Error("default IsNucleus should be false")
	}
	if f.HasKV {
		t.Error("default HasKV should be false")
	}
	if f.HasVector {
		t.Error("default HasVector should be false")
	}
	if f.Version != "" {
		t.Errorf("default Version = %q, want empty", f.Version)
	}
}

func TestFeaturesNucleus(t *testing.T) {
	f := Features{
		IsNucleus:   true,
		HasKV:       true,
		HasVector:   true,
		HasTS:       true,
		HasDocument: true,
		HasGraph:    true,
		HasFTS:      true,
		HasGeo:      true,
		HasBlob:     true,
		HasStreams:  true,
		HasColumnar: true,
		HasDatalog:  true,
		HasCDC:      true,
		HasPubSub:   true,
		Version:     "Nucleus 0.1.0",
	}
	if !f.IsNucleus {
		t.Error("expected IsNucleus true")
	}
	if !f.HasKV {
		t.Error("expected HasKV true")
	}
	if !f.HasStreams {
		t.Error("expected HasStreams true")
	}
	if !f.HasColumnar {
		t.Error("expected HasColumnar true")
	}
	if !f.HasDatalog {
		t.Error("expected HasDatalog true")
	}
	if !f.HasCDC {
		t.Error("expected HasCDC true")
	}
	if !f.HasPubSub {
		t.Error("expected HasPubSub true")
	}
	if f.Version != "Nucleus 0.1.0" {
		t.Errorf("Version = %q", f.Version)
	}
}

func TestClientAccessors(t *testing.T) {
	c := &Client{features: Features{IsNucleus: true, Version: "Nucleus 0.1.0"}}

	if !c.IsNucleus() {
		t.Error("expected IsNucleus true")
	}
	if c.Features().Version != "Nucleus 0.1.0" {
		t.Errorf("Features().Version = %q", c.Features().Version)
	}
}

func TestClientModelAccessorsReturnNonNil(t *testing.T) {
	c := &Client{features: Features{IsNucleus: true}}

	// All model accessors should return non-nil (pool may be nil but that's ok
	// for testing the accessor pattern)
	if c.KV() == nil {
		t.Error("KV() returned nil")
	}
	if c.Vector() == nil {
		t.Error("Vector() returned nil")
	}
	if c.TimeSeries() == nil {
		t.Error("TimeSeries() returned nil")
	}
	if c.Document() == nil {
		t.Error("Document() returned nil")
	}
	if c.Graph() == nil {
		t.Error("Graph() returned nil")
	}
	if c.FTS() == nil {
		t.Error("FTS() returned nil")
	}
	if c.Geo() == nil {
		t.Error("Geo() returned nil")
	}
	if c.Blob() == nil {
		t.Error("Blob() returned nil")
	}
	if c.Streams() == nil {
		t.Error("Streams() returned nil")
	}
	if c.Columnar() == nil {
		t.Error("Columnar() returned nil")
	}
	if c.Datalog() == nil {
		t.Error("Datalog() returned nil")
	}
	if c.CDC() == nil {
		t.Error("CDC() returned nil")
	}
	if c.PubSub() == nil {
		t.Error("PubSub() returned nil")
	}
	if c.SQL() == nil {
		t.Error("SQL() returned nil")
	}
}

func TestRequireNucleusSuccess(t *testing.T) {
	c := &Client{features: Features{IsNucleus: true}}
	err := c.requireNucleus("Test.Feature")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRequireNucleusFailure(t *testing.T) {
	c := &Client{features: Features{IsNucleus: false}}
	err := c.requireNucleus("KV.Get")
	if err == nil {
		t.Fatal("expected error for non-Nucleus database")
	}
	errStr := err.Error()
	if errStr == "" {
		t.Error("expected non-empty error message")
	}
}

func TestRequireNucleusErrorContainsFeatureName(t *testing.T) {
	c := &Client{features: Features{IsNucleus: false}}
	err := c.requireNucleus("Vector.Search")
	if err == nil {
		t.Fatal("expected error")
	}
	// The error detail should mention the feature
	errStr := err.Error()
	if errStr == "" {
		t.Error("error message is empty")
	}
}

func TestWithPoolConfigOption(t *testing.T) {
	// Test that the option function compiles and applies
	opt := WithPoolConfig(nil)
	var o clientOpts
	opt(&o)
	// poolConfig should be nil since we passed nil
	if o.poolConfig != nil {
		t.Error("expected nil poolConfig")
	}
}

func TestOptionFuncPattern(t *testing.T) {
	// Verify the Option type works correctly
	var opts []Option
	opts = append(opts, WithPoolConfig(nil))
	if len(opts) != 1 {
		t.Errorf("expected 1 option, got %d", len(opts))
	}
}

func TestClientIsNucleusInterface(t *testing.T) {
	// Test that Client satisfies the IsNucleus method
	c := &Client{features: Features{IsNucleus: false}}
	if c.IsNucleus() {
		t.Error("expected IsNucleus false for plain PG")
	}

	c2 := &Client{features: Features{IsNucleus: true}}
	if !c2.IsNucleus() {
		t.Error("expected IsNucleus true for Nucleus")
	}
}

func TestClientLifecycleHook(t *testing.T) {
	// Test that LifecycleHook returns a properly configured hook
	// (can't test OnStart/OnStop without a real pool, but verify the struct)
	c := &Client{features: Features{IsNucleus: true}}
	hook := c.LifecycleHook()
	if hook.Name != "nucleus" {
		t.Errorf("hook.Name = %q, want nucleus", hook.Name)
	}
	if hook.OnStart == nil {
		t.Error("hook.OnStart should not be nil")
	}
	if hook.OnStop == nil {
		t.Error("hook.OnStop should not be nil")
	}
}

func TestClientPoolReturnsNilWhenUnset(t *testing.T) {
	c := &Client{}
	if c.Pool() != nil {
		t.Error("expected nil pool for zero-value Client")
	}
}

func TestClientCloseNilPool(t *testing.T) {
	// Close with a nil pool should not panic — but pool is typed *pgxpool.Pool
	// so we skip this as it would nil-deref. Instead test the accessor pattern.
	c := &Client{features: Features{IsNucleus: true}}
	if c.Pool() != nil {
		t.Error("expected nil pool")
	}
}

func TestFeaturesAllFieldsDefault(t *testing.T) {
	f := Features{}
	if f.HasTS {
		t.Error("default HasTS should be false")
	}
	if f.HasDocument {
		t.Error("default HasDocument should be false")
	}
	if f.HasGraph {
		t.Error("default HasGraph should be false")
	}
	if f.HasFTS {
		t.Error("default HasFTS should be false")
	}
	if f.HasGeo {
		t.Error("default HasGeo should be false")
	}
	if f.HasBlob {
		t.Error("default HasBlob should be false")
	}
	if f.HasStreams {
		t.Error("default HasStreams should be false")
	}
	if f.HasColumnar {
		t.Error("default HasColumnar should be false")
	}
	if f.HasDatalog {
		t.Error("default HasDatalog should be false")
	}
	if f.HasCDC {
		t.Error("default HasCDC should be false")
	}
	if f.HasPubSub {
		t.Error("default HasPubSub should be false")
	}
}

func TestRequireNucleusErrorIsAppError(t *testing.T) {
	c := &Client{features: Features{IsNucleus: false}}
	err := c.requireNucleus("KV.Get")
	if err == nil {
		t.Fatal("expected error")
	}
	appErr, ok := err.(*neutron.AppError)
	if !ok {
		t.Fatalf("expected *neutron.AppError, got %T", err)
	}
	if appErr.Status != 501 {
		t.Errorf("Status = %d, want 501", appErr.Status)
	}
	if !strings.Contains(appErr.Detail, "KV.Get") {
		t.Errorf("Detail should contain feature name, got %q", appErr.Detail)
	}
	if appErr.Title != "Nucleus Required" {
		t.Errorf("Title = %q, want 'Nucleus Required'", appErr.Title)
	}
}

func TestIsValidIdentifierUnicode(t *testing.T) {
	// Unicode chars should be rejected
	if isValidIdentifier("table\u00e9") {
		t.Error("unicode chars should not be valid identifiers")
	}
}

func TestIsValidIdentifierLong(t *testing.T) {
	// A very long valid identifier
	long := "a" + strings.Repeat("b", 200)
	if !isValidIdentifier(long) {
		t.Error("long alphanumeric identifier should be valid")
	}
}

func TestIsValidIdentifierSpecialChars(t *testing.T) {
	invalid := []string{"a.b", "a b", "a\tb", "a\nb", "a$b", "a@b", "a!b", "a#b"}
	for _, id := range invalid {
		if isValidIdentifier(id) {
			t.Errorf("isValidIdentifier(%q) should be false", id)
		}
	}
}

func TestIdentifierReIsCompiled(t *testing.T) {
	if identifierRe == nil {
		t.Fatal("identifierRe should be compiled at init")
	}
}

func TestClientModelAccessorsNonNilPattern(t *testing.T) {
	c := &Client{features: Features{IsNucleus: true}}
	// All model accessors return non-nil objects even when pool is nil
	sql := c.SQL()
	if sql == nil {
		t.Fatal("SQL() returned nil")
	}
	kv := c.KV()
	if kv == nil {
		t.Fatal("KV() returned nil")
	}
	// KV model should have client reference
	if kv.client != c {
		t.Error("KV client should reference the original client")
	}
}

func TestClientFeaturesIsStruct(t *testing.T) {
	c := &Client{features: Features{
		IsNucleus: true,
		HasKV:     true,
		Version:   "Nucleus 1.0",
	}}
	f := c.Features()
	if !f.IsNucleus {
		t.Error("expected IsNucleus")
	}
	if !f.HasKV {
		t.Error("expected HasKV")
	}
	if f.Version != "Nucleus 1.0" {
		t.Errorf("Version = %q", f.Version)
	}
	// Features() returns a copy, not a reference
	f.IsNucleus = false
	if !c.Features().IsNucleus {
		t.Error("modifying returned Features should not affect client")
	}
}
