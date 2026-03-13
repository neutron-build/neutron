package nucleus

import (
	"testing"
)

func TestGeoPoint(t *testing.T) {
	p := GeoPoint{Lat: 37.7749, Lon: -122.4194}
	if p.Lat != 37.7749 {
		t.Errorf("Lat = %f", p.Lat)
	}
	if p.Lon != -122.4194 {
		t.Errorf("Lon = %f", p.Lon)
	}
}

func TestJoinHelper(t *testing.T) {
	got := join([]string{"a", "b", "c"}, ", ")
	want := "a, b, c"
	if got != want {
		t.Errorf("join = %q, want %q", got, want)
	}
}

func TestJoinEmpty(t *testing.T) {
	got := join(nil, ", ")
	if got != "" {
		t.Errorf("join empty = %q", got)
	}
}

func TestGeoFeatureStruct(t *testing.T) {
	f := GeoFeature{
		ID:         "poi-1",
		Lat:        37.7749,
		Lon:        -122.4194,
		Properties: map[string]any{"name": "San Francisco"},
	}
	if f.ID != "poi-1" {
		t.Errorf("ID = %q", f.ID)
	}
	if f.Lat != 37.7749 {
		t.Errorf("Lat = %f", f.Lat)
	}
	if f.Lon != -122.4194 {
		t.Errorf("Lon = %f", f.Lon)
	}
	if f.Properties["name"] != "San Francisco" {
		t.Errorf("Properties = %v", f.Properties)
	}
}
