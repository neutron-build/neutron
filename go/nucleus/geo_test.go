package nucleus

import (
	"context"
	"strings"
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

func TestGeoRequiresNucleus(t *testing.T) {
	q := &mockCDCQuerier{}
	client := plainPGClient()
	g := &GeoModel{pool: q, client: client}

	tests := []struct {
		name string
		fn   func() error
	}{
		{"Distance", func() error {
			_, err := g.Distance(context.Background(), GeoPoint{0, 0}, GeoPoint{1, 1})
			return err
		}},
		{"DistanceEuclidean", func() error {
			_, err := g.DistanceEuclidean(context.Background(), GeoPoint{0, 0}, GeoPoint{1, 1})
			return err
		}},
		{"Within", func() error {
			_, err := g.Within(context.Background(), GeoPoint{0, 0}, GeoPoint{1, 1}, 1000)
			return err
		}},
		{"Area", func() error {
			_, err := g.Area(context.Background(), []GeoPoint{{0, 0}, {1, 0}, {1, 1}})
			return err
		}},
		{"MakePoint", func() error { _, err := g.MakePoint(context.Background(), 0, 0); return err }},
		{"PointX", func() error { _, err := g.PointX(context.Background(), nil); return err }},
		{"PointY", func() error { _, err := g.PointY(context.Background(), nil); return err }},
		{"NearestTo", func() error {
			_, err := g.NearestTo(context.Background(), "layer", GeoPoint{0, 0}, 1000, 10)
			return err
		}},
		{"WithinBBox", func() error {
			_, err := g.WithinBBox(context.Background(), "layer", 0, 0, 1, 1)
			return err
		}},
		{"WithinPolygon", func() error {
			_, err := g.WithinPolygon(context.Background(), "layer", [][2]float64{{0, 0}, {1, 0}, {1, 1}})
			return err
		}},
		{"Insert", func() error {
			return g.Insert(context.Background(), "layer", 0, 0, nil)
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn()
			if err == nil {
				t.Fatal("expected error for non-Nucleus database")
			}
		})
	}
}

func TestGeoInvalidIdentifier(t *testing.T) {
	q := &mockCDCQuerier{}
	g := &GeoModel{pool: q, client: nucleusClient()}

	_, err := g.NearestTo(context.Background(), "bad-name", GeoPoint{0, 0}, 1000, 10)
	if err == nil {
		t.Fatal("expected error for invalid identifier")
	}
	if !strings.Contains(err.Error(), "invalid layer name") {
		t.Errorf("error = %q", err.Error())
	}

	_, err = g.WithinBBox(context.Background(), "123bad", 0, 0, 1, 1)
	if err == nil {
		t.Fatal("expected error for invalid identifier")
	}

	_, err = g.WithinPolygon(context.Background(), "drop;table", [][2]float64{{0, 0}, {1, 0}, {1, 1}})
	if err == nil {
		t.Fatal("expected error for invalid identifier")
	}

	err = g.Insert(context.Background(), "bad name", 0, 0, nil)
	if err == nil {
		t.Fatal("expected error for invalid identifier")
	}
}

func TestGeoAreaTooFewPoints(t *testing.T) {
	q := &mockCDCQuerier{}
	g := &GeoModel{pool: q, client: nucleusClient()}

	_, err := g.Area(context.Background(), []GeoPoint{{0, 0}, {1, 1}})
	if err == nil {
		t.Fatal("expected error for fewer than 3 points")
	}
	if !strings.Contains(err.Error(), "at least 3 points") {
		t.Errorf("error = %q", err.Error())
	}
}

func TestJoinSingle(t *testing.T) {
	got := join([]string{"only"}, ", ")
	if got != "only" {
		t.Errorf("join single = %q", got)
	}
}
