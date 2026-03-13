package nucleus

import (
	"context"
	"encoding/json"
	"fmt"
)

// GeoModel provides geospatial operations over Nucleus SQL functions.
type GeoModel struct {
	pool   querier
	client *Client
}

// GeoPoint represents a geographic coordinate.
type GeoPoint struct {
	Lat float64
	Lon float64
}

// GeoFeature represents a geospatial feature with ID, location, and properties.
type GeoFeature struct {
	ID         string         `json:"id"`
	Lat        float64        `json:"lat"`
	Lon        float64        `json:"lon"`
	Properties map[string]any `json:"properties,omitempty"`
}

// Distance calculates the haversine distance in meters between two points.
func (g *GeoModel) Distance(ctx context.Context, a, b GeoPoint) (float64, error) {
	if err := g.client.requireNucleus("Geo.Distance"); err != nil {
		return 0, err
	}
	var d float64
	err := g.pool.QueryRow(ctx, "SELECT GEO_DISTANCE($1, $2, $3, $4)",
		a.Lat, a.Lon, b.Lat, b.Lon).Scan(&d)
	return d, wrapErr("geo distance", err)
}

// DistanceEuclidean calculates the Euclidean distance between two points.
func (g *GeoModel) DistanceEuclidean(ctx context.Context, a, b GeoPoint) (float64, error) {
	if err := g.client.requireNucleus("Geo.DistanceEuclidean"); err != nil {
		return 0, err
	}
	var d float64
	err := g.pool.QueryRow(ctx, "SELECT GEO_DISTANCE_EUCLIDEAN($1, $2, $3, $4)",
		a.Lat, a.Lon, b.Lat, b.Lon).Scan(&d)
	return d, wrapErr("geo distance_euclidean", err)
}

// Within checks if point b is within radius meters of point a.
func (g *GeoModel) Within(ctx context.Context, a, b GeoPoint, radiusMeters float64) (bool, error) {
	if err := g.client.requireNucleus("Geo.Within"); err != nil {
		return false, err
	}
	var ok bool
	err := g.pool.QueryRow(ctx, "SELECT GEO_WITHIN($1, $2, $3, $4, $5)",
		a.Lat, a.Lon, b.Lat, b.Lon, radiusMeters).Scan(&ok)
	return ok, wrapErr("geo within", err)
}

// Area calculates the area of a polygon defined by alternating lon/lat pairs.
func (g *GeoModel) Area(ctx context.Context, points []GeoPoint) (float64, error) {
	if err := g.client.requireNucleus("Geo.Area"); err != nil {
		return 0, err
	}
	if len(points) < 3 {
		return 0, fmt.Errorf("nucleus: geo area requires at least 3 points")
	}
	// Build argument list: lon1, lat1, lon2, lat2, ...
	args := make([]any, 0, len(points)*2)
	placeholders := make([]string, 0, len(points)*2)
	for i, p := range points {
		args = append(args, p.Lon, p.Lat)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i*2+1), fmt.Sprintf("$%d", i*2+2))
	}
	q := "SELECT GEO_AREA(" + join(placeholders, ", ") + ")"
	var area float64
	err := g.pool.QueryRow(ctx, q, args...).Scan(&area)
	return area, wrapErr("geo area", err)
}

// MakePoint creates a PostGIS point from longitude and latitude.
func (g *GeoModel) MakePoint(ctx context.Context, lon, lat float64) (any, error) {
	if err := g.client.requireNucleus("Geo.MakePoint"); err != nil {
		return nil, err
	}
	var point any
	err := g.pool.QueryRow(ctx, "SELECT ST_MAKEPOINT($1, $2)", lon, lat).Scan(&point)
	return point, wrapErr("geo makepoint", err)
}

// PointX extracts the X coordinate (longitude) from a point.
func (g *GeoModel) PointX(ctx context.Context, point any) (float64, error) {
	if err := g.client.requireNucleus("Geo.PointX"); err != nil {
		return 0, err
	}
	var x float64
	err := g.pool.QueryRow(ctx, "SELECT ST_X($1)", point).Scan(&x)
	return x, wrapErr("geo st_x", err)
}

// PointY extracts the Y coordinate (latitude) from a point.
func (g *GeoModel) PointY(ctx context.Context, point any) (float64, error) {
	if err := g.client.requireNucleus("Geo.PointY"); err != nil {
		return 0, err
	}
	var y float64
	err := g.pool.QueryRow(ctx, "SELECT ST_Y($1)", point).Scan(&y)
	return y, wrapErr("geo st_y", err)
}

// NearestTo finds features within a radius of a point using a custom table/layer.
// Assumes the table has columns: id TEXT, lat FLOAT8, lon FLOAT8, properties JSONB.
func (g *GeoModel) NearestTo(ctx context.Context, layer string, point GeoPoint, radiusMeters float64, limit int) ([]GeoFeature, error) {
	if err := g.client.requireNucleus("Geo.NearestTo"); err != nil {
		return nil, err
	}
	if !isValidIdentifier(layer) {
		return nil, fmt.Errorf("nucleus: geo nearest: invalid layer name %q", layer)
	}

	q := fmt.Sprintf(
		`SELECT id, lat, lon, properties, GEO_DISTANCE($1, $2, lat, lon) AS dist
		 FROM %s
		 WHERE GEO_WITHIN($1, $2, lat, lon, $3)
		 ORDER BY dist
		 LIMIT $4`, layer)

	rows, err := g.pool.Query(ctx, q, point.Lat, point.Lon, radiusMeters, limit)
	if err != nil {
		return nil, wrapErr("geo nearest", err)
	}
	defer rows.Close()

	var results []GeoFeature
	for rows.Next() {
		var id string
		var lat, lon, dist float64
		var propsRaw []byte
		if err := rows.Scan(&id, &lat, &lon, &propsRaw, &dist); err != nil {
			return nil, fmt.Errorf("nucleus: geo scan: %w", err)
		}
		props := make(map[string]any)
		if len(propsRaw) > 0 {
			_ = json.Unmarshal(propsRaw, &props)
		}
		props["distance"] = dist
		results = append(results, GeoFeature{
			ID:         id,
			Lat:        lat,
			Lon:        lon,
			Properties: props,
		})
	}
	return results, rows.Err()
}

// WithinBBox returns features within a bounding box defined by SW and NE corners.
func (g *GeoModel) WithinBBox(ctx context.Context, layer string, minLat, minLon, maxLat, maxLon float64) ([]GeoFeature, error) {
	if err := g.client.requireNucleus("Geo.WithinBBox"); err != nil {
		return nil, err
	}
	if !isValidIdentifier(layer) {
		return nil, fmt.Errorf("nucleus: geo within_bbox: invalid layer name %q", layer)
	}

	q := fmt.Sprintf(
		`SELECT id, lat, lon, properties FROM %s
		 WHERE lat >= $1 AND lat <= $3 AND lon >= $2 AND lon <= $4`,
		layer)

	rows, err := g.pool.Query(ctx, q, minLat, minLon, maxLat, maxLon)
	if err != nil {
		return nil, wrapErr("geo within_bbox", err)
	}
	defer rows.Close()

	var results []GeoFeature
	for rows.Next() {
		var id string
		var lat, lon float64
		var propsRaw []byte
		if err := rows.Scan(&id, &lat, &lon, &propsRaw); err != nil {
			return nil, fmt.Errorf("nucleus: geo scan: %w", err)
		}
		props := make(map[string]any)
		if len(propsRaw) > 0 {
			_ = json.Unmarshal(propsRaw, &props)
		}
		results = append(results, GeoFeature{
			ID:         id,
			Lat:        lat,
			Lon:        lon,
			Properties: props,
		})
	}
	return results, rows.Err()
}

// WithinPolygon returns features within a polygon defined by a list of [lat, lon] pairs.
func (g *GeoModel) WithinPolygon(ctx context.Context, layer string, polygon [][2]float64) ([]GeoFeature, error) {
	if err := g.client.requireNucleus("Geo.WithinPolygon"); err != nil {
		return nil, err
	}
	if !isValidIdentifier(layer) {
		return nil, fmt.Errorf("nucleus: geo within_polygon: invalid layer name %q", layer)
	}

	// Use a spatial query that checks point-in-polygon
	// The polygon coordinates are passed as a parameter
	q := fmt.Sprintf(
		`SELECT id, lat, lon, properties FROM %s
		 WHERE ST_CONTAINS(ST_GEOMFROMGEOJSON($1), ST_MAKEPOINT(lon, lat))`,
		layer)

	// Build GeoJSON polygon with [lon, lat] coordinate order
	coords := make([][]float64, len(polygon))
	for i, p := range polygon {
		coords[i] = []float64{p[1], p[0]} // GeoJSON is [lon, lat]
	}
	// Close the ring if not already closed
	if len(coords) > 0 && (coords[0][0] != coords[len(coords)-1][0] || coords[0][1] != coords[len(coords)-1][1]) {
		coords = append(coords, coords[0])
	}
	geoJSON := map[string]any{
		"type":        "Polygon",
		"coordinates": []any{coords},
	}
	geoJSONBytes, err := json.Marshal(geoJSON)
	if err != nil {
		return nil, fmt.Errorf("nucleus: geo marshal geojson: %w", err)
	}

	rows, err := g.pool.Query(ctx, q, string(geoJSONBytes))
	if err != nil {
		return nil, wrapErr("geo within_polygon", err)
	}
	defer rows.Close()

	var results []GeoFeature
	for rows.Next() {
		var id string
		var lat, lon float64
		var propsRaw []byte
		if err := rows.Scan(&id, &lat, &lon, &propsRaw); err != nil {
			return nil, fmt.Errorf("nucleus: geo scan: %w", err)
		}
		props := make(map[string]any)
		if len(propsRaw) > 0 {
			_ = json.Unmarshal(propsRaw, &props)
		}
		results = append(results, GeoFeature{
			ID:         id,
			Lat:        lat,
			Lon:        lon,
			Properties: props,
		})
	}
	return results, rows.Err()
}

// Insert inserts a geographic feature into a layer table.
func (g *GeoModel) Insert(ctx context.Context, layer string, lat, lon float64, props map[string]any) error {
	if err := g.client.requireNucleus("Geo.Insert"); err != nil {
		return err
	}
	if !isValidIdentifier(layer) {
		return fmt.Errorf("nucleus: geo insert: invalid layer name %q", layer)
	}
	propsJSON, err := json.Marshal(props)
	if err != nil {
		return fmt.Errorf("nucleus: geo marshal props: %w", err)
	}
	q := fmt.Sprintf("INSERT INTO %s (lat, lon, properties) VALUES ($1, $2, $3)", layer)
	_, err = g.pool.Exec(ctx, q, lat, lon, string(propsJSON))
	return wrapErr("geo insert", err)
}

func join(s []string, sep string) string {
	if len(s) == 0 {
		return ""
	}
	result := s[0]
	for _, v := range s[1:] {
		result += sep + v
	}
	return result
}
