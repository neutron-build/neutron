//! Geospatial model — GEO_DISTANCE, GEO_DISTANCE_EUCLIDEAN, GEO_WITHIN,
//! GEO_AREA, ST_MAKEPOINT, ST_X, ST_Y.

use crate::error::NucleusError;
use crate::pool::NucleusPool;

/// A geographic coordinate.
#[derive(Debug, Clone, Copy)]
pub struct GeoPoint {
    pub lat: f64,
    pub lon: f64,
}

impl GeoPoint {
    pub fn new(lat: f64, lon: f64) -> Self {
        Self { lat, lon }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn geo_point_new() {
        let p = GeoPoint::new(40.7128, -74.0060);
        assert!((p.lat - 40.7128).abs() < f64::EPSILON);
        assert!((p.lon - (-74.0060)).abs() < f64::EPSILON);
    }

    #[test]
    fn geo_point_zero() {
        let p = GeoPoint::new(0.0, 0.0);
        assert_eq!(p.lat, 0.0);
        assert_eq!(p.lon, 0.0);
    }

    #[test]
    fn geo_point_negative_coords() {
        let p = GeoPoint::new(-33.8688, 151.2093); // Sydney
        assert!(p.lat < 0.0);
        assert!(p.lon > 0.0);
    }

    #[test]
    fn geo_point_clone() {
        let p = GeoPoint::new(51.5074, -0.1278); // London
        let p2 = p;
        assert!((p.lat - p2.lat).abs() < f64::EPSILON);
        assert!((p.lon - p2.lon).abs() < f64::EPSILON);
    }

    #[test]
    fn geo_point_debug() {
        let p = GeoPoint::new(1.0, 2.0);
        let dbg = format!("{:?}", p);
        assert!(dbg.contains("GeoPoint"));
        assert!(dbg.contains("1.0"));
        assert!(dbg.contains("2.0"));
    }

    #[test]
    fn geo_point_extreme_values() {
        let north_pole = GeoPoint::new(90.0, 0.0);
        assert_eq!(north_pole.lat, 90.0);

        let south_pole = GeoPoint::new(-90.0, 0.0);
        assert_eq!(south_pole.lat, -90.0);

        let date_line = GeoPoint::new(0.0, 180.0);
        assert_eq!(date_line.lon, 180.0);
    }
}

/// Handle for geospatial operations.
pub struct GeoModel {
    pool: NucleusPool,
}

impl GeoModel {
    pub(crate) fn new(pool: NucleusPool) -> Self {
        Self { pool }
    }

    /// Calculate the haversine distance in meters between two points.
    pub async fn distance(&self, a: GeoPoint, b: GeoPoint) -> Result<f64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one(
                "SELECT GEO_DISTANCE($1, $2, $3, $4)",
                &[&a.lat, &a.lon, &b.lat, &b.lon],
            )
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, f64>(0))
    }

    /// Calculate the Euclidean distance between two points.
    pub async fn distance_euclidean(
        &self,
        a: GeoPoint,
        b: GeoPoint,
    ) -> Result<f64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one(
                "SELECT GEO_DISTANCE_EUCLIDEAN($1, $2, $3, $4)",
                &[&a.lat, &a.lon, &b.lat, &b.lon],
            )
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, f64>(0))
    }

    /// Check if point `b` is within `radius_meters` of point `a`.
    pub async fn within(
        &self,
        a: GeoPoint,
        b: GeoPoint,
        radius_meters: f64,
    ) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one(
                "SELECT GEO_WITHIN($1, $2, $3, $4, $5)",
                &[&a.lat, &a.lon, &b.lat, &b.lon, &radius_meters],
            )
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
    }

    /// Create a PostGIS-compatible point from longitude and latitude.
    /// Returns the point as a string representation.
    pub async fn make_point(&self, lon: f64, lat: f64) -> Result<String, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT ST_MAKEPOINT($1, $2)::TEXT", &[&lon, &lat])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, String>(0))
    }

    /// Extract the X coordinate (longitude) from a point expression.
    pub async fn point_x(&self, point_expr: &str) -> Result<f64, NucleusError> {
        let conn = self.pool.get().await?;
        let sql = format!("SELECT ST_X({})", point_expr);
        let row = conn
            .client()
            .query_one(&sql, &[])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, f64>(0))
    }

    /// Extract the Y coordinate (latitude) from a point expression.
    pub async fn point_y(&self, point_expr: &str) -> Result<f64, NucleusError> {
        let conn = self.pool.get().await?;
        let sql = format!("SELECT ST_Y({})", point_expr);
        let row = conn
            .client()
            .query_one(&sql, &[])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, f64>(0))
    }
}
