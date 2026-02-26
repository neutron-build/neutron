use std::fmt;

#[derive(Debug)]
pub enum CacheError {
    Serialization(String),
    Backend(String),
}

impl fmt::Display for CacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CacheError::Serialization(s) => write!(f, "cache serialize error: {s}"),
            CacheError::Backend(s)       => write!(f, "cache backend error: {s}"),
        }
    }
}

impl std::error::Error for CacheError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialization_error_display() {
        let e = CacheError::Serialization("bad json".to_string());
        assert_eq!(e.to_string(), "cache serialize error: bad json");
    }

    #[test]
    fn backend_error_display() {
        let e = CacheError::Backend("connection refused".to_string());
        assert_eq!(e.to_string(), "cache backend error: connection refused");
    }

    #[test]
    fn serialization_error_debug() {
        let e = CacheError::Serialization("oops".to_string());
        let dbg = format!("{:?}", e);
        assert!(dbg.contains("Serialization"));
    }

    #[test]
    fn backend_error_debug() {
        let e = CacheError::Backend("timeout".to_string());
        let dbg = format!("{:?}", e);
        assert!(dbg.contains("Backend"));
    }

    #[test]
    fn is_std_error() {
        let e: Box<dyn std::error::Error> =
            Box::new(CacheError::Backend("x".to_string()));
        assert!(e.to_string().contains("cache backend error"));
    }

    #[test]
    fn serialization_empty_string() {
        let e = CacheError::Serialization(String::new());
        assert_eq!(e.to_string(), "cache serialize error: ");
    }

    #[test]
    fn backend_empty_string() {
        let e = CacheError::Backend(String::new());
        assert_eq!(e.to_string(), "cache backend error: ");
    }
}
