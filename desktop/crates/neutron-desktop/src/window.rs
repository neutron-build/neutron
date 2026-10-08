/// Configuration for the main application window.
#[derive(Clone, Debug, PartialEq)]
pub struct WindowConfig {
    pub title: String,
    pub width: f64,
    pub height: f64,
    pub resizable: bool,
    pub decorations: bool,
    pub transparent: bool,
    pub fullscreen: bool,
    pub min_width: Option<f64>,
    pub min_height: Option<f64>,
}

impl WindowConfig {
    pub fn validate(&self) -> Result<(), std::io::Error> {
        let positive = |v: f64| v.is_finite() && v > 0.0;
        if !positive(self.width)
            || !positive(self.height)
            || self.min_width.is_some() != self.min_height.is_some()
            || self
                .min_width
                .is_some_and(|v| !positive(v) || v > self.width)
            || self
                .min_height
                .is_some_and(|v| !positive(v) || v > self.height)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid window dimensions; minimum dimensions must be paired and within initial size",
            ));
        }
        // Tauri requires a special macOS private-API feature for transparency.
        #[cfg(target_os = "macos")]
        if self.transparent {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Transparent windows require the macOS private API and are unsupported by this build",
            ));
        }
        Ok(())
    }

    pub fn title(&mut self, title: impl Into<String>) -> &mut Self {
        self.title = title.into();
        self
    }

    pub fn size(&mut self, width: f64, height: f64) -> &mut Self {
        self.width = width;
        self.height = height;
        self
    }

    pub fn min_size(&mut self, width: f64, height: f64) -> &mut Self {
        self.min_width = Some(width);
        self.min_height = Some(height);
        self
    }

    pub fn resizable(&mut self, resizable: bool) -> &mut Self {
        self.resizable = resizable;
        self
    }

    pub fn decorations(&mut self, decorations: bool) -> &mut Self {
        self.decorations = decorations;
        self
    }

    pub fn transparent(&mut self, transparent: bool) -> &mut Self {
        self.transparent = transparent;
        self
    }

    pub fn fullscreen(&mut self, fullscreen: bool) -> &mut Self {
        self.fullscreen = fullscreen;
        self
    }
}

impl Default for WindowConfig {
    fn default() -> Self {
        Self {
            title: "Neutron App".to_string(),
            width: 1200.0,
            height: 800.0,
            resizable: true,
            decorations: true,
            transparent: false,
            fullscreen: false,
            min_width: None,
            min_height: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_bad_dimensions() {
        let mut c = WindowConfig::default();
        c.width = f64::NAN;
        assert!(c.validate().is_err());
        c.width = 1200.;
        c.min_width = Some(10.);
        assert!(c.validate().is_err());
        c.min_height = Some(10.);
        assert!(c.validate().is_ok());
    }
}
