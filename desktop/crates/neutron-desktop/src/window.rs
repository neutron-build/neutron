/// Configuration for the main application window.
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
