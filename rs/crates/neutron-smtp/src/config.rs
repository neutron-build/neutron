//! SMTP connection configuration.

/// TLS mode for the SMTP connection.
#[derive(Debug, Clone, PartialEq)]
pub enum TlsMode {
    /// No encryption (plain SMTP, port 25).  Not recommended for production.
    None,
    /// STARTTLS — upgrade an initially plain connection (port 587).
    StartTls,
    /// Implicit TLS — TLS from the first byte (port 465).
    Tls,
}

/// Configuration for an SMTP connection.
///
/// # Example
///
/// ```rust,ignore
/// let cfg = SmtpConfig::new("smtp.example.com")
///     .port(587)
///     .starttls()
///     .credentials("user@example.com", "s3cr3t")
///     .default_from("noreply@example.com");
/// ```
#[derive(Debug, Clone)]
pub struct SmtpConfig {
    pub(crate) host:         String,
    pub(crate) port:         u16,
    pub(crate) tls:          TlsMode,
    pub(crate) username:     Option<String>,
    pub(crate) password:     Option<String>,
    pub(crate) default_from: Option<String>,
}

impl SmtpConfig {
    /// Create a new config targeting `host`.  Defaults to port 587 and STARTTLS.
    pub fn new(host: impl Into<String>) -> Self {
        SmtpConfig {
            host:         host.into(),
            port:         587,
            tls:          TlsMode::StartTls,
            username:     None,
            password:     None,
            default_from: None,
        }
    }

    /// Override the port.
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Use plain (unencrypted) SMTP.
    pub fn plain(mut self) -> Self {
        self.tls = TlsMode::None;
        self
    }

    /// Use STARTTLS (the default).
    pub fn starttls(mut self) -> Self {
        self.tls = TlsMode::StartTls;
        self
    }

    /// Use implicit TLS (SMTPS, port 465).
    pub fn tls(mut self) -> Self {
        self.tls = TlsMode::Tls;
        self.port = 465;
        self
    }

    /// Set SMTP credentials for authentication.
    pub fn credentials(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }

    /// Set the default `From` address used when `Email::from()` is not called.
    pub fn default_from(mut self, addr: impl Into<String>) -> Self {
        self.default_from = Some(addr.into());
        self
    }
}
