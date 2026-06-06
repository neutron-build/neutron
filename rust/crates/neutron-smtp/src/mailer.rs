//! Async SMTP mailer — wraps `lettre::AsyncSmtpTransport`.

use std::sync::Arc;

use lettre::transport::smtp::authentication::Credentials;
use lettre::transport::smtp::AsyncSmtpTransport;
use lettre::AsyncTransport;
use lettre::Tokio1Executor;

use crate::config::{SmtpConfig, TlsMode};
use crate::error::SmtpError;
use crate::message::Email;

type SmtpTransport = AsyncSmtpTransport<Tokio1Executor>;

// ---------------------------------------------------------------------------
// Mailer
// ---------------------------------------------------------------------------

/// An async SMTP mailer.
///
/// Cheaply cloneable — all state is behind an `Arc`.
///
/// # Example
///
/// ```rust,ignore
/// let mailer = Mailer::new(
///     SmtpConfig::new("smtp.example.com")
///         .credentials("user", "pass")
///         .default_from("noreply@example.com"),
/// )?;
///
/// mailer.send(
///     Email::new()
///         .to("alice@example.com")
///         .subject("Welcome!")
///         .html("<h1>Hi Alice</h1>"),
/// ).await?;
/// ```
#[derive(Clone)]
pub struct Mailer(Arc<MailerInner>);

struct MailerInner {
    transport:    SmtpTransport,
    default_from: Option<String>,
}

impl Mailer {
    /// Create a new mailer from config.
    pub fn new(config: SmtpConfig) -> Result<Self, SmtpError> {
        let transport = build_transport(&config)?;
        Ok(Mailer(Arc::new(MailerInner {
            transport,
            default_from: config.default_from,
        })))
    }

    /// Send an email.  Returns the lettre `Response` on success.
    pub async fn send(&self, email: Email) -> Result<(), SmtpError> {
        let msg = email.into_message(self.0.default_from.as_deref())?;
        self.0.transport.send(msg).await.map_err(SmtpError::from)?;
        Ok(())
    }

    /// Test the SMTP connection (EHLO handshake only, no message sent).
    pub async fn test_connection(&self) -> bool {
        self.0.transport.test_connection().await.unwrap_or(false)
    }
}

// ---------------------------------------------------------------------------
// Build the transport from config
// ---------------------------------------------------------------------------

fn build_transport(cfg: &SmtpConfig) -> Result<SmtpTransport, SmtpError> {
    let builder = match cfg.tls {
        TlsMode::None => {
            AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&cfg.host)
                .port(cfg.port)
        }
        TlsMode::StartTls => {
            AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&cfg.host)
                .map_err(SmtpError::from)?
                .port(cfg.port)
        }
        TlsMode::Tls => {
            AsyncSmtpTransport::<Tokio1Executor>::relay(&cfg.host)
                .map_err(SmtpError::from)?
                .port(cfg.port)
        }
    };

    let builder = if let (Some(u), Some(p)) = (&cfg.username, &cfg.password) {
        builder.credentials(Credentials::new(u.clone(), p.clone()))
    } else {
        builder
    };

    Ok(builder.build())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SmtpConfig;

    /// Building a Mailer with TlsMode::None should succeed without a real server.
    #[test]
    fn mailer_new_plain() {
        let cfg = SmtpConfig::new("localhost").plain().port(2525);
        assert!(Mailer::new(cfg).is_ok());
    }

    /// Building a Mailer with STARTTLS should succeed.
    #[test]
    fn mailer_new_starttls() {
        let cfg = SmtpConfig::new("localhost").starttls().port(587);
        assert!(Mailer::new(cfg).is_ok());
    }

    /// Building a Mailer with TLS should succeed.
    #[test]
    fn mailer_new_tls() {
        let cfg = SmtpConfig::new("localhost").tls();
        assert!(Mailer::new(cfg).is_ok());
    }

    /// Mailer is Clone (Arc-backed).
    #[test]
    fn mailer_clone() {
        let cfg = SmtpConfig::new("localhost").plain();
        let m1 = Mailer::new(cfg).unwrap();
        let _m2 = m1.clone();
    }
}
