//! Async SMTP / transactional email for the Neutron web framework.
//!
//! # Quick start
//!
//! ```rust,ignore
//! use neutron_smtp::{Email, Mailer, SmtpConfig};
//!
//! // Build a mailer (once, at startup)
//! let mailer = Mailer::new(
//!     SmtpConfig::new("smtp.example.com")
//!         .starttls()
//!         .credentials("user@example.com", "s3cr3t")
//!         .default_from("noreply@example.com"),
//! )?;
//!
//! // Send an email
//! mailer.send(
//!     Email::new()
//!         .to("alice@example.com")
//!         .subject("Welcome to Neutron!")
//!         .text("Hello, Alice!")
//!         .html("<p>Hello, <strong>Alice</strong>!</p>"),
//! ).await?;
//! ```
//!
//! # Router integration
//!
//! Wrap the mailer in `neutron::data::Data<Mailer>` (or any state extractor) to
//! inject it into handlers:
//!
//! ```rust,ignore
//! use neutron::router::Router;
//! use neutron::data::Data;
//! use neutron_smtp::{Email, Mailer, SmtpConfig};
//!
//! let mailer = Mailer::new(SmtpConfig::new("localhost").plain())?;
//!
//! let app = Router::new()
//!     .post("/contact", send_email)
//!     .state(Data::new(mailer));
//!
//! async fn send_email(Data(mailer): Data<Mailer>, /* … */) -> impl IntoResponse {
//!     mailer.send(Email::new().to("admin@example.com").subject("…").text("…")).await.ok();
//!     "sent"
//! }
//! ```

pub mod config;
pub mod error;
pub mod mailer;
pub mod message;

pub use config::{SmtpConfig, TlsMode};
pub use error::SmtpError;
pub use mailer::Mailer;
pub use message::{Email, EmailAttachment};
