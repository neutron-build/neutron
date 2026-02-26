//! SMTP error type.

use std::fmt;

/// Errors that can occur when sending email.
#[derive(Debug)]
pub enum SmtpError {
    /// Failed to build the MIME message.
    Build(String),
    /// SMTP transport error (connection, auth, etc.).
    Transport(String),
    /// An invalid address was supplied.
    Address(String),
    /// Configuration is missing or invalid.
    Config(String),
}

impl fmt::Display for SmtpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SmtpError::Build(s)     => write!(f, "email build error: {s}"),
            SmtpError::Transport(s) => write!(f, "SMTP transport error: {s}"),
            SmtpError::Address(s)   => write!(f, "invalid email address: {s}"),
            SmtpError::Config(s)    => write!(f, "SMTP config error: {s}"),
        }
    }
}

impl std::error::Error for SmtpError {}

impl From<lettre::error::Error> for SmtpError {
    fn from(e: lettre::error::Error) -> Self {
        SmtpError::Build(e.to_string())
    }
}

impl From<lettre::transport::smtp::Error> for SmtpError {
    fn from(e: lettre::transport::smtp::Error) -> Self {
        SmtpError::Transport(e.to_string())
    }
}

impl From<lettre::address::AddressError> for SmtpError {
    fn from(e: lettre::address::AddressError) -> Self {
        SmtpError::Address(e.to_string())
    }
}
