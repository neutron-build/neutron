//! Email message builder.

use lettre::message::{header::ContentType, Attachment, Mailbox, MultiPart, SinglePart};
use lettre::Message;

use crate::error::SmtpError;

/// An email message under construction.
///
/// # Example
///
/// ```rust,ignore
/// let email = Email::new()
///     .to("alice@example.com")
///     .subject("Hello!")
///     .text("Plain text body")
///     .html("<p>HTML body</p>");
/// ```
#[derive(Debug, Default)]
pub struct Email {
    pub(crate) from:       Option<String>,
    pub(crate) reply_to:   Option<String>,
    pub(crate) to:         Vec<String>,
    pub(crate) cc:         Vec<String>,
    pub(crate) bcc:        Vec<String>,
    pub(crate) subject:    Option<String>,
    pub(crate) text:       Option<String>,
    pub(crate) html:       Option<String>,
    pub(crate) attachments: Vec<EmailAttachment>,
}

/// A file attachment.
#[derive(Debug)]
pub struct EmailAttachment {
    /// File name shown to the recipient.
    pub filename:     String,
    /// Raw bytes of the attachment.
    pub data:         Vec<u8>,
    /// MIME content type, e.g. `"application/pdf"`.
    pub content_type: String,
}

impl Email {
    /// Create a new empty email.
    pub fn new() -> Self {
        Email::default()
    }

    /// Set the `From` address.  If not set, the mailer's `default_from` is used.
    pub fn from(mut self, addr: impl Into<String>) -> Self {
        self.from = Some(addr.into());
        self
    }

    /// Set the `Reply-To` address.
    pub fn reply_to(mut self, addr: impl Into<String>) -> Self {
        self.reply_to = Some(addr.into());
        self
    }

    /// Add a `To` recipient.
    pub fn to(mut self, addr: impl Into<String>) -> Self {
        self.to.push(addr.into());
        self
    }

    /// Add a `CC` recipient.
    pub fn cc(mut self, addr: impl Into<String>) -> Self {
        self.cc.push(addr.into());
        self
    }

    /// Add a `BCC` recipient.
    pub fn bcc(mut self, addr: impl Into<String>) -> Self {
        self.bcc.push(addr.into());
        self
    }

    /// Set the email subject.
    pub fn subject(mut self, subj: impl Into<String>) -> Self {
        self.subject = Some(subj.into());
        self
    }

    /// Set a plain-text body.
    pub fn text(mut self, body: impl Into<String>) -> Self {
        self.text = Some(body.into());
        self
    }

    /// Set an HTML body.  If `text` is also set, a `multipart/alternative` message is built.
    pub fn html(mut self, body: impl Into<String>) -> Self {
        self.html = Some(body.into());
        self
    }

    /// Add a file attachment.
    pub fn attach(
        mut self,
        filename: impl Into<String>,
        data: Vec<u8>,
        content_type: impl Into<String>,
    ) -> Self {
        self.attachments.push(EmailAttachment {
            filename:     filename.into(),
            data,
            content_type: content_type.into(),
        });
        self
    }

    // -----------------------------------------------------------------------
    // Build the lettre Message
    // -----------------------------------------------------------------------

    /// Convert into a [`lettre::Message`] ready to send.
    ///
    /// `fallback_from` is used when `Email::from()` was not called.
    pub(crate) fn into_message(self, fallback_from: Option<&str>) -> Result<Message, SmtpError> {
        let from_str = self.from.as_deref()
            .or(fallback_from)
            .ok_or_else(|| SmtpError::Config("no From address — set Email::from() or SmtpConfig::default_from()".into()))?;

        let from: Mailbox = from_str.parse().map_err(SmtpError::from)?;

        if self.to.is_empty() {
            return Err(SmtpError::Build("at least one To address is required".into()));
        }

        let mut builder = Message::builder().from(from);

        if let Some(rt) = &self.reply_to {
            let mb: Mailbox = rt.parse().map_err(SmtpError::from)?;
            builder = builder.reply_to(mb);
        }

        for addr in &self.to {
            let mb: Mailbox = addr.parse().map_err(SmtpError::from)?;
            builder = builder.to(mb);
        }

        for addr in &self.cc {
            let mb: Mailbox = addr.parse().map_err(SmtpError::from)?;
            builder = builder.cc(mb);
        }

        for addr in &self.bcc {
            let mb: Mailbox = addr.parse().map_err(SmtpError::from)?;
            builder = builder.bcc(mb);
        }

        builder = builder.subject(self.subject.unwrap_or_default());

        // Build body
        let msg = if self.attachments.is_empty() {
            builder.multipart(build_body(self.text.as_deref(), self.html.as_deref())?)?
        } else {
            // multipart/mixed: body part + attachments
            let mut mixed = MultiPart::mixed().multipart(
                build_body(self.text.as_deref(), self.html.as_deref())?
            );

            for att in self.attachments {
                let ct: ContentType = att.content_type.parse()
                    .unwrap_or(ContentType::TEXT_PLAIN);
                let part = Attachment::new(att.filename)
                    .body(att.data, ct);
                mixed = mixed.singlepart(part);
            }

            builder.multipart(mixed)?
        };

        Ok(msg)
    }
}

/// Build a `multipart/alternative` or a simple text/html part.
fn build_body(text: Option<&str>, html: Option<&str>) -> Result<MultiPart, SmtpError> {
    match (text, html) {
        (Some(t), Some(h)) => {
            Ok(MultiPart::alternative()
                .singlepart(
                    SinglePart::builder()
                        .header(ContentType::TEXT_PLAIN)
                        .body(t.to_string()),
                )
                .singlepart(
                    SinglePart::builder()
                        .header(ContentType::TEXT_HTML)
                        .body(h.to_string()),
                ))
        }
        (Some(t), None) => {
            Ok(MultiPart::alternative().singlepart(
                SinglePart::builder()
                    .header(ContentType::TEXT_PLAIN)
                    .body(t.to_string()),
            ))
        }
        (None, Some(h)) => {
            Ok(MultiPart::alternative().singlepart(
                SinglePart::builder()
                    .header(ContentType::TEXT_HTML)
                    .body(h.to_string()),
            ))
        }
        (None, None) => {
            // Empty body — send a blank plain-text message
            Ok(MultiPart::alternative().singlepart(
                SinglePart::builder()
                    .header(ContentType::TEXT_PLAIN)
                    .body(String::new()),
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn email_builder_basic() {
        let e = Email::new()
            .from("sender@example.com")
            .to("recv@example.com")
            .subject("Hi")
            .text("hello");
        assert_eq!(e.from.as_deref(), Some("sender@example.com"));
        assert_eq!(e.to.len(), 1);
        assert_eq!(e.subject.as_deref(), Some("Hi"));
        assert_eq!(e.text.as_deref(), Some("hello"));
    }

    #[test]
    fn email_multi_recipients() {
        let e = Email::new()
            .from("a@b.com")
            .to("x@b.com")
            .to("y@b.com")
            .cc("z@b.com")
            .bcc("w@b.com");
        assert_eq!(e.to.len(), 2);
        assert_eq!(e.cc.len(), 1);
        assert_eq!(e.bcc.len(), 1);
    }

    #[test]
    fn into_message_no_from_no_default_fails() {
        let e = Email::new().to("recv@example.com").subject("x").text("y");
        assert!(e.into_message(None).is_err());
    }

    #[test]
    fn into_message_no_to_fails() {
        let e = Email::new().from("a@b.com").subject("x").text("y");
        assert!(e.into_message(None).is_err());
    }

    #[test]
    fn into_message_fallback_from() {
        let e = Email::new().to("recv@example.com").subject("x").text("y");
        let msg = e.into_message(Some("default@example.com"));
        assert!(msg.is_ok(), "{:?}", msg);
    }

    #[test]
    fn into_message_html_only() {
        let e = Email::new()
            .from("a@b.com")
            .to("recv@example.com")
            .subject("test")
            .html("<p>hi</p>");
        assert!(e.into_message(None).is_ok());
    }

    #[test]
    fn into_message_text_and_html() {
        let e = Email::new()
            .from("a@b.com")
            .to("recv@example.com")
            .subject("test")
            .text("plain")
            .html("<p>rich</p>");
        assert!(e.into_message(None).is_ok());
    }

    #[test]
    fn into_message_with_attachment() {
        let e = Email::new()
            .from("a@b.com")
            .to("recv@example.com")
            .subject("attachment")
            .text("see attached")
            .attach("file.txt", b"hello".to_vec(), "text/plain");
        assert!(e.into_message(None).is_ok());
    }

    #[test]
    fn email_attachment_fields() {
        let e = Email::new()
            .from("a@b.com")
            .to("r@b.com")
            .attach("data.csv", vec![1, 2, 3], "text/csv");
        assert_eq!(e.attachments.len(), 1);
        assert_eq!(e.attachments[0].filename, "data.csv");
        assert_eq!(e.attachments[0].content_type, "text/csv");
    }
}
