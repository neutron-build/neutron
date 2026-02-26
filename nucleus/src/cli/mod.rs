//! Interactive CLI client (psql-like shell) building blocks for Nucleus.
//!
//! Provides display formatting, meta-command parsing, help text, and a
//! minimal Postgres wire protocol client that speaks the simple query protocol.
//! The actual REPL loop (using rustyline) lives in `main.rs`; this module
//! supplies the underlying machinery.

use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

// ============================================================================
// Error type
// ============================================================================

/// Errors that can occur in the CLI client.
#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error("connection error: {0}")]
    ConnectionError(String),
    #[error("protocol error: {0}")]
    ProtocolError(String),
    #[error("I/O error: {0}")]
    IoError(#[from] io::Error),
    #[error("server error: {0}")]
    ServerError(String),
}

// ============================================================================
// Display formatting
// ============================================================================

/// Result of formatting a query response for display.
#[derive(Debug, Clone)]
pub struct TableDisplay {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

impl TableDisplay {
    /// Create a new table display from column names and rows.
    pub fn new(columns: Vec<String>, rows: Vec<Vec<String>>) -> Self {
        Self { columns, rows }
    }

    /// Compute the maximum width needed for each column, considering both
    /// the column header and every value in that column.
    fn column_widths(&self) -> Vec<usize> {
        let mut widths: Vec<usize> = self.columns.iter().map(|c| c.len()).collect();
        for row in &self.rows {
            for (i, val) in row.iter().enumerate() {
                if i < widths.len() && val.len() > widths[i] {
                    widths[i] = val.len();
                }
            }
        }
        widths
    }

    /// Format the table as an aligned text table with borders.
    ///
    /// Example output:
    /// ```text
    ///  id | name  | age
    /// ----+-------+-----
    ///   1 | Alice |  30
    ///   2 | Bob   |  25
    /// (2 rows)
    /// ```
    pub fn format(&self) -> String {
        if self.rows.is_empty() {
            return self.format_empty();
        }

        let widths = self.column_widths();
        let mut out = String::new();

        // Header row
        self.write_row(&mut out, &self.columns, &widths);
        out.push('\n');

        // Separator
        self.write_separator(&mut out, &widths);
        out.push('\n');

        // Data rows
        for row in &self.rows {
            self.write_row(&mut out, row, &widths);
            out.push('\n');
        }

        // Row count
        let n = self.rows.len();
        if n == 1 {
            out.push_str("(1 row)");
        } else {
            out.push_str(&format!("({n} rows)"));
        }

        out
    }

    /// Format with no rows (just the column header and "(0 rows)").
    pub fn format_empty(&self) -> String {
        let widths = self.column_widths();
        let mut out = String::new();

        // Header row
        self.write_row(&mut out, &self.columns, &widths);
        out.push('\n');

        // Separator
        self.write_separator(&mut out, &widths);
        out.push('\n');

        out.push_str("(0 rows)");

        out
    }

    /// Write a single row (header or data) with padding.
    fn write_row(&self, out: &mut String, values: &[String], widths: &[usize]) {
        for (i, val) in values.iter().enumerate() {
            if i > 0 {
                out.push_str(" | ");
            } else {
                out.push(' ');
            }
            let w = widths.get(i).copied().unwrap_or(val.len());
            // Right-align if value looks numeric, left-align otherwise
            if looks_numeric(val) {
                out.push_str(&format!("{val:>w$}"));
            } else {
                out.push_str(&format!("{val:<w$}"));
            }
        }
    }

    /// Write the separator line between header and data.
    fn write_separator(&self, out: &mut String, widths: &[usize]) {
        for (i, &w) in widths.iter().enumerate() {
            if i > 0 {
                out.push('+');
            }
            // Each column gets w+2 dashes (1 space padding on each side)
            for _ in 0..w + 2 {
                out.push('-');
            }
        }
    }
}

/// Heuristic: does this string look like a plain integer or decimal number?
fn looks_numeric(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let s = s.trim();
    let mut saw_dot = false;
    let mut chars = s.chars().peekable();
    // Optional leading minus
    if chars.peek() == Some(&'-') {
        chars.next();
    }
    if chars.peek().is_none() {
        return false;
    }
    for c in chars {
        if c == '.' && !saw_dot {
            saw_dot = true;
        } else if !c.is_ascii_digit() {
            return false;
        }
    }
    true
}

// ============================================================================
// Meta-commands
// ============================================================================

/// A parsed meta-command (backslash commands like `\dt`, `\d`, `\q`).
#[derive(Debug, Clone, PartialEq)]
pub enum MetaCommand {
    /// `\dt` — list all tables
    ListTables,
    /// `\d tablename` — describe a table's columns
    DescribeTable(String),
    /// `\timing` — toggle query timing display
    ToggleTiming,
    /// `\q` or `\quit` — exit the shell
    Quit,
    /// `\?` or `\help` — show help
    Help,
    /// `\status` — show server status
    ShowStatus,
    /// Unrecognized backslash command
    Unknown(String),
}

/// Parse a backslash command string into a [`MetaCommand`].
pub fn parse_meta_command(input: &str) -> MetaCommand {
    let trimmed = input.trim();
    match trimmed {
        "\\dt" => MetaCommand::ListTables,
        "\\timing" => MetaCommand::ToggleTiming,
        "\\q" | "\\quit" => MetaCommand::Quit,
        "\\?" | "\\help" => MetaCommand::Help,
        "\\status" => MetaCommand::ShowStatus,
        s if s.starts_with("\\d ") => {
            let table = s[3..].trim().to_string();
            MetaCommand::DescribeTable(table)
        }
        _ => MetaCommand::Unknown(trimmed.to_string()),
    }
}

/// Generate the SQL query that implements a meta-command, or `None` if the
/// command is handled locally (help, timing, quit).
pub fn meta_command_to_sql(cmd: &MetaCommand) -> Option<String> {
    match cmd {
        MetaCommand::ListTables => Some("SHOW TABLES".to_string()),
        MetaCommand::DescribeTable(name) => Some(format!("DESCRIBE {name}")),
        MetaCommand::ShowStatus => {
            Some("SELECT 'Nucleus' AS engine, '0.1.0' AS version".to_string())
        }
        MetaCommand::Help => None,
        MetaCommand::ToggleTiming => None,
        MetaCommand::Quit => None,
        MetaCommand::Unknown(_) => None,
    }
}

// ============================================================================
// Help text
// ============================================================================

/// Return the built-in help text for the interactive shell.
pub fn help_text() -> &'static str {
    r#"Nucleus Shell Commands:
  \dt           List all tables
  \d TABLE      Describe a table's columns
  \timing       Toggle query timing display
  \status       Show server status
  \q, \quit     Exit the shell
  \?, \help     Show this help message

SQL commands end with a semicolon (;) and are sent to the server."#
}

// ============================================================================
// Query result
// ============================================================================

/// The result returned by a simple-query exchange with the server.
#[derive(Debug)]
pub enum QueryResult {
    /// SELECT result with columns and rows.
    Select {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    /// Command result (INSERT, CREATE, etc.).
    Command { tag: String },
    /// Error from server.
    Error { message: String },
}

// ============================================================================
// Postgres wire protocol client
// ============================================================================

/// A minimal Postgres wire protocol client for the CLI shell.
///
/// Implements the simple query protocol only. This is intentionally bare-bones:
/// just enough to send SQL text and read back rows or command tags. It does
/// **not** support SSL negotiation, SASL authentication, extended query, COPY,
/// or notifications.
pub struct PgClient {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl PgClient {
    /// Connect to a Nucleus / Postgres-compatible server at `host:port`.
    ///
    /// Performs the startup handshake:
    /// 1. Sends a StartupMessage (protocol 3.0, `user=nucleus`).
    /// 2. Reads `AuthenticationOk` (or handles cleartext password if needed).
    /// 3. Drains `ParameterStatus` messages until `ReadyForQuery`.
    pub async fn connect(host: &str, port: u16) -> Result<Self, CliError> {
        let stream = TcpStream::connect((host, port))
            .await
            .map_err(|e| CliError::ConnectionError(format!("{host}:{port}: {e}")))?;

        let (read_half, write_half) = stream.into_split();
        let mut client = Self {
            reader: BufReader::new(read_half),
            writer: write_half,
        };

        client.send_startup("nucleus").await?;
        client.read_startup_response().await?;

        Ok(client)
    }

    // ---- startup helpers ---------------------------------------------------

    /// Build and send a StartupMessage (protocol version 3.0).
    ///
    /// Layout:
    /// ```text
    ///   i32  total length (including self)
    ///   i32  protocol version (196608 = 3 << 16)
    ///   cstr "user"
    ///   cstr <username>
    ///   u8   0x00  (terminator)
    /// ```
    async fn send_startup(&mut self, user: &str) -> Result<(), CliError> {
        let mut body = Vec::new();
        // Protocol 3.0
        body.extend_from_slice(&196608_i32.to_be_bytes());
        // "user" parameter
        body.extend_from_slice(b"user\0");
        body.extend_from_slice(user.as_bytes());
        body.push(0);
        // Terminator for parameter list
        body.push(0);

        let len = (body.len() as i32) + 4; // length field includes itself
        self.writer.write_all(&len.to_be_bytes()).await?;
        self.writer.write_all(&body).await?;
        self.writer.flush().await?;
        Ok(())
    }

    /// Read the server's response to the startup message.
    ///
    /// Handles `AuthenticationOk` (R with code 0), `ParameterStatus` (S),
    /// `BackendKeyData` (K), and waits for `ReadyForQuery` (Z).
    async fn read_startup_response(&mut self) -> Result<(), CliError> {
        loop {
            let (tag, payload) = self.read_message().await?;
            match tag {
                b'R' => {
                    // Authentication response
                    if payload.len() < 4 {
                        return Err(CliError::ProtocolError(
                            "authentication message too short".into(),
                        ));
                    }
                    let auth_type =
                        i32::from_be_bytes(payload[0..4].try_into().unwrap());
                    match auth_type {
                        0 => { /* AuthenticationOk — continue */ }
                        3 => {
                            // AuthenticationCleartextPassword — send empty password
                            self.send_password("").await?;
                        }
                        _ => {
                            return Err(CliError::ProtocolError(format!(
                                "unsupported authentication type: {auth_type}"
                            )));
                        }
                    }
                }
                b'S' => { /* ParameterStatus — ignore */ }
                b'K' => { /* BackendKeyData — ignore */ }
                b'Z' => {
                    // ReadyForQuery — startup complete
                    return Ok(());
                }
                b'E' => {
                    let msg = Self::parse_error_response(&payload);
                    return Err(CliError::ServerError(msg));
                }
                other => {
                    return Err(CliError::ProtocolError(format!(
                        "unexpected message type during startup: '{}'",
                        other as char,
                    )));
                }
            }
        }
    }

    /// Send a PasswordMessage ('p').
    async fn send_password(&mut self, password: &str) -> Result<(), CliError> {
        let pw_bytes = password.as_bytes();
        let len = 4 + pw_bytes.len() as i32 + 1; // length + password + null
        self.writer.write_u8(b'p').await?;
        self.writer.write_all(&len.to_be_bytes()).await?;
        self.writer.write_all(pw_bytes).await?;
        self.writer.write_u8(0).await?;
        self.writer.flush().await?;
        Ok(())
    }

    // ---- simple query protocol ---------------------------------------------

    /// Send a simple query and return the result.
    ///
    /// Protocol:
    /// 1. Send `Query` message (`'Q'` + length + sql + `'\0'`).
    /// 2. Read responses until `ReadyForQuery` (`'Z'`).
    pub async fn simple_query(&mut self, sql: &str) -> Result<QueryResult, CliError> {
        // Send Query message
        let sql_bytes = sql.as_bytes();
        let len = 4 + sql_bytes.len() as i32 + 1;
        self.writer.write_u8(b'Q').await?;
        self.writer.write_all(&len.to_be_bytes()).await?;
        self.writer.write_all(sql_bytes).await?;
        self.writer.write_u8(0).await?;
        self.writer.flush().await?;

        // Read response messages
        let mut columns: Vec<String> = Vec::new();
        let mut rows: Vec<Vec<String>> = Vec::new();
        let mut command_tag: Option<String> = None;
        let mut error_message: Option<String> = None;

        loop {
            let (tag, payload) = self.read_message().await?;
            match tag {
                b'T' => {
                    // RowDescription
                    columns = Self::parse_row_description(&payload)?;
                }
                b'D' => {
                    // DataRow
                    let row = Self::parse_data_row(&payload)?;
                    rows.push(row);
                }
                b'C' => {
                    // CommandComplete
                    command_tag = Some(Self::parse_cstring(&payload));
                }
                b'E' => {
                    // ErrorResponse
                    error_message = Some(Self::parse_error_response(&payload));
                }
                b'I' => {
                    // EmptyQueryResponse — treat like a command with no tag
                    command_tag = Some(String::new());
                }
                b'Z' => {
                    // ReadyForQuery — done
                    break;
                }
                b'N' => { /* NoticeResponse — ignore */ }
                _ => {
                    // Ignore unknown messages
                }
            }
        }

        if let Some(msg) = error_message {
            return Ok(QueryResult::Error { message: msg });
        }

        if !columns.is_empty() {
            Ok(QueryResult::Select { columns, rows })
        } else if let Some(tag) = command_tag {
            Ok(QueryResult::Command { tag })
        } else {
            Ok(QueryResult::Command {
                tag: String::new(),
            })
        }
    }

    /// Close the connection gracefully by sending a Terminate message ('X').
    pub async fn close(mut self) -> Result<(), CliError> {
        // Terminate: 'X' + i32 length (4)
        self.writer.write_u8(b'X').await?;
        self.writer.write_all(&4_i32.to_be_bytes()).await?;
        self.writer.flush().await?;
        Ok(())
    }

    // ---- low-level message I/O --------------------------------------------

    /// Read one wire-protocol message: a 1-byte tag followed by a 4-byte
    /// big-endian length (including itself) and then `length - 4` payload bytes.
    async fn read_message(&mut self) -> Result<(u8, Vec<u8>), CliError> {
        let tag = self.reader.read_u8().await?;
        let len = self.reader.read_i32().await?;
        if len < 4 {
            return Err(CliError::ProtocolError(format!(
                "invalid message length {len} for tag '{}'",
                tag as char,
            )));
        }
        let payload_len = (len - 4) as usize;
        let mut payload = vec![0u8; payload_len];
        if payload_len > 0 {
            self.reader.read_exact(&mut payload).await?;
        }
        Ok((tag, payload))
    }

    // ---- message parsing helpers ------------------------------------------

    /// Parse a RowDescription message payload into column names.
    ///
    /// Layout:
    /// ```text
    ///   i16  field count
    ///   for each field:
    ///     cstr   name
    ///     i32    table OID
    ///     i16    column attribute number
    ///     i32    type OID
    ///     i16    type size
    ///     i32    type modifier
    ///     i16    format code
    /// ```
    fn parse_row_description(payload: &[u8]) -> Result<Vec<String>, CliError> {
        if payload.len() < 2 {
            return Err(CliError::ProtocolError(
                "RowDescription too short".into(),
            ));
        }
        let field_count =
            i16::from_be_bytes(payload[0..2].try_into().unwrap()) as usize;
        let mut columns = Vec::with_capacity(field_count);
        let mut pos = 2;

        for _ in 0..field_count {
            // Read null-terminated column name
            let name_start = pos;
            while pos < payload.len() && payload[pos] != 0 {
                pos += 1;
            }
            let name = String::from_utf8_lossy(&payload[name_start..pos]).to_string();
            pos += 1; // skip null terminator

            // Skip the fixed-size fields: table OID(4) + col attr(2) + type OID(4)
            //                            + type size(2) + type mod(4) + format(2) = 18
            pos += 18;

            columns.push(name);
        }

        Ok(columns)
    }

    /// Parse a DataRow message payload into string values.
    ///
    /// Layout:
    /// ```text
    ///   i16  column count
    ///   for each column:
    ///     i32  value length (-1 for NULL)
    ///     bytes  value data (if length >= 0)
    /// ```
    fn parse_data_row(payload: &[u8]) -> Result<Vec<String>, CliError> {
        if payload.len() < 2 {
            return Err(CliError::ProtocolError("DataRow too short".into()));
        }
        let col_count =
            i16::from_be_bytes(payload[0..2].try_into().unwrap()) as usize;
        let mut values = Vec::with_capacity(col_count);
        let mut pos = 2;

        for _ in 0..col_count {
            if pos + 4 > payload.len() {
                return Err(CliError::ProtocolError(
                    "DataRow truncated".into(),
                ));
            }
            let val_len =
                i32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap());
            pos += 4;

            if val_len < 0 {
                values.push("NULL".to_string());
            } else {
                let len = val_len as usize;
                if pos + len > payload.len() {
                    return Err(CliError::ProtocolError(
                        "DataRow value extends past end".into(),
                    ));
                }
                let val = String::from_utf8_lossy(&payload[pos..pos + len]).to_string();
                pos += len;
                values.push(val);
            }
        }

        Ok(values)
    }

    /// Parse an ErrorResponse payload into a human-readable message.
    ///
    /// The payload is a sequence of `(type_byte, cstr)` pairs terminated by a
    /// zero byte. We look for the 'M' (message) field.
    fn parse_error_response(payload: &[u8]) -> String {
        let mut pos = 0;
        let mut message = String::new();
        let mut severity = String::new();

        while pos < payload.len() {
            let field_type = payload[pos];
            pos += 1;
            if field_type == 0 {
                break;
            }
            let str_start = pos;
            while pos < payload.len() && payload[pos] != 0 {
                pos += 1;
            }
            let value = String::from_utf8_lossy(&payload[str_start..pos]).to_string();
            pos += 1; // skip null terminator

            match field_type {
                b'M' => message = value,
                b'S' => severity = value,
                _ => {}
            }
        }

        if severity.is_empty() {
            message
        } else {
            format!("{severity}: {message}")
        }
    }

    /// Read a C-style null-terminated string from the start of a byte slice.
    fn parse_cstring(data: &[u8]) -> String {
        let end = data.iter().position(|&b| b == 0).unwrap_or(data.len());
        String::from_utf8_lossy(&data[..end]).to_string()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // --- TableDisplay tests -------------------------------------------------

    #[test]
    fn test_table_display_format() {
        let display = TableDisplay::new(
            vec!["id".into(), "name".into(), "age".into()],
            vec![
                vec!["1".into(), "Alice".into(), "30".into()],
                vec!["2".into(), "Bob".into(), "25".into()],
            ],
        );
        let output = display.format();
        // Check header
        assert!(output.contains("id"));
        assert!(output.contains("name"));
        assert!(output.contains("age"));
        // Check separator
        assert!(output.contains("+"));
        assert!(output.contains("---"));
        // Check data
        assert!(output.contains("Alice"));
        assert!(output.contains("Bob"));
        // Check row count
        assert!(output.contains("(2 rows)"));
        // Numbers should be right-aligned (id and age columns)
        // Verify the separator line has the right structure
        let lines: Vec<&str> = output.lines().collect();
        assert!(lines.len() >= 5); // header + sep + 2 data rows + count
    }

    #[test]
    fn test_table_display_empty() {
        let display = TableDisplay::new(
            vec!["id".into(), "name".into()],
            vec![],
        );
        let output = display.format_empty();
        assert!(output.contains("id"));
        assert!(output.contains("name"));
        assert!(output.contains("(0 rows)"));
        // Should not contain any data rows
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 3); // header + sep + count
    }

    #[test]
    fn test_table_display_single_column() {
        let display = TableDisplay::new(
            vec!["count".into()],
            vec![vec!["42".into()]],
        );
        let output = display.format();
        assert!(output.contains("count"));
        assert!(output.contains("42"));
        assert!(output.contains("(1 row)"));
    }

    #[test]
    fn test_table_display_long_values() {
        let display = TableDisplay::new(
            vec!["id".into(), "description".into()],
            vec![
                vec![
                    "1".into(),
                    "This is a very long description value".into(),
                ],
                vec!["2".into(), "Short".into()],
            ],
        );
        let output = display.format();
        // The column should be wide enough for the long value
        assert!(output.contains("This is a very long description value"));
        assert!(output.contains("Short"));
        // The separator should be wide enough too
        let sep_line = output.lines().nth(1).unwrap();
        // Total separator width should accommodate the long value
        assert!(sep_line.len() > 30);
    }

    #[test]
    fn test_table_display_format_calls_format_empty_when_no_rows() {
        let display = TableDisplay::new(
            vec!["x".into()],
            vec![],
        );
        // format() should delegate to format_empty() when rows is empty
        let f = display.format();
        let e = display.format_empty();
        assert_eq!(f, e);
    }

    // --- Meta-command parsing tests -----------------------------------------

    #[test]
    fn test_parse_meta_command_dt() {
        assert_eq!(parse_meta_command("\\dt"), MetaCommand::ListTables);
    }

    #[test]
    fn test_parse_meta_command_describe() {
        assert_eq!(
            parse_meta_command("\\d users"),
            MetaCommand::DescribeTable("users".into()),
        );
        // Extra whitespace should be trimmed
        assert_eq!(
            parse_meta_command("\\d   orders  "),
            MetaCommand::DescribeTable("orders".into()),
        );
    }

    #[test]
    fn test_parse_meta_command_quit() {
        assert_eq!(parse_meta_command("\\q"), MetaCommand::Quit);
        assert_eq!(parse_meta_command("\\quit"), MetaCommand::Quit);
    }

    #[test]
    fn test_parse_meta_command_help() {
        assert_eq!(parse_meta_command("\\?"), MetaCommand::Help);
        assert_eq!(parse_meta_command("\\help"), MetaCommand::Help);
    }

    #[test]
    fn test_parse_meta_command_timing() {
        assert_eq!(parse_meta_command("\\timing"), MetaCommand::ToggleTiming);
    }

    #[test]
    fn test_parse_meta_command_unknown() {
        assert_eq!(
            parse_meta_command("\\foo"),
            MetaCommand::Unknown("\\foo".into()),
        );
        assert_eq!(
            parse_meta_command("\\bar baz"),
            MetaCommand::Unknown("\\bar baz".into()),
        );
    }

    // --- meta_command_to_sql tests ------------------------------------------

    #[test]
    fn test_meta_command_to_sql() {
        assert_eq!(
            meta_command_to_sql(&MetaCommand::ListTables),
            Some("SHOW TABLES".to_string()),
        );
        assert_eq!(
            meta_command_to_sql(&MetaCommand::DescribeTable("users".into())),
            Some("DESCRIBE users".to_string()),
        );
        assert_eq!(
            meta_command_to_sql(&MetaCommand::ShowStatus),
            Some("SELECT 'Nucleus' AS engine, '0.1.0' AS version".to_string()),
        );
        assert_eq!(meta_command_to_sql(&MetaCommand::Help), None);
        assert_eq!(meta_command_to_sql(&MetaCommand::ToggleTiming), None);
        assert_eq!(meta_command_to_sql(&MetaCommand::Quit), None);
        assert_eq!(
            meta_command_to_sql(&MetaCommand::Unknown("\\x".into())),
            None,
        );
    }

    // --- help text test -----------------------------------------------------

    #[test]
    fn test_help_text_not_empty() {
        let text = help_text();
        assert!(!text.is_empty());
        assert!(text.contains("\\dt"));
        assert!(text.contains("\\q"));
        assert!(text.contains("\\help"));
    }

    // --- looks_numeric helper -----------------------------------------------

    #[test]
    fn test_looks_numeric() {
        assert!(looks_numeric("42"));
        assert!(looks_numeric("-1"));
        assert!(looks_numeric("3.14"));
        assert!(looks_numeric("0"));
        assert!(!looks_numeric(""));
        assert!(!looks_numeric("hello"));
        assert!(!looks_numeric("-"));
        assert!(!looks_numeric("12abc"));
    }

    // --- parse_error_response -----------------------------------------------

    #[test]
    fn test_parse_error_response() {
        // Build a minimal ErrorResponse payload
        let mut payload = Vec::new();
        // Severity field
        payload.push(b'S');
        payload.extend_from_slice(b"ERROR\0");
        // Message field
        payload.push(b'M');
        payload.extend_from_slice(b"something went wrong\0");
        // Terminator
        payload.push(0);

        let msg = PgClient::parse_error_response(&payload);
        assert_eq!(msg, "ERROR: something went wrong");
    }

    // --- parse_cstring ------------------------------------------------------

    #[test]
    fn test_parse_cstring() {
        let data = b"INSERT 0 1\0extra";
        let s = PgClient::parse_cstring(data);
        assert_eq!(s, "INSERT 0 1");
    }

    // --- parse_row_description / parse_data_row -----------------------------

    #[test]
    fn test_parse_row_description() {
        // Build a RowDescription for one column named "name"
        let mut payload = Vec::new();
        payload.extend_from_slice(&1_i16.to_be_bytes()); // 1 field
        payload.extend_from_slice(b"name\0"); // column name
        payload.extend_from_slice(&0_i32.to_be_bytes()); // table OID
        payload.extend_from_slice(&0_i16.to_be_bytes()); // column attr
        payload.extend_from_slice(&25_i32.to_be_bytes()); // type OID (text)
        payload.extend_from_slice(&(-1_i16).to_be_bytes()); // type size
        payload.extend_from_slice(&(-1_i32).to_be_bytes()); // type modifier
        payload.extend_from_slice(&0_i16.to_be_bytes()); // format code

        let cols = PgClient::parse_row_description(&payload).unwrap();
        assert_eq!(cols, vec!["name".to_string()]);
    }

    #[test]
    fn test_parse_data_row() {
        // Build a DataRow with two values: "hello" and NULL
        let mut payload = Vec::new();
        payload.extend_from_slice(&2_i16.to_be_bytes()); // 2 columns
        // First value: "hello"
        payload.extend_from_slice(&5_i32.to_be_bytes());
        payload.extend_from_slice(b"hello");
        // Second value: NULL
        payload.extend_from_slice(&(-1_i32).to_be_bytes());

        let row = PgClient::parse_data_row(&payload).unwrap();
        assert_eq!(row, vec!["hello".to_string(), "NULL".to_string()]);
    }
}
