//! Secret redaction for logs and operator-facing status output.
//!
//! One place decides what counts as a secret, so a password cannot reach a
//! log line just because a new call site formatted a struct with `{:?}` or
//! echoed a statement back to the operator.
//!
//! The rule is deliberately conservative: anything whose *name* looks like a
//! credential is masked, and SQL text is scrubbed of the literal that follows
//! a credential-bearing keyword before it is logged.

/// The replacement written in place of any secret.
pub const REDACTED: &str = "[REDACTED]";

/// Substrings that mark a configuration/parameter name as credential-bearing.
const SECRET_MARKERS: [&str; 17] = [
    "password",
    "passwd",
    "passphrase",
    "secret",
    "token",
    "credential",
    "private_key",
    "privatekey",
    "api_key",
    "apikey",
    "encrypt_key",
    "encryption_key",
    "signing_key",
    "master_key",
    "session_key",
    "tls_key",
    "ssl_key",
];

/// Whether a configuration key, environment variable, or parameter name
/// designates a secret.
pub fn is_secret_key(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    if SECRET_MARKERS.iter().any(|m| lower.contains(m)) {
        return true;
    }
    // A bare `key` setting is a credential. Deliberately NOT a generic
    // `*_key` suffix rule: `primary_key`, `partition_key`, and `sort_key`
    // are structural, and masking them would corrupt status output while
    // teaching operators to ignore the redaction marker.
    lower == "key" || lower.ends_with(".key") || lower.ends_with("/key")
}

/// Mask a value if its key names a secret; otherwise return it unchanged.
pub fn redact_value<'a>(key: &str, value: &'a str) -> std::borrow::Cow<'a, str> {
    if is_secret_key(key) && !value.is_empty() {
        std::borrow::Cow::Borrowed(REDACTED)
    } else {
        std::borrow::Cow::Borrowed(value)
    }
}

/// Redact a `key = value`, `key: value`, or `KEY=value` line.
///
/// Lines whose key is not credential-bearing are returned unchanged, so this
/// is safe to run over an entire rendered config block.
pub fn redact_line(line: &str) -> String {
    for sep in ['=', ':'] {
        if let Some(idx) = line.find(sep) {
            let key = line[..idx].trim().trim_matches('"');
            if is_secret_key(key) {
                let value_start = idx + sep.len_utf8();
                let value = &line[value_start..];
                if value.trim().is_empty() {
                    return line.to_string();
                }
                // Preserve the original leading whitespace of the value so
                // rendered blocks keep their alignment.
                let ws: String = value.chars().take_while(|c| c.is_whitespace()).collect();
                return format!("{}{sep}{ws}{REDACTED}", &line[..idx]);
            }
        }
    }
    line.to_string()
}

/// Strip the password from a URI-style connection string.
///
/// `postgres://user:hunter2@host:5432/db` → `postgres://user:[REDACTED]@host:5432/db`
pub fn redact_connection_string(conn: &str) -> String {
    let Some(scheme_end) = conn.find("://") else {
        return redact_kv_connection_string(conn);
    };
    let (scheme, rest) = conn.split_at(scheme_end + 3);
    // The userinfo section ends at the first `@` before any `/`.
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    let Some(at) = authority.rfind('@') else {
        return conn.to_string();
    };
    let userinfo = &authority[..at];
    let Some(colon) = userinfo.find(':') else {
        return conn.to_string();
    };
    format!("{scheme}{}:{REDACTED}{}", &userinfo[..colon], &rest[at..])
}

/// Redact `password=...` in libpq keyword/value connection strings.
fn redact_kv_connection_string(conn: &str) -> String {
    conn.split_whitespace()
        .map(redact_line)
        .collect::<Vec<_>>()
        .join(" ")
}

/// Keywords after which a SQL literal or identifier is a credential.
const SQL_SECRET_KEYWORDS: [&str; 4] = ["password", "passphrase", "secret", "encrypted password"];

/// Remove credential literals from SQL text before it is logged.
///
/// `CREATE ROLE app PASSWORD 'hunter2'` → `CREATE ROLE app PASSWORD [REDACTED]`
///
/// This is a *log-safety* scrubber, not a parser: it errs toward masking, and
/// it never changes the statement that is actually executed.
pub fn redact_sql(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    let mut out = String::with_capacity(sql.len());
    let mut cursor = 0usize;

    while cursor < sql.len() {
        // Find the earliest credential keyword at or after `cursor`.
        let Some((kw_start, kw_len)) = SQL_SECRET_KEYWORDS
            .iter()
            .filter_map(|kw| lower[cursor..].find(kw).map(|i| (cursor + i, kw.len())))
            .min_by_key(|(start, len)| (*start, std::cmp::Reverse(*len)))
        else {
            break;
        };

        let kw_end = kw_start + kw_len;
        // Require a word boundary so `password_hash_column` is not treated as
        // the keyword `password`.
        let before_ok = kw_start == 0
            || !sql.as_bytes()[kw_start - 1].is_ascii_alphanumeric()
                && sql.as_bytes()[kw_start - 1] != b'_';
        let after = &sql[kw_end..];
        let after_ok = after
            .as_bytes()
            .first()
            .is_none_or(|b| !b.is_ascii_alphanumeric() && *b != b'_');
        if !before_ok || !after_ok {
            out.push_str(&sql[cursor..kw_end]);
            cursor = kw_end;
            continue;
        }

        out.push_str(&sql[cursor..kw_end]);
        // Skip whitespace and an optional `=`.
        let mut idx = kw_end;
        let bytes = sql.as_bytes();
        while idx < sql.len() && (bytes[idx].is_ascii_whitespace() || bytes[idx] == b'=') {
            idx += 1;
        }
        if idx >= sql.len() {
            out.push_str(&sql[kw_end..]);
            cursor = sql.len();
            break;
        }
        out.push_str(&sql[kw_end..idx]);

        // Mask the following literal/identifier.
        let value_end = if bytes[idx] == b'\'' || bytes[idx] == b'"' {
            let quote = bytes[idx];
            let mut j = idx + 1;
            while j < sql.len() {
                if bytes[j] == quote {
                    // Doubled quote is an escaped quote inside the literal.
                    if j + 1 < sql.len() && bytes[j + 1] == quote {
                        j += 2;
                        continue;
                    }
                    j += 1;
                    break;
                }
                j += 1;
            }
            j
        } else {
            let mut j = idx;
            while j < sql.len() && !bytes[j].is_ascii_whitespace() && bytes[j] != b';' {
                j += 1;
            }
            j
        };
        out.push_str(REDACTED);
        cursor = value_end;
    }

    if cursor < sql.len() {
        out.push_str(&sql[cursor..]);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_names_are_detected() {
        for k in [
            "password",
            "PASSWORD",
            "server.password",
            "NUCLEUS_PASSWORD",
            "passphrase",
            "NUCLEUS_ENCRYPT_KEY",
            "tls_key",
            "api_key",
            "auth_token",
            "client_secret",
            "private_key",
        ] {
            assert!(is_secret_key(k), "{k} should be treated as a secret");
        }
        for k in [
            "host",
            "port",
            "max_connections",
            "primary_key",
            "keyspace",
            "monkey",
            "data_dir",
        ] {
            assert!(!is_secret_key(k), "{k} should NOT be treated as a secret");
        }
    }

    #[test]
    fn key_value_lines_are_masked() {
        assert_eq!(redact_line("password = hunter2"), "password = [REDACTED]");
        assert_eq!(
            redact_line("  NUCLEUS_ENCRYPT_KEY=deadbeef"),
            "  NUCLEUS_ENCRYPT_KEY=[REDACTED]"
        );
        assert_eq!(redact_line("host = 127.0.0.1"), "host = 127.0.0.1");
        // Empty values stay as-is so status output does not imply a secret
        // exists where none is configured.
        assert_eq!(redact_line("password ="), "password =");
    }

    #[test]
    fn connection_strings_lose_their_password() {
        assert_eq!(
            redact_connection_string("postgres://app:hunter2@db.internal:5432/prod"),
            "postgres://app:[REDACTED]@db.internal:5432/prod"
        );
        // No password present — leave it alone rather than mangling it.
        assert_eq!(
            redact_connection_string("postgres://app@db.internal:5432/prod"),
            "postgres://app@db.internal:5432/prod"
        );
        // An `@` in the database name must not confuse the userinfo split.
        assert_eq!(
            redact_connection_string("postgres://app:pw@host/db@name"),
            "postgres://app:[REDACTED]@host/db@name"
        );
        assert_eq!(
            redact_connection_string("host=db user=app password=hunter2"),
            "host=db user=app password=[REDACTED]"
        );
    }

    #[test]
    fn sql_credentials_are_scrubbed_before_logging() {
        assert_eq!(
            redact_sql("CREATE ROLE app LOGIN PASSWORD 'hunter2'"),
            "CREATE ROLE app LOGIN PASSWORD [REDACTED]"
        );
        assert_eq!(
            redact_sql("ALTER ROLE app PASSWORD 'a''b' ;"),
            "ALTER ROLE app PASSWORD [REDACTED] ;"
        );
        assert_eq!(
            redact_sql("alter role app password='x'"),
            "alter role app password=[REDACTED]"
        );
        // Column names that merely contain the word are not literals.
        assert_eq!(
            redact_sql("SELECT password_hash FROM users"),
            "SELECT password_hash FROM users"
        );
        // Ordinary SQL is untouched.
        let plain = "SELECT id, email FROM users WHERE id = 7";
        assert_eq!(redact_sql(plain), plain);
    }

    #[test]
    fn scrubbed_sql_never_retains_the_secret() {
        let secret = "s3cr3t-value";
        for sql in [
            format!("CREATE ROLE r PASSWORD '{secret}'"),
            format!("CREATE ROLE r ENCRYPTED PASSWORD '{secret}'"),
            format!("ALTER ROLE r WITH PASSWORD '{secret}';"),
            format!("CREATE ROLE r PASSWORD {secret}"),
        ] {
            let scrubbed = redact_sql(&sql);
            assert!(
                !scrubbed.contains(secret),
                "secret leaked through redact_sql: {scrubbed}"
            );
            assert!(scrubbed.contains(REDACTED), "{scrubbed}");
        }
    }

    #[test]
    fn redact_value_only_masks_secret_keys() {
        assert_eq!(redact_value("password", "hunter2"), REDACTED);
        assert_eq!(redact_value("host", "hunter2"), "hunter2");
        assert_eq!(redact_value("password", ""), "");
    }
}
