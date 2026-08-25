//! PostgreSQL reader: connects over the wire with `tokio-postgres` (already
//! in the dependency tree behind the `server` feature) and reads schema plus
//! data from a live server. The schema path is deliberately narrow — tables,
//! columns, types, constraints, rows — not an information_schema scraper:
//! views, functions, triggers, partitions and sequences are not enumerated,
//! and the in-tree client has no TLS support, so connections are plaintext.

use super::{
    BoxFut, RowStream, SourceColumn, SourceConstraint, SourceConstraintKind, SourceDb, SourceError,
    SourceTable, SourceValue,
};
use tokio_postgres::NoTls;

use std::str::FromStr as _;

pub struct PgSource {
    client: tokio_postgres::Client,
    detail: String,
}

impl PgSource {
    /// Connect to `url` (e.g. `postgres://user:pass@host:5432/db`). SslMode
    /// Prefer/Require are downgraded to Disabled because no TLS connector is
    /// compiled in; a server that requires TLS will refuse the connection.
    pub async fn connect(url: &str) -> Result<Self, SourceError> {
        let mut config = tokio_postgres::Config::from_str(url)
            .map_err(|e| SourceError(format!("parsing connection string: {e}")))?;
        if matches!(
            config.get_ssl_mode(),
            tokio_postgres::config::SslMode::Prefer | tokio_postgres::config::SslMode::Require
        ) {
            config.ssl_mode(tokio_postgres::config::SslMode::Disable);
        }
        let (client, connection) = config
            .connect(NoTls)
            .await
            .map_err(|e| SourceError(format!("connecting: {e}")))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(Self {
            client,
            detail: redact_url(url),
        })
    }
}

/// Mask the password in a URL for display. Only the userinfo segment between
/// "://" and the next "@" is touched.
fn redact_url(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let rest = &url[scheme_end + 3..];
    let Some(at) = rest.find('@') else {
        return url.to_string();
    };
    let userinfo = &rest[..at];
    match userinfo.find(':') {
        Some(colon) => format!(
            "{}{}:***@{}",
            &url[..scheme_end + 3],
            &userinfo[..colon],
            &rest[at + 1..]
        ),
        None => url.to_string(),
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

impl SourceDb for PgSource {
    fn kind(&self) -> &'static str {
        "postgresql"
    }

    fn detail(&self) -> String {
        self.detail.clone()
    }

    fn tables(&mut self) -> BoxFut<'_, Result<Vec<SourceTable>, SourceError>> {
        Box::pin(async move {
            let table_rows = self
                .client
                .query(
                    "SELECT table_name FROM information_schema.tables \
                     WHERE table_schema = 'public' AND table_type = 'BASE TABLE' \
                     ORDER BY table_name",
                    &[],
                )
                .await
                .map_err(|e| SourceError(format!("reading table list: {e}")))?;
            let column_rows = self
                .client
                .query(
                    "SELECT table_name, column_name, is_nullable, data_type, udt_name, \
                     column_default FROM information_schema.columns \
                     WHERE table_schema = 'public' ORDER BY table_name, ordinal_position",
                    &[],
                )
                .await
                .map_err(|e| SourceError(format!("reading columns: {e}")))?;
            let constraint_rows = self
                .client
                .query(
                    "SELECT conrelid::regclass::text AS table_name, conname, contype, \
                     pg_get_constraintdef(oid) AS def FROM pg_constraint \
                     WHERE connamespace = 'public'::regnamespace ORDER BY conrelid, conname",
                    &[],
                )
                .await
                .map_err(|e| SourceError(format!("reading constraints: {e}")))?;

            let mut tables: Vec<SourceTable> = table_rows
                .iter()
                .map(|r| SourceTable {
                    name: r.get("table_name"),
                    columns: Vec::new(),
                    constraints: Vec::new(),
                })
                .collect();
            for row in &column_rows {
                let table_name: String = row.get("table_name");
                let Some(table) = tables.iter_mut().find(|t| t.name == table_name) else {
                    continue;
                };
                let nullable: String = row.get("is_nullable");
                let default: Option<String> = row.get("column_default");
                table.columns.push(SourceColumn {
                    name: row.get("column_name"),
                    type_name: row.get("data_type"),
                    udt_name: Some(row.get("udt_name")),
                    nullable: nullable.eq_ignore_ascii_case("yes"),
                    default_expr: default,
                });
            }
            for row in &constraint_rows {
                let table_name: String = row.get("table_name");
                let Some(table) = tables.iter_mut().find(|t| t.name == table_name) else {
                    continue;
                };
                let contype: i8 = row.get("contype");
                let kind = match contype as u8 as char {
                    'p' => SourceConstraintKind::PrimaryKey,
                    'u' => SourceConstraintKind::Unique,
                    'f' => SourceConstraintKind::ForeignKey,
                    'c' => SourceConstraintKind::Check,
                    _ => continue,
                };
                table.constraints.push(SourceConstraint {
                    kind,
                    name: Some(row.get("conname")),
                    definition: row.get("def"),
                });
            }
            Ok(tables)
        })
    }

    fn scan<'a>(
        &'a mut self,
        table: &'a SourceTable,
    ) -> BoxFut<'a, Result<Box<dyn RowStream + 'a>, SourceError>> {
        Box::pin(async move {
            // Every column is cast to text server-side: Nucleus's INSERT
            // coercion then parses the literal per target column type, the
            // same path a logical-dump restore takes. NULL stays NULL.
            let select_list = table
                .columns
                .iter()
                .map(|c| format!("{}::text", quote_ident(&c.name)))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT {select_list} FROM public.{}",
                quote_ident(&table.name)
            );
            let rows = self
                .client
                .query(&sql, &[])
                .await
                .map_err(|e| SourceError(format!("reading table {}: {e}", table.name)))?;
            let materialized: Result<Vec<Vec<SourceValue>>, String> = rows
                .iter()
                .map(|row| {
                    (0..table.columns.len())
                        .map(|i| {
                            match row.try_get::<_, Option<String>>(i) {
                                Ok(Some(s)) => Ok(SourceValue::Quoted(s)),
                                Ok(None) => Ok(SourceValue::Null),
                                // Every column was cast to text server-side,
                                // so a decode failure is a real defect — fail
                                // the scan loudly rather than write NULLs.
                                Err(e) => Err(format!(
                                    "table {} column {}: {e}",
                                    table.name, table.columns[i].name
                                )),
                            }
                        })
                        .collect()
                })
                .collect();
            let rows =
                materialized.map_err(|e| SourceError(format!("decoding row values: {e}")))?;
            Ok(Box::new(PgRows { rows, pos: 0 }) as Box<dyn RowStream>)
        })
    }
}

struct PgRows {
    rows: Vec<Vec<SourceValue>>,
    pos: usize,
}

impl RowStream for PgRows {
    fn next_batch<'a>(
        &'a mut self,
    ) -> BoxFut<'a, Result<Option<Vec<Vec<SourceValue>>>, SourceError>> {
        Box::pin(async move {
            if self.pos >= self.rows.len() {
                return Ok(None);
            }
            let take = &self.rows[self.pos..];
            let n = take.len().min(256);
            let batch = take[..n].to_vec();
            self.pos += n;
            Ok(Some(batch))
        })
    }
}
