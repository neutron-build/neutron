//! SQLite reader: opens a database file read-only via `rusqlite` (bundled,
//! feature-gated) and reads tables, columns, foreign keys and rows. UNIQUE
//! constraints are reconstructed from `PRAGMA index_list`/`index_info`.

use super::{
    BoxFut, RowStream, SourceColumn, SourceConstraint, SourceConstraintKind, SourceDb, SourceError,
    SourceTable, SourceValue,
};
use rusqlite::OpenFlags;
use rusqlite::types::ValueRef;
use std::path::Path;

pub struct SqliteSource {
    conn: rusqlite::Connection,
    detail: String,
}

impl SqliteSource {
    pub fn open(path: &Path) -> Result<Self, SourceError> {
        let conn = rusqlite::Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|e| SourceError(format!("opening {}: {e}", path.display())))?;
        Ok(Self {
            conn,
            detail: path.display().to_string(),
        })
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

impl SourceDb for SqliteSource {
    fn kind(&self) -> &'static str {
        "sqlite"
    }

    fn detail(&self) -> String {
        self.detail.clone()
    }

    fn tables(&mut self) -> BoxFut<'_, Result<Vec<SourceTable>, SourceError>> {
        Box::pin(async move {
            let mut stmt = self
                .conn
                .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
                .map_err(|e| SourceError(format!("reading sqlite_master: {e}")))?;
            let names: Vec<String> = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|e| SourceError(e.to_string()))?
                .filter_map(|r| r.ok())
                .collect();
            drop(stmt);

            let mut tables = Vec::new();
            for name in names {
                let mut columns = Vec::new();
                let mut constraints: Vec<SourceConstraint> = Vec::new();
                let mut pk_cols: Vec<(i64, String)> = Vec::new();

                let pragma = format!("PRAGMA table_info({})", quote_ident(&name));
                let mut stmt = self
                    .conn
                    .prepare(&pragma)
                    .map_err(|e| SourceError(format!("{pragma}: {e}")))?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, Option<String>>(4)?,
                            row.get::<_, i64>(5)?,
                        ))
                    })
                    .map_err(|e| SourceError(format!("{pragma}: {e}")))?;
                for col in rows.flatten() {
                    let (col_name, declared, notnull, default, pk) = col;
                    if pk > 0 {
                        pk_cols.push((pk, col_name.clone()));
                    }
                    columns.push(SourceColumn {
                        name: col_name,
                        type_name: declared,
                        udt_name: None,
                        nullable: notnull == 0,
                        default_expr: default,
                    });
                }
                drop(stmt);

                if !pk_cols.is_empty() {
                    pk_cols.sort();
                    constraints.push(SourceConstraint {
                        kind: SourceConstraintKind::PrimaryKey,
                        name: None,
                        definition: format!(
                            "PRIMARY KEY ({})",
                            pk_cols
                                .iter()
                                .map(|(_, c)| quote_ident(c))
                                .collect::<Vec<_>>()
                                .join(", ")
                        ),
                    });
                }

                let pragma = format!("PRAGMA foreign_key_list({})", quote_ident(&name));
                let mut stmt = self
                    .conn
                    .prepare(&pragma)
                    .map_err(|e| SourceError(format!("{pragma}: {e}")))?;
                let fks = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                        ))
                    })
                    .map_err(|e| SourceError(format!("{pragma}: {e}")))?;
                for fk in fks.flatten() {
                    let (ref_table, from, to) = fk;
                    constraints.push(SourceConstraint {
                        kind: SourceConstraintKind::ForeignKey,
                        name: None,
                        definition: format!(
                            "FOREIGN KEY ({}) REFERENCES {} ({})",
                            quote_ident(&from),
                            quote_ident(&ref_table),
                            quote_ident(&to)
                        ),
                    });
                }
                drop(stmt);

                // UNIQUE constraints surface as auto indexes; skip partial
                // indexes (they are not constraints) and the PK's own index.
                let pragma = format!("PRAGMA index_list({})", quote_ident(&name));
                let mut stmt = match self.conn.prepare(&pragma) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let idx_rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, i64>(4)?,
                        ))
                    })
                    .map_err(|e| SourceError(format!("{pragma}: {e}")))?;
                for (idx_name, unique, origin, partial) in idx_rows.flatten() {
                    if unique == 0 || partial != 0 || origin == "pk" {
                        continue;
                    }
                    let info_pragma = format!("PRAGMA index_info({})", quote_ident(&idx_name));
                    let Ok(mut info) = self.conn.prepare(&info_pragma) else {
                        continue;
                    };
                    let cols = info
                        .query_map([], |row| row.get::<_, String>(2))
                        .map(|mapped| mapped.filter_map(|r| r.ok()).collect::<Vec<_>>())
                        .unwrap_or_default();
                    if cols.is_empty() {
                        continue;
                    }
                    constraints.push(SourceConstraint {
                        kind: SourceConstraintKind::Unique,
                        name: None,
                        definition: format!(
                            "UNIQUE ({})",
                            cols.iter()
                                .map(|c| quote_ident(c))
                                .collect::<Vec<_>>()
                                .join(", ")
                        ),
                    });
                }
                drop(stmt);

                tables.push(SourceTable {
                    name,
                    columns,
                    constraints,
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
            let sql = format!("SELECT * FROM {}", quote_ident(&table.name));
            let mut stmt = self
                .conn
                .prepare(&sql)
                .map_err(|e| SourceError(format!("reading table {}: {e}", table.name)))?;
            let ncols = stmt.column_count();
            let rows = stmt
                .query_map([], |row| {
                    (0..ncols)
                        .map(|i| match row.get_ref(i) {
                            Ok(ValueRef::Null) => Ok(SourceValue::Null),
                            Ok(ValueRef::Integer(n)) => Ok(SourceValue::Raw(n.to_string())),
                            Ok(ValueRef::Real(f)) => Ok(SourceValue::Raw(f.to_string())),
                            Ok(ValueRef::Text(t)) => {
                                Ok(SourceValue::Quoted(String::from_utf8_lossy(t).into_owned()))
                            }
                            // Bytea columns are imported through the same
                            // '\x' hex text form a logical dump uses.
                            Ok(ValueRef::Blob(b)) => Ok(SourceValue::Quoted(format!(
                                "\\x{}",
                                b.iter().map(|x| format!("{x:02x}")).collect::<String>()
                            ))),
                            Err(e) => Err(e),
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .map_err(|e| SourceError(format!("reading table {}: {e}", table.name)))?;
            let mut materialized = Vec::new();
            for row in rows {
                let row = row.map_err(|e| SourceError(format!("decoding row: {e}")))?;
                materialized.push(row);
            }
            Ok(Box::new(SqliteRows {
                rows: materialized,
                pos: 0,
            }) as Box<dyn RowStream>)
        })
    }
}

struct SqliteRows {
    rows: Vec<Vec<SourceValue>>,
    pos: usize,
}

impl RowStream for SqliteRows {
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
