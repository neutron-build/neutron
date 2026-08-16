//! Document/JSON model — DOC_INSERT, DOC_GET, DOC_UPDATE, DOC_DELETE,
//! DOC_QUERY, DOC_PATH, DOC_COUNT.
//!
//! Every method comes in two forms: the plain one addresses the default
//! (unnamed) collection, and the `_in` one addresses a named collection. A
//! document belongs to exactly one collection and an operation naming one sees
//! only that one, so a document in another collection reads as ABSENT rather
//! than erroring — an id must not be usable to probe across the boundary.
//!
//! Ids are bound as TEXT. Nucleus reports a parameter whose type it cannot
//! infer as TEXT, and the driver then refuses to bind an `i64` to it; the
//! engine parses a text-encoded integer id for exactly this reason, so sending
//! the digits is the supported encoding rather than a workaround.

use serde_json;

use crate::error::NucleusError;
use crate::row_ext::RowExt;
use crate::pool::NucleusPool;

/// Handle for document store operations.
pub struct DocumentModel {
    pool: NucleusPool,
}

impl DocumentModel {
    pub(crate) fn new(pool: NucleusPool) -> Self {
        Self { pool }
    }

    /// Insert a JSON document into the default collection.
    pub async fn insert(&self, doc: &serde_json::Value) -> Result<i64, NucleusError> {
        self.insert_in("", doc).await
    }

    /// Insert a JSON document into `collection`. Returns the generated ID.
    pub async fn insert_in(
        &self,
        collection: &str,
        doc: &serde_json::Value,
    ) -> Result<i64, NucleusError> {
        let json_str =
            serde_json::to_string(doc).map_err(|e| NucleusError::Serde(e.to_string()))?;
        let conn = self.pool.get().await?;
        // The one-argument form when no collection is named, so this still
        // works against a server that predates collections.
        let row = if collection.is_empty() {
            conn.client()
                .query_one("SELECT DOC_INSERT($1)", &[&json_str])
                .await
        } else {
            conn.client()
                .query_one("SELECT DOC_INSERT($1, $2)", &[&collection, &json_str])
                .await
        }
        .map_err(NucleusError::Query)?;
        row.get_ck::<i64>(0)
    }

    /// Retrieve a document by ID from the default collection.
    pub async fn get(&self, id: i64) -> Result<Option<serde_json::Value>, NucleusError> {
        self.get_in("", id).await
    }

    /// Retrieve a document by ID from `collection`. A document in another
    /// collection reads as `None`.
    pub async fn get_in(
        &self,
        collection: &str,
        id: i64,
    ) -> Result<Option<serde_json::Value>, NucleusError> {
        let id_text = id.to_string();
        let conn = self.pool.get().await?;
        let row = if collection.is_empty() {
            conn.client()
                .query_one("SELECT DOC_GET($1)", &[&id_text])
                .await
        } else {
            conn.client()
                .query_one("SELECT DOC_GET($1, $2)", &[&collection, &id_text])
                .await
        }
        .map_err(NucleusError::Query)?;
        let raw: Option<String> = row.get(0);
        match raw {
            Some(s) => {
                let val: serde_json::Value =
                    serde_json::from_str(&s).map_err(|e| NucleusError::Serde(e.to_string()))?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    /// Replace a document by ID in the default collection, preserving its ID.
    pub async fn update(&self, id: i64, doc: &serde_json::Value) -> Result<bool, NucleusError> {
        self.update_in("", id, doc).await
    }

    /// Replace a document by ID within `collection`. Returns `false` — not an
    /// error — when the document belongs to a different collection, so one
    /// collection can never overwrite another's document.
    pub async fn update_in(
        &self,
        collection: &str,
        id: i64,
        doc: &serde_json::Value,
    ) -> Result<bool, NucleusError> {
        let json_str =
            serde_json::to_string(doc).map_err(|e| NucleusError::Serde(e.to_string()))?;
        let id_text = id.to_string();
        let conn = self.pool.get().await?;
        let row = if collection.is_empty() {
            conn.client()
                .query_one("SELECT DOC_UPDATE($1, $2)", &[&id_text, &json_str])
                .await
        } else {
            conn.client()
                .query_one(
                    "SELECT DOC_UPDATE($1, $2, $3)",
                    &[&collection, &id_text, &json_str],
                )
                .await
        }
        .map_err(NucleusError::Query)?;
        row.get_ck::<bool>(0)
    }

    /// Delete a document by ID from the default collection.
    pub async fn delete(&self, id: i64) -> Result<bool, NucleusError> {
        self.delete_in("", id).await
    }

    /// Delete a document by ID from `collection`. A document in another
    /// collection is reported as absent and is not removed.
    pub async fn delete_in(&self, collection: &str, id: i64) -> Result<bool, NucleusError> {
        let id_text = id.to_string();
        let conn = self.pool.get().await?;
        let row = if collection.is_empty() {
            conn.client()
                .query_one("SELECT DOC_DELETE($1)", &[&id_text])
                .await
        } else {
            conn.client()
                .query_one("SELECT DOC_DELETE($1, $2)", &[&collection, &id_text])
                .await
        }
        .map_err(NucleusError::Query)?;
        row.get_ck::<bool>(0)
    }

    /// Query the default collection. Returns matching document IDs.
    pub async fn query(&self, filter: &serde_json::Value) -> Result<Vec<i64>, NucleusError> {
        self.query_in("", filter).await
    }

    /// Query one collection. Matches in other collections are not returned.
    pub async fn query_in(
        &self,
        collection: &str,
        filter: &serde_json::Value,
    ) -> Result<Vec<i64>, NucleusError> {
        let filter_str =
            serde_json::to_string(filter).map_err(|e| NucleusError::Serde(e.to_string()))?;
        let conn = self.pool.get().await?;
        let row = if collection.is_empty() {
            conn.client()
                .query_one("SELECT DOC_QUERY($1)", &[&filter_str])
                .await
        } else {
            conn.client()
                .query_one("SELECT DOC_QUERY($1, $2)", &[&collection, &filter_str])
                .await
        }
        .map_err(NucleusError::Query)?;
        let raw: String = row.get(0);
        if raw.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<i64> = raw
            .split(',')
            .filter_map(|s| s.trim().parse::<i64>().ok())
            .collect();
        Ok(ids)
    }

    /// Extract a nested value from a default-collection document.
    pub async fn path(&self, id: i64, keys: &[&str]) -> Result<Option<String>, NucleusError> {
        self.path_in("", id, keys).await
    }

    /// Extract a nested value from a document in `collection`.
    ///
    /// The scoped form is a distinct FUNCTION (`DOC_PATH_IN`) rather than an
    /// extra argument: the key tail is variadic, so a leading collection could
    /// not be told apart from a leading id.
    pub async fn path_in(
        &self,
        collection: &str,
        id: i64,
        keys: &[&str],
    ) -> Result<Option<String>, NucleusError> {
        if keys.is_empty() {
            return Ok(None);
        }
        let scoped = !collection.is_empty();
        let id_text = id.to_string();
        // Build: SELECT DOC_PATH($1, $2, ...) / DOC_PATH_IN($1, $2, $3, ...)
        let base = if scoped { 3 } else { 2 };
        let mut params: Vec<String> = if scoped {
            vec!["$1".to_string(), "$2".to_string()]
        } else {
            vec!["$1".to_string()]
        };
        for (i, _) in keys.iter().enumerate() {
            params.push(format!("${}", i + base));
        }
        let sql = format!(
            "SELECT {}({})",
            if scoped { "DOC_PATH_IN" } else { "DOC_PATH" },
            params.join(", ")
        );

        let conn = self.pool.get().await?;
        // Build dynamic params
        let mut query_params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();
        if scoped {
            query_params.push(&collection);
        }
        query_params.push(&id_text);
        for key in keys {
            query_params.push(key);
        }
        let row = conn
            .client()
            .query_one(&sql, &query_params)
            .await
            .map_err(NucleusError::Query)?;
        row.get_ck::<Option<String>>(0)
    }

    /// Number of documents in the default collection.
    pub async fn count(&self) -> Result<i64, NucleusError> {
        self.count_in("").await
    }

    /// Number of documents in `collection`.
    pub async fn count_in(&self, collection: &str) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = if collection.is_empty() {
            conn.client().query_one("SELECT DOC_COUNT()", &[]).await
        } else {
            conn.client()
                .query_one("SELECT DOC_COUNT($1)", &[&collection])
                .await
        }
        .map_err(NucleusError::Query)?;
        row.get_ck::<i64>(0)
    }
    /// Find whole documents matching `filter` in `collection`.
    ///
    /// `query_in` answers with ids; the engine has no filter surface that
    /// returns documents, so this resolves ids and fetches each one. That is
    /// what the Python and Elixir clients do — the cross-SDK contract is
    /// defined by behaviour, not by where the loop runs.
    pub async fn find(
        &self,
        collection: &str,
        filter: &serde_json::Value,
        limit: usize,
        skip: usize,
    ) -> Result<Vec<serde_json::Value>, NucleusError> {
        Ok(self
            .find_with_ids(collection, filter, limit, skip)
            .await?
            .into_iter()
            .map(|(_id, doc)| doc)
            .collect())
    }

    /// Find the first document matching `filter`, or `None`.
    ///
    /// Absence is `Ok(None)`, not an error — matching `get_in`.
    pub async fn find_one(
        &self,
        collection: &str,
        filter: &serde_json::Value,
    ) -> Result<Option<serde_json::Value>, NucleusError> {
        Ok(self
            .find_with_ids(collection, filter, 1, 0)
            .await?
            .into_iter()
            .next()
            .map(|(_id, doc)| doc))
    }

    /// Merge `patch` into every document matching `filter`. Returns the count.
    ///
    /// A PARTIAL update: keys in `patch` overwrite, keys absent from it
    /// survive. `update_in` replaces a whole document by id and is a different
    /// operation despite the shared name root.
    pub async fn update_where(
        &self,
        collection: &str,
        filter: &serde_json::Value,
        patch: &serde_json::Value,
    ) -> Result<usize, NucleusError> {
        let pairs = self.find_with_ids(collection, filter, 10_000, 0).await?;
        let mut n = 0;
        for (id, existing) in pairs {
            let mut merged = existing;
            if let (Some(dst), Some(src)) = (merged.as_object_mut(), patch.as_object()) {
                for (k, v) in src {
                    dst.insert(k.clone(), v.clone());
                }
            }
            let ok = if collection.is_empty() {
                self.update(id, &merged).await?
            } else {
                self.update_in(collection, id, &merged).await?
            };
            if ok {
                n += 1;
            }
        }
        Ok(n)
    }

    /// Delete every document matching `filter`. Returns the count.
    pub async fn delete_where(
        &self,
        collection: &str,
        filter: &serde_json::Value,
    ) -> Result<usize, NucleusError> {
        let pairs = self.find_with_ids(collection, filter, 10_000, 0).await?;
        let mut n = 0;
        for (id, _doc) in pairs {
            let ok = if collection.is_empty() {
                self.delete(id).await?
            } else {
                self.delete_in(collection, id).await?
            };
            if ok {
                n += 1;
            }
        }
        Ok(n)
    }

    async fn find_with_ids(
        &self,
        collection: &str,
        filter: &serde_json::Value,
        limit: usize,
        skip: usize,
    ) -> Result<Vec<(i64, serde_json::Value)>, NucleusError> {
        let ids = if collection.is_empty() {
            self.query(filter).await?
        } else {
            self.query_in(collection, filter).await?
        };
        let mut out = Vec::new();
        for id in ids.into_iter().skip(skip).take(limit) {
            let doc = if collection.is_empty() {
                self.get(id).await?
            } else {
                self.get_in(collection, id).await?
            };
            if let Some(d) = doc {
                out.push((id, d));
            }
        }
        Ok(out)
    }

}
