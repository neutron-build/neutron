//! Typed DDL entry points (C-T6 §3, §10). Every DDL here is transactional:
//! the catalog rows go through the §5 write path inside the caller's txn,
//! so an abort or `ROLLBACK TO` leaves no row and no mapped storage.

use nucleus_codec::row::TableSchema;
use nucleus_kv::OrderedKv;

use nucleus_txn::catalog::{Ddl, RelKind};

use crate::db::{Db, Stmt};
use crate::error::DbError;

impl<K: OrderedKv> Db<K> {
    /// CREATE TABLE: encode the definition (§3 bytes), then
    /// [`Catalog::catalog_create_table`](nucleus_txn::catalog::Catalog::catalog_create_table)
    /// — AccessExclusive on the fresh oid, the storage range mapped, the
    /// `/sys/catalog/rel/{oid}` row placed through the write path.
    /// Returns `(oid, storage_id)`.
    pub fn create_table(
        &self,
        st: &Stmt<'_>,
        name: &[u8],
        schema: &TableSchema,
    ) -> Result<(u64, u64), DbError> {
        let def = schema.encode()?;
        let ddl = Ddl::new(&self.core, st.txn(), st.ctx());
        self.catalog
            .catalog_create_table(&ddl, name, RelKind::Table, &def)
            .map_err(DbError::Txn)
    }
}
