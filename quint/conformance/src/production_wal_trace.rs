//! Public Nucleus WAL trace adapter, pinned by scripts/production_trace.py.
//! Independent oracle is a literal ordered event list, not the WAL decoder's logic.
#[cfg(test)]
mod tests {
    use nucleus::storage::page::PAGE_SIZE;
    use nucleus::storage::wal::{Wal, read_wal_records};
    use std::{fs, time::{SystemTime, UNIX_EPOCH}};

    #[test]
    fn public_wal_append_sync_reopen_trace() {
        let dir = std::env::temp_dir().join(format!("nucleus-public-wal-{}-{}",std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()));
        fs::create_dir(&dir).unwrap();
        let path = dir.join("trace.wal");
        let image = [0x5a; PAGE_SIZE];
        let body = [9u8, 8, 7];
        let lsns;
        {
            let wal = Wal::open(&path).unwrap();
            lsns = vec![wal.log_page_write(10, 7, &image).unwrap(),
                wal.log_commit(10, Some(&body)).unwrap(),
                wal.log_abort(20).unwrap(), wal.log_checkpoint().unwrap()];
            wal.sync().unwrap();
        }
        let expected = [(10,0,7),(10,1,0),(20,2,0),(0,3,0)];
        for _ in 0..2 {
            let wal = Wal::open(&path).unwrap();
            let records = read_wal_records(&path).unwrap();
            assert_eq!(records.len(), expected.len(), "exact committed record set");
            for (index, (record, &(tx, kind, page))) in records.iter().zip(&expected).enumerate() {
                assert_eq!((record.txn_id,record.record_type,record.page_id),(tx,kind,page));
                assert_eq!(record.lsn,lsns[index]);
                if index > 0 { assert!(record.lsn > records[index-1].lsn); }
            }
            assert_eq!(records[0].page_image.as_ref().unwrap().as_ref(), &image);
            assert_eq!(records[1].control_body.as_deref(),Some(body.as_slice()));
            assert!(records[2].control_body.is_none());
            drop(wal);
        }
        fs::remove_dir_all(dir).unwrap();
    }
}
