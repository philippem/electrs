use std::collections::BTreeSet;
use std::convert::TryInto;
use std::str;

use itertools::Itertools;
use log::{debug, info, trace};
use rocksdb::WriteBatch;

use bitcoin::hashes::Hash;

use electrs::chain::{BlockHash, Txid};
use electrs::new_index::db::DBFlush;
use electrs::new_index::schema::{
    lookup_confirmations, FullHash, Store, TxConfRow as V2TxConfRow, TxEdgeRow as V2TxEdgeRow,
    TxHistoryKey,
};
use electrs::util::bincode::{deserialize_big, deserialize_little, serialize_little};
use electrs::{config::Config, metrics::Metrics};

const FROM_DB_VERSION: u32 = 1;
const TO_DB_VERSION: u32 = 2;

const BATCH_SIZE: usize = 15000;
const PROGRESS_EVERY: usize = BATCH_SIZE * 50;

// For Elements-based chains the 'I' asset history index is migrated too
#[cfg(not(feature = "liquid"))]
const HISTORY_PREFIXES: [u8; 1] = [b'H'];
#[cfg(feature = "liquid")]
const HISTORY_PREFIXES: [u8; 2] = [b'H', b'I'];

fn main() {
    let config = Config::from_args();
    let metrics = Metrics::new(config.monitoring_addr);
    let store = Store::open(&config, &metrics, false);

    let txstore_db = store.txstore_db();
    let history_db = store.history_db();
    let cache_db = store.cache_db();
    let headers = store.headers();
    let tip_height = headers.best_height() as u32;

    // Check the DB version under `V` matches the expected version
    for db in [txstore_db, history_db, cache_db] {
        let ver_bytes = db.get(b"V").expect("missing DB version");
        let ver: u32 = deserialize_little(&ver_bytes[0..4]).unwrap();
        assert_eq!(ver, FROM_DB_VERSION, "unexpected DB version {}", ver);
    }

    // Utility to log progress once every PROGRESS_EVERY ticks
    let mut tick = 0usize;
    macro_rules! progress {
        ($($arg:tt)+) => {{
            tick = tick.wrapping_add(1);
            if tick % PROGRESS_EVERY == 0 {
                debug!($($arg)+);
            }
        }};
    }

    // 1. Migrate the address prefix search index
    // Moved as-is from the history db to the txstore db
    info!("[1/4] migrating address prefix search index...");
    let address_iter = history_db.iter_scan(b"a");
    for chunk in &address_iter.chunks(BATCH_SIZE) {
        let mut batch = WriteBatch::default();
        for row in chunk {
            progress!("[1/4] at {}", str::from_utf8(&row.key[1..]).unwrap());
            batch.put(row.key, row.value);
        }
        // Write batches without flushing (sync and WAL disabled)
        trace!("[1/4] writing batch of {} ops", batch.len());
        txstore_db.write_batch(batch, DBFlush::Disable);
    }
    // Flush the txstore db, only then delete the original rows from the history db
    info!("[1/4] flushing V2 address index to txstore db");
    txstore_db.flush();
    info!("[1/4] deleting V1 address index from history db");
    history_db.delete_range(b"a", b"b", DBFlush::Enable);

    // 2. Migrate the TxConf transaction confirmation index
    // - Moved from the txstore db to the history db
    // - Changed from a set of blocks seen to include the tx to a single block (that is part of the best chain)
    // - Changed from the block hash to the block height
    // - Entries originating from stale blocks are removed
    // Steps 3/4 depend on this index getting migrated first
    info!("[2/4] migrating TxConf index...");
    let txconf_iter = txstore_db.iter_scan(b"C");
    for chunk in &txconf_iter.chunks(BATCH_SIZE) {
        let mut batch = WriteBatch::default();
        for v1_row in chunk {
            let v1_txconf: V1TxConfKey =
                deserialize_little(&v1_row.key).expect("invalid TxConfKey");
            let blockhash = BlockHash::from_byte_array(v1_txconf.blockhash);
            if let Some(header) = headers.header_by_blockhash(&blockhash) {
                // The blockhash is still part of the best chain, use its height to construct the V2 row
                let v2_row = V2TxConfRow::new(v1_txconf.txid, header.height() as u32).into_row();
                batch.put(v2_row.key, v2_row.value);
            } else {
                // The transaction was reorged, don't write the V2 entry
                // trace!("[2/4] skipping reorged TxConf for {}", Txid::from_byte_array(txconf.txid));
            }
            progress!(
                "[2/4] migrating TxConf index ~{:.2}%",
                est_hash_progress(&v1_txconf.txid)
            );
        }
        // Write batches without flushing (sync and WAL disabled)
        trace!("[2/4] writing batch of {} ops", batch.len());
        history_db.write_batch(batch, DBFlush::Disable);
    }
    // Flush the history db, only then delete the original rows from the txstore db
    info!("[2/4] flushing V2 TxConf to history db");
    history_db.flush();
    info!("[2/4] deleting V1 TxConf from txstore db");
    txstore_db.delete_range(b"C", b"D", DBFlush::Enable);

    // 3. Migrate the TxEdge spending index
    // - Changed from a set of inputs seen to spend the outpoint to a single spending input (that is part of the best chain)
    // - Keep the height of the spending tx
    // - Entries originating from stale blocks are removed
    info!("[3/4] migrating TxEdge index...");
    let txedge_iter = history_db.iter_scan(b"S");
    for chunk in &txedge_iter.chunks(BATCH_SIZE) {
        let mut v1_edges = Vec::with_capacity(BATCH_SIZE);
        let mut spending_txids = BTreeSet::new();
        for v1_row in chunk {
            if let Ok(v1_edge) = deserialize_little::<V1TxEdgeKey>(&v1_row.key) {
                spending_txids.insert(Txid::from_byte_array(v1_edge.spending_txid));
                v1_edges.push((v1_edge, v1_row.key));
            }
            // Rows with keys that cannot be deserialized into V1TxEdgeKey are assumed to already be upgraded, and skipped
            // This is necessary to properly recover if the migration stops halfway through.
        }

        // Lookup the confirmation status for the entire chunk using a MultiGet operation
        let confirmations = lookup_confirmations(history_db, tip_height, spending_txids);

        let mut batch = WriteBatch::default();
        for (v1_edge, v1_db_key) in v1_edges {
            let spending_txid = Txid::from_byte_array(v1_edge.spending_txid);

            // Remove the old V1 entry. V2 entries use a different key.
            batch.delete(v1_db_key);

            if let Some(spending_height) = confirmations.get(&spending_txid) {
                // Re-add the V2 entry if it is still part of the best chain
                let v2_row = V2TxEdgeRow::new(
                    v1_edge.funding_txid,
                    v1_edge.funding_vout,
                    v1_edge.spending_txid,
                    v1_edge.spending_vin,
                    *spending_height, // now with the height included
                )
                .into_row();
                batch.put(v2_row.key, v2_row.value);
            } else {
                // The spending transaction was reorged, don't write the V2 entry
                //trace!("[3/4] skipping reorged TxEdge for {}", spending_txid);
            }

            progress!(
                "[3/4] migrating TxEdge index ~{:.2}%",
                est_hash_progress(&v1_edge.funding_txid)
            );
        }
        // Write batches without flushing (sync and WAL disabled)
        trace!("[3/4] writing batch of {} ops", batch.len());
        history_db.write_batch(batch, DBFlush::Disable);
    }
    info!("[3/4] flushing V2 TxEdge index to history db");
    history_db.flush();

    // 4. Migrate the TxHistory index
    // Entries originating from stale blocks are removed, with no other changes
    info!("[4/4] migrating TxHistory index...");
    for prefix in HISTORY_PREFIXES {
        let txhistory_iter = history_db.iter_scan(&[prefix]);
        info!("[4/4] migrating TxHistory index {}", prefix as char);
        for chunk in &txhistory_iter.chunks(BATCH_SIZE) {
            let mut history_entries = Vec::with_capacity(BATCH_SIZE);
            let mut history_txids = BTreeSet::new();
            for row in chunk {
                let hist: TxHistoryKey = deserialize_big(&row.key).expect("invalid TxHistoryKey");
                history_txids.insert(hist.txinfo.get_txid());
                history_entries.push((hist, row.key));
            }

            // Lookup the confirmation status for the entire chunk using a MultiGet operation
            let confirmations = lookup_confirmations(history_db, tip_height, history_txids);

            let mut batch = WriteBatch::default();
            for (hist, db_key) in history_entries {
                let hist_txid = hist.txinfo.get_txid();
                if confirmations.get(&hist_txid) != Some(&hist.confirmed_height) {
                    // The history entry originated from a stale block, remove it
                    batch.delete(db_key);
                    // trace!("[4/4] removing reorged TxHistory for {}", hist.txinfo.get_txid());
                }
                progress!(
                    "[4/4] migrating TxHistory index {} ~{:.2}%",
                    prefix as char,
                    est_hash_progress(&hist.hash)
                );
            }
            // Write batches without flushing (sync and WAL disabled)
            trace!("[4/4] writing batch of {} deletions", batch.len());
            if !batch.is_empty() {
                history_db.write_batch(batch, DBFlush::Disable);
            }
        }
    }
    info!("[4/4] flushing TxHistory deletions to history db");
    history_db.flush();

    // Update the DB version under `V`
    let ver_bytes = serialize_little(&(TO_DB_VERSION, config.light_mode)).unwrap();
    for db in [txstore_db, history_db, cache_db] {
        db.put_sync(b"V", &ver_bytes);
    }

    // Compact everything once at the end
    txstore_db.full_compaction();
    history_db.full_compaction();
}

// Estimates progress using the first 4 bytes, relying on RocksDB's lexicographic key ordering and uniform hash distribution
fn est_hash_progress(hash: &FullHash) -> f32 {
    u32::from_be_bytes(hash[0..4].try_into().unwrap()) as f32 / u32::MAX as f32 * 100f32
}

#[derive(Debug, serde::Deserialize)]
struct V1TxConfKey {
    #[allow(dead_code)]
    code: u8,
    txid: FullHash,
    blockhash: FullHash,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct V1TxEdgeKey {
    code: u8,
    funding_txid: FullHash,
    funding_vout: u16,
    spending_txid: FullHash,
    spending_vin: u16,
}

/*
use bitcoin::hex::DisplayHex;

fn dump_db(db: &DB, label: &str, prefix: &[u8]) {
    debug!("dumping {}", label);
    for item in db.iter_scan(prefix) {
        trace!(
            "[{}] {} => {}",
            label,
            fmt_key(&item.key),
            &item.value.to_lower_hex_string()
        );
    }
}

fn debug_batch(batch: &WriteBatch, label: &'static str) {
    debug!("batch {} with {} ops", label, batch.len());
    batch.iterate(&mut WriteBatchLogIterator(label));
}

struct WriteBatchLogIterator(&'static str);
impl rocksdb::WriteBatchIterator for WriteBatchLogIterator {
    fn put(&mut self, key: Box<[u8]>, value: Box<[u8]>) {
        trace!(
            "[batch {}] PUT {} => {}",
            self.0,
            fmt_key(&key),
            value.to_lower_hex_string()
        );
    }
    fn delete(&mut self, key: Box<[u8]>) {
        trace!("[batch {}] DELETE {}", self.0, fmt_key(&key));
    }
}

fn fmt_key(key: &[u8]) -> String {
    format!("{}-{}", key[0] as char, &key[1..].to_lower_hex_string())
}
*/
