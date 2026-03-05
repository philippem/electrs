use crate::metrics::{GaugeVec, MetricOpts, Metrics};

#[derive(Debug)]
pub struct RocksDbMetrics {
    // Memory table metrics
    pub num_immutable_mem_table: GaugeVec,
    pub mem_table_flush_pending: GaugeVec,
    pub cur_size_active_mem_table: GaugeVec,
    pub cur_size_all_mem_tables: GaugeVec,
    pub size_all_mem_tables: GaugeVec,
    pub num_entries_active_mem_table: GaugeVec,
    pub num_entries_imm_mem_tables: GaugeVec,
    pub num_deletes_active_mem_table: GaugeVec,
    pub num_deletes_imm_mem_tables: GaugeVec,

    // Compaction metrics
    pub compaction_pending: GaugeVec,
    pub estimate_pending_compaction_bytes: GaugeVec,
    pub num_running_compactions: GaugeVec,
    pub num_running_flushes: GaugeVec,

    // Error metrics
    pub background_errors: GaugeVec,

    // Key and data size estimates
    pub estimate_num_keys: GaugeVec,
    pub estimate_live_data_size: GaugeVec,
    pub estimate_oldest_key_time: GaugeVec,

    // Table reader memory
    pub estimate_table_readers_mem: GaugeVec,

    // File and SST metrics
    pub is_file_deletions_enabled: GaugeVec,
    pub total_sst_files_size: GaugeVec,
    pub live_sst_files_size: GaugeVec,
    pub min_obsolete_sst_number_to_keep: GaugeVec,

    // Snapshot metrics
    pub num_snapshots: GaugeVec,
    pub oldest_snapshot_time: GaugeVec,

    // Version metrics
    pub num_live_versions: GaugeVec,
    pub current_super_version_number: GaugeVec,

    // Log metrics
    pub min_log_number_to_keep: GaugeVec,

    // Level metrics
    pub base_level: GaugeVec,
    pub num_files_at_level: GaugeVec,

    // Write metrics
    pub actual_delayed_write_rate: GaugeVec,
    pub is_write_stopped: GaugeVec,

    // Block cache metrics
    pub block_cache_capacity: GaugeVec,
    pub block_cache_usage: GaugeVec,
    pub block_cache_pinned_usage: GaugeVec,
}

impl RocksDbMetrics {
    pub fn new(metrics: &Metrics) -> Self {
        let labels = &["db"];

        Self {
            // Memory table metrics
            num_immutable_mem_table: metrics.gauge_vec(MetricOpts::new(
                "rocksdb_num_immutable_mem_table",
                "Number of immutable memtables that have not yet been flushed."
            ), labels),
            mem_table_flush_pending: metrics.gauge_vec(MetricOpts::new(
                "rocksdb_mem_table_flush_pending",
                "1 if a memtable flush is pending and 0 otherwise."
            ), labels),
            cur_size_active_mem_table: metrics.gauge_vec(MetricOpts::new(
                "rocksdb_cur_size_active_mem_table_bytes",
                "Approximate size of active memtable in bytes."
            ), labels),
            cur_size_all_mem_tables: metrics.gauge_vec(MetricOpts::new(
                "rocksdb_cur_size_all_mem_tables_bytes",
                "Approximate size of active and unflushed immutable memtables in bytes."
            ), labels),
            size_all_mem_tables: metrics.gauge_vec(MetricOpts::new(
                "rocksdb_size_all_mem_tables_bytes",
                "Approximate size of active, unflushed immutable, and pinned immutable memtables in bytes."
            ), labels),
            num_entries_active_mem_table: metrics.gauge_vec(MetricOpts::new(
                "rocksdb_num_entries_active_mem_table",
                "Total number of entries in the active memtable."
            ), labels),
            num_entries_imm_mem_tables: metrics.gauge_vec(MetricOpts::new(
                "rocksdb_num_entries_imm_mem_tables",
                "Total number of entries in the unflushed immutable memtables."
            ), labels),
            num_deletes_active_mem_table: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_num_deletes_active_mem_table"),
                "Total number of delete entries in the active memtable."
            ), labels),
            num_deletes_imm_mem_tables: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_num_deletes_imm_mem_tables"),
                "Total number of delete entries in the unflushed immutable memtables."
            ), labels),

            // Compaction metrics
            compaction_pending: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_compaction_pending"),
                "1 if at least one compaction is pending; otherwise, 0."
            ), labels),

            estimate_pending_compaction_bytes: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_estimate_pending_compaction_bytes"),
                "Estimated total number of bytes compaction needs to rewrite."
            ), labels),

            num_running_compactions: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_num_running_compactions"),
                "Number of currently running compactions."
            ), labels),

            num_running_flushes: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_num_running_flushes"),
                "Number of currently running flushes."
            ), labels),

            // Error metrics
            background_errors: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_background_errors_total"),
                "Accumulated number of background errors."
            ), labels),

            // Key and data size estimates
            estimate_num_keys: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_estimate_num_keys"),
                "Estimated number of total keys in the active and unflushed immutable memtables and storage."
            ), labels),

            estimate_live_data_size: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_estimate_live_data_size_bytes"),
                "Estimated live data size in bytes."
            ), labels),

            estimate_oldest_key_time: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_estimate_oldest_key_time_seconds"),
                "Estimated oldest key timestamp."
            ), labels),

            // Table reader memory
            estimate_table_readers_mem: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_estimate_table_readers_mem_bytes"),
                "Estimated memory used for reading SST tables, excluding memory used in block cache."
            ), labels),

            // File and SST metrics
            is_file_deletions_enabled: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_is_file_deletions_enabled"),
                "0 if deletion of obsolete files is enabled; otherwise, non-zero."
            ), labels),

            total_sst_files_size: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_total_sst_files_size_bytes"),
                "Total size of all SST files in bytes."
            ), labels),

            live_sst_files_size: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_live_sst_files_size_bytes"),
                "Total size (bytes) of all SST files belonging to any of the CF's versions."
            ), labels),

            min_obsolete_sst_number_to_keep: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_min_obsolete_sst_number_to_keep"),
                "Minimum file number for an obsolete SST to be kept, or maximum uint64_t value if obsolete files can be deleted."
            ), labels),

            // Snapshot metrics
            num_snapshots: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_num_snapshots"),
                "Number of unreleased snapshots of the database."
            ), labels),
            oldest_snapshot_time: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_oldest_snapshot_time_seconds"),
                "Unix timestamp of oldest unreleased snapshot."
            ), labels),

            // Version metrics
            num_live_versions: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_num_live_versions"),
                "Number of live versions."
            ), labels),
            current_super_version_number: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_current_super_version_number"),
                "Number of current LSM version. Incremented after any change to LSM tree."
            ), labels),

            // Log metrics
            min_log_number_to_keep: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_min_log_number_to_keep"),
                "Minimum log number to keep."
            ), labels),

            // Level metrics
            base_level: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_base_level"),
                "Base level for compaction."
            ), labels),
            num_files_at_level: metrics.gauge_vec(MetricOpts::new(
                "rocksdb_num_files_at_level",
                "Number of SST files at each compaction level."
            ), &["db", "level"]),

            // Write metrics
            actual_delayed_write_rate: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_actual_delayed_write_rate"),
                "The current actual delayed write rate. 0 means no delay."
            ), labels),
            is_write_stopped: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_is_write_stopped"),
                "1 if write has been stopped."
            ), labels),

            // Block cache metrics
            block_cache_capacity: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_block_cache_capacity_bytes"),
                "The block cache capacity in bytes."
            ), labels),
            block_cache_usage: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_block_cache_usage_bytes"),
                "The memory size for the entries residing in block cache."
            ), labels),
            block_cache_pinned_usage: metrics.gauge_vec(MetricOpts::new(
                format!("rocksdb_block_cache_pinned_usage_bytes"),
                "The memory size for the entries being pinned."
            ), labels),
        }
    }
}
