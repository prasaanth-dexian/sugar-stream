// Copyright (c) 2026 Dexian India Technologies Private Limited.
// All rights reserved.
//
// This file is proprietary and confidential. Unauthorised copying,
// distribution or use of this file, via any medium, is strictly
// prohibited without the prior written permission of Dexian India
// Technologies Private Limited.

use crate::models::{SyncBatchRow, TelemetryRecord, TrendData, TrendSeries};
use parking_lot::Mutex;
use rusqlite::{params, Connection, Result as SqlResult};
use std::collections::HashMap;

/// Dual-connection SQLite pool for concurrent read/write access.
///
/// WAL mode allows one writer and multiple readers simultaneously.
/// We maintain two separate connections:
/// - `writer`: exclusively used by the OPC flush worker and sync marker
/// - `reader`: exclusively used by IPC queries (trend data, stats)
///
/// This prevents the 500ms batch insert cycle from blocking historical
/// trend queries on the UI thread.
pub struct DbPool {
    writer: Mutex<Connection>,
    reader: Mutex<Connection>,
    db_path: String,
}

impl DbPool {
    /// Open dual connections to the SQLite database and initialize schema.
    pub fn new(db_path: &str) -> SqlResult<Self> {
        let writer_conn = Connection::open(db_path)?;
        let reader_conn = Connection::open(db_path)?;

        // Configure both connections with performance PRAGMAs
        for conn in [&writer_conn, &reader_conn] {
            conn.execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = NORMAL;
                 PRAGMA temp_store = MEMORY;
                 PRAGMA cache_size = -64000;
                 PRAGMA mmap_size = 268435456;
                 PRAGMA busy_timeout = 5000;",
            )?;
        }

        // Create tables and indexes (writer only)
        writer_conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS tags (
                tag_id    INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id   TEXT UNIQUE NOT NULL,
                tag_name  TEXT NOT NULL,
                eng_unit  TEXT DEFAULT '',
                is_active INTEGER DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS tag_telemetry (
                timestamp INTEGER NOT NULL,
                tag_id    INTEGER NOT NULL,
                value     REAL NOT NULL,
                quality   INTEGER DEFAULT 192,
                is_synced INTEGER DEFAULT 0,
                FOREIGN KEY(tag_id) REFERENCES tags(tag_id)
            );

            CREATE INDEX IF NOT EXISTS idx_tag_time
                ON tag_telemetry(tag_id, timestamp DESC);

            CREATE INDEX IF NOT EXISTS idx_sync_queue
                ON tag_telemetry(is_synced) WHERE is_synced = 0;",
        )?;

        Ok(Self {
            writer: Mutex::new(writer_conn),
            reader: Mutex::new(reader_conn),
            db_path: db_path.to_string(),
        })
    }

    // ── WRITER Operations ─────────────────────────────────────────────────

    /// Batch-insert telemetry records in a single atomic transaction.
    ///
    /// Uses prepared statement reuse for maximum throughput.
    /// Typically called by the flush worker with 500-record batches.
    pub fn insert_telemetry_batch(&self, records: &[TelemetryRecord]) -> SqlResult<usize> {
        if records.is_empty() {
            return Ok(0);
        }

        let conn = self.writer.lock();
        let tx = conn.unchecked_transaction()?;

        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO tag_telemetry (timestamp, tag_id, value, quality, is_synced)
                 VALUES (?1, ?2, ?3, ?4, 0)",
            )?;

            for rec in records {
                stmt.execute(params![rec.timestamp, rec.tag_id, rec.value, rec.quality])?;
            }
        }

        tx.commit()?;
        Ok(records.len())
    }

    /// Register a tag in the tags table. Returns the tag_id.
    pub fn register_tag(
        &self,
        node_id: &str,
        tag_name: &str,
        eng_unit: &str,
    ) -> SqlResult<i64> {
        let conn = self.writer.lock();

        conn.execute(
            "INSERT OR IGNORE INTO tags (node_id, tag_name, eng_unit, is_active)
             VALUES (?1, ?2, ?3, 1)",
            params![node_id, tag_name, eng_unit],
        )?;

        let tag_id: i64 = conn.query_row(
            "SELECT tag_id FROM tags WHERE node_id = ?1",
            params![node_id],
            |row| row.get(0),
        )?;

        Ok(tag_id)
    }

    /// Mark a set of rowids as synced after successful cloud upload.
    pub fn mark_synced(&self, rowids: &[i64]) -> SqlResult<()> {
        if rowids.is_empty() {
            return Ok(());
        }

        let conn = self.writer.lock();
        let placeholders: Vec<String> = rowids.iter().map(|_| "?".to_string()).collect();
        let sql = format!(
            "UPDATE tag_telemetry SET is_synced = 1 WHERE rowid IN ({})",
            placeholders.join(",")
        );

        let mut stmt = conn.prepare(&sql)?;
        let params: Vec<&dyn rusqlite::types::ToSql> = rowids
            .iter()
            .map(|id| id as &dyn rusqlite::types::ToSql)
            .collect();
        stmt.execute(params.as_slice())?;

        Ok(())
    }

    /// Delete old synced rows to prevent disk bloat.
    ///
    /// Removes telemetry rows that are:
    /// 1. Already synced to cloud (is_synced = 1)
    /// 2. Older than `max_age_days` days
    ///
    /// Then runs incremental vacuum to reclaim disk space.
    pub fn prune_old_synced(&self, max_age_days: i64) -> SqlResult<u64> {
        let conn = self.writer.lock();

        let cutoff_ms = chrono::Utc::now().timestamp_millis() - (max_age_days * 86_400_000);

        let deleted = conn.execute(
            "DELETE FROM tag_telemetry WHERE is_synced = 1 AND timestamp < ?1",
            params![cutoff_ms],
        )?;

        // Reclaim disk space
        conn.execute_batch("PRAGMA incremental_vacuum;")?;

        Ok(deleted as u64)
    }

    // ── READER Operations ─────────────────────────────────────────────────

    /// Query trend data for multiple tags within a time window.
    ///
    /// Returns aligned TrendData with a shared timestamp array and
    /// per-tag value arrays (with None for gaps).
    pub fn query_trend(
        &self,
        tag_ids: &[i64],
        start_time: i64,
        end_time: i64,
    ) -> SqlResult<TrendData> {
        if tag_ids.is_empty() {
            return Ok(TrendData {
                timestamps: vec![],
                series: vec![],
            });
        }

        let conn = self.reader.lock();

        // Load tag metadata
        let tag_meta: HashMap<i64, (String, String)> = {
            let mut stmt =
                conn.prepare("SELECT tag_id, tag_name, eng_unit FROM tags WHERE is_active = 1")?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    (row.get::<_, String>(1)?, row.get::<_, String>(2)?),
                ))
            })?;
            rows.filter_map(|r| r.ok()).collect()
        };

        // Collect all unique timestamps across all requested tags
        let mut all_timestamps = std::collections::BTreeSet::new();
        let mut tag_data: HashMap<i64, HashMap<i64, f64>> = HashMap::new();

        for &tag_id in tag_ids {
            let mut stmt = conn.prepare_cached(
                "SELECT timestamp, value FROM tag_telemetry
                 WHERE tag_id = ?1 AND timestamp BETWEEN ?2 AND ?3
                 ORDER BY timestamp",
            )?;

            let rows = stmt.query_map(params![tag_id, start_time, end_time], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, f64>(1)?))
            })?;

            let mut points: HashMap<i64, f64> = HashMap::new();
            for row in rows {
                let (ts, val) = row?;
                all_timestamps.insert(ts);
                points.insert(ts, val);
            }
            tag_data.insert(tag_id, points);
        }

        // Build aligned output
        let timestamps: Vec<f64> = all_timestamps.iter().map(|&ts| ts as f64 / 1000.0).collect();

        let series: Vec<TrendSeries> = tag_ids
            .iter()
            .map(|&tag_id| {
                let (tag_name, eng_unit) = tag_meta
                    .get(&tag_id)
                    .cloned()
                    .unwrap_or_else(|| (format!("Tag_{}", tag_id), String::new()));

                let values: Vec<Option<f64>> = all_timestamps
                    .iter()
                    .map(|ts| tag_data.get(&tag_id).and_then(|m| m.get(ts).copied()))
                    .collect();

                TrendSeries {
                    tag_id,
                    tag_name,
                    eng_unit,
                    values,
                }
            })
            .collect();

        Ok(TrendData { timestamps, series })
    }

    /// Get a batch of un-synced rows for the cloud sync worker.
    pub fn get_unsynced_batch(&self, limit: usize) -> SqlResult<Vec<SyncBatchRow>> {
        let conn = self.reader.lock();

        let mut stmt = conn.prepare(
            "SELECT rowid, timestamp, tag_id, value, quality
             FROM tag_telemetry
             WHERE is_synced = 0
             ORDER BY timestamp
             LIMIT ?1",
        )?;

        let rows = stmt.query_map(params![limit as i64], |row| {
            Ok(SyncBatchRow {
                rowid: row.get(0)?,
                timestamp: row.get(1)?,
                tag_id: row.get(2)?,
                value: row.get(3)?,
                quality: row.get(4)?,
            })
        })?;

        rows.collect()
    }

    /// Get database statistics for the system health display.
    ///
    /// Returns (db_file_size_mb, total_row_count, pending_sync_count).
    pub fn get_stats(&self) -> SqlResult<(f64, i64, i64)> {
        let db_size_mb = match std::fs::metadata(&self.db_path) {
            Ok(meta) => meta.len() as f64 / (1024.0 * 1024.0),
            Err(_) => 0.0,
        };

        let conn = self.reader.lock();

        let total_rows: i64 =
            conn.query_row("SELECT COUNT(*) FROM tag_telemetry", [], |row| row.get(0))?;

        let pending_sync: i64 = conn.query_row(
            "SELECT COUNT(*) FROM tag_telemetry WHERE is_synced = 0",
            [],
            |row| row.get(0),
        )?;

        Ok((db_size_mb, total_rows, pending_sync))
    }

    /// Get the database file path.
    pub fn get_db_path(&self) -> &str {
        &self.db_path
    }

    /// Get all active tags.
    #[allow(dead_code)]
    pub fn get_all_tags(&self) -> SqlResult<Vec<(i64, String, String, String)>> {
        let conn = self.reader.lock();

        let mut stmt = conn.prepare(
            "SELECT tag_id, node_id, tag_name, eng_unit FROM tags WHERE is_active = 1",
        )?;

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;

        rows.collect()
    }
}
