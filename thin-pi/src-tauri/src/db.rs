// Copyright (c) 2026 Dexian India Technologies Private Limited.
// All rights reserved.
//
// This file is proprietary and confidential. Unauthorised copying,
// distribution or use of this file, via any medium, is strictly
// prohibited without the prior written permission of Dexian India
// Technologies Private Limited.

use crate::models::{SyncBatchRow, TagMeta, TelemetryRecord, TrendData, TrendSeries};
use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension, Result as SqlResult};
use std::collections::HashMap;

/// Schema of the telemetry buffer.
///
/// `seq` is the delivery identity of a reading: AUTOINCREMENT guarantees it is
/// never reused, even after synced rows are pruned, so the edge can de-duplicate
/// resent batches and detect gaps. (The implicit SQLite rowid used before can be
/// handed out again once the newest rows are deleted.)
const TELEMETRY_TABLE: &str = "CREATE TABLE IF NOT EXISTS tag_telemetry (
        seq       INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER NOT NULL,
        tag_id    INTEGER NOT NULL,
        value     REAL NOT NULL,
        quality   INTEGER DEFAULT 192,
        is_synced INTEGER DEFAULT 0,
        FOREIGN KEY(tag_id) REFERENCES tags(tag_id)
    );";

/// Max rows deleted per prune statement, so pruning never holds the writer
/// lock long enough to stall the flush worker.
const PRUNE_CHUNK: i64 = 50_000;

/// Result of one flush into the buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InsertOutcome {
    /// Readings stored in `tag_telemetry` (they will be delivered).
    pub inserted: usize,
    /// Readings with a NaN/infinite value, kept in `dead_letter` instead.
    pub non_finite: usize,
    /// Readings for a tag_id missing from `tags`, kept in `dead_letter` instead.
    pub unknown_tag: usize,
}

/// Dual-connection SQLite pool for concurrent read/write access.
///
/// WAL mode allows one writer and multiple readers simultaneously.
/// We maintain two separate connections:
/// - `writer`: exclusively used by the OPC flush worker and sync marker
/// - `reader`: exclusively used by IPC queries (trend data, stats)
///
/// This prevents the batch insert cycle from blocking historical
/// trend queries on the UI thread.
///
/// Durability: `synchronous = FULL` makes every committed transaction survive a
/// power cut (WAL is fsynced on commit). The buffer is the only copy of a
/// reading until the edge confirms it, so NORMAL (which can lose the last
/// transactions on power loss) is not acceptable here.
pub struct DbPool {
    writer: Mutex<Connection>,
    reader: Mutex<Connection>,
    db_path: String,
    epoch: i64,
}

impl DbPool {
    /// Open dual connections to the SQLite database and initialize schema.
    pub fn new(db_path: &str) -> SqlResult<Self> {
        let writer_conn = Connection::open(db_path)?;
        let reader_conn = Connection::open(db_path)?;

        for conn in [&writer_conn, &reader_conn] {
            conn.execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = FULL;
                 PRAGMA temp_store = MEMORY;
                 PRAGMA cache_size = -64000;
                 PRAGMA mmap_size = 268435456;
                 PRAGMA busy_timeout = 5000;",
            )?;
        }

        writer_conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS tags (
                tag_id    INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id   TEXT UNIQUE NOT NULL,
                tag_name  TEXT NOT NULL,
                eng_unit  TEXT DEFAULT '',
                is_active INTEGER DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS meta (
                k TEXT PRIMARY KEY,
                v TEXT NOT NULL
            );

            -- Readings that will never be delivered, with the reason: values that
            -- were NaN/infinite at the source (seq is NULL) and rows the edge
            -- rejected. Kept so nothing disappears silently.
            CREATE TABLE IF NOT EXISTS dead_letter (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                seq       INTEGER,
                timestamp INTEGER NOT NULL,
                tag_id    INTEGER NOT NULL,
                value     TEXT,
                reason    TEXT NOT NULL,
                at        INTEGER NOT NULL
            );

            -- Periods in which acquisition was paused (buffer full, disk budget),
            -- i.e. readings that were never captured.
            CREATE TABLE IF NOT EXISTS acq_gaps (
                start_ts INTEGER NOT NULL,
                end_ts   INTEGER NOT NULL,
                reason   TEXT NOT NULL
            );",
        )?;

        migrate_telemetry_table(&writer_conn)?;

        writer_conn.execute_batch(&format!(
            "{TELEMETRY_TABLE}

            CREATE INDEX IF NOT EXISTS idx_tag_time
                ON tag_telemetry(tag_id, timestamp DESC);

            DROP INDEX IF EXISTS idx_sync_queue;

            CREATE INDEX IF NOT EXISTS idx_unsynced_seq
                ON tag_telemetry(seq) WHERE is_synced = 0;

            CREATE INDEX IF NOT EXISTS idx_unsynced_ts
                ON tag_telemetry(timestamp) WHERE is_synced = 0;"
        ))?;

        let epoch = load_or_create_epoch(&writer_conn)?;

        Ok(Self {
            writer: Mutex::new(writer_conn),
            reader: Mutex::new(reader_conn),
            db_path: db_path.to_string(),
            epoch,
        })
    }

    /// Random id created once per database. Together with `seq` it identifies a
    /// reading across collector reinstalls (a new database starts a new epoch).
    pub fn epoch(&self) -> i64 {
        self.epoch
    }

    // ── WRITER Operations ─────────────────────────────────────────────────

    /// Batch-insert telemetry records in a single atomic transaction.
    ///
    /// Two kinds of reading can never be stored and would otherwise fail the
    /// whole batch on every retry, pausing acquisition for good: non-finite
    /// values (the column is NOT NULL and SQLite stores NaN as NULL) and readings
    /// for a tag_id missing from `tags` (foreign keys are enforced). Both are
    /// written to `dead_letter` with the reason, in the same transaction.
    /// On any other error nothing is committed and the caller keeps the batch.
    pub fn insert_telemetry_batch(&self, records: &[TelemetryRecord]) -> SqlResult<InsertOutcome> {
        let mut outcome = InsertOutcome {
            inserted: 0,
            non_finite: 0,
            unknown_tag: 0,
        };
        if records.is_empty() {
            return Ok(outcome);
        }

        let now = chrono::Utc::now().timestamp_millis();
        let conn = self.writer.lock();
        let tx = conn.unchecked_transaction()?;

        {
            let mut insert = tx.prepare_cached(
                "INSERT INTO tag_telemetry (timestamp, tag_id, value, quality, is_synced)
                 SELECT ?1, ?2, ?3, ?4, 0 WHERE EXISTS (SELECT 1 FROM tags WHERE tag_id = ?2)",
            )?;
            let mut dead = tx.prepare_cached(
                "INSERT INTO dead_letter (seq, timestamp, tag_id, value, reason, at)
                 VALUES (NULL, ?1, ?2, ?3, ?4, ?5)",
            )?;

            for rec in records {
                let reason = if !rec.value.is_finite() {
                    outcome.non_finite += 1;
                    "non-finite value at source"
                } else if insert.execute(params![
                    rec.timestamp,
                    rec.tag_id,
                    rec.value,
                    rec.quality
                ])? == 1
                {
                    outcome.inserted += 1;
                    continue;
                } else {
                    outcome.unknown_tag += 1;
                    "unknown tag_id"
                };
                dead.execute(params![
                    rec.timestamp,
                    rec.tag_id,
                    rec.value.to_string(),
                    reason,
                    now
                ])?;
            }
        }

        tx.commit()?;
        Ok(outcome)
    }

    /// Record a period in which readings were not captured.
    pub fn record_acquisition_gap(
        &self,
        start_ts: i64,
        end_ts: i64,
        reason: &str,
    ) -> SqlResult<()> {
        self.writer.lock().execute(
            "INSERT INTO acq_gaps (start_ts, end_ts, reason) VALUES (?1, ?2, ?3)",
            params![start_ts, end_ts, reason],
        )?;
        Ok(())
    }

    /// Space used by data in MB: allocated pages minus free pages, plus the WAL.
    ///
    /// The file size alone never shrinks after pruning (free pages are reused),
    /// so it cannot be used to decide when there is room again.
    pub fn used_mb(&self) -> SqlResult<f64> {
        let conn = self.reader.lock();
        let pages: i64 = conn.query_row("PRAGMA page_count", [], |r| r.get(0))?;
        let free: i64 = conn.query_row("PRAGMA freelist_count", [], |r| r.get(0))?;
        let page_size: i64 = conn.query_row("PRAGMA page_size", [], |r| r.get(0))?;
        let wal = std::fs::metadata(format!("{}-wal", self.db_path))
            .map(|m| m.len())
            .unwrap_or(0);
        Ok(((pages - free) * page_size) as f64 / 1_048_576.0 + wal as f64 / 1_048_576.0)
    }

    /// Register a tag in the tags table. Returns the tag_id.
    pub fn register_tag(&self, node_id: &str, tag_name: &str, eng_unit: &str) -> SqlResult<i64> {
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

    /// Mark readings as delivered after the edge confirmed them, in one
    /// transaction. Readings the edge rejected are copied to `dead_letter` with
    /// the edge's reason first, then marked too, so they stop blocking the queue.
    /// If this fails nothing changes and the readings are simply sent again
    /// (the edge treats a resend as a no-op).
    pub fn mark_synced(&self, seqs: &[i64], rejected: &[(i64, String)]) -> SqlResult<()> {
        if seqs.is_empty() {
            return Ok(());
        }

        let now = chrono::Utc::now().timestamp_millis();
        let conn = self.writer.lock();
        let tx = conn.unchecked_transaction()?;
        {
            let mut dead = tx.prepare_cached(
                "INSERT INTO dead_letter (seq, timestamp, tag_id, value, reason, at)
                 SELECT seq, timestamp, tag_id, CAST(value AS TEXT), ?2, ?3
                 FROM tag_telemetry WHERE seq = ?1",
            )?;
            for (seq, reason) in rejected {
                dead.execute(params![seq, reason, now])?;
            }
        }
        for chunk in seqs.chunks(500) {
            let placeholders = vec!["?"; chunk.len()].join(",");
            let sql =
                format!("UPDATE tag_telemetry SET is_synced = 1 WHERE seq IN ({placeholders})");
            let params: Vec<&dyn rusqlite::types::ToSql> = chunk
                .iter()
                .map(|id| id as &dyn rusqlite::types::ToSql)
                .collect();
            tx.execute(&sql, params.as_slice())?;
        }
        tx.commit()
    }

    /// Delete delivered readings older than `max_age_hours`.
    ///
    /// Delivered readings are kept for a while so the edge can be re-filled
    /// from the collector if it loses data (restore from backup + replay).
    /// Unsynced readings are never deleted here. Deletes in chunks so the
    /// writer lock is released between statements.
    pub fn prune_old_synced(&self, max_age_hours: i64) -> SqlResult<u64> {
        let cutoff_ms = chrono::Utc::now().timestamp_millis() - max_age_hours * 3_600_000;
        let mut total = 0u64;
        loop {
            let deleted = self.writer.lock().execute(
                "DELETE FROM tag_telemetry WHERE seq IN (
                     SELECT seq FROM tag_telemetry
                     WHERE is_synced = 1 AND timestamp < ?1 LIMIT ?2)",
                params![cutoff_ms, PRUNE_CHUNK],
            )?;
            total += deleted as u64;
            if (deleted as i64) < PRUNE_CHUNK {
                break;
            }
        }

        // Only reclaims space if the database was created with
        // auto_vacuum = INCREMENTAL; otherwise freed pages are reused.
        self.writer
            .lock()
            .execute_batch("PRAGMA incremental_vacuum;")?;

        Ok(total)
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
        let timestamps: Vec<f64> = all_timestamps
            .iter()
            .map(|&ts| ts as f64 / 1000.0)
            .collect();

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

    /// Unsynced readings taken at or after `cutoff_ms` (the live window), oldest first.
    pub fn get_unsynced_live(&self, cutoff_ms: i64, limit: usize) -> SqlResult<Vec<SyncBatchRow>> {
        self.query_sync_rows(
            "SELECT seq, timestamp, tag_id, value, quality FROM tag_telemetry
             WHERE is_synced = 0 AND timestamp >= ?1 ORDER BY seq LIMIT ?2",
            cutoff_ms,
            limit,
        )
    }

    /// Unsynced readings older than `cutoff_ms` (the backlog), oldest first.
    pub fn get_unsynced_backfill(
        &self,
        cutoff_ms: i64,
        limit: usize,
    ) -> SqlResult<Vec<SyncBatchRow>> {
        self.query_sync_rows(
            "SELECT seq, timestamp, tag_id, value, quality FROM tag_telemetry
             WHERE is_synced = 0 AND timestamp < ?1 ORDER BY seq LIMIT ?2",
            cutoff_ms,
            limit,
        )
    }

    fn query_sync_rows(
        &self,
        sql: &str,
        cutoff_ms: i64,
        limit: usize,
    ) -> SqlResult<Vec<SyncBatchRow>> {
        let conn = self.reader.lock();
        let mut stmt = conn.prepare_cached(sql)?;
        let rows = stmt.query_map(params![cutoff_ms, limit as i64], |row| {
            Ok(SyncBatchRow {
                seq: row.get(0)?,
                timestamp: row.get(1)?,
                tag_id: row.get(2)?,
                value: row.get(3)?,
                quality: row.get(4)?,
            })
        })?;
        rows.collect()
    }

    /// (lowest unsynced seq, highest seq ever assigned). The edge uses these to
    /// tell a real gap (below the lowest unsynced seq) from data still queued.
    /// Lowest unsynced is `max_seq + 1` when nothing is waiting.
    pub fn sync_cursor(&self) -> SqlResult<(i64, i64)> {
        let conn = self.reader.lock();
        let max_seq: i64 = conn
            .query_row(
                "SELECT seq FROM sqlite_sequence WHERE name = 'tag_telemetry'",
                [],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0);
        let min_unsynced: Option<i64> = conn.query_row(
            "SELECT MIN(seq) FROM tag_telemetry WHERE is_synced = 0",
            [],
            |r| r.get(0),
        )?;
        Ok((min_unsynced.unwrap_or(max_seq + 1), max_seq))
    }

    /// Number of readings waiting for delivery (uses the partial index).
    pub fn pending_count(&self) -> SqlResult<i64> {
        self.reader.lock().query_row(
            "SELECT COUNT(*) FROM tag_telemetry WHERE is_synced = 0",
            [],
            |r| r.get(0),
        )
    }

    /// Name, node id and unit for the given tags, sent with each batch so the
    /// edge does not need access to this database.
    pub fn tag_meta(&self, tag_ids: &[i64]) -> SqlResult<HashMap<i64, TagMeta>> {
        let conn = self.reader.lock();
        let mut stmt =
            conn.prepare_cached("SELECT node_id, tag_name, eng_unit FROM tags WHERE tag_id = ?1")?;
        let mut out = HashMap::new();
        for &id in tag_ids {
            if out.contains_key(&id) {
                continue;
            }
            let meta = stmt
                .query_row(params![id], |r| {
                    Ok(TagMeta {
                        node_id: r.get(0)?,
                        tag_name: r.get(1)?,
                        eng_unit: r.get::<_, Option<String>>(2)?.unwrap_or_default(),
                    })
                })
                .optional()?;
            if let Some(m) = meta {
                out.insert(id, m);
            }
        }
        Ok(out)
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

        let mut stmt = conn
            .prepare("SELECT tag_id, node_id, tag_name, eng_unit FROM tags WHERE is_active = 1")?;

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

/// Move a pre-`seq` telemetry table (implicit rowid) to the current schema.
///
/// Runs once, in one transaction: rows keep their order and get fresh `seq`
/// values. The database gets a new epoch at the same time (the meta row does not
/// exist yet), so the edge never confuses old and new numbering.
fn migrate_telemetry_table(conn: &Connection) -> SqlResult<()> {
    let exists: bool = conn.query_row(
        "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type = 'table' AND name = 'tag_telemetry'",
        [],
        |r| r.get(0),
    )?;
    if !exists {
        return Ok(());
    }
    let has_seq: bool = conn.query_row(
        "SELECT COUNT(*) > 0 FROM pragma_table_info('tag_telemetry') WHERE name = 'seq'",
        [],
        |r| r.get(0),
    )?;
    if has_seq {
        return Ok(());
    }

    log::warn!("Migrating tag_telemetry to the seq schema (one-time)");
    let tx = conn.unchecked_transaction()?;
    tx.execute_batch(&format!(
        "ALTER TABLE tag_telemetry RENAME TO tag_telemetry_old;
         {TELEMETRY_TABLE}
         INSERT INTO dead_letter (seq, timestamp, tag_id, value, reason, at)
             SELECT NULL, timestamp, tag_id, CAST(value AS TEXT), 'unknown tag_id (migration)',
                    CAST(strftime('%s','now') AS INTEGER) * 1000
             FROM tag_telemetry_old WHERE tag_id NOT IN (SELECT tag_id FROM tags);
         INSERT INTO tag_telemetry (timestamp, tag_id, value, quality, is_synced)
             SELECT timestamp, tag_id, value, quality, is_synced
             FROM tag_telemetry_old WHERE tag_id IN (SELECT tag_id FROM tags) ORDER BY rowid;
         DROP TABLE tag_telemetry_old;
         DELETE FROM meta WHERE k = 'epoch';"
    ))?;
    tx.commit()
}

fn load_or_create_epoch(conn: &Connection) -> SqlResult<i64> {
    if let Some(v) = conn
        .query_row("SELECT v FROM meta WHERE k = 'epoch'", [], |r| {
            r.get::<_, String>(0)
        })
        .optional()?
    {
        if let Ok(epoch) = v.parse::<i64>() {
            return Ok(epoch);
        }
    }
    // Positive and below 2^31 so it is exact in every JSON / InfluxDB client.
    let epoch = (rand::random::<u32>() & 0x7fff_ffff).max(1) as i64;
    conn.execute(
        "INSERT OR REPLACE INTO meta (k, v) VALUES ('epoch', ?1)",
        params![epoch.to_string()],
    )?;
    Ok(epoch)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TempDb(std::path::PathBuf);

    impl TempDb {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "thinpi-test-{}-{}-{}",
                name,
                std::process::id(),
                rand::random::<u32>()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            TempDb(dir.join("thinpi.db"))
        }
        fn path(&self) -> &str {
            self.0.to_str().unwrap()
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(self.0.parent().unwrap());
        }
    }

    fn rec(ts: i64, tag_id: i64, value: f64) -> TelemetryRecord {
        TelemetryRecord {
            timestamp: ts,
            tag_id,
            value,
            quality: 192,
        }
    }

    /// Open a test database with tags 1 and 2 registered.
    fn open(t: &TempDb) -> DbPool {
        let db = DbPool::new(t.path()).unwrap();
        // Fresh databases get ids 1 and 2, which the tests use.
        db.register_tag("ns=2;s=A", "A", "bar").unwrap();
        db.register_tag("ns=2;s=B", "B", "").unwrap();
        db
    }

    fn all_seqs(db: &DbPool) -> Vec<i64> {
        let conn = db.reader.lock();
        let mut stmt = conn
            .prepare("SELECT seq FROM tag_telemetry ORDER BY seq")
            .unwrap();
        let rows = stmt.query_map([], |r| r.get(0)).unwrap();
        rows.map(|r| r.unwrap()).collect()
    }

    #[test]
    fn uses_full_sync_and_keeps_epoch_across_reopen() {
        let t = TempDb::new("epoch");
        let db = open(&t);
        let sync: i64 = db
            .writer
            .lock()
            .query_row("PRAGMA synchronous", [], |r| r.get(0))
            .unwrap();
        assert_eq!(sync, 2, "synchronous must be FULL");
        let epoch = db.epoch();
        assert!(epoch > 0);
        drop(db);
        assert_eq!(DbPool::new(t.path()).unwrap().epoch(), epoch);
    }

    #[test]
    fn seq_is_never_reused_after_rows_are_pruned() {
        let t = TempDb::new("seq");
        let db = open(&t);
        let old = chrono::Utc::now().timestamp_millis() - 100 * 3_600_000;
        db.insert_telemetry_batch(&[rec(old, 1, 1.0), rec(old, 2, 2.0)])
            .unwrap();
        db.mark_synced(&[1, 2], &[]).unwrap();
        assert_eq!(db.prune_old_synced(72).unwrap(), 2);
        assert!(all_seqs(&db).is_empty());
        db.insert_telemetry_batch(&[rec(old, 1, 3.0)]).unwrap();
        assert_eq!(all_seqs(&db), vec![3]);
    }

    #[test]
    fn non_finite_values_go_to_dead_letter_and_do_not_fail_the_batch() {
        let t = TempDb::new("nan");
        let db = open(&t);
        let out = db
            .insert_telemetry_batch(&[
                rec(1, 1, 1.0),
                rec(2, 1, f64::NAN),
                rec(3, 1, f64::INFINITY),
                rec(4, 1, 4.0),
            ])
            .unwrap();
        assert_eq!(
            out,
            InsertOutcome {
                inserted: 2,
                non_finite: 2,
                unknown_tag: 0
            }
        );
        let dead: i64 = db
            .reader
            .lock()
            .query_row(
                "SELECT COUNT(*) FROM dead_letter WHERE seq IS NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(dead, 2);
    }

    #[test]
    fn live_and_backfill_split_by_time_and_rejected_rows_are_dead_lettered() {
        let t = TempDb::new("split");
        let db = open(&t);
        db.insert_telemetry_batch(&[rec(100, 1, 1.0), rec(200, 1, 2.0), rec(300, 1, 3.0)])
            .unwrap();
        let live: Vec<i64> = db
            .get_unsynced_live(200, 10)
            .unwrap()
            .iter()
            .map(|r| r.seq)
            .collect();
        let back: Vec<i64> = db
            .get_unsynced_backfill(200, 10)
            .unwrap()
            .iter()
            .map(|r| r.seq)
            .collect();
        assert_eq!(live, vec![2, 3]);
        assert_eq!(back, vec![1]);
        assert_eq!(db.sync_cursor().unwrap(), (1, 3));

        db.mark_synced(&[1, 2], &[(2, "bad timestamp".into())])
            .unwrap();
        assert_eq!(db.sync_cursor().unwrap(), (3, 3));
        assert_eq!(db.pending_count().unwrap(), 1);
        let reason: String = db
            .reader
            .lock()
            .query_row("SELECT reason FROM dead_letter WHERE seq = 2", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(reason, "bad timestamp");

        db.mark_synced(&[3], &[]).unwrap();
        assert_eq!(db.sync_cursor().unwrap(), (4, 3));
    }

    #[test]
    fn prune_never_deletes_unsynced_or_recent_rows() {
        let t = TempDb::new("prune");
        let db = open(&t);
        let now = chrono::Utc::now().timestamp_millis();
        let old = now - 100 * 3_600_000;
        db.insert_telemetry_batch(&[rec(old, 1, 1.0), rec(old, 1, 2.0), rec(now, 1, 3.0)])
            .unwrap();
        db.mark_synced(&[1, 3], &[]).unwrap();
        assert_eq!(db.prune_old_synced(72).unwrap(), 1);
        assert_eq!(all_seqs(&db), vec![2, 3]);
    }

    #[test]
    fn migrates_old_rowid_table_without_losing_unsynced_rows() {
        let t = TempDb::new("migrate");
        {
            let conn = Connection::open(t.path()).unwrap();
            conn.execute_batch(
                "CREATE TABLE tags (tag_id INTEGER PRIMARY KEY AUTOINCREMENT, node_id TEXT UNIQUE NOT NULL,
                     tag_name TEXT NOT NULL, eng_unit TEXT DEFAULT '', is_active INTEGER DEFAULT 1);
                 CREATE TABLE tag_telemetry (timestamp INTEGER NOT NULL, tag_id INTEGER NOT NULL,
                     value REAL NOT NULL, quality INTEGER DEFAULT 192, is_synced INTEGER DEFAULT 0);
                 CREATE INDEX idx_sync_queue ON tag_telemetry(is_synced) WHERE is_synced = 0;
                 INSERT INTO tags (node_id, tag_name) VALUES ('ns=2;s=A', 'A');
                 INSERT INTO tag_telemetry VALUES (5, 99, 9.0, 192, 0);
                 INSERT INTO tag_telemetry VALUES (10, 1, 1.0, 192, 1);
                 INSERT INTO tag_telemetry VALUES (20, 1, 2.0, 192, 0);
                 INSERT INTO tag_telemetry VALUES (30, 1, 3.0, 192, 0);",
            )
            .unwrap();
        }
        let db = open(&t);
        assert_eq!(all_seqs(&db), vec![1, 2, 3]);
        assert_eq!(db.pending_count().unwrap(), 2);
        assert_eq!(db.sync_cursor().unwrap(), (2, 3));
        let orphans: i64 = db
            .reader
            .lock()
            .query_row(
                "SELECT COUNT(*) FROM dead_letter WHERE reason = 'unknown tag_id (migration)'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(orphans, 1, "orphan row kept in dead_letter, not dropped");
        drop(db);
        // Second open is a no-op.
        assert_eq!(all_seqs(&DbPool::new(t.path()).unwrap()), vec![1, 2, 3]);
    }

    #[test]
    fn used_mb_reports_live_data_not_file_size() {
        let t = TempDb::new("used");
        let db = open(&t);
        let recs: Vec<_> = (0..20_000).map(|i| rec(i, 1, i as f64)).collect();
        db.insert_telemetry_batch(&recs).unwrap();
        assert!(db.used_mb().unwrap() > 0.1);
    }

    #[test]
    fn unknown_tag_goes_to_dead_letter_instead_of_failing_the_batch() {
        let t = TempDb::new("unknown");
        let db = open(&t);
        let out = db
            .insert_telemetry_batch(&[rec(1, 1, 1.0), rec(2, 77, 2.0), rec(3, 2, 3.0)])
            .unwrap();
        assert_eq!(
            out,
            InsertOutcome {
                inserted: 2,
                non_finite: 0,
                unknown_tag: 1
            }
        );
        assert_eq!(all_seqs(&db), vec![1, 2]);
    }
}
