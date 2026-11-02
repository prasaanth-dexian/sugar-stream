// Copyright (c) 2026 Dexian India Technologies Private Limited.
// All rights reserved.
//
// This file is proprietary and confidential. Unauthorised copying,
// distribution or use of this file, via any medium, is strictly
// prohibited without the prior written permission of Dexian India
// Technologies Private Limited.

use crate::db::DbPool;
use crate::models::SyncStatusEvent;

use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::Write;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::time::{Duration, Instant};

/// Configuration for the Store-and-Forward sync worker.
pub struct SyncConfig {
    /// Cloud API endpoint URL.
    pub endpoint_url: String,
    /// Bearer token for API authentication.
    pub api_key: String,
    /// Max rows per sync batch.
    pub batch_size: usize,
    /// Base interval between sync attempts (seconds).
    pub interval_secs: u64,
    /// Delete synced rows older than this many days.
    pub prune_after_days: i64,
    /// Run auto-prune every N hours.
    pub prune_interval_hours: u64,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            endpoint_url: "https://api.lovable.cloud/v1/telemetry/ingest".into(),
            api_key: String::new(),
            batch_size: 500,
            interval_secs: 3,
            prune_after_days: 30,
            prune_interval_hours: 6,
        }
    }
}

/// Start the Store-and-Forward cloud sync background worker.
///
/// This function runs indefinitely (until shutdown signal) and performs:
/// 1. **Sync loop** (every `interval_secs`): queries un-synced rows, compresses
///    and POSTs them to the cloud API, marks as synced on success.
/// 2. **Prune loop** (every `prune_interval_hours`): deletes old synced rows
///    to prevent disk bloat, then runs incremental vacuum.
///
/// On network failure, applies exponential backoff (3s → 6s → 12s → max 60s)
/// and never blocks the UI thread or local data logging.
pub async fn start_sync_worker(
    db: Arc<DbPool>,
    config: SyncConfig,
    app_handle: tauri::AppHandle,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap_or_default();

    let mut backoff_secs = config.interval_secs;
    let max_backoff = 60u64;
    let prune_interval = Duration::from_secs(config.prune_interval_hours * 3600);
    let mut last_prune = Instant::now();

    log::info!(
        "Sync worker started: endpoint={}, batch_size={}, interval={}s, prune_after={}d",
        config.endpoint_url,
        config.batch_size,
        config.interval_secs,
        config.prune_after_days
    );

    loop {
        let sleep_duration = Duration::from_secs(backoff_secs);

        tokio::select! {
            _ = shutdown_rx.changed() => {
                log::info!("Sync worker shutting down");
                break;
            }
            _ = tokio::time::sleep(sleep_duration) => {
                // ── Sync Cycle ────────────────────────────────────────
                let sync_result = run_sync_cycle(
                    &db,
                    &client,
                    &config.endpoint_url,
                    &config.api_key,
                    config.batch_size,
                ).await;

                match sync_result {
                    Ok(synced_count) => {
                        // Reset backoff on success
                        backoff_secs = config.interval_secs;

                        // Emit status to frontend
                        let pending = get_pending_count(&db).await;
                        let _ = emit_sync_status(&app_handle, synced_count, pending, None);
                    }
                    Err(err) => {
                        // Exponential backoff
                        backoff_secs = (backoff_secs * 2).min(max_backoff);
                        log::warn!(
                            "Sync failed (next retry in {}s): {}",
                            backoff_secs,
                            err
                        );

                        let pending = get_pending_count(&db).await;
                        let _ = emit_sync_status(
                            &app_handle,
                            0,
                            pending,
                            Some(err),
                        );
                    }
                }

                // ── Auto-Prune Check ─────────────────────────────────
                if last_prune.elapsed() >= prune_interval {
                    run_prune_cycle(&db, config.prune_after_days).await;
                    last_prune = Instant::now();
                }
            }
        }
    }
}

/// Execute a single sync cycle: query → compress → POST → mark synced.
async fn run_sync_cycle(
    db: &Arc<DbPool>,
    client: &reqwest::Client,
    endpoint_url: &str,
    api_key: &str,
    batch_size: usize,
) -> Result<i64, String> {
    // 1. Query un-synced batch (on blocking thread)
    let db_clone = db.clone();
    let limit = batch_size;
    let batch = tokio::task::spawn_blocking(move || db_clone.get_unsynced_batch(limit))
        .await
        .map_err(|e| format!("spawn_blocking error: {}", e))?
        .map_err(|e| format!("DB query error: {}", e))?;

    if batch.is_empty() {
        return Ok(0);
    }

    let rowids: Vec<i64> = batch.iter().map(|r| r.rowid).collect();
    let count = batch.len() as i64;

    // 2. Serialize to JSON
    let json_payload =
        serde_json::to_vec(&batch).map_err(|e| format!("JSON serialization error: {}", e))?;

    // 3. Gzip compress
    let compressed = compress_gzip(&json_payload)?;

    log::debug!(
        "Sync batch: {} rows, raw={} bytes, compressed={} bytes",
        count,
        json_payload.len(),
        compressed.len()
    );

    // 4. POST to cloud
    let mut request = client
        .post(endpoint_url)
        .header("Content-Type", "application/json")
        .header("Content-Encoding", "gzip")
        .header("X-ThinPI-Batch-Count", count.to_string())
        .body(compressed);

    if !api_key.is_empty() {
        request = request.header("Authorization", format!("Bearer {}", api_key));
    }

    let response = request
        .send()
        .await
        .map_err(|e| format!("Network error: {}", e))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("HTTP {} — {}", status, body));
    }

    // 5. Mark as synced (on blocking thread)
    let db_clone = db.clone();
    tokio::task::spawn_blocking(move || db_clone.mark_synced(&rowids))
        .await
        .map_err(|e| format!("spawn_blocking error: {}", e))?
        .map_err(|e| format!("DB mark_synced error: {}", e))?;

    log::info!("Synced {} rows to cloud", count);
    Ok(count)
}

/// Execute an auto-prune cycle: delete old synced rows + vacuum.
async fn run_prune_cycle(db: &Arc<DbPool>, max_age_days: i64) {
    let db_clone = db.clone();
    let result =
        tokio::task::spawn_blocking(move || db_clone.prune_old_synced(max_age_days)).await;

    match result {
        Ok(Ok(deleted)) => {
            if deleted > 0 {
                log::info!(
                    "Auto-prune: deleted {} synced rows older than {} days",
                    deleted,
                    max_age_days
                );
            }
        }
        Ok(Err(e)) => {
            log::error!("Auto-prune DB error: {}", e);
        }
        Err(e) => {
            log::error!("Auto-prune spawn_blocking error: {}", e);
        }
    }
}

/// Gzip-compress a byte slice.
fn compress_gzip(data: &[u8]) -> Result<Vec<u8>, String> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder
        .write_all(data)
        .map_err(|e| format!("Gzip write error: {}", e))?;
    encoder
        .finish()
        .map_err(|e| format!("Gzip finish error: {}", e))
}

/// Get the current pending sync count from the database.
async fn get_pending_count(db: &Arc<DbPool>) -> i64 {
    let db_clone = db.clone();
    tokio::task::spawn_blocking(move || {
        db_clone
            .get_stats()
            .map(|(_, _, pending)| pending)
            .unwrap_or(0)
    })
    .await
    .unwrap_or(0)
}

/// Emit a sync status event to the frontend.
fn emit_sync_status(
    app_handle: &tauri::AppHandle,
    synced_count: i64,
    pending_count: i64,
    last_error: Option<String>,
) -> Result<(), String> {
    use tauri::Emitter;

    let event = SyncStatusEvent {
        synced_count,
        pending_count,
        last_error,
    };

    app_handle
        .emit("sync-status", event)
        .map_err(|e| format!("Failed to emit sync-status event: {}", e))
}
