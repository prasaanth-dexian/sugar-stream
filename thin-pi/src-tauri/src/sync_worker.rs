// Copyright (c) 2026 Dexian India Technologies Private Limited.
// All rights reserved.
//
// This file is proprietary and confidential. Unauthorised copying,
// distribution or use of this file, via any medium, is strictly
// prohibited without the prior written permission of Dexian India
// Technologies Private Limited.

//! Store-and-forward delivery to the Dexian edge ingest gateway.
//!
//! Delivery contract (see `thin-pi/edge-gateway/README.md`):
//! - The local SQLite buffer is the only copy of a reading until the gateway
//!   confirms it. A reading is marked synced only after the gateway answered
//!   200 with `accepted` equal to the number of rows sent, which it does only
//!   after the rows are durably stored. Anything else (timeout, 5xx, a proxy's
//!   HTML page, a short `accepted`) leaves the rows unsynced and they are sent
//!   again. Resends are harmless: the gateway is idempotent on (epoch, seq).
//! - Rows the gateway rejects (invalid values) are moved to `dead_letter` with
//!   the reason, so one bad row never blocks the queue.
//! - Two independent senders: **live** (readings from the last few seconds,
//!   every 250 ms) and **backfill** (the backlog, oldest first, several batches
//!   in parallel). A large backlog after an outage never delays live data.

use crate::db::DbPool;
use crate::models::{SyncBatchRow, SyncStatusEvent};

use flate2::write::GzEncoder;
use flate2::Compression;
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::json;
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{watch, Notify};
use tokio::task::JoinSet;
use tokio::time::{Duration, Instant};

/// Configuration for the store-and-forward sync worker.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Ingest gateway endpoint (`THINPI_SYNC_URL`).
    pub endpoint_url: String,
    /// Bearer token for the gateway (`THINPI_SYNC_API_KEY`).
    pub api_key: String,
    /// Stable collector id, unique per plant collector (`THINPI_COLLECTOR_ID`).
    pub collector_id: String,
    /// Live sender cycle.
    pub interval_ms: u64,
    /// Readings newer than this go through the live sender.
    pub live_window_ms: i64,
    /// Max rows per live request.
    pub live_max: usize,
    /// Max rows per backfill request (`THINPI_BACKFILL_BATCH`).
    pub backfill_max: usize,
    /// Backfill requests in flight at once (`THINPI_BACKFILL_PARALLEL`).
    pub backfill_parallel: usize,
    /// Keep delivered rows this long so the edge can be re-filled (`THINPI_RETAIN_SYNCED_HOURS`).
    pub retain_synced_hours: i64,
    /// How often delivered rows older than the retention are deleted.
    pub prune_interval_secs: u64,
    /// HTTP request timeout.
    pub request_timeout_secs: u64,
}

impl SyncConfig {
    /// Defaults, overridable through `THINPI_*` environment variables.
    pub fn from_env(default_collector_id: &str) -> Self {
        Self {
            endpoint_url: std::env::var("THINPI_SYNC_URL")
                .unwrap_or_else(|_| "https://api.lovable.cloud/v1/telemetry/ingest".into()),
            api_key: std::env::var("THINPI_SYNC_API_KEY").unwrap_or_default(),
            collector_id: std::env::var("THINPI_COLLECTOR_ID")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| default_collector_id.to_string()),
            interval_ms: 250,
            live_window_ms: 10_000,
            live_max: 5_000,
            backfill_max: env_or("THINPI_BACKFILL_BATCH", 10_000usize).max(1),
            backfill_parallel: env_or("THINPI_BACKFILL_PARALLEL", 2usize).clamp(1, 8),
            retain_synced_hours: env_or("THINPI_RETAIN_SYNCED_HOURS", 72i64).max(1),
            prune_interval_secs: 3_600,
            request_timeout_secs: 30,
        }
    }
}

fn env_or<T: FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

/// The gateway's 200 reply.
#[derive(Debug, Deserialize)]
struct IngestReply {
    accepted: usize,
    #[serde(default)]
    rejected: Vec<(i64, String)>,
}

#[derive(Default)]
struct Shared {
    synced_since_emit: i64,
    last_error: Option<String>,
}

struct Ctx {
    db: Arc<DbPool>,
    client: reqwest::Client,
    cfg: SyncConfig,
    shared: Mutex<Shared>,
    /// Signalled by the live sender when the gateway is reachable again, so the
    /// backfill sender stops its backoff and starts draining immediately.
    recovered: Notify,
}

/// Start the store-and-forward worker. Runs until the shutdown signal.
pub async fn start_sync_worker(
    db: Arc<DbPool>,
    config: SyncConfig,
    app_handle: tauri::AppHandle,
    shutdown_rx: watch::Receiver<bool>,
) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(config.request_timeout_secs))
        .build()
        .unwrap_or_default();

    log::info!(
        "Sync worker started: endpoint={}, collector={}, epoch={}, backfill={}x{}, retain={}h",
        config.endpoint_url,
        config.collector_id,
        db.epoch(),
        config.backfill_parallel,
        config.backfill_max,
        config.retain_synced_hours
    );

    let ctx = Arc::new(Ctx {
        db,
        client,
        cfg: config,
        shared: Mutex::new(Shared::default()),
        recovered: Notify::new(),
    });

    let mut tasks = JoinSet::new();
    tasks.spawn(live_loop(ctx.clone(), shutdown_rx.clone()));
    tasks.spawn(backfill_loop(ctx.clone(), shutdown_rx.clone()));
    tasks.spawn(prune_loop(ctx.clone(), shutdown_rx.clone()));
    tasks.spawn(status_loop(ctx, app_handle, shutdown_rx));
    while tasks.join_next().await.is_some() {}
    log::info!("Sync worker shut down");
}

/// Sleep for `d`, returning false if shutdown was requested meanwhile.
async fn pause(shutdown: &mut watch::Receiver<bool>, d: Duration) -> bool {
    if *shutdown.borrow() {
        return false;
    }
    if d.is_zero() {
        return true;
    }
    tokio::select! {
        r = shutdown.changed() => r.is_ok() && !*shutdown.borrow(),
        _ = tokio::time::sleep(d) => true,
    }
}

async fn live_loop(ctx: Arc<Ctx>, mut shutdown: watch::Receiver<bool>) {
    let interval = Duration::from_millis(ctx.cfg.interval_ms);
    let mut backoff = interval;
    let mut failing = false;
    let mut last_log: Option<Instant> = None;
    loop {
        let cutoff = now_ms() - ctx.cfg.live_window_ms;
        let db = ctx.db.clone();
        let limit = ctx.cfg.live_max;
        let wait = match blocking(move || db.get_unsynced_live(cutoff, limit)).await {
            Ok(rows) if rows.is_empty() => interval,
            Ok(rows) => match send_batch(&ctx, "live", rows).await {
                Ok(n) => {
                    backoff = interval;
                    if std::mem::take(&mut failing) {
                        log::info!("Gateway reachable again");
                        ctx.recovered.notify_one();
                    }
                    if n >= limit {
                        Duration::ZERO
                    } else {
                        interval
                    }
                }
                Err(e) => {
                    failing = true;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                    note_error(&ctx, "live", &e, backoff, &mut last_log);
                    backoff
                }
            },
            Err(e) => {
                note_error(&ctx, "live", &e, interval, &mut last_log);
                interval
            }
        };
        if !pause(&mut shutdown, wait).await {
            return;
        }
    }
}

async fn backfill_loop(ctx: Arc<Ctx>, mut shutdown: watch::Receiver<bool>) {
    let mut backoff = Duration::from_secs(1);
    let mut last_log: Option<Instant> = None;
    loop {
        let cutoff = now_ms() - ctx.cfg.live_window_ms;
        let db = ctx.db.clone();
        let per_request = ctx.cfg.backfill_max;
        let limit = per_request * ctx.cfg.backfill_parallel;
        let wait = match blocking(move || db.get_unsynced_backfill(cutoff, limit)).await {
            Ok(rows) if rows.is_empty() => Duration::from_secs(1),
            Ok(rows) => {
                // Contiguous chunks in seq order, sent in parallel.
                let mut sends = JoinSet::new();
                for chunk in rows.chunks(per_request) {
                    let ctx = ctx.clone();
                    let chunk = chunk.to_vec();
                    sends.spawn(async move { send_batch(&ctx, "backfill", chunk).await });
                }
                let mut first_err = None;
                while let Some(r) = sends.join_next().await {
                    match r {
                        Ok(Ok(_)) => {}
                        Ok(Err(e)) => first_err = first_err.or(Some(e)),
                        Err(e) => first_err = first_err.or(Some(format!("send task: {e}"))),
                    }
                }
                match first_err {
                    None => {
                        backoff = Duration::from_secs(1);
                        Duration::ZERO
                    }
                    Some(e) => {
                        backoff = (backoff * 2).min(Duration::from_secs(30));
                        note_error(&ctx, "backfill", &e, backoff, &mut last_log);
                        backoff
                    }
                }
            }
            Err(e) => {
                note_error(&ctx, "backfill", &e, backoff, &mut last_log);
                backoff
            }
        };
        if wait.is_zero() {
            continue;
        }
        tokio::select! {
            r = shutdown.changed() => {
                if r.is_err() || *shutdown.borrow() {
                    return;
                }
            }
            _ = ctx.recovered.notified() => backoff = Duration::from_secs(1),
            _ = tokio::time::sleep(wait) => {}
        }
    }
}

async fn prune_loop(ctx: Arc<Ctx>, mut shutdown: watch::Receiver<bool>) {
    let mut wait = Duration::from_secs(60);
    loop {
        if !pause(&mut shutdown, wait).await {
            return;
        }
        wait = Duration::from_secs(ctx.cfg.prune_interval_secs);
        let db = ctx.db.clone();
        let hours = ctx.cfg.retain_synced_hours;
        match blocking(move || db.prune_old_synced(hours)).await {
            Ok(0) => {}
            Ok(n) => log::info!("Pruned {n} delivered readings older than {hours}h"),
            Err(e) => log::error!("Prune failed: {e}"),
        }
    }
}

async fn status_loop(
    ctx: Arc<Ctx>,
    app_handle: tauri::AppHandle,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if !pause(&mut shutdown, Duration::from_secs(2)).await {
            return;
        }
        let db = ctx.db.clone();
        let pending = blocking(move || db.pending_count()).await.unwrap_or(-1);
        let (synced, last_error) = {
            let mut s = ctx.shared.lock();
            (
                std::mem::take(&mut s.synced_since_emit),
                s.last_error.take(),
            )
        };
        let _ = emit_sync_status(&app_handle, synced, pending, last_error);
    }
}

fn note_error(
    ctx: &Ctx,
    sender: &str,
    err: &str,
    retry_in: Duration,
    last_log: &mut Option<Instant>,
) {
    ctx.shared.lock().last_error = Some(err.to_string());
    if last_log.is_none_or(|t| t.elapsed() >= Duration::from_secs(30)) {
        *last_log = Some(Instant::now());
        log::warn!(
            "Sync ({sender}) failed, retrying in {:.2}s: {err}",
            retry_in.as_secs_f64()
        );
    }
}

/// Send one batch and mark it synced only on a complete confirmation.
async fn send_batch(ctx: &Ctx, kind: &str, rows: Vec<SyncBatchRow>) -> Result<usize, String> {
    if rows.is_empty() {
        return Ok(0);
    }
    let tag_ids: Vec<i64> = rows.iter().map(|r| r.tag_id).collect();
    let db = ctx.db.clone();
    let (meta, (min_unsynced, max_seq)) =
        blocking(move || Ok((db.tag_meta(&tag_ids)?, db.sync_cursor()?))).await?;

    let body = build_payload(
        &ctx.cfg.collector_id,
        ctx.db.epoch(),
        kind,
        min_unsynced,
        max_seq,
        &meta,
        &rows,
    );
    let json_payload = serde_json::to_vec(&body).map_err(|e| format!("JSON: {e}"))?;
    let compressed = compress_gzip(&json_payload)?;

    let mut request = ctx
        .client
        .post(&ctx.cfg.endpoint_url)
        .header("Content-Type", "application/json")
        .header("Content-Encoding", "gzip")
        .header("X-ThinPI-Batch-Count", rows.len().to_string())
        .body(compressed);
    if !ctx.cfg.api_key.is_empty() {
        request = request.header("Authorization", format!("Bearer {}", ctx.cfg.api_key));
    }

    let response = request.send().await.map_err(|e| format!("network: {e}"))?;
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(format!("HTTP {status}: {}", truncate(&text, 200)));
    }
    let reply: IngestReply = serde_json::from_str(&text).map_err(|_| {
        format!(
            "HTTP {status} but not a gateway confirmation: {}",
            truncate(&text, 120)
        )
    })?;
    if reply.accepted != rows.len() {
        return Err(format!(
            "gateway confirmed {} of {} rows; resending",
            reply.accepted,
            rows.len()
        ));
    }

    let seqs: Vec<i64> = rows.iter().map(|r| r.seq).collect();
    let n = seqs.len();
    let rejected = reply.rejected;
    if !rejected.is_empty() {
        log::warn!(
            "Gateway rejected {} readings (moved to dead_letter), e.g. seq {}: {}",
            rejected.len(),
            rejected[0].0,
            rejected[0].1
        );
    }
    let db = ctx.db.clone();
    blocking(move || db.mark_synced(&seqs, &rejected)).await?;
    ctx.shared.lock().synced_since_emit += n as i64;
    log::debug!("Synced {n} {kind} readings");
    Ok(n)
}

/// Wire format, version 1:
/// `{schema, collector_id, epoch, kind, min_unsynced_seq, max_seq,
///   tags: {"<tag_id>": {node_id, name, unit}},
///   rows: [[seq, timestamp_ms, tag_id, value, quality], ...]}`
fn build_payload(
    collector_id: &str,
    epoch: i64,
    kind: &str,
    min_unsynced: i64,
    max_seq: i64,
    meta: &std::collections::HashMap<i64, crate::models::TagMeta>,
    rows: &[SyncBatchRow],
) -> serde_json::Value {
    let tags: serde_json::Map<String, serde_json::Value> = meta
        .iter()
        .map(|(id, m)| {
            (
                id.to_string(),
                json!({"node_id": m.node_id, "name": m.tag_name, "unit": m.eng_unit}),
            )
        })
        .collect();
    let rows: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| json!([r.seq, r.timestamp, r.tag_id, r.value, r.quality]))
        .collect();
    json!({
        "schema": 1,
        "collector_id": collector_id,
        "epoch": epoch,
        "kind": kind,
        "min_unsynced_seq": min_unsynced,
        "max_seq": max_seq,
        "tags": tags,
        "rows": rows,
    })
}

async fn blocking<T, F>(f: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> rusqlite::Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| format!("spawn_blocking: {e}"))?
        .map_err(|e| format!("DB: {e}"))
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn truncate(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::TagMeta;

    #[test]
    fn payload_has_the_documented_shape() {
        let mut meta = std::collections::HashMap::new();
        meta.insert(
            7,
            TagMeta {
                node_id: "ns=2;s=Boiler.Pressure".into(),
                tag_name: "Boiler Pressure".into(),
                eng_unit: "bar".into(),
            },
        );
        let rows = vec![SyncBatchRow {
            seq: 42,
            timestamp: 1_700_000_000_000,
            tag_id: 7,
            value: 63.5,
            quality: 192,
        }];
        let v = build_payload("mill-1", 99, "live", 42, 50, &meta, &rows);
        assert_eq!(v["collector_id"], "mill-1");
        assert_eq!(v["epoch"], 99);
        assert_eq!(v["min_unsynced_seq"], 42);
        assert_eq!(v["max_seq"], 50);
        assert_eq!(v["tags"]["7"]["name"], "Boiler Pressure");
        assert_eq!(
            v["rows"][0],
            json!([42, 1_700_000_000_000i64, 7, 63.5, 192])
        );
    }

    #[test]
    fn reply_requires_accepted_and_parses_rejections() {
        let r: IngestReply =
            serde_json::from_str(r#"{"accepted":3,"rejected":[[2,"bad timestamp"]]}"#).unwrap();
        assert_eq!(r.accepted, 3);
        assert_eq!(r.rejected, vec![(2, "bad timestamp".to_string())]);
        assert!(serde_json::from_str::<IngestReply>(r#"{"ok":true}"#).is_err());
        assert!(serde_json::from_str::<IngestReply>("<html>proxy</html>").is_err());
    }

    #[test]
    fn config_reads_environment() {
        std::env::set_var("THINPI_BACKFILL_PARALLEL", "99");
        let c = SyncConfig::from_env("thinpi-abc");
        assert_eq!(c.backfill_parallel, 8, "clamped");
        assert!(!c.collector_id.is_empty());
        std::env::remove_var("THINPI_BACKFILL_PARALLEL");
    }
}
