// Copyright (c) 2026 Dexian India Technologies Private Limited.
// All rights reserved.
//
// This file is proprietary and confidential. Unauthorised copying,
// distribution or use of this file, via any medium, is strictly
// prohibited without the prior written permission of Dexian India
// Technologies Private Limited.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod db;
mod license;
mod lttb;
mod models;
mod opc_engine;
mod sync_worker;

use db::DbPool;
use license::{get_community_license, get_hardware_id, verify_license};
use models::*;
use opc_engine::OpcEngine;
use sync_worker::SyncConfig;

use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Instant;
use tauri::{Manager, State};
use tokio::sync::{mpsc, watch};

// ── Application State ─────────────────────────────────────────────────────────

struct AppState {
    db: Arc<DbPool>,
    opc: Arc<Mutex<OpcEngine>>,
    /// Receiver end of the OPC telemetry channel — taken once by flush_worker
    opc_rx: Mutex<Option<mpsc::Receiver<TelemetryRecord>>>,
    license: Arc<Mutex<LicenseStatus>>,
    start_time: Instant,
    /// Sync worker shutdown channel
    sync_shutdown: Mutex<Option<watch::Sender<bool>>>,
}

// ── IPC Commands ──────────────────────────────────────────────────────────────

/// Return the SHA-256 hardware fingerprint of this machine.
#[tauri::command]
fn get_hardware_id_cmd() -> Result<String, String> {
    get_hardware_id()
}

/// Verify and apply a license key. Updates the active license tier.
#[tauri::command]
async fn apply_license_key(
    state: State<'_, AppState>,
    license_str: String,
) -> Result<LicenseStatus, String> {
    let status = verify_license(&license_str)?;

    // Update in-memory license state
    let mut lic = state.license.lock();
    *lic = status.clone();

    // Save license to disk for persistence across restarts
    let db_path = format!("{}/../thinpi_license.key", state.db.get_db_path());
    let _ = std::fs::write(&db_path, license_str.trim());

    log::info!(
        "License activated: tier={}, licensee={}, max_tags={}",
        status.tier,
        status.licensee,
        status.max_tags
    );

    Ok(status)
}

/// Connect to an OPC UA server or start the simulation engine.
///
/// If `endpoint_url` is empty or "simulator", starts the built-in simulation.
#[tauri::command]
async fn connect_opc_server(
    state: State<'_, AppState>,
    app_handle: tauri::AppHandle,
    endpoint_url: String,
) -> Result<bool, String> {
    let db = state.db.clone();

    // Connect to OPC UA or start simulation
    {
        let opc = state.opc.lock();
        opc.connect(&endpoint_url, db.clone())?;
    }

    // Take the receiver and start the flush worker (only once)
    let maybe_rx = state.opc_rx.lock().take();
    if let Some(rx) = maybe_rx {
        let db_for_flush = db.clone();
        tauri::async_runtime::spawn(async move {
            opc_engine::flush_worker(rx, db_for_flush).await;
        });
        log::info!("Flush worker spawned");
    }

    // Start sync worker if licensed for cloud sync
    let cloud_sync_enabled = {
        let lic = state.license.lock();
        lic.cloud_sync
    };

    if cloud_sync_enabled {
        start_sync_if_needed(&state, &app_handle);
    }

    log::info!(
        "Connected to: {}",
        if endpoint_url.is_empty() {
            "simulator"
        } else {
            &endpoint_url
        }
    );

    Ok(true)
}

/// Browse available OPC tags (returns hierarchical tree).
#[tauri::command]
fn browse_opc_tags(state: State<'_, AppState>) -> Result<Vec<OpcTagNode>, String> {
    let opc = state.opc.lock();
    Ok(opc.get_tag_tree())
}

/// Auto-discover local OPC UA and OPC DA servers.
#[tauri::command]
async fn discover_local_servers() -> Result<Vec<DiscoveredServer>, String> {
    tokio::task::spawn_blocking(opc_engine::discover_servers)
        .await
        .map_err(|e| format!("Discovery task failed: {}", e))
}

/// Subscribe to a set of tags for trending.
#[tauri::command]
async fn subscribe_tags(state: State<'_, AppState>, tag_ids: Vec<String>) -> Result<bool, String> {
    let opc = state.opc.lock();
    opc.subscribe(tag_ids)
}

/// Query historical trend data with LTTB downsampling.
///
/// If `viewport_width` is provided and the raw data exceeds 2× that value,
/// LTTB downsampling is applied to keep the IPC payload under ~2000 points.
/// This prevents the 200–500ms JSON serialization bottleneck.
#[tauri::command]
async fn query_historical_trend(
    state: State<'_, AppState>,
    tag_ids: Vec<i64>,
    start_time: i64,
    end_time: i64,
    viewport_width: Option<usize>,
) -> Result<TrendData, String> {
    let db = state.db.clone();

    // Query raw data on blocking thread (READER connection)
    let raw_trend =
        tokio::task::spawn_blocking(move || db.query_trend(&tag_ids, start_time, end_time))
            .await
            .map_err(|e| format!("Query task error: {}", e))?
            .map_err(|e| format!("DB query error: {}", e))?;

    // Apply LTTB downsampling if needed
    let target = viewport_width.unwrap_or(2000).max(100);
    let raw_len = raw_trend.timestamps.len();

    if raw_len <= target * 2 || raw_trend.series.is_empty() {
        return Ok(raw_trend);
    }

    let all_values: Vec<Vec<Option<f64>>> =
        raw_trend.series.iter().map(|s| s.values.clone()).collect();

    let (ds_timestamps, ds_values) =
        lttb::downsample_multi_series(&raw_trend.timestamps, &all_values, target);

    let ds_series: Vec<TrendSeries> = raw_trend
        .series
        .iter()
        .enumerate()
        .map(|(i, s)| TrendSeries {
            tag_id: s.tag_id,
            tag_name: s.tag_name.clone(),
            eng_unit: s.eng_unit.clone(),
            values: ds_values[i].clone(),
        })
        .collect();

    log::trace!(
        "LTTB: {} raw points → {} downsampled (viewport={}px)",
        raw_len,
        ds_timestamps.len(),
        target
    );

    Ok(TrendData {
        timestamps: ds_timestamps,
        series: ds_series,
    })
}

/// Get system health metrics for the status bar.
#[tauri::command]
fn get_system_stats(state: State<'_, AppState>) -> Result<SystemHealth, String> {
    let (db_size_mb, total_rows, pending_sync) = state
        .db
        .get_stats()
        .map_err(|e| format!("Stats query error: {}", e))?;

    let opc = state.opc.lock();
    let lic = state.license.lock();

    Ok(SystemHealth {
        db_size_mb,
        total_rows,
        pending_sync,
        opc_connected: opc.is_connected(),
        opc_latency_ms: opc.get_latency(),
        license_tier: lic.tier.clone(),
        uptime_secs: state.start_time.elapsed().as_secs(),
    })
}

/// Export trend data as CSV for the given tags and time window.
#[tauri::command]
async fn export_csv(
    state: State<'_, AppState>,
    tag_ids: Vec<i64>,
    start_time: i64,
    end_time: i64,
) -> Result<String, String> {
    let db = state.db.clone();

    // Query raw (non-downsampled) data
    let trend = tokio::task::spawn_blocking(move || db.query_trend(&tag_ids, start_time, end_time))
        .await
        .map_err(|e| format!("Export task error: {}", e))?
        .map_err(|e| format!("DB query error: {}", e))?;

    if trend.timestamps.is_empty() {
        return Ok("Timestamp\n".into());
    }

    // Build CSV
    let mut csv = String::with_capacity(trend.timestamps.len() * 64);

    // Header
    csv.push_str("Timestamp");
    for s in &trend.series {
        csv.push(',');
        csv.push_str(&format!("{} ({})", s.tag_name, s.eng_unit));
    }
    csv.push('\n');

    // Rows
    for (i, ts) in trend.timestamps.iter().enumerate() {
        // Convert epoch seconds to ISO 8601
        let dt = chrono::DateTime::from_timestamp(*ts as i64, 0)
            .map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| format!("{}", ts));

        csv.push_str(&dt);

        for s in &trend.series {
            csv.push(',');
            match s.values.get(i) {
                Some(Some(v)) => csv.push_str(&format!("{:.4}", v)),
                _ => {} // empty cell for gaps
            }
        }
        csv.push('\n');
    }

    Ok(csv)
}

/// Generate a development license for testing (dev builds only).
#[tauri::command]
fn generate_dev_license_cmd() -> Result<String, String> {
    let hwid = get_hardware_id()?;
    license::generate_dev_license(&hwid)
}

// ── Helper Functions ──────────────────────────────────────────────────────────

fn start_sync_if_needed(state: &AppState, app_handle: &tauri::AppHandle) {
    let mut guard = state.sync_shutdown.lock();
    if guard.is_some() {
        return; // already running
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    *guard = Some(shutdown_tx);

    let db = state.db.clone();
    let handle = app_handle.clone();
    let config = SyncConfig::default();

    tauri::async_runtime::spawn(async move {
        sync_worker::start_sync_worker(db, config, handle, shutdown_rx).await;
    });

    log::info!("Sync worker started (cloud sync enabled)");
}

// ── Main Entry Point ──────────────────────────────────────────────────────────

fn main() {
    // Initialize logger for development
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    tauri::Builder::default()
        .setup(|app| {
            // Resolve app data directory for the database
            let app_data_dir = app
                .path()
                .app_data_dir()
                .map_err(|e| format!("Failed to resolve app data dir: {}", e))?;

            std::fs::create_dir_all(&app_data_dir)
                .map_err(|e| format!("Failed to create app data dir: {}", e))?;

            let db_path = app_data_dir.join("thinpi.db");
            let db_path_str = db_path.to_string_lossy().to_string();

            // Initialize database with dual connections
            let db = Arc::new(
                DbPool::new(&db_path_str)
                    .map_err(|e| format!("Failed to initialize database: {}", e))?,
            );

            // Load saved license or default to Community
            let license_path = app_data_dir.join("thinpi_license.key");
            let license = if license_path.exists() {
                match std::fs::read_to_string(&license_path) {
                    Ok(key) => verify_license(&key).unwrap_or_else(|e| {
                        log::warn!("Saved license invalid: {}", e);
                        get_community_license()
                    }),
                    Err(_) => get_community_license(),
                }
            } else {
                get_community_license()
            };

            log::info!("License tier: {} ({})", license.tier, license.licensee);

            // Create OPC engine with 10K buffer
            let (opc_engine, opc_rx) = OpcEngine::new(10_000);

            // Manage state
            app.manage(AppState {
                db: db.clone(),
                opc: Arc::new(Mutex::new(opc_engine)),
                opc_rx: Mutex::new(Some(opc_rx)),
                license: Arc::new(Mutex::new(license)),
                start_time: Instant::now(),
                sync_shutdown: Mutex::new(None),
            });

            log::info!("THIN PI System initialized — DB: {}", db_path.display());

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            get_hardware_id_cmd,
            apply_license_key,
            connect_opc_server,
            browse_opc_tags,
            discover_local_servers,
            subscribe_tags,
            query_historical_trend,
            get_system_stats,
            export_csv,
            generate_dev_license_cmd,
        ])
        .run(tauri::generate_context!())
        .expect("Error while running THIN PI System");
}
