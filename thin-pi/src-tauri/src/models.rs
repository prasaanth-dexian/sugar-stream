// Copyright (c) 2026 Dexian India Technologies Private Limited.
// All rights reserved.
//
// This file is proprietary and confidential. Unauthorised copying,
// distribution or use of this file, via any medium, is strictly
// prohibited without the prior written permission of Dexian India
// Technologies Private Limited.

use serde::{Deserialize, Serialize};

// ── Telemetry Record (ingested from OPC/Simulation) ───────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryRecord {
    pub timestamp: i64,
    pub tag_id: i64,
    pub value: f64,
    pub quality: i32,
}

// ── Trend Data (sent to frontend, uPlot-compatible) ───────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendData {
    pub timestamps: Vec<f64>,
    pub series: Vec<TrendSeries>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendSeries {
    pub tag_id: i64,
    pub tag_name: String,
    pub eng_unit: String,
    pub values: Vec<Option<f64>>,
}

// ── OPC Tag Tree Node (hierarchical browse result) ────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpcTagNode {
    #[serde(default)]
    pub tag_id: i64,
    pub node_id: String,
    pub name: String,
    pub eng_unit: String,
    pub children: Vec<OpcTagNode>,
}

// ── System Health (status bar data) ───────────────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemHealth {
    pub db_size_mb: f64,
    pub total_rows: i64,
    pub pending_sync: i64,
    pub opc_connected: bool,
    pub opc_latency_ms: u32,
    pub license_tier: String,
    pub uptime_secs: u64,
}

// ── License Types ─────────────────────────────────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LicenseStatus {
    pub tier: String,
    pub licensee: String,
    pub max_tags: i32,
    pub cloud_sync: bool,
    pub expires_at: i64,
    pub valid: bool,
}

// ── Auto-Discovered Industrial Server ─────────────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredServer {
    pub name: String,
    pub endpoint: String,
    pub server_type: String, // "OPC UA" | "OPC DA"
    pub description: String,
    pub is_online: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LicensePayload {
    pub hwid: String,
    pub tier: String,
    pub max_tags: i32,
    pub cloud_sync: bool,
    pub issued_at: i64,
    pub expires_at: i64,
    pub licensee: String,
}

// ── Sync Worker Row ───────────────────────────────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncBatchRow {
    /// Delivery identity, never reused (see `db::TELEMETRY_TABLE`).
    pub seq: i64,
    pub timestamp: i64,
    pub tag_id: i64,
    pub value: f64,
    pub quality: i32,
}

// ── Tag metadata sent with each sync batch ────────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TagMeta {
    pub node_id: String,
    pub tag_name: String,
    pub eng_unit: String,
}

// ── Sync Status Event (emitted to frontend) ──────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncStatusEvent {
    pub synced_count: i64,
    pub pending_count: i64,
    pub last_error: Option<String>,
}
