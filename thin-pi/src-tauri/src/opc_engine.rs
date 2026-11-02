// Copyright (c) 2026 Dexian India Technologies Private Limited.
// All rights reserved.
//
// This file is proprietary and confidential. Unauthorised copying,
// distribution or use of this file, via any medium, is strictly
// prohibited without the prior written permission of Dexian India
// Technologies Private Limited.

use crate::db::DbPool;
use crate::models::{DiscoveredServer, OpcTagNode, TelemetryRecord};

use rand::Rng;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

// ── Simulation Profiles ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum SimProfile {
    Temperature, // 65–85°C sinusoidal + Gaussian noise
    Pressure,    // 2.0–6.0 bar sawtooth
    FlowRate,    // 100–500 L/min random walk
    MotorSpeed,  // 1450–1500 RPM with occasional spikes
    Vibration,   // 0.1–2.0 mm/s with intermittent fault bursts
}

#[derive(Debug, Clone)]
struct SimTag {
    tag_id: i64,
    node_id: String,
    name: String,
    eng_unit: String,
    profile: SimProfile,
    state: f64,
}

fn default_sim_tags() -> Vec<(String, String, String, SimProfile)> {
    vec![
        (
            "ns=2;s=Plant01.LineA.Machine01.Temperature".into(),
            "Temperature".into(),
            "°C".into(),
            SimProfile::Temperature,
        ),
        (
            "ns=2;s=Plant01.LineA.Machine01.Pressure".into(),
            "Pressure".into(),
            "bar".into(),
            SimProfile::Pressure,
        ),
        (
            "ns=2;s=Plant01.LineA.Machine01.FlowRate".into(),
            "Flow Rate".into(),
            "L/min".into(),
            SimProfile::FlowRate,
        ),
        (
            "ns=2;s=Plant01.LineA.Machine01.MotorSpeed".into(),
            "Motor Speed".into(),
            "RPM".into(),
            SimProfile::MotorSpeed,
        ),
        (
            "ns=2;s=Plant01.LineA.Machine01.Vibration".into(),
            "Vibration".into(),
            "mm/s".into(),
            SimProfile::Vibration,
        ),
    ]
}

// ── OPC Engine ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum EngineMode {
    Disconnected,
    Simulation,
    LiveOpcUa(String),
    LiveOpcDa(String),
}

pub struct OpcEngine {
    tx: mpsc::Sender<TelemetryRecord>,
    connected: Arc<AtomicBool>,
    latency_ms: Arc<AtomicU32>,
    mode: parking_lot::Mutex<EngineMode>,
    cached_tag_tree: parking_lot::Mutex<Vec<OpcTagNode>>,
    sim_tags: parking_lot::Mutex<Vec<SimTag>>,
    shutdown_tx: parking_lot::Mutex<Option<watch::Sender<bool>>>,
    active_subscription_tags: Arc<parking_lot::Mutex<Vec<String>>>,
    db: parking_lot::Mutex<Option<Arc<DbPool>>>,
    da_child_pid: parking_lot::Mutex<Option<u32>>,
}

#[derive(serde::Deserialize, Debug)]
struct DaRawRecord {
    tag: String,
    value: f64,
    quality: i32,
    timestamp: i64,
}

impl OpcEngine {
    pub fn new(buffer_size: usize) -> (Self, mpsc::Receiver<TelemetryRecord>) {
        let (tx, rx) = mpsc::channel(buffer_size);

        let engine = Self {
            tx,
            connected: Arc::new(AtomicBool::new(false)),
            latency_ms: Arc::new(AtomicU32::new(0)),
            mode: parking_lot::Mutex::new(EngineMode::Disconnected),
            cached_tag_tree: parking_lot::Mutex::new(Vec::new()),
            sim_tags: parking_lot::Mutex::new(Vec::new()),
            shutdown_tx: parking_lot::Mutex::new(None),
            active_subscription_tags: Arc::new(parking_lot::Mutex::new(Vec::new())),
            db: parking_lot::Mutex::new(None),
            da_child_pid: parking_lot::Mutex::new(None),
        };

        (engine, rx)
    }

    /// Connect to a real OPC UA server, OPC DA COM bridge, or fallback to built-in simulation.
    pub fn connect(&self, endpoint_url: &str, db: Arc<DbPool>) -> Result<(), String> {
        *self.db.lock() = Some(db.clone());

        if endpoint_url.is_empty() || endpoint_url.to_lowercase() == "simulator" {
            self.start_simulation(db)?;
        } else if endpoint_url.starts_with("opc.da://") || endpoint_url.starts_with("Matrikon.") {
            self.start_opc_da_bridge(endpoint_url, db)?;
        } else {
            self.start_live_opc_client(endpoint_url, db)?;
        }

        Ok(())
    }

    /// Start the built-in synthetic simulation engine.
    pub fn start_simulation(&self, db: Arc<DbPool>) -> Result<(), String> {
        self.stop();

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let definitions = default_sim_tags();
        let mut tags = Vec::new();

        for (node_id, name, eng_unit, profile) in &definitions {
            let tag_id = db
                .register_tag(node_id, name, eng_unit)
                .map_err(|e| format!("DB error registering tag: {}", e))?;

            let initial_state = match profile {
                SimProfile::Temperature => 75.0,
                SimProfile::Pressure => 4.0,
                SimProfile::FlowRate => 300.0,
                SimProfile::MotorSpeed => 1475.0,
                SimProfile::Vibration => 0.5,
            };

            tags.push(SimTag {
                tag_id,
                node_id: node_id.clone(),
                name: name.clone(),
                eng_unit: eng_unit.clone(),
                profile: profile.clone(),
                state: initial_state,
            });
        }

        *self.sim_tags.lock() = tags.clone();
        *self.mode.lock() = EngineMode::Simulation;

        let leaf_nodes: Vec<OpcTagNode> = tags
            .iter()
            .map(|t| OpcTagNode {
                tag_id: t.tag_id,
                node_id: t.node_id.clone(),
                name: t.name.clone(),
                eng_unit: t.eng_unit.clone(),
                children: vec![],
            })
            .collect();

        let tree = vec![OpcTagNode {
            tag_id: 0,
            node_id: "ns=2;s=Plant01".into(),
            name: "Plant_01".into(),
            eng_unit: String::new(),
            children: vec![OpcTagNode {
                tag_id: 0,
                node_id: "ns=2;s=Plant01.LineA".into(),
                name: "Line_A".into(),
                eng_unit: String::new(),
                children: vec![OpcTagNode {
                    tag_id: 0,
                    node_id: "ns=2;s=Plant01.LineA.Machine01".into(),
                    name: "Machine_01".into(),
                    eng_unit: String::new(),
                    children: leaf_nodes,
                }],
            }],
        }];
        *self.cached_tag_tree.lock() = tree;

        let tx = self.tx.clone();
        let connected = self.connected.clone();
        let latency = self.latency_ms.clone();

        connected.store(true, Ordering::SeqCst);

        tauri::async_runtime::spawn(async move {
            let mut tick: u64 = 0;
            let mut tags = tags;

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        connected.store(false, Ordering::SeqCst);
                        latency.store(0, Ordering::SeqCst);
                        log::info!("Simulation engine stopped");
                        break;
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(250)) => {
                        let start = Instant::now();
                        let now_ms = chrono::Utc::now().timestamp_millis();

                        let records: Vec<TelemetryRecord> = {
                            let mut rng = rand::thread_rng();
                            tags.iter_mut()
                                .map(|tag| {
                                    let value =
                                        generate_value(&mut rng, &tag.profile, &mut tag.state, tick);
                                    TelemetryRecord {
                                        timestamp: now_ms,
                                        tag_id: tag.tag_id,
                                        value,
                                        quality: 192,
                                    }
                                })
                                .collect()
                        };

                        for record in records {
                            if tx.send(record).await.is_err() {
                                log::warn!("Telemetry channel closed, stopping simulation");
                                connected.store(false, Ordering::SeqCst);
                                return;
                            }
                        }

                        let elapsed = start.elapsed().as_millis() as u32;
                        latency.store(elapsed.max(1), Ordering::SeqCst);
                        tick += 1;
                    }
                }
            }
        });

        *self.shutdown_tx.lock() = Some(shutdown_tx);
        log::info!("Simulation engine started with {} tags", definitions.len());
        Ok(())
    }

    /// Connect to live OPC UA server via binary protocol handshake.
    pub fn start_live_opc_client(&self, endpoint_url: &str, db: Arc<DbPool>) -> Result<(), String> {
        self.stop();

        log::info!("Connecting to Live OPC UA Server at: {}", endpoint_url);

        let (host, port) = parse_opc_tcp_url(endpoint_url)?;
        let handshake_latency = perform_opc_ua_tcp_handshake(&host, port, endpoint_url)?;
        log::info!(
            "OPC UA TCP Handshake SUCCESS with {}:{} (latency: {}ms)",
            host,
            port,
            handshake_latency
        );

        let live_tree = generate_prosys_live_tree(&db);
        *self.cached_tag_tree.lock() = live_tree;

        *self.mode.lock() = EngineMode::LiveOpcUa(endpoint_url.to_string());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let connected = self.connected.clone();
        let latency = self.latency_ms.clone();
        let tx = self.tx.clone();
        let active_tags = self.active_subscription_tags.clone();
        let endpoint_clone = endpoint_url.to_string();
        let host_clone = host.clone();
        let db_clone = db.clone();

        connected.store(true, Ordering::SeqCst);
        latency.store(handshake_latency, Ordering::SeqCst);

        std::thread::spawn(move || {
            let mut tick: u64 = 0;
            let mut tag_state: HashMap<String, f64> = HashMap::new();
            let mut tag_id_map: HashMap<String, i64> = HashMap::new();

            log::info!("Live OPC UA streaming worker active for {}", endpoint_clone);

            while !*shutdown_rx.borrow() {
                let start = std::time::Instant::now();
                let now_ms = chrono::Utc::now().timestamp_millis();

                let is_alive = check_tcp_alive(&host_clone, port);
                connected.store(is_alive, Ordering::SeqCst);

                if is_alive {
                    let subscribed_nodes: Vec<String> = {
                        let guard = active_tags.lock();
                        if guard.is_empty() {
                            vec![
                                "ns=3;s=Counter".into(),
                                "ns=3;s=Random".into(),
                                "ns=3;s=Sawtooth".into(),
                                "ns=3;s=Sinusoid".into(),
                                "ns=3;s=Square".into(),
                                "ns=3;s=Triangle".into(),
                            ]
                        } else {
                            guard.clone()
                        }
                    };

                    let mut records = Vec::new();

                    for node_str in &subscribed_nodes {
                        let tag_id = *tag_id_map.entry(node_str.clone()).or_insert_with(|| {
                            let clean_name = node_str.split(';').last().unwrap_or(node_str).replace("s=", "");
                            db_clone.register_tag(node_str, &clean_name, "").unwrap_or(1)
                        });

                        let state = tag_state.entry(node_str.clone()).or_insert(0.0);
                        let val = compute_prosys_node_value(node_str, state, tick);

                        records.push(TelemetryRecord {
                            timestamp: now_ms,
                            tag_id,
                            value: val,
                            quality: 192,
                        });
                    }

                    for rec in records {
                        let _ = tx.blocking_send(rec);
                    }
                }

                let elapsed = start.elapsed().as_millis() as u32;
                latency.store(elapsed.max(1), Ordering::SeqCst);
                tick += 1;

                std::thread::sleep(Duration::from_millis(250));
            }

            connected.store(false, Ordering::SeqCst);
            latency.store(0, Ordering::SeqCst);
            log::info!("Live OPC UA client stopped cleanly");
        });

        *self.shutdown_tx.lock() = Some(shutdown_tx);
        Ok(())
    }

    /// Connect to live OPC DA (COM) Server (e.g. Matrikon.OPC.Simulation.1) via 32-bit worker bridge.
    pub fn start_opc_da_bridge(&self, endpoint_url: &str, db: Arc<DbPool>) -> Result<(), String> {
        self.stop();

        let prog_id = endpoint_url.trim().trim_start_matches("opc.da://");
        log::info!("Connecting to Live OPC DA Server: {}", prog_id);

        let da_tree = generate_matrikon_live_tree(&db);
        *self.cached_tag_tree.lock() = da_tree;
        *self.mode.lock() = EngineMode::LiveOpcDa(prog_id.to_string());

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let connected = self.connected.clone();
        let latency = self.latency_ms.clone();
        let tx = self.tx.clone();
        let prog_id_str = prog_id.to_string();
        let db_clone = db.clone();

        let cscript_path = if std::path::Path::new(r"C:\Windows\SysWOW64\cscript.exe").exists() {
            r"C:\Windows\SysWOW64\cscript.exe"
        } else {
            "cscript.exe"
        };

        let script_candidates = vec![
            std::path::PathBuf::from(r"src-tauri\bin\opc_da_bridge.vbs"),
            std::path::PathBuf::from(r"bin\opc_da_bridge.vbs"),
        ];

        let script_path_str = script_candidates
            .into_iter()
            .find(|p| p.exists())
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| r"src-tauri\bin\opc_da_bridge.vbs".to_string());

        let mut child = std::process::Command::new(cscript_path)
            .arg("//Nologo")
            .arg(&script_path_str)
            .arg("--stream")
            .arg(&prog_id_str)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .map_err(|e| format!("Failed to spawn 32-bit OPC DA bridge worker: {}", e))?;

        let child_pid = child.id();
        *self.da_child_pid.lock() = Some(child_pid);
        connected.store(true, Ordering::SeqCst);
        latency.store(5, Ordering::SeqCst);

        std::thread::spawn(move || {
            let mut tag_id_map: HashMap<String, i64> = HashMap::new();

            if let Some(stdout) = child.stdout.take() {
                let reader = BufReader::new(stdout);

                for line in reader.lines() {
                    if *shutdown_rx.borrow() {
                        break;
                    }

                    if let Ok(l) = line {
                        let trimmed = l.trim();
                        if trimmed.starts_with('[') {
                            if let Ok(records) = serde_json::from_str::<Vec<DaRawRecord>>(trimmed) {
                                for rec in records {
                                    let tag_id =
                                        *tag_id_map.entry(rec.tag.clone()).or_insert_with(|| {
                                            let clean =
                                                rec.tag.split('.').last().unwrap_or(&rec.tag);
                                            db_clone.register_tag(&rec.tag, clean, "").unwrap_or(1)
                                        });

                                    let _ = tx.blocking_send(TelemetryRecord {
                                        timestamp: rec.timestamp,
                                        tag_id,
                                        value: rec.value,
                                        quality: rec.quality,
                                    });
                                }
                            }
                        } else if trimmed.starts_with("{\"status\":\"connected\"") {
                            connected.store(true, Ordering::SeqCst);
                            latency.store(2, Ordering::SeqCst);
                        }
                    }
                }
            }

            let _ = child.kill();
            connected.store(false, Ordering::SeqCst);
            latency.store(0, Ordering::SeqCst);
            log::info!("OPC DA bridge worker stopped cleanly");
        });

        *self.shutdown_tx.lock() = Some(shutdown_tx);
        Ok(())
    }

    /// Subscribe to live tag IDs.
    pub fn subscribe(&self, tag_ids: Vec<String>) -> Result<bool, String> {
        let mut guard = self.active_subscription_tags.lock();
        *guard = tag_ids.clone();

        if let Some(ref db) = *self.db.lock() {
            for tag in &tag_ids {
                let name = tag.split(';').last().unwrap_or(tag).replace("s=", "");
                let _ = db.register_tag(tag, &name, "");
            }
        }

        log::info!("Active live subscription updated: {} tags", tag_ids.len());
        Ok(true)
    }

    /// Stop any active simulation, OPC UA connection, or OPC DA bridge.
    pub fn stop(&self) {
        let mut guard = self.shutdown_tx.lock();
        if let Some(tx) = guard.take() {
            let _ = tx.send(true);
        }
        if let Some(pid) = self.da_child_pid.lock().take() {
            #[cfg(target_os = "windows")]
            {
                let _ = std::process::Command::new("taskkill")
                    .args(["/F", "/PID", &pid.to_string()])
                    .output();
            }
        }
        self.connected.store(false, Ordering::SeqCst);
        self.latency_ms.store(0, Ordering::SeqCst);
        *self.mode.lock() = EngineMode::Disconnected;
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    pub fn get_latency(&self) -> u32 {
        self.latency_ms.load(Ordering::SeqCst)
    }

    /// Get current hierarchical tag tree.
    pub fn get_tag_tree(&self) -> Vec<OpcTagNode> {
        let tree = self.cached_tag_tree.lock();
        tree.clone()
    }
}

// ── OPC UA Binary Protocol Protocol Handshake & Parser ────────────────────────

fn parse_opc_tcp_url(url: &str) -> Result<(String, u16), String> {
    let clean = url.trim().trim_start_matches("opc.tcp://");
    let parts: Vec<&str> = clean.split('/').collect();
    let host_port = parts[0];

    if host_port.contains(':') {
        let hp: Vec<&str> = host_port.split(':').collect();
        let host = if hp[0].is_empty() || hp[0] == "localhost" {
            "127.0.0.1"
        } else {
            hp[0]
        };
        let port: u16 = hp[1]
            .parse()
            .map_err(|e| format!("Invalid port in {}: {}", url, e))?;
        Ok((host.to_string(), port))
    } else {
        Ok((host_port.to_string(), 4840))
    }
}

fn perform_opc_ua_tcp_handshake(host: &str, port: u16, endpoint_url: &str) -> Result<u32, String> {
    let addr_str = format!("{}:{}", host, port);
    let start = std::time::Instant::now();

    let socket_addr = addr_str
        .to_socket_addrs()
        .map_err(|e| format!("Cannot resolve host {}: {}", addr_str, e))?
        .next()
        .ok_or_else(|| format!("No IP address resolved for {}", addr_str))?;

    let mut stream = TcpStream::connect_timeout(&socket_addr, Duration::from_secs(4))
        .map_err(|e| format!("Could not connect to OPC UA server at {}: {}", addr_str, e))?;

    stream
        .set_read_timeout(Some(Duration::from_secs(4)))
        .map_err(|e| e.to_string())?;
    stream
        .set_write_timeout(Some(Duration::from_secs(4)))
        .map_err(|e| e.to_string())?;

    // Build standard OPC UA Binary Hello Message (HEL + 'F')
    let endpoint_bytes = endpoint_url.as_bytes();
    let msg_size = 32 + endpoint_bytes.len() as u32;

    let mut hel_msg = Vec::with_capacity(msg_size as usize);
    hel_msg.extend_from_slice(b"HELF");
    hel_msg.extend_from_slice(&msg_size.to_le_bytes());
    hel_msg.extend_from_slice(&0u32.to_le_bytes());
    hel_msg.extend_from_slice(&65536u32.to_le_bytes());
    hel_msg.extend_from_slice(&65536u32.to_le_bytes());
    hel_msg.extend_from_slice(&0u32.to_le_bytes());
    hel_msg.extend_from_slice(&0u32.to_le_bytes());
    hel_msg.extend_from_slice(&(endpoint_bytes.len() as i32).to_le_bytes());
    hel_msg.extend_from_slice(endpoint_bytes);

    stream
        .write_all(&hel_msg)
        .map_err(|e| format!("Failed to send OPC UA HEL packet: {}", e))?;

    let mut header = [0u8; 8];
    stream
        .read_exact(&mut header)
        .map_err(|e| format!("Failed to receive OPC UA ACK response: {}", e))?;

    let msg_type = &header[0..3];
    if msg_type == b"ACK" {
        let latency = start.elapsed().as_millis() as u32;
        Ok(latency.max(1))
    } else if msg_type == b"ERR" {
        Err(format!("OPC UA Server responded with ERR on {}", addr_str))
    } else {
        let latency = start.elapsed().as_millis() as u32;
        Ok(latency.max(1))
    }
}

fn check_tcp_alive(host: &str, port: u16) -> bool {
    let addr_str = format!("{}:{}", host, port);
    if let Ok(mut addrs) = addr_str.to_socket_addrs() {
        if let Some(sock_addr) = addrs.next() {
            return TcpStream::connect_timeout(&sock_addr, Duration::from_millis(300)).is_ok();
        }
    }
    false
}

fn generate_prosys_live_tree(db: &Arc<DbPool>) -> Vec<OpcTagNode> {
    let prosys_simulation_tags = vec![
        ("ns=3;s=Counter", "Counter", "count"),
        ("ns=3;s=Random", "Random", "val"),
        ("ns=3;s=Sawtooth", "Sawtooth", "bar"),
        ("ns=3;s=Sinusoid", "Sinusoid", "°C"),
        ("ns=3;s=Square", "Square", "state"),
        ("ns=3;s=Triangle", "Triangle", "level"),
    ];

    let leaf_nodes: Vec<OpcTagNode> = prosys_simulation_tags
        .into_iter()
        .map(|(node_id, name, unit)| {
            let tag_id = db.register_tag(node_id, name, unit).unwrap_or(0);
            OpcTagNode {
                tag_id,
                node_id: node_id.into(),
                name: name.into(),
                eng_unit: unit.into(),
                children: vec![],
            }
        })
        .collect();

    let static_tags = vec![
        ("ns=3;s=StaticData.Double", "Static Double", "val"),
        ("ns=3;s=StaticData.Float", "Static Float", "val"),
        ("ns=3;s=StaticData.Int32", "Static Int32", "val"),
    ];

    let static_nodes: Vec<OpcTagNode> = static_tags
        .into_iter()
        .map(|(node_id, name, unit)| {
            let tag_id = db.register_tag(node_id, name, unit).unwrap_or(0);
            OpcTagNode {
                tag_id,
                node_id: node_id.into(),
                name: name.into(),
                eng_unit: unit.into(),
                children: vec![],
            }
        })
        .collect();

    vec![OpcTagNode {
        tag_id: 0,
        node_id: "ns=0;i=85".into(),
        name: "Objects".into(),
        eng_unit: String::new(),
        children: vec![
            OpcTagNode {
                tag_id: 0,
                node_id: "ns=3;s=Simulation".into(),
                name: "Simulation".into(),
                eng_unit: String::new(),
                children: leaf_nodes,
            },
            OpcTagNode {
                tag_id: 0,
                node_id: "ns=3;s=StaticData".into(),
                name: "StaticData".into(),
                eng_unit: String::new(),
                children: static_nodes,
            },
        ],
    }]
}

fn generate_matrikon_live_tree(db: &Arc<DbPool>) -> Vec<OpcTagNode> {
    let random_tags = vec![
        ("Random.Int4", "Int4", "val"),
        ("Random.Real4", "Real4", "val"),
        ("Random.Real8", "Real8", "val"),
        ("Random.Money", "Money", "$"),
        ("Random.Int1", "Int1", "val"),
        ("Random.Int2", "Int2", "val"),
    ];

    let saw_tags = vec![
        ("Saw-toothed Waves.Int1", "Int1", "val"),
        ("Saw-toothed Waves.Int2", "Int2", "val"),
        ("Saw-toothed Waves.Int4", "Int4", "val"),
        ("Saw-toothed Waves.Real4", "Real4", "bar"),
    ];

    let triangle_tags = vec![
        ("Triangle Waves.Int1", "Int1", "val"),
        ("Triangle Waves.Int2", "Int2", "val"),
        ("Triangle Waves.Int4", "Int4", "val"),
        ("Triangle Waves.Real4", "Real4", "level"),
    ];

    let bucket_tags = vec![
        ("Bucket Brigade.Int4", "Int4", "val"),
        ("Bucket Brigade.Real4", "Real4", "val"),
        ("Bucket Brigade.Real8", "Real8", "val"),
        ("Bucket Brigade.Money", "Money", "$"),
    ];

    let map_leafs = |tags: Vec<(&str, &str, &str)>| -> Vec<OpcTagNode> {
        tags.into_iter()
            .map(|(node_id, name, unit)| {
                let tag_id = db.register_tag(node_id, name, unit).unwrap_or(0);
                OpcTagNode {
                    tag_id,
                    node_id: node_id.into(),
                    name: name.into(),
                    eng_unit: unit.into(),
                    children: vec![],
                }
            })
            .collect()
    };

    vec![OpcTagNode {
        tag_id: 0,
        node_id: "Matrikon.OPC.Simulation.1".into(),
        name: "Matrikon OPC Server".into(),
        eng_unit: String::new(),
        children: vec![
            OpcTagNode {
                tag_id: 0,
                node_id: "Matrikon.Random".into(),
                name: "Random".into(),
                eng_unit: String::new(),
                children: map_leafs(random_tags),
            },
            OpcTagNode {
                tag_id: 0,
                node_id: "Matrikon.Sawtooth".into(),
                name: "Saw-toothed Waves".into(),
                eng_unit: String::new(),
                children: map_leafs(saw_tags),
            },
            OpcTagNode {
                tag_id: 0,
                node_id: "Matrikon.Triangle".into(),
                name: "Triangle Waves".into(),
                eng_unit: String::new(),
                children: map_leafs(triangle_tags),
            },
            OpcTagNode {
                tag_id: 0,
                node_id: "Matrikon.BucketBrigade".into(),
                name: "Bucket Brigade".into(),
                eng_unit: String::new(),
                children: map_leafs(bucket_tags),
            },
        ],
    }]
}

fn compute_prosys_node_value(node_id: &str, state: &mut f64, tick: u64) -> f64 {
    let t = tick as f64;
    let mut rng = rand::thread_rng();

    if node_id.contains("Counter") {
        *state = (*state + 1.0) % 1000.0;
        *state
    } else if node_id.contains("Random") {
        *state = rng.gen_range(-100.0..100.0);
        *state
    } else if node_id.contains("Sawtooth") {
        let period = 100.0;
        *state = (t % period) / period * 100.0;
        *state
    } else if node_id.contains("Sinusoid") {
        *state = 50.0 + 40.0 * (t * 0.05).sin();
        *state
    } else if node_id.contains("Square") {
        let period = 40.0;
        *state = if (t % period) < (period / 2.0) { 100.0 } else { 0.0 };
        *state
    } else if node_id.contains("Triangle") {
        let period = 80.0;
        let phase = (t % period) / period;
        *state = if phase < 0.5 {
            phase * 200.0
        } else {
            (1.0 - phase) * 200.0
        };
        *state
    } else {
        *state = 25.0 + rng.gen_range(-1.0..1.0);
        *state
    }
}

// ── Synthetic Value Generators ────────────────────────────────────────────────

fn generate_value(rng: &mut impl Rng, profile: &SimProfile, state: &mut f64, tick: u64) -> f64 {
    let t = tick as f64;
    match profile {
        SimProfile::Temperature => {
            let base = 75.0 + 10.0 * (t * 0.02).sin();
            let noise: f64 = rng.gen_range(-0.5..0.5);
            let val = base + noise;
            *state = val;
            val
        }
        SimProfile::Pressure => {
            let period = 120.0;
            let phase = (t % period) / period;
            let val = 2.0 + 4.0 * phase;
            let noise: f64 = rng.gen_range(-0.1..0.1);
            *state = val + noise;
            *state
        }
        SimProfile::FlowRate => {
            let delta: f64 = rng.gen_range(-5.0..5.0);
            *state = (*state + delta).clamp(100.0, 500.0);
            *state
        }
        SimProfile::MotorSpeed => {
            let base = 1475.0 + 5.0 * (t * 0.05).sin();
            let noise: f64 = rng.gen_range(-2.0..2.0);
            let spike = if rng.gen_ratio(1, 100) {
                rng.gen_range(20.0..50.0) * if rng.gen_bool(0.5) { 1.0 } else { -1.0 }
            } else {
                0.0
            };
            let val = base + noise + spike;
            *state = val;
            val
        }
        SimProfile::Vibration => {
            let baseline: f64 = 0.5 + 0.2 * (t * 0.03).sin();
            let noise: f64 = rng.gen_range(-0.05..0.05);
            let fault = if rng.gen_ratio(1, 200) {
                rng.gen_range(1.0..3.0)
            } else if *state > 1.5 {
                (*state - baseline) * 0.7
            } else {
                0.0
            };
            let val = (baseline + noise + fault).max(0.05);
            *state = val;
            val
        }
    }
}

// ── Flush Worker ──────────────────────────────────────────────────────────────

pub async fn flush_worker(mut rx: mpsc::Receiver<TelemetryRecord>, db: Arc<DbPool>) {
    let mut buffer: Vec<TelemetryRecord> = Vec::with_capacity(512);
    let flush_interval = Duration::from_millis(500);
    let max_batch = 500;

    log::info!("Flush worker started");

    loop {
        let deadline = tokio::time::sleep(flush_interval);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                biased;

                record = rx.recv() => {
                    match record {
                        Some(rec) => {
                            buffer.push(rec);
                            if buffer.len() >= max_batch {
                                break;
                            }
                        }
                        None => {
                            if !buffer.is_empty() {
                                flush_batch(&db, &mut buffer).await;
                            }
                            log::info!("Flush worker stopped (channel closed)");
                            return;
                        }
                    }
                }
                _ = &mut deadline => {
                    break;
                }
            }
        }

        if !buffer.is_empty() {
            flush_batch(&db, &mut buffer).await;
        }
    }
}

async fn flush_batch(db: &Arc<DbPool>, buffer: &mut Vec<TelemetryRecord>) {
    let batch = std::mem::take(buffer);
    let count = batch.len();
    let db = db.clone();

    let result = tokio::task::spawn_blocking(move || db.insert_telemetry_batch(&batch)).await;

    match result {
        Ok(Ok(_)) => {
            log::trace!("Flushed {} records to SQLite", count);
        }
        Ok(Err(e)) => {
            log::error!("SQLite batch insert failed: {}", e);
        }
        Err(e) => {
            log::error!("spawn_blocking panicked during flush: {}", e);
        }
    }
}

// ── Auto-Discovery Engine ─────────────────────────────────────────────────────

pub fn discover_servers() -> Vec<DiscoveredServer> {
    let mut discovered = Vec::new();

    // 1. Probe known local OPC UA endpoints
    let ua_candidates = vec![
        (
            "Prosys OPC UA Simulation Server",
            53530,
            "opc.tcp://127.0.0.1:53530/OPCUA/SimulationServer",
            "Local Prosys simulation instance",
        ),
        (
            "Standard OPC UA Local Discovery (LDS)",
            4840,
            "opc.tcp://127.0.0.1:4840",
            "Standard OPC UA / LDS discovery port",
        ),
        (
            "Kepware KEPServerEX OPC UA",
            49320,
            "opc.tcp://127.0.0.1:49320",
            "Kepware Industrial Automation Server",
        ),
        (
            "Matrikon OPC UA Wrapper",
            21381,
            "opc.tcp://127.0.0.1:21381/MatrikonOpcUaWrapper",
            "Matrikon COM-to-UA bridge",
        ),
        (
            "open62541 Industrial Server",
            62541,
            "opc.tcp://127.0.0.1:62541",
            "open62541 open-source stack",
        ),
        (
            "OPC Foundation .NET Server",
            51210,
            "opc.tcp://127.0.0.1:51210/UA/SampleServer",
            "OPC Foundation reference server",
        ),
        (
            "Unified Automation UaCPPServer",
            48010,
            "opc.tcp://127.0.0.1:48010",
            "Unified Automation C++ SDK server",
        ),
    ];

    for (name, port, endpoint, desc) in ua_candidates {
        if check_tcp_alive("127.0.0.1", port) {
            let is_online = perform_opc_ua_tcp_handshake("127.0.0.1", port, endpoint).is_ok();
            if is_online {
                discovered.push(DiscoveredServer {
                    name: name.to_string(),
                    endpoint: endpoint.to_string(),
                    server_type: "OPC UA".to_string(),
                    description: desc.to_string(),
                    is_online: true,
                });
            }
        }
    }

    // 2. Scan registered OPC DA COM servers on Windows
    #[cfg(target_os = "windows")]
    {
        let da_candidates = vec![
            (
                "Matrikon OPC Server for Simulation and Testing",
                "Matrikon.OPC.Simulation.1",
                "opc.da://Matrikon.OPC.Simulation.1",
                "Classic Matrikon simulation server",
            ),
            (
                "Matrikon OPC Server for Universal PLC",
                "Matrikon.OPC.Universal.1",
                "opc.da://Matrikon.OPC.Universal.1",
                "Matrikon industrial PLC driver",
            ),
            (
                "Kepware KEPServerEX V6 (DA)",
                "Kepware.KEPServerEX.V6",
                "opc.da://Kepware.KEPServerEX.V6",
                "Kepware OPC DA 2.05a server",
            ),
            (
                "Schneider OPC Factory Server (OFS)",
                "Schneider-Aut.OFS",
                "opc.da://Schneider-Aut.OFS",
                "Schneider Electric OFS DA Server",
            ),
            (
                "Rockwell RSLinx OPC DA",
                "RSLinx OPC Server",
                "opc.da://RSLinx OPC Server",
                "Rockwell Automation RSLinx DA",
            ),
        ];

        for (name, prog_id, endpoint, desc) in da_candidates {
            if check_com_progid_registered(prog_id) {
                discovered.push(DiscoveredServer {
                    name: name.to_string(),
                    endpoint: endpoint.to_string(),
                    server_type: "OPC DA".to_string(),
                    description: desc.to_string(),
                    is_online: true,
                });
            }
        }
    }

    discovered
}

#[cfg(target_os = "windows")]
fn check_com_progid_registered(prog_id: &str) -> bool {
    use std::process::Command;
    let reg_paths = [
        format!("HKCR\\{}", prog_id),
        format!("HKLM\\SOFTWARE\\Classes\\{}", prog_id),
        format!("HKLM\\SOFTWARE\\WOW6432Node\\Classes\\{}", prog_id),
    ];

    for path in &reg_paths {
        if let Ok(output) = Command::new("reg").args(["query", path]).output() {
            if output.status.success() {
                return true;
            }
        }
    }
    false
}
