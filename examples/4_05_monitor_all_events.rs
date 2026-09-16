//! Example 4_05: Comprehensive real-time event monitor handling all SatelEvent variants.
//!
//! ============================================================================
//! 1. EVENT STREAMING OVERVIEW & EXHAUSTIVE MATCHING:
//! ============================================================================
//! This universal event monitor demonstrates complete, exhaustive pattern matching
//! on every variant of `SatelEvent` emitted by the `satel_integra` broadcast channel.
//!
//! Event Categories Covered:
//!   1. Connection & System Metadata:
//!        - `ConnectionChanged`
//!        - `IntegraVersionReceived`, `EthmVersionReceived`
//!        - `ZoneNameReceived`, `OutputNameReceived`, `PartitionNameReceived`
//!        - `SystemStatusChanged` (RTC, Service mode, Trouble flags)
//!   2. Physical & Logical Security States:
//!        - Zones: `ZoneViolation`, `ZoneTamper`, `ZoneAlarm`, `ZoneTamperAlarm`,
//!          `ZoneAlarmMemory`, `ZoneTamperAlarmMemory`, `ZoneBypass`,
//!          `ZoneNoViolationTrouble`, `ZoneLongViolationTrouble`
//!        - Outputs: `OutputChanged`
//!        - Partitions: `PartitionArmed`, `PartitionArmedReally`, `PartitionAlarm`,
//!          `PartitionAlarmMemory`, `PartitionEntryTime`, `PartitionExitTimeGt10s`,
//!          `PartitionExitTimeLt10s`
//!   3. Environmental & Analog:
//!        - `ZoneTemperatureChanged`
//!   4. System Diagnostics:
//!        - `Trouble`, `TroubleMemory`
//!   5. Protocol Notifications:
//!        - `AutoReadConfigured`, `PanelMessage`
//!
//! ============================================================================
//! 2. ARCHITECTURAL PATTERN:
//! ============================================================================
//! Spawns a dedicated background listener task with timestamped log formatting
//! (`%H:%M:%S%.3f`). Smart temperature error blocking (`temp_blocking_enabled: true`)
//! is enabled to protect the communication queue.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 4_05_monitor_all_events
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use chrono::Local;
use satel_integra::{Config, ConnectionConfig, SatelCommand, SatelEvent, SatelIntegra};
use std::env;
use std::time::Duration;
use tokio::time::sleep;

/// Sample temperature sensor zone to query
const TEST_TEMP_ZONE: u16 = 21;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Connection parameters
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Universal Event Monitor (3_05)");
    println!("==================================================");

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,
        temp_blocking_enabled: true, // Enable smart temperature queue protection
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 2. Subscribe to event broadcast before connecting
    let mut rx = satel.subscribe();

    // 3. Spawn comprehensive background listener task
    let listener_handle = tokio::spawn(async move {
        println!("[Universal Listener] Active. Receiving all system events...\n");

        while let Ok(event) = rx.recv().await {
            let ts = Local::now().format("%H:%M:%S%.3f");

            match event {
                // --- Connection & System Metadata ---
                SatelEvent::ConnectionChanged(state) => {
                    println!("[{}] [CONNECTION]        State -> {:?}", ts, state);
                }
                SatelEvent::IntegraVersionReceived(ver) => {
                    println!("[{}] [INTEGRA VERSION]   Model: {} | Firmware: {} | I/O: {}", ts, ver.model, ver.firmware_version, ver.io_count);
                }
                SatelEvent::EthmVersionReceived(ethm) => {
                    println!("[{}] [ETHM VERSION]      Module: {} (32B frames: {})", ts, ethm.version_raw, ethm.capabilities.support_32_byte_frames);
                }
                SatelEvent::PartitionNameReceived { id, name } => {
                    println!("[{}] [NAME]              Partition #{:02}: \"{}\"", ts, id, name);
                }
                SatelEvent::ZoneNameReceived { id, name } => {
                    println!("[{}] [NAME]              Zone      #{:03}: \"{}\"", ts, id, name);
                }
                SatelEvent::OutputNameReceived { id, name } => {
                    println!("[{}] [NAME]              Output    #{:03}: \"{}\"", ts, id, name);
                }
                SatelEvent::SystemStatusChanged(status) => {
                    println!(
                        "[{}] [SYSTEM STATUS]     RTC: {} | Service: {} | Troubles: {}",
                        ts, status.rtc.format("%Y-%m-%d %H:%M:%S"), status.service_mode, status.troubles_present
                    );
                }

                // --- Zone (Input) Events ---
                SatelEvent::ZoneViolation { id, state } => {
                    println!("[{}] [ZONE VIOLATION]    Zone #{:03} -> {}", ts, id, if state { "VIOLATED" } else { "NORMAL" });
                }
                SatelEvent::ZoneTamper { id, state } => {
                    println!("[{}] [ZONE TAMPER]       Zone #{:03} -> {}", ts, id, if state { "TAMPERED" } else { "OK" });
                }
                SatelEvent::ZoneAlarm { id, state } => {
                    println!("[{}] [ZONE ALARM]        Zone #{:03} -> {}", ts, id, if state { "ALARM ACTIVE!" } else { "RESTORED" });
                }
                SatelEvent::ZoneTamperAlarm { id, state } => {
                    println!("[{}] [ZONE TAMPER ALARM] Zone #{:03} -> {}", ts, id, if state { "TAMPER ALARM!" } else { "RESTORED" });
                }
                SatelEvent::ZoneAlarmMemory { id, state } => {
                    println!("[{}] [ZONE ALARM MEMORY] Zone #{:03} -> {}", ts, id, if state { "LATCHED" } else { "CLEARED" });
                }
                SatelEvent::ZoneTamperAlarmMemory { id, state } => {
                    println!("[{}] [ZONE TMP MEMORY]   Zone #{:03} -> {}", ts, id, if state { "LATCHED" } else { "CLEARED" });
                }
                SatelEvent::ZoneBypass { id, state } => {
                    println!("[{}] [ZONE BYPASS]       Zone #{:03} -> {}", ts, id, if state { "BYPASSED" } else { "ACTIVE" });
                }
                SatelEvent::ZoneNoViolationTrouble { id, state } => {
                    println!("[{}] [ZONE NO-VIOLATION] Zone #{:03} -> {}", ts, id, if state { "TROUBLE" } else { "OK" });
                }
                SatelEvent::ZoneLongViolationTrouble { id, state } => {
                    println!("[{}] [ZONE LONG-VIOLAT.] Zone #{:03} -> {}", ts, id, if state { "TROUBLE" } else { "OK" });
                }

                // --- Output (Relay) Events ---
                SatelEvent::OutputChanged { id, state } => {
                    println!("[{}] [OUTPUT CHANGE]     Output #{:03} -> {}", ts, id, if state { "ON" } else { "OFF" });
                }

                // --- Partition Events ---
                SatelEvent::PartitionArmed { id, state } => {
                    println!("[{}] [PARTITION ARMED]   Partition #{:02} -> {}", ts, id, if state { "ARMED" } else { "DISARMED" });
                }
                SatelEvent::PartitionArmedReally { id, state } => {
                    println!("[{}] [PARTITION REALLY]  Partition #{:02} -> {}", ts, id, if state { "ARMED REALLY" } else { "DISARMED" });
                }
                SatelEvent::PartitionAlarm { id, state } => {
                    println!("[{}] [PARTITION ALARM]   Partition #{:02} -> {}", ts, id, if state { "ALARM ACTIVE!" } else { "CLEARED" });
                }
                SatelEvent::PartitionAlarmMemory { id, state } => {
                    println!("[{}] [PARTITION MEMORY]  Partition #{:02} -> {}", ts, id, if state { "ALARM LATCHED" } else { "CLEARED" });
                }
                SatelEvent::PartitionEntryTime { id, state } => {
                    println!("[{}] [ENTRY DELAY]       Partition #{:02} -> {}", ts, id, if state { "COUNTDOWN ACTIVE" } else { "STOPPED" });
                }
                SatelEvent::PartitionExitTimeGt10s { id, state } => {
                    if state {
                        println!("[{}] [EXIT DELAY >10S]   Partition #{:02} -> Exit countdown active", ts, id);
                    }
                }
                SatelEvent::PartitionExitTimeLt10s { id, state } => {
                    if state {
                        println!("[{}] [EXIT DELAY <10S]   Partition #{:02} -> Final exit countdown seconds!", ts, id);
                    }
                }

                // --- Temperature Events ---
                SatelEvent::ZoneTemperatureChanged { id, temperature } => {
                    println!("[{}] [TEMPERATURE]       Zone #{:03} -> {:>5.1}°C", ts, id, temperature);
                }
                SatelEvent::ZoneTemperatureError { id, status } => {
                    println!("[{}] [TEMP FAULT]        Zone #{:03} -> Status: {:?}", ts, id, status);
                }

                // --- Diagnostic Troubles ---
                SatelEvent::Trouble(kind, state) => {
                    println!("[{}] [ACTIVE TROUBLE]    {:?} -> {}", ts, kind, if state { "PRESENT" } else { "RESTORED" });
                }
                SatelEvent::TroubleMemory(kind, state) => {
                    println!("[{}] [TROUBLE MEMORY]    {:?} -> {}", ts, kind, if state { "LATCHED" } else { "CLEARED" });
                }

                // --- Protocol Messages ---
                SatelEvent::AcuJamLevel { module, level } => {
                    println!("[{}] [ACU JAM]         Module #{:03} -> Level {}", ts, module, level);
                }
                SatelEvent::CmeError { source, sim, code, memory } => {
                    println!("[{}] [CME ERROR]       Source {:?} SIM{} -> Code {} (Memory: {})", ts, source, sim, code, memory);
                }
                SatelEvent::AutoReadConfigured(report) => {
                    println!("[{}] [AUTOREAD CONFIG]   Active Items: {}/{}", ts, report.success_count, report.total_requested);
                }
                SatelEvent::PanelMessage(result) => {
                    println!("[{}] [PANEL MESSAGE]     Result code: {:?}", ts, result);
                }

                // --- Batch Synchronization Lifecycle ---
                SatelEvent::SyncStarted { category, total } => {
                    println!("[{}] [SYNC STARTED]      {} -> 0/{}", ts, category, total);
                }
                SatelEvent::SyncProgress { category, current, total, name } => {
                    println!("[{}] [SYNC PROGRESS]     {} -> #{}/{} \"{}\"", ts, category, current, total, name);
                }
                SatelEvent::SyncFinished { category, total, success_count, error } => {
                    println!("[{}] [SYNC FINISHED]     {} -> {}/{} (error: {:?})", ts, category, success_count, total, error);
                }
                SatelEvent::ConfigUpdated => {
                    println!("[{}] [CONFIG UPDATED]     Configuration reloaded in place", ts);
                }
            }
        }
        println!("[Universal Listener] Event channel closed.");
    });

    // 4. Connect to the panel
    println!("Connecting to the panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 5. Query states to generate a rich event stream
    println!("--- Triggering sample queries across all domains ---");
    let _ = satel.get_system_status().await;
    let _ = satel.get_partition_name(1).await;
    let _ = satel.get_zone_name(1).await;
    let _ = satel.get_output_name(1).await;
    let _ = satel.get_zones_violation().await;
    let _ = satel.get_outputs_state().await;
    let _ = satel.get_partitions_armed_really().await;
    let _ = satel.get_troubles(SatelCommand::TroublesPart1).await;

    // Sample temperature query
    match satel.get_zone_temperature(TEST_TEMP_ZONE).await {
        Ok(_) => {}
        Err(e) => {
            eprintln!("  [Temp Query Note] Zone #{:03}: {}", TEST_TEMP_ZONE, e);
        }
    }

    // Keep monitoring for 15 seconds
    println!("\nMonitoring event stream for 15 seconds (walk in front of a PIR or trigger an action)...");
    sleep(Duration::from_secs(15)).await;

    // 6. Disconnect cleanly
    println!("\nDisconnecting...");
    satel.disconnect().await?;
    sleep(Duration::from_millis(500)).await;

    // Abort background listener
    listener_handle.abort();
    println!("Disconnected cleanly. Done.");

    Ok(())
}
