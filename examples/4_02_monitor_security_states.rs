//! Example 4_02: Real-time event monitoring for security states (Zones, Outputs, Partitions).
//!
//! ============================================================================
//! 1. EVENT STREAMING OVERVIEW & FILTERED DOMAIN (SECURITY STATES):
//! ============================================================================
//! This specialized listener focuses entirely on physical and logical security states:
//!   - Zone (Input) Events:
//!       * `ZoneViolation { id, state }`          (PIR movement, magnetic contact open/close)
//!       * `ZoneTamper { id, state }`             (Line or casing sabotage)
//!       * `ZoneAlarm { id, state }`              (Active alarm triggered by zone)
//!       * `ZoneTamperAlarm { id, state }`       (Tamper alarm triggered)
//!       * `ZoneAlarmMemory { id, state }`        (Stored alarm flag in memory)
//!       * `ZoneBypass { id, state }`             (Zone disabled / bypassed)
//!   - Output (Relay) Events:
//!       * `OutputChanged { id, state }`          (Siren, light, relay ON / OFF)
//!   - Partition Events:
//!       * `PartitionArmed { id, state }`         (Partition armed state)
//!       * `PartitionArmedReally { id, state }`   (Full physical arming active)
//!       * `PartitionAlarm { id, state }`         (Active alarm in partition)
//!       * `PartitionAlarmMemory { id, state }`   (Alarm latched in partition memory)
//!       * `PartitionEntryTime { id, state }`     (Entry delay countdown active)
//!       * `PartitionExitTimeGt10s { id, state }` (Exit delay countdown > 10s)
//!       * `PartitionExitTimeLt10s { id, state }` (Exit delay countdown < 10s)
//!
//! All system information, temperatures, and diagnostic troubles are ignored (`_ => {}`).
//!
//! ============================================================================
//! 2. AUTOMATIC CHANGE DETECTION & DEDUPLICATION:
//! ============================================================================
//! The library features built-in change detection (`old_state != new_state`):
//!   - Initial State: Only inputs/outputs/partitions that are currently active (e.g. violated PIR)
//!     trigger an initial event upon first read (`false -> true`). Normal / idle zones (false)
//!     remain completely silent without generating unnecessary traffic.
//!   - Subsequent Cycles: If nothing changes between queries, zero duplicate events are emitted.
//!     Events fire strictly when physical transitions occur (e.g. PIR movement, contact restored).
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 4_02_monitor_security_states
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelEvent, SatelIntegra};
use std::env;
use std::time::Duration;
use tokio::time::sleep;

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
    println!(" SATEL INTEGRA - Monitor Security States (3_02)");
    println!("==================================================");

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 2. Subscribe to event broadcast before connecting
    let mut rx = satel.subscribe();

    // 3. Spawn specialized security event listener
    let listener_handle = tokio::spawn(async move {
        println!("[Security Listener] Task active. Monitoring state changes...\n");

        while let Ok(event) = rx.recv().await {
            match event {
                // --- Zone (Input) Events ---
                SatelEvent::ZoneViolation { id, state } => {
                    let status = if state { "VIOLATED (Movement / Open)" } else { "NORMAL (Restored)" };
                    println!("[ZONE VIOLATION] Zone #{:03} -> {}", id, status);
                }
                SatelEvent::ZoneTamper { id, state } => {
                    let status = if state { "TAMPERED (Sabotage!)" } else { "NORMAL (OK)" };
                    println!("[ZONE TAMPER]    Zone #{:03} -> {}", id, status);
                }
                SatelEvent::ZoneAlarm { id, state } => {
                    let status = if state { "ALARM ACTIVE!" } else { "ALARM RESTORED" };
                    println!("[ZONE ALARM]     Zone #{:03} -> {}", id, status);
                }
                SatelEvent::ZoneBypass { id, state } => {
                    let status = if state { "BYPASSED (Disabled)" } else { "UNBYPASSED (Active)" };
                    println!("[ZONE BYPASS]    Zone #{:03} -> {}", id, status);
                }

                // --- Output (Relay) Events ---
                SatelEvent::OutputChanged { id, state } => {
                    let status = if state { "ON (Active)" } else { "OFF (Inactive)" };
                    println!("[OUTPUT CHANGE]  Output #{:03} -> {}", id, status);
                }

                // --- Partition Events ---
                SatelEvent::PartitionArmed { id, state } => {
                    let status = if state { "ARMED" } else { "DISARMED" };
                    println!("[PARTITION ARM]  Partition #{:02} -> {}", id, status);
                }
                SatelEvent::PartitionArmedReally { id, state } => {
                    let status = if state { "ARMED REALLY (Full Protection)" } else { "DISARMED" };
                    println!("[PARTITION REAL] Partition #{:02} -> {}", id, status);
                }
                SatelEvent::PartitionAlarm { id, state } => {
                    let status = if state { "ALARM ACTIVE!" } else { "Alarm Cleared" };
                    println!("[PARTITION ALARM]Partition #{:02} -> {}", id, status);
                }
                SatelEvent::PartitionEntryTime { id, state } => {
                    let status = if state { "COUNTDOWN ACTIVE" } else { "Expired/Stopped" };
                    println!("[ENTRY DELAY]    Partition #{:02} -> Entry delay {}", id, status);
                }
                SatelEvent::PartitionExitTimeGt10s { id, state } => {
                    if state {
                        println!("[EXIT DELAY]     Partition #{:02} -> Exit delay > 10s remaining", id);
                    }
                }
                SatelEvent::PartitionExitTimeLt10s { id, state } => {
                    if state {
                        println!("[EXIT DELAY]     Partition #{:02} -> Exit delay < 10s remaining (Last seconds!)", id);
                    }
                }

                // All other events are ignored by this security monitor
                _ => {}
            }
        }
        println!("[Security Listener] Event channel closed.");
    });

    // 4. Connect to the panel
    println!("Connecting to the panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 5. Query cycles (demonstrating built-in change detection & zero duplication)
    println!("--- Querying Security States Across 3 Cycles (2-second intervals) ---");
    println!("Events will appear below only for active states or live transitions.\n");

    for cycle in 1..=3 {
        println!(">>> Query Cycle {}/3 (Fetching current panel states)...", cycle);
        let _ = satel.get_zones_violation().await;
        let _ = satel.get_zones_tamper().await;
        let _ = satel.get_outputs_state().await;
        let _ = satel.get_partitions_armed_really().await;
        let _ = satel.get_partitions_alarm().await;

        sleep(Duration::from_secs(2)).await;
    }

    println!("\nAll 3 query cycles completed. Monitoring for another 5 seconds...");
    sleep(Duration::from_secs(5)).await;

    // 6. Disconnect cleanly
    println!("\nDisconnecting...");
    satel.disconnect().await?;
    sleep(Duration::from_millis(500)).await;

    // Abort background listener
    listener_handle.abort();
    println!("Disconnected cleanly. Done.");

    Ok(())
}
