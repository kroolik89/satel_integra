//! Example 3_04: Real-time event monitoring for system troubles and trouble memory.
//!
//! ============================================================================
//! 1. EVENT STREAMING OVERVIEW & FILTERED DOMAIN (SYSTEM TROUBLES):
//! ============================================================================
//! This specialized listener filters and processes only system diagnostic events:
//!   - `SatelEvent::Trouble(trouble_type, state)`:
//!       Emitted when an active hardware/communication trouble appears (`state = true`)
//!       or is resolved/restored (`state = false`).
//!   - `SatelEvent::TroubleMemory(trouble_type, state)`:
//!       Emitted when a trouble condition is recorded into non-volatile panel memory (`true`)
//!       or cleared from trouble memory (`false`).
//!   - `SatelEvent::SystemStatusChanged(status)`:
//!       Emitted when general trouble indicator flags change (`troubles_present`, `troubles_memory`).
//!
//! All zone violations, output states, armed partitions, and temperatures are
//! ignored (`_ => {}`) by this specialized diagnostic listener.
//!
//! ============================================================================
//! 2. TROUBLE DIAGNOSTICS & CHANGE DETECTION:
//! ============================================================================
//! - Initial Cycle: If the panel currently has active troubles (e.g. AC loss, low battery,
//!   GSM error), initial `Trouble(..., true)` events are emitted immediately upon first read.
//! - Subsequent Cycles: Zero duplicate events are emitted if the diagnostic state remains
//!   unchanged. Events fire strictly when a trouble occurs or gets restored.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 3_04_monitor_troubles
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelCommand, SatelEvent, SatelIntegra};
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
    println!(" SATEL INTEGRA - Monitor System Troubles (3_04)");
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

    // 3. Spawn specialized trouble diagnostic listener
    let listener_handle = tokio::spawn(async move {
        println!("[Troubles Listener] Task active. Monitoring diagnostic faults...\n");

        while let Ok(event) = rx.recv().await {
            match event {
                SatelEvent::Trouble(kind, state) => {
                    let status = if state { "FAULT ACTIVE (Present!)" } else { "FAULT RESTORED (OK)" };
                    println!("[ACTIVE TROUBLE]  {:?} -> {}", kind, status);
                }
                SatelEvent::TroubleMemory(kind, state) => {
                    let status = if state { "LATCHED IN MEMORY" } else { "MEMORY CLEARED" };
                    println!("[TROUBLE MEMORY]  {:?} -> {}", kind, status);
                }
                SatelEvent::SystemStatusChanged(status) => {
                    println!(
                        "[SYSTEM STATUS]   Troubles Present: {} | In Memory: {} | Service Mode: {}",
                        status.troubles_present,
                        status.troubles_memory,
                        status.service_mode
                    );
                }
                // All other security/temperature/names events are ignored
                _ => {}
            }
        }
        println!("[Troubles Listener] Event channel closed.");
    });

    // 4. Connect to the panel
    println!("Connecting to the panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 5. Query system status and trouble parts across 2 diagnostic cycles
    let active_trouble_cmds = [
        SatelCommand::TroublesPart1,
        SatelCommand::TroublesPart2,
        SatelCommand::TroublesPart3,
        SatelCommand::TroublesPart4,
        SatelCommand::TroublesPart5,
        SatelCommand::TroublesPart6,
        SatelCommand::TroublesPart7,
        SatelCommand::TroublesPart8,
    ];

    let memory_trouble_cmds = [
        SatelCommand::TroublesMemoryPart1,
        SatelCommand::TroublesMemoryPart2,
        SatelCommand::TroublesMemoryPart3,
        SatelCommand::TroublesMemoryPart4,
        SatelCommand::TroublesMemoryPart5,
        SatelCommand::TroublesMemoryPart6,
        SatelCommand::TroublesMemoryPart7,
        SatelCommand::TroublesMemoryPart8,
    ];

    println!("--- Querying System Troubles Across 2 Diagnostic Cycles ---");
    for cycle in 1..=2 {
        println!(">>> Diagnostic Query Cycle {}/2...", cycle);
        let _ = satel.get_system_status().await;

        for cmd in &active_trouble_cmds {
            let _ = satel.get_system_troubles(*cmd).await;
            sleep(Duration::from_millis(50)).await;
        }

        for cmd in &memory_trouble_cmds {
            let _ = satel.get_system_troubles(*cmd).await;
            sleep(Duration::from_millis(50)).await;
        }

        sleep(Duration::from_secs(3)).await;
    }

    // Keep listening for 5 more seconds
    println!("\nDiagnostic cycles completed. Monitoring for another 5 seconds...");
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
