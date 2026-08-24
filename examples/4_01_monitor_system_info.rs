//! Example 4_01: Asynchronous event stream monitoring for System Info, Names, and RTC clock.
//!
//! ============================================================================
//! 1. EVENT STREAMING OVERVIEW & FILTERED DOMAIN (SYSTEM INFO):
//! ============================================================================
//! The `satel_integra` library uses a multi-producer multi-consumer broadcast channel
//! (`tokio::sync::broadcast`) for real-time reactive event delivery.
//!
//! This example filters and processes only System Information events:
//!   - `SatelEvent::ConnectionChanged(state)`:
//!       Connection lifecycle transitions (Connecting, Handshake, Connected, ConnectionLost).
//!   - `SatelEvent::IntegraVersionReceived(version)`:
//!       Alarm panel model, firmware version, and I/O capacity.
//!   - `SatelEvent::EthmVersionReceived(ethm_version)`:
//!       ETHM-1 / INT-RS module capabilities and hardware flags.
//!   - `SatelEvent::PartitionNameReceived { id, name }`:
//!       Decoded 16-character partition names.
//!   - `SatelEvent::ZoneNameReceived { id, name }`:
//!       Decoded 16-character zone (input) names.
//!   - `SatelEvent::OutputNameReceived { id, name }`:
//!       Decoded 16-character output names.
//!   - `SatelEvent::SystemStatusChanged(status)`:
//!       RTC clock updates, service mode state, and trouble presence flags.
//!
//! All other event variants (violations, armed states, temperatures, troubles) are
//! ignored (`_ => {}`) by this specialized listener.
//!
//! ============================================================================
//! 2. AUTOMATIC HANDSHAKE VS EXPLICIT QUERIES:
//! ============================================================================
//! - Automatic Handshake Events:
//!     During `satel.connect().await`, the client library automatically performs an internal
//!     handshake by querying Integra version (0x7E) and ETHM-1 module capabilities (0x7C)
//!     to determine panel capacity (I/O count) and 32-byte frame support.
//!     Subscribers attached *before* calling `connect()` receive these handshake events
//!     immediately upon connection establishment.
//! - Explicit / Manual Queries:
//!     Subsequent explicit method calls (e.g. `satel.get_integra_version().await`,
//!     `satel.get_zone_name(id).await`) query the panel and automatically re-broadcast
//!     fresh event copies across the channel.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 4_01_monitor_system_info
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
    println!(" SATEL INTEGRA - Monitor System Info Events (3_01)");
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

    // 2. Subscribe to event broadcast BEFORE connecting
    // This allows capturing automatic connection handshake events (0x7E, 0x7C)
    let mut rx = satel.subscribe();

    // 3. Spawn specialized background listener task
    let listener_handle = tokio::spawn(async move {
        println!("[Listener Task] Started listening for System Info events...\n");

        while let Ok(event) = rx.recv().await {
            match event {
                SatelEvent::ConnectionChanged(state) => {
                    println!("[EVENT: CONNECTION] State changed -> {:?}", state);
                }
                SatelEvent::IntegraVersionReceived(ver) => {
                    println!(
                        "[EVENT: VERSION] Panel: {} | Firmware: {} | Language: {} | I/O: {}",
                        ver.model, ver.firmware_version, ver.language, ver.io_count
                    );
                }
                SatelEvent::EthmVersionReceived(ethm) => {
                    println!(
                        "[EVENT: ETHM] Module: {} (32B frames: {})",
                        ethm.version_raw, ethm.capabilities.support_32_byte_frames
                    );
                }
                SatelEvent::PartitionNameReceived { id, name } => {
                    println!("[EVENT: NAME] Partition #{:02}: \"{}\"", id, name);
                }
                SatelEvent::ZoneNameReceived { id, name } => {
                    println!("[EVENT: NAME] Zone      #{:03}: \"{}\"", id, name);
                }
                SatelEvent::OutputNameReceived { id, name } => {
                    println!("[EVENT: NAME] Output    #{:03}: \"{}\"", id, name);
                }
                SatelEvent::SystemStatusChanged(status) => {
                    println!(
                        "[EVENT: RTC/STATUS] Time: {} | Service: {} | Troubles: {}",
                        status.rtc.format("%Y-%m-%d %H:%M:%S"),
                        status.service_mode,
                        status.troubles_present
                    );
                }
                // All other security/temperature/trouble events are ignored in this listener
                _ => {}
            }
        }
        println!("[Listener Task] Event channel closed.");
    });

    // 4. Connect to the panel
    // Note: The library performs an automatic handshake querying 0x7E & 0x7C,
    // which triggers the first emission of version events in the listener above.
    println!("Connecting to the panel (automatic handshake queries 0x7E & 0x7C)...");
    satel.connect().await?;
    println!("Connected successfully!\n");

    // Allow handshake events to be processed and printed
    sleep(Duration::from_millis(500)).await;

    // 5. Trigger explicit network queries (re-emitting events and fetching names/RTC)
    println!("--- Triggering explicit queries (fetching names & RTC status) ---");
    let _ = satel.get_system_status().await;

    // Fetch first 3 partition names and first 3 zone names
    for id in 1..=3 {
        let _ = satel.get_partition_name(id).await;
        let _ = satel.get_zone_name(id).await;
    }

    // Keep listening for 10 seconds
    println!("\nMonitoring events for 10 seconds...");
    sleep(Duration::from_secs(10)).await;

    // 6. Disconnect cleanly
    println!("\nDisconnecting...");
    satel.disconnect().await?;
    sleep(Duration::from_millis(500)).await;

    // Abort background listener
    listener_handle.abort();
    println!("Disconnected cleanly. Done.");

    Ok(())
}
