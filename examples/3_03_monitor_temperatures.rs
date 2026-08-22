//! Example 3_03: Real-time event monitoring for wireless temperature sensors (ABAX 2).
//!
//! ============================================================================
//! 1. EVENT STREAMING OVERVIEW & FILTERED DOMAIN (TEMPERATURES):
//! ============================================================================
//! This specialized listener filters and processes only temperature measurement events:
//!   - `SatelEvent::ZoneTemperatureChanged { id, temperature }`:
//!       Emitted whenever a new temperature reading (0x7D) is successfully received
//!       from a wireless or wired sensor (e.g. ATD-100, APD-200, AOCD-260).
//!       Temperature is provided as an `f32` value in degrees Celsius (°C) with 0.5°C resolution.
//!
//! All other security, system, and trouble events are ignored (`_ => {}`) by this listener.
//!
//! ============================================================================
//! 2. TEMPERATURE DISPATCH & POLLING ARCHITECTURE:
//! ============================================================================
//! - Unlike digital states (0x00..0x17) which support 0x7F push bitmasks, temperature
//!   sensors (0x7D) are queried per zone ID.
//! - Whenever `satel.get_zone_temperature(zone_id).await` is called (or executed by
//!   an automation task), the library parses the 16-bit sensor word, updates local cache,
//!   and broadcasts `ZoneTemperatureChanged` to all active subscribers.
//! - Smart error blocking (`temp_blocking_enabled: true`) protects the communication queue
//!   from being blocked by missing/unpaired sensor IDs.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 3_03_monitor_temperatures
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelEvent, SatelIntegra};
use std::env;
use std::time::Duration;
use tokio::time::sleep;

/// List of zone IDs with actual temperature sensors installed (e.g. ATD-100, APD-200)
const TEMPERATURE_ZONES: &[u16] = &[21, 23, 26, 28, 30];

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
    println!(" SATEL INTEGRA - Monitor Temperatures (3_03)");
    println!("==================================================");
    println!("Target Temperature Zones: {:?}\n", TEMPERATURE_ZONES);

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,
        temp_blocking_enabled: true, // Protect communication queue from broken sensors
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 2. Subscribe to event broadcast before connecting
    let mut rx = satel.subscribe();

    // 3. Spawn specialized temperature event listener
    let listener_handle = tokio::spawn(async move {
        println!("[Temperature Listener] Task active. Monitoring temperature updates...\n");

        while let Ok(event) = rx.recv().await {
            match event {
                SatelEvent::ZoneTemperatureChanged { id, temperature } => {
                    println!(
                        "[TEMPERATURE EVENT] Zone #{:03} -> {:>5.1}°C",
                        id, temperature
                    );
                }
                // All other security/system events are ignored
                _ => {}
            }
        }
        println!("[Temperature Listener] Event channel closed.");
    });

    // 4. Connect to the panel
    println!("Connecting to the panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 5. Query temperature sensors across 2 measurement cycles
    println!("--- Triggering Temperature Queries Across 2 Cycles ---");
    println!("Successful reads will emit events to the background listener.");
    println!("Failed reads (missing sensors / timeouts) will print error diagnostics below:\n");

    for cycle in 1..=2 {
        println!(">>> Measurement Cycle {}/2...", cycle);
        for &zone_id in TEMPERATURE_ZONES {
            match satel.get_zone_temperature(zone_id).await {
                Ok(_) => {
                    // Successful read -> event will be captured and printed by [Temperature Listener]
                }
                Err(e) => {
                    // Failed read -> sensor is missing, unconfigured, or queue-blocked
                    eprintln!("  [Query Failed] Zone #{:03} -> Error: {} (No temperature event emitted)", zone_id, e);
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
        sleep(Duration::from_secs(3)).await;
    }

    // Keep listening for 5 more seconds
    println!("\nMeasurement cycles finished. Monitoring for another 5 seconds...");
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
