//! Example 5_03: Automated cyclic background polling for temperatures over AES-192 encrypted connection.
//!
//! ============================================================================
//! 1. OVERVIEW:
//! ============================================================================
//! This example combines automated background temperature polling (`SatelPollingWorker`)
//! with transparent AES-192 encrypted TCP communication (`EncryptedStream`).
//!
//! When configured:
//!   1. Encrypted TCP connection is established using the integration key.
//!   2. A background worker queries configured ABAX / wired temperature zones every 1 minute.
//!   3. The application runs for 10 minutes (600 seconds) to test long-term encrypted connection stability.
//!
//! ============================================================================
//! 2. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 5_03_auto_poll_temperatures_encrypted
//!
//! Environment variables (optional):
//!   SATEL_HOST            - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT            - TCP port (default: 7094)
//!   SATEL_INTEGRATION_KEY - Integration key configured in DLOADX (default: "Jmtp")
//!   SATEL_CODE            - User access code (default: "1234")

use chrono::Local;
use satel_integra::{Config, ConnectionConfig, SatelEvent, SatelIntegra, config::TemperatureProbe};
use std::env;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Read connection parameters
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let integration_key = env::var("SATEL_INTEGRATION_KEY").unwrap_or_else(|_| "Jmtp".to_string());
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Encrypted Temperature Poller (5_03)");
    println!("==================================================");

    // 2. Configure encrypted connection & background temperature polling
    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        encryption: true,
        integration_key: Some(integration_key.clone()),
        user_code,

        // --- Background Temperature Polling Configuration ---
        temperature_probes: vec![
            TemperatureProbe {
                zone_id: 21,
                interval_minutes: 1,
                max_timeout_errors: 3,
                max_sensor_errors: 3,
                unblock_enabled: true,
                unblock_after_cycles: 10,
            },
            TemperatureProbe {
                zone_id: 22,
                interval_minutes: 1,
                max_timeout_errors: 3,
                max_sensor_errors: 3,
                unblock_enabled: true,
                unblock_after_cycles: 10,
            },
            TemperatureProbe {
                zone_id: 23,
                interval_minutes: 1,
                max_timeout_errors: 3,
                max_sensor_errors: 3,
                unblock_enabled: true,
                unblock_after_cycles: 10,
            },
            TemperatureProbe {
                zone_id: 24,
                interval_minutes: 1,
                max_timeout_errors: 3,
                max_sensor_errors: 3,
                unblock_enabled: true,
                unblock_after_cycles: 10,
            },
        ],
        emit_unchanged_temperatures: true,

        // --- Smart Queue Protection & Blocking Parameters ---
        temp_blocking_enabled: true,
        temp_max_timeout_errors: 3,
        temp_max_sensor_errors: 3,

        ..Config::default()
    };

    println!("Configuration:");
    println!("  - Target:                {}:{}", host, port);
    println!("  - Encryption:            ENABLED (AES-192 ECB session)");
    println!("  - Integration Key:       {}", integration_key);
    println!("  - Polling Temp Active:   {}", config.is_polling_enabled());
    println!("  - Temperature Probes:    {}", config.temperature_probes.len());
    println!("  - Smart Blocking:        {}", config.temp_blocking_enabled);
    println!("  - Runtime Duration:      10 minutes (600s)\n");

    let satel = SatelIntegra::new(config);

    // 3. Subscribe to the event channel BEFORE connecting
    let mut rx = satel.subscribe();

    // 4. Spawn listener task to receive background temperature updates
    let listener_handle = tokio::spawn(async move {
        println!("[Event Listener] Active. Awaiting background temperature events...\n");

        while let Ok(event) = rx.recv().await {
            let ts = Local::now().format("%H:%M:%S%.3f");

            match event {
                SatelEvent::ZoneTemperatureChanged { id, temperature } => {
                    println!(
                        "[{}] [TEMPERATURE UPDATE] Zone #{:03} -> {:.1}°C",
                        ts, id, temperature
                    );
                }
                SatelEvent::ZoneTemperatureError { id, status } => {
                    println!(
                        "[{}] [TEMPERATURE ERROR]  Zone #{:03} -> Fault Status: {:?}",
                        ts, id, status
                    );
                }
                SatelEvent::ConnectionChanged(state) => {
                    println!("[{}] [CONNECTION] State -> {:?}", ts, state);
                }
                _ => {}
            }
        }
        println!("[Event Listener] Channel closed.");
    });

    // 5. Connect over encrypted TCP socket
    println!("Connecting over encrypted TCP socket...");
    satel.connect().await?;
    println!("Encrypted connection established successfully!\n");

    // 6. Passive monitoring loop for 10 minutes (with progress logging every minute)
    println!("==================================================================");
    println!(" PASSIVE ENCRYPTED MONITORING ACTIVE (10 minutes)");
    println!(" Temperature updates will appear automatically every 1 minute.");
    println!("==================================================================\n");

    for minute in 1..=10 {
        sleep(Duration::from_secs(60)).await;
        println!(
            "[{}] --- Status: {} of 10 minutes elapsed (connection active) ---",
            Local::now().format("%H:%M:%S"),
            minute
        );
    }

    // 7. Check cached values before exit
    println!("\n--- Cached Temperature Values at End of Run ---");
    for zone_id in [21, 22, 23, 24] {
        if let Ok(Some(temp)) = satel.get_cached_zone_temperature(zone_id) {
            println!(
                "  Zone #{:03}: {:.1}°C (read at: {})",
                zone_id,
                temp.temperature,
                temp.read_at.format("%H:%M:%S")
            );
        } else {
            println!("  Zone #{:03}: No cached data", zone_id);
        }
    }

    // 8. Clean disconnect
    println!("\nDisconnecting encrypted session...");
    satel.disconnect().await?;
    sleep(Duration::from_millis(500)).await;

    listener_handle.abort();
    println!("Disconnected cleanly. 10-minute stability test completed successfully!");

    Ok(())
}
