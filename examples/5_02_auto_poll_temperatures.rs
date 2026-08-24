//! Example 5_02: Automated cyclic background polling for wireless/wired temperatures.
//!
//! ============================================================================
//! 1. TEMPERATURE POLLING OVERVIEW & ARCHITECTURE:
//! ============================================================================
//! In the Satel Integra protocol, wireless ABAX temperature sensors (Command `0x7D`)
//! are NOT part of the hardware 0x7F Auto-Push subscription mask. Therefore,
//! temperature values cannot be streamed unsolicited by the ETHM-1 module.
//!
//! To provide fully autonomous, hands-off temperature monitoring without requiring
//! custom polling loops in user applications, the library includes a built-in
//! universal background worker (`SatelPollingWorker`):
//!
//!   - `polling_temperatures_zones`: List of zone IDs (1..256) with temperature sensors.
//!   - `polling_temperatures_interval_minutes`: Polling interval in minutes (min: 1).
//!
//! When configured:
//!   1. A background task runs alongside the network worker and state worker.
//!   2. Every N minutes, it queries each configured zone sequentially (with a 100ms
//!      queue safety pause between sensors).
//!   3. The poller automatically uses smart error blocking (`temp_blocking_enabled`),
//!      protecting the queue from missing or broken sensors.
//!   4. When a temperature changes by > 0.01°C, `SatelEvent::ZoneTemperatureChanged`
//!      is broadcast to all subscribers.
//!
//! ============================================================================
//! 2. 100% PASSIVE CONSUMER PATTERN:
//! ============================================================================
//! In this example, `main()` does NOT call `satel.get_zone_temperature()` directly!
//! The application simply subscribes to the event stream, connects, and receives
//! periodic temperature updates automatically.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 5_02_auto_poll_temperatures
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use chrono::Local;
use satel_integra::{Config, ConnectionConfig, SatelEvent, SatelIntegra};
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
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Background Temperature Poller (4_02)");
    println!("==================================================");

    // 2. Configure background temperature polling
    // Define which zones have temperature sensors and set the polling cycle
    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,

        // --- Background Temperature Polling Configuration ---
        // Explicitly enable background temperature polling
        polling_temperatures: true,

        // List zone IDs where temperature probes are installed (e.g. ABAX wireless sensors)
        polling_temperatures_zones: vec![21, 22, 23, 24],

        // Polling interval in full minutes (minimum enforced: 1 minute)
        polling_temperatures_interval_minutes: 1,

        // Emit temperature events on every polling cycle, even if unchanged
        emit_unchanged_temperatures: true,

        // --- Smart Queue Protection & Blocking Parameters ---
        // Protect communication queue by blocking faulty/missing sensors
        temp_blocking_enabled: true,
        // Block sensor after 2 consecutive timeouts (default is 4)
        temp_max_timeout_errors: 3,
        // Block sensor after 2 consecutive 0xFFFF communication faults (default is 10)
        temp_max_sensor_errors: 3,

        // All auto_read_* flags are disabled (false) by default in Config::default()
        ..Config::default()
    };

    println!("Configuration:");
    println!("  - Target:                {}:{}", host, port);
    println!("  - Polling Temp Active:   {}", config.polling_temperatures);
    println!("  - Polled Temp Zones:     {:?}", config.polling_temperatures_zones);
    println!("  - Polling Interval:      {} min", config.polling_temperatures_interval_minutes);
    println!("  - Smart Blocking:        {}", config.temp_blocking_enabled);
    println!("  - Max Timeout Errors:    {}", config.temp_max_timeout_errors);
    println!("  - Max Sensor Errors:     {}", config.temp_max_sensor_errors);
    println!("  - Emit Unchanged Temps:  {}\n", config.emit_unchanged_temperatures);

    let satel = SatelIntegra::new(config);

    // 3. Subscribe to the event channel BEFORE connecting
    let mut rx = satel.subscribe();

    // 4. Spawn listener task to receive background temperature updates
    let listener_handle = tokio::spawn(async move {
        println!("[Event Listener] Active. Awaiting background temperature events...\n");

        while let Ok(event) = rx.recv().await {
            let ts = Local::now().format("%H:%M:%S%.3f");

            match event {
                // Background Temperature Updates
                SatelEvent::ZoneTemperatureChanged { id, temperature } => {
                    println!(
                        "[{}] [TEMPERATURE UPDATE] Zone #{:03} -> {:.1}°C",
                        ts, id, temperature
                    );
                }

                // Temperature Sensor Faults / Timeouts
                SatelEvent::ZoneTemperatureError { id, status } => {
                    println!(
                        "[{}] [TEMPERATURE ERROR]  Zone #{:03} -> Fault Status: {:?}",
                        ts, id, status
                    );
                }

                // Connection State Changes
                SatelEvent::ConnectionChanged(state) => {
                    println!("[{}] [CONNECTION] State -> {:?}", ts, state);
                }

                _ => {}
            }
        }
        println!("[Event Listener] Channel closed.");
    });

    // 5. Connect to the panel (spawns network worker, push receiver, and temperature poller)
    println!("Connecting to the panel...");
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 6. Passive listening mode (watch background cycles arrive)
    println!("==================================================================");
    println!(" PASSIVE MONITORING ACTIVE (250 seconds)");
    println!(" The background poller will query configured temperature zones");
    println!(" every 1 minute. Temperature events will appear above.");
    println!(" Notice: Zero manual get_zone_temperature() calls in main()!");
    println!("==================================================================\n");

    sleep(Duration::from_secs(250)).await;

    // 7. Check cached values before exit
    println!("\n--- Cached Temperature Values ---");
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
    println!("\nDisconnecting...");
    satel.disconnect().await?;
    sleep(Duration::from_millis(500)).await;

    listener_handle.abort();
    println!("Disconnected cleanly. Done.");

    Ok(())
}
