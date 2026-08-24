//! Example 2_07: Demonstration of Smart Temperature Error Blocking & Self-Healing.
//!
//! ============================================================================
//! 1. SMART ERROR BLOCKING & QUEUE PROTECTION OVERVIEW:
//! ============================================================================
//! Querying temperature from a non-existent, broken, or unconfigured wireless sensor
//! normally results in a 2000ms protocol timeout.
//!
//! If an automation loop queries multiple missing sensors, timeout delays can quickly
//! block the entire communication queue for 10-30 seconds, starving other vital
//! alarm events and relay commands.
//!
//! The `satel_integra` library includes built-in protective logic:
//!   - Method: `satel.get_zone_temperature(zone_id).await` (automatically applies blocking when `temp_blocking_enabled: true`)
//!   - If consecutive timeouts reach `temp_max_timeout_errors` (e.g. 3):
//!       * The sensor is marked as `TemperatureSensorStatus::NotSupported`.
//!       * Subsequent queries return `Err(SatelError::TempTooManyErrors)` INSTANTLY (0 ms),
//!         preventing queue starvation!
//!   - If consecutive 0xFFFF errors reach `temp_max_sensor_errors` (e.g. 10):
//!       * The sensor is marked as `TemperatureSensorStatus::CommunicationError`.
//!       * Subsequent queries are blocked instantly (0 ms).
//!
//! ============================================================================
//! 2. AUTOMATIC SELF-HEALING MECHANISM:
//! ============================================================================
//! Temporary radio interference or low battery events should not permanently lock out
//! a real sensor.
//!
//! Whenever a previously failing sensor successfully returns a temperature reading,
//! its internal error counter is automatically decremented (`counter -= 1`).
//! Once errors drop back below the threshold, the sensor fully self-heals without
//! requiring client restart!
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 2_07_get_temperatures_smart_blocking
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelError, SatelIntegra};
use std::env;
use std::time::Instant;
use tokio::time::{sleep, Duration};

/// IMPORTANT: Specify zone IDs where NO temperature sensor is connected (e.g. regular PIR zones)
const ZONES_WITHOUT_TEMP_SENSORS: &[u16] = &[1, 2];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Temperature Blocking & Self-Healing Demo");
    println!("==================================================");
    println!(
        "Testing missing sensors on zones: {:?}",
        ZONES_WITHOUT_TEMP_SENSORS
    );
    println!("Expected behavior: First 3 queries will timeout (~2000ms), subsequent queries will be blocked instantly (0ms).\n");

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,
        temp_read_timeout_ms: 2000,  // 2s timeout for sensor response
        temp_blocking_enabled: true, // Enable smart error blocking
        temp_max_timeout_errors: 3,  // Block after 3 consecutive timeouts (missing sensor)
        temp_max_sensor_errors: 10,  // Block after 10 sensor 0xFFFF communication errors
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    println!("Connecting to the panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    // Run 11 test cycles (sufficient to trigger both timeout and 0xFFFF sensor error limits)
    for cycle in 1..=11 {
        println!("--- Test Cycle {} / 11 ---", cycle);

        for &zone_id in ZONES_WITHOUT_TEMP_SENSORS {
            let start = Instant::now();

            match satel.get_zone_temperature(zone_id).await {
                Ok(temp) => {
                    let elapsed = start.elapsed();
                    println!(
                        "  Zone #{:03}: Got {:.1}°C (Time: {:?}) -> Error counter decremented (Self-Healing)",
                        zone_id, temp.temperature, elapsed
                    );
                }
                Err(SatelError::TemperatureNotSupportedOrTimeOut) | Err(SatelError::Timeout) => {
                    let elapsed = start.elapsed();
                    println!(
                        "  Zone #{:03}: TIMEOUT (waited: {:?}) -> Counted as failed attempt",
                        zone_id, elapsed
                    );
                }
                Err(SatelError::TempTooManyErrors) => {
                    let elapsed = start.elapsed();
                    println!(
                        "  Zone #{:03}: BLOCKED! Response time: {:?} (Zero wait time - queue protected!)",
                        zone_id, elapsed
                    );
                }
                Err(e) => {
                    let elapsed = start.elapsed();
                    println!("  Zone #{:03}: Error: {} (Time: {:?})", zone_id, e, elapsed);
                }
            }
        }

        println!();
        sleep(Duration::from_millis(500)).await;
    }

    println!("Demo completed successfully. Disconnecting...");
    satel.disconnect().await?;
    println!("Done.");

    Ok(())
}
