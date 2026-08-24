//! Example 2_06: Query temperature from zones with connected wireless/wired sensors.
//!
//! ============================================================================
//! 1. 2-STEP TEMPERATURE WORKFLOW (NETWORK FETCH VS CACHE READ):
//! ============================================================================
//! The client operates with a 2-step data model:
//!
//! STEP 1: Network Query (Fetches temperature from panel via 0x7D & updates internal cache):
//!   Command | Async Method                            | Description
//!   --------+-----------------------------------------+---------------------------------------------------
//!   0x7D    | `satel.get_zone_temperature(zone_id)`   | Query zone temperature (raw 16-bit word, 0.5°C step)
//!
//! STEP 2: Instant Cache Inspection (Synchronous, zero network overhead):
//!   - `satel.get_cached_zone_temperature(zone_id)`:
//!       Returns `Option<ZoneTemperature>` (temperature in °C, read_at timestamp).
//!   - `satel.get_cached_zone_status(zone_id)`:
//!       Returns aggregated `ZoneStatus`, where `.temperature` contains `Option<f32>`.
//!   - `satel.state_handle()`:
//!       Access `state.zones[index]` directly for `temperature_value`, `temperature_read_at`,
//!       and `temperature_status` (Ok / Timeout / CommunicationError / NotSupported).
//!
//! ============================================================================
//! 2. HARDWARE & PROTOCOL SPECIFICS:
//! ============================================================================
//! - Supported wireless sensors: Satel ABAX 2 (e.g. ATD-100, APD-200, AOCD-260, APMD-250).
//! - Measurement range: -55.0°C to +72.5°C with 0.5°C step resolution.
//! - Error 0xFFFF: The panel returns 0xFFFF when the sensor probe is damaged or disconnected.
//! - Timeout: If no sensor is paired with the given zone ID, the query times out after `temp_read_timeout_ms`.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 2_06_get_temperatures
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelError, SatelIntegra};
use std::env;

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
    println!(" SATEL INTEGRA - Query Temperature Sensors");
    println!("==================================================");
    println!("Target temperature zones: {:?}\n", TEMPERATURE_ZONES);

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 2. Connect
    println!("Connecting to the panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 3. STEP 1: Query temperature from each configured zone over the network
    println!("--- Step 1: Querying Temperature Over Network (0x7D) ---");
    println!("{:-<55}", "");
    println!("{: <5} | {: <14} | {: <10} | {: <12}", "ID", "Temperature", "Read Time", "Query Status");
    println!("{:-<55}", "");

    let mut sensors_found = 0;

    for &zone_id in TEMPERATURE_ZONES {
        match satel.get_zone_temperature(zone_id).await {
            Ok(temp) => {
                sensors_found += 1;
                println!(
                    "#{:03}  | {: >6.1} °C      | {: <10} | OK",
                    zone_id,
                    temp.temperature,
                    temp.read_at.format("%H:%M:%S")
                );
            }
            Err(SatelError::TemperatureNotSupportedOrTimeOut) | Err(SatelError::Timeout) => {
                println!("#{:03}  | [No sensor]    | -          | Timeout", zone_id);
            }
            Err(SatelError::TemperatureSensorError) => {
                println!("#{:03}  | [Probe Error]  | -          | Error 0xFFFF", zone_id);
            }
            Err(e) => {
                println!("#{:03}  | [Error]        | -          | {}", zone_id, e);
            }
        }
    }

    println!("{:-<55}", "");
    println!(
        "Summary: Successfully read {} / {} temperature sensors.\n",
        sensors_found,
        TEMPERATURE_ZONES.len()
    );

    // 4. STEP 2: Instant cache inspection (synchronous local read)
    println!("--- Step 2: Instant Cache Verification (Zero Network I/O) ---");
    for &zone_id in TEMPERATURE_ZONES {
        if let Ok(Some(cached_temp)) = satel.get_cached_zone_temperature(zone_id) {
            println!(
                "  Cache #{:03}: {:.1} °C (Timestamp: {})",
                zone_id,
                cached_temp.temperature,
                cached_temp.read_at.format("%H:%M:%S")
            );
        }
    }
    println!();

    // 5. Disconnect cleanly
    println!("Disconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}
