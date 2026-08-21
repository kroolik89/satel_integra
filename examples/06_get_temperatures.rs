//! Example 06: Query temperature from zones with connected wireless/wired sensors.
//!
//! Run with default settings:
//!   cargo run --example 06_get_temperatures
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

    // 3. Query temperature from each configured zone
    println!("--- Temperature Readings ---");
    println!("{:-<45}", "");
    println!("{: <5} | {: <14} | {: <10}", "ID", "Temperature", "Read Time");
    println!("{:-<45}", "");

    let mut sensors_found = 0;

    for &zone_id in TEMPERATURE_ZONES {
        match satel.get_zone_temperature(zone_id).await {
            Ok(temp) => {
                sensors_found += 1;
                println!(
                    "#{:03}  | {: >6.1} °C      | {: <10}",
                    zone_id,
                    temp.temperature,
                    temp.read_at.format("%H:%M:%S")
                );
            }
            Err(SatelError::TemperatureNotSupportedOrTimeOut) | Err(SatelError::Timeout) => {
                println!("#{:03}  | [No sensor / Timeout] | -", zone_id);
            }
            Err(SatelError::TemperatureSensorError) => {
                println!("#{:03}  | [Sensor Error 0xFFFF]  | -", zone_id);
            }
            Err(e) => {
                println!("#{:03}  | Error: {: <15} | -", zone_id, e);
            }
        }
    }

    println!("{:-<45}", "");
    println!(
        "Summary: Successfully read {} / {} temperature sensors.\n",
        sensors_found,
        TEMPERATURE_ZONES.len()
    );

    // 4. Disconnect cleanly
    println!("Disconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}
