//! Example 03: Query status of all zones (violation, tamper, alarm, alarm memory, bypass, trouble).
//!
//! Run with default settings:
//!   cargo run --example 03_get_zones_status
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelIntegra};
use std::env;

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
    println!(" SATEL INTEGRA - Query Zones Real-Time Status");
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

    // 2. Connect
    println!("Connecting to the panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 3. Query panel version to determine the exact number of supported zones
    let version = satel.get_integra_version().await?;
    let io_count = version.io_count;
    println!("Integra panel has {} zones.\n", io_count);

    // 4. Refresh all zone statuses from panel
    println!("Fetching zone statuses from control panel...");
    satel.get_zones_violation().await?;
    satel.get_zones_tamper().await?;
    satel.get_zones_alarm().await?;
    satel.get_zones_tamper_alarm().await?;
    satel.get_zones_alarm_memory().await?;
    satel.get_zones_tamper_alarm_memory().await?;
    satel.get_zones_bypass().await?;
    satel.get_zones_no_violation_trouble().await?;
    satel.get_zones_long_violation_trouble().await?;
    println!("Zone statuses refreshed successfully.\n");

    // 5. Display ALL zones status table
    println!("--- Zones Status Table (1..={}) ---", io_count);
    println!("{:-<105}", "");
    println!(
        "{: <5} | {: <11} | {: <8} | {: <7} | {: <10} | {: <8} | {: <16}",
        "ID", "Violation", "Tamper", "Alarm", "Alarm Mem", "Bypass", "Trouble (No/Long)"
    );
    println!("{:-<105}", "");

    let mut violation_count = 0;
    let mut tamper_count = 0;
    let mut alarm_count = 0;
    let mut bypass_count = 0;

    for zone_id in 1..=io_count {
        if let Ok(Some(status)) = satel.get_cached_zone_status(zone_id) {
            if status.violation_state {
                violation_count += 1;
            }
            if status.tamper_state || status.tamper_alarm_state {
                tamper_count += 1;
            }
            if status.alarm_state || status.tamper_alarm_state {
                alarm_count += 1;
            }
            if status.bypass_state {
                bypass_count += 1;
            }

            let trouble_text =
                match (status.no_violation_trouble_state, status.long_violation_trouble_state) {
                    (true, true) => "NoViol+LongViol",
                    (true, false) => "No Violation",
                    (false, true) => "Long Violation",
                    (false, false) => "ok",
                };

            println!(
                "#{:03}  | {: <11} | {: <8} | {: <7} | {: <10} | {: <8} | {: <16}",
                status.id,
                if status.violation_state { "VIOLATED" } else { "ok" },
                if status.tamper_state { "TAMPER" } else { "ok" },
                if status.alarm_state { "ALARM" } else { "ok" },
                if status.alarm_memory_state { "MEMORY" } else { "ok" },
                if status.bypass_state { "BYPASSED" } else { "ok" },
                trouble_text
            );
        }
    }
    println!("{:-<105}", "");
    println!(
        "Summary: Violations: {} | Tampers: {} | Alarms: {} | Bypassed: {}\n",
        violation_count, tamper_count, alarm_count, bypass_count
    );

    // 6. Disconnect cleanly
    println!("Disconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}
