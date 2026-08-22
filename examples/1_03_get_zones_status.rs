//! Example 1_03: Query real-time status of all zones with Software State Inversion demo.
//!
//! ============================================================================
//! 1. 2-STEP ZONE STATUS WORKFLOW (NETWORK FETCH VS CACHE READ):
//! ============================================================================
//! The client operates with a 2-step data model:
//!
//! STEP 1: Network Queries (Fetches latest data from panel & updates internal cache):
//!   Command | Async Method                              | Description
//!   --------+-------------------------------------------+---------------------------------------------------
//!   0x00    | `satel.get_zones_violation().await`       | Active violation (e.g. PIR movement, contact opened)
//!   0x01    | `satel.get_zones_tamper().await`          | Line/case tamper state
//!   0x02    | `satel.get_zones_alarm().await`           | Active alarm triggered by zone
//!   0x03    | `satel.get_zones_tamper_alarm().await`    | Active tamper alarm triggered by zone
//!   0x04    | `satel.get_zones_alarm_memory().await`    | Alarm memory indicator (latched until cleared)
//!   0x05    | `satel.get_zones_tamper_alarm_memory().await` | Tamper alarm memory indicator
//!   0x06    | `satel.get_zones_bypass().await`          | Bypassed / disabled zone state
//!   0x07    | `satel.get_zones_no_violation_trouble().await` | Trouble: No violation detected within expected time
//!   0x08    | `satel.get_zones_long_violation_trouble().await` | Trouble: Zone continuously violated for too long
//!
//! STEP 2: Instant Cache Inspection (Synchronous, zero network overhead):
//!   - `satel.get_cached_zone_status(zone_id)`:
//!       Returns aggregated `ZoneStatus` struct for a single zone (name, temperature,
//!       all 9 state booleans, and individual timestamps).
//!   - `satel.get_cached_zone_temperature(zone_id)`:
//!       Returns `Option<ZoneTemperature>` from local cache.
//!   - `satel.get_cached_zone_name(zone_id)`:
//!       Returns `Option<ZoneName>` from local cache.
//!   - `satel.state_handle()`:
//!       Returns an `RwLock` read handle to entire internal `SatelState` for bulk read.
//!
//! ============================================================================
//! 2. LOGICAL STATE INVERSION (INVERT LIST):
//! ============================================================================
//! The library provides optional software-level logical inversion for any of the 9 zone states.
//! This directly toggles the reported boolean value:
//!   - Violated / Active (true)  <--> Inverted to Normal / OK (false)
//!   - Normal / OK (false)       <--> Inverted to Violated / Active (true)
//!
//! IMPORTANT DISTINCTION:
//! This is purely logical state inversion in the client software. It is completely
//! independent and distinct from hardware zone wiring configurations (NO, NC, EOL, 2EOL)
//! which are configured on the panel level using DLOADX.
//!
//! AVAILABLE INVERSION FIELDS IN `Config`:
//!   - `io_violation_invert`:            Vec<u16> (0x00 Violation <-> Normal)
//!   - `io_tamper_invert`:               Vec<u16> (0x01 Tamper <-> Normal)
//!   - `io_alarm_invert`:                Vec<u16> (0x02 Alarm <-> Normal)
//!   - `io_tamper_alarm_invert`:         Vec<u16> (0x03 Tamper Alarm <-> Normal)
//!   - `io_alarm_memory_invert`:         Vec<u16> (0x04 Alarm Memory <-> Normal)
//!   - `io_tamper_alarm_memory_invert`:  Vec<u16> (0x05 Tamper Alarm Memory <-> Normal)
//!   - `io_bypass_invert`:               Vec<u16> (0x06 Bypass <-> Normal)
//!   - `io_no_violation_trouble_invert`: Vec<u16> (0x07 "No violation" trouble <-> Normal)
//!   - `io_long_violation_trouble_invert`: Vec<u16> (0x08 "Long violation" trouble <-> Normal)
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 1_03_get_zones_status
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelIntegra};
use std::env;

/// Example list of zone IDs to apply software inversion to (e.g. zones 1 and 2)
const DEMO_INVERT_ZONES: &[u16] = &[1, 2];

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
    println!("Software inversion demo enabled for zones: {:?}", DEMO_INVERT_ZONES);
    println!("(Logical states for these zones will be inverted: Active <-> Normal)\n");

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,
        // Demonstrating logical state inversion for zones 1 and 2:
        io_violation_invert: DEMO_INVERT_ZONES.to_vec(),
        io_tamper_invert: DEMO_INVERT_ZONES.to_vec(),
        io_alarm_invert: DEMO_INVERT_ZONES.to_vec(),
        io_tamper_alarm_invert: DEMO_INVERT_ZONES.to_vec(),
        io_alarm_memory_invert: DEMO_INVERT_ZONES.to_vec(),
        io_tamper_alarm_memory_invert: DEMO_INVERT_ZONES.to_vec(),
        io_bypass_invert: DEMO_INVERT_ZONES.to_vec(),
        io_no_violation_trouble_invert: DEMO_INVERT_ZONES.to_vec(),
        io_long_violation_trouble_invert: DEMO_INVERT_ZONES.to_vec(),
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
    println!("Integra panel model: {}, supported zones: {}\n", version.model, io_count);

    // 4. Refresh all zone statuses from panel
    println!("Fetching zone statuses from control panel (applying inversion lists)...");
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
    println!("{:-<110}", "");
    println!(
        "{: <8} | {: <11} | {: <8} | {: <7} | {: <10} | {: <8} | {: <16}",
        "Zone", "Violation", "Tamper", "Alarm", "Alarm Mem", "Bypass", "Trouble (No/Long)"
    );
    println!("{:-<110}", "");

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

            let is_inverted = DEMO_INVERT_ZONES.contains(&zone_id);
            let zone_label = if is_inverted {
                format!("#{:03}*", status.id)
            } else {
                format!("#{:03} ", status.id)
            };

            println!(
                "{: <8} | {: <11} | {: <8} | {: <7} | {: <10} | {: <8} | {: <16}",
                zone_label,
                if status.violation_state { "VIOLATED" } else { "ok" },
                if status.tamper_state { "TAMPER" } else { "ok" },
                if status.alarm_state { "ALARM" } else { "ok" },
                if status.alarm_memory_state { "MEMORY" } else { "ok" },
                if status.bypass_state { "BYPASSED" } else { "ok" },
                trouble_text
            );
        }
    }
    println!("{:-<110}", "");
    println!("* Marked zone has active software inversion applied in Config.");
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
