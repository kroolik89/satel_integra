//! Example 1_09: Query real-time clock (RTC) and system time from Satel Integra panel.
//!
//! ============================================================================
//! 1. 2-STEP RTC TIME WORKFLOW (NETWORK FETCH VS CACHE READ):
//! ============================================================================
//! The client operates with a 2-step data model:
//!
//! STEP 1: Network Queries (Fetches RTC clock & basic status from panel via 0x1A):
//!   Command | Async Method                        | Description
//!   --------+-------------------------------------+---------------------------------------------------
//!   0x1A    | `satel.get_satel_time().await`      | Query current DateTime<Local> directly from panel RTC
//!   0x1A    | `satel.get_system_status().await`   | Query full SystemStatus (RTC, Service Mode, Troubles)
//!
//! STEP 2: Instant Cache Inspection (Synchronous, zero network overhead):
//!   - `satel.state_handle()`:
//!       Returns an `RwLock` read handle to entire internal `SatelState`.
//!       Access `state.system_status` for:
//!         * `rtc`: Last recorded DateTime<Local> from panel
//!         * `service_mode`: Boolean (service technician mode active)
//!         * `troubles_present`: Boolean (active hardware faults detected)
//!         * `troubles_memory`: Boolean (stored trouble memory flags)
//!
//! Time Drift Inspection:
//!   - Compares panel RTC against local PC system clock (`chrono::Local::now()`)
//!     and displays precise drift delta in seconds.
//!
//! ============================================================================
//! 2. HARDWARE & PROTOCOL NOTES:
//! ============================================================================
//! - Satel Integra panels maintain an internal battery-backed RTC clock.
//! - Protocol frame 0x1A returns BCD-encoded date and time (Century, Year, Month,
//!   Day, Hour, Minute, Second) along with system status bit flags.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 1_09_get_time
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use chrono::Local;
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
    println!(" SATEL INTEGRA - Query System RTC Clock (0x1A)");
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

    // 3. STEP 1: Query panel RTC time over the network (0x1A)
    println!("--- Step 1: Querying RTC Time Over Network ---");
    let panel_time = satel.get_satel_time().await?;
    let pc_time = Local::now();
    let drift_seconds = (panel_time.timestamp() - pc_time.timestamp()).abs();

    println!("  Panel RTC Time:   {}", panel_time.format("%Y-%m-%d %H:%M:%S"));
    println!("  Local PC Time:    {}", pc_time.format("%Y-%m-%d %H:%M:%S"));
    println!("  Clock Difference: {} seconds\n", drift_seconds);

    // Also fetch full status
    let status = satel.get_system_status().await?;
    println!("  System Status Flags:");
    println!("    - Service Mode:       {}", if status.service_mode { "ACTIVE" } else { "Normal operation" });
    println!("    - Troubles Present:   {}", if status.troubles_present { "YES (Active Faults)" } else { "No (OK)" });
    println!("    - Troubles in Memory: {}", if status.troubles_memory { "YES" } else { "No" });
    println!();

    // 4. STEP 2: Instant Cache Inspection (Zero Network I/O)
    println!("--- Step 2: Instant Cache Verification (Zero Network I/O) ---");
    {
        let handle = satel.state_handle();
        let state = handle.read().unwrap();

        if let Some(cached_status) = &state.system_status {
            println!(
                "  Cached Panel RTC Time: {}",
                cached_status.rtc.format("%Y-%m-%d %H:%M:%S")
            );
        } else {
            println!("  No cached status found in memory.");
        }
    }
    println!();

    // 5. Disconnect cleanly
    println!("Disconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}
