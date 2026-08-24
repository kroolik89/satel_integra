//! Example 3_03: Set and synchronize real-time clock (RTC) in Satel Integra panel.
//!
//! ============================================================================
//! 1. RTC CONTROL OVERVIEW & API METHODS:
//! ============================================================================
//! The Satel Integra panel allows reading and updating its internal RTC clock:
//!
//!   Command | Async Method                                     | Description
//!   --------+--------------------------------------------------+---------------------------------------------
//!   0x1A    | `satel.get_satel_time().await`                   | Query current DateTime<Local> from panel
//!   0x8E    | `satel.set_satel_time(datetime, pin).await`      | Set panel RTC clock (YYYYMMDDhhmmss)
//!
//! PIN Authentication Modes:
//!   - Global PIN (`pin = None`):
//!       Uses the system code pre-configured at startup in `Config.user_code`.
//!       Ideal for automated background NTP synchronization daemons.
//!   - Dynamic / Per-call PIN (`pin = Some("123456")`):
//!       Allows passing a dynamic user PIN per operation.
//!       Ideal for UI applications and administrative management tools.
//!
//! ============================================================================
//! 2. PROTOCOL CONFIRMATION & SECURITY BEHAVIOR:
//! ============================================================================
//! Receiving a successful response (`Ok(())` / "Command received") indicates that
//! the command packet was successfully received and queued by the ETHM-1 module.
//!
//! Important Satel protocol note: The panel does NOT return error frames when:
//!   - The user PIN is invalid ("Wrong PIN"),
//!   - The user lacks authority to change system time ("No Access").
//! In unauthorized cases, the Integra panel silently drops the clock update
//! and logs an unauthorized access attempt to its internal event log.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 3_03_control_time
//!
//! Environment variables (optional):
//!   SATEL_HOST           - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT           - TCP port (default: 7094)
//!   SATEL_CODE           - Global user access code (default: "1234")
//!   SATEL_DEDICATED_CODE - Dedicated/dynamic user access code (default: "123456")

use chrono::Local;
use satel_integra::{Config, ConnectionConfig, SatelIntegra};
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
    let dedicated_code = env::var("SATEL_DEDICATED_CODE").unwrap_or_else(|_| "123456".to_string());

    println!("==================================================");
    println!(" SATEL INTEGRA - Synchronize RTC Clock (0x8E)");
    println!("==================================================");
    println!("Global PIN (Config): \"{}\"", user_code.as_deref().unwrap_or("[None]"));
    println!("Dedicated PIN:       \"{}\"\n", dedicated_code);

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code: user_code.clone(),
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 2. Connect
    println!("Connecting to the panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 3. Initial Clock Inspection
    println!("--- Initial Clock Inspection ---");
    let initial_panel_time = satel.get_satel_time().await?;
    let local_pc_time = Local::now();
    let initial_drift = (initial_panel_time.timestamp() - local_pc_time.timestamp()).abs();

    println!("  Panel RTC Time:   {}", initial_panel_time.format("%Y-%m-%d %H:%M:%S"));
    println!("  Local PC Time:    {}", local_pc_time.format("%Y-%m-%d %H:%M:%S"));
    println!("  Clock Difference: {} seconds\n", initial_drift);

    // ==========================================================
    // ROUND 1: Sync RTC using Global PIN (passing None)
    // ==========================================================
    println!(
        "--- ROUND 1: Synchronize with Global PIN (Config: \"{}\") ---",
        user_code.as_deref().unwrap_or("[None]")
    );

    let target_time = Local::now();
    println!("  1. Setting panel clock to current PC time ({})", target_time.format("%H:%M:%S"));
    satel.set_satel_time(target_time, None).await?;
    println!("     -> Command received (Set RTC Time)");
    sleep(Duration::from_secs(2)).await;

    // Verify
    let updated_panel_time = satel.get_satel_time().await?;
    println!(
        "  2. Verification: Panel RTC is now: {}\n",
        updated_panel_time.format("%Y-%m-%d %H:%M:%S")
    );

    // ==========================================================
    // ROUND 2: Sync RTC using Dedicated per-call PIN
    // ==========================================================
    println!(
        "--- ROUND 2: Synchronize with Dedicated PIN (Some(\"{}\")) ---",
        dedicated_code
    );

    let target_time = Local::now();
    println!("  1. Setting panel clock with dedicated PIN ({})", target_time.format("%H:%M:%S"));
    satel.set_satel_time(target_time, Some(&dedicated_code)).await?;
    println!("     -> Command received (Set RTC Time)");
    sleep(Duration::from_secs(2)).await;

    // Verify
    let updated_panel_time = satel.get_satel_time().await?;
    println!(
        "  2. Verification: Panel RTC is now: {}\n",
        updated_panel_time.format("%Y-%m-%d %H:%M:%S")
    );

    // 4. Disconnect cleanly
    println!("Disconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}
