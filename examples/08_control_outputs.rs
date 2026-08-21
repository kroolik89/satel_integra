//! Example 08: Controlling outputs (ON, OFF, TOGGLE) using global and per-call user codes.
//!
//! PIN AUTHENTICATION MODES:
//!   1. Global PIN (Config::user_code):
//!      Best for background daemons and automated smart home integrations where
//!      a single system PIN is configured once at startup.
//!
//!   2. Dynamic / Per-call PIN (Some("...")):
//!      Created specifically for interactive applications (web portals, mobile apps,
//!      multi-user systems) where you do NOT want to hardcode or store a static PIN
//!      at startup. Instead, the end-user must provide their own personal PIN dynamically
//!      for each individual operation.
//!
//! IMPORTANT PROTOCOL NOTE:
//!   Receiving a command confirmation ("Command received") from the ETHM-1 module
//!   only indicates that the command packet was successfully received and queued
//!   by the communication interface.
//!   The Satel Integra protocol does NOT return error frames when:
//!     - The user PIN is incorrect ("Invalid PIN"),
//!     - The user lacks authority to control outputs ("No Access"),
//!     - The target output is not configured for user switching (e.g. static power supply).
//!   In all these unauthorized cases, the Integra panel silently ignores physical
//!   relay execution and records an unauthorized access event in its internal event log.
//!
//! Run with default settings:
//!   cargo run --example 08_control_outputs
//!
//! Environment variables (optional):
//!   SATEL_HOST           - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT           - TCP port (default: 7094)
//!   SATEL_CODE           - Global user access code (default: "1234")
//!   SATEL_DEDICATED_CODE - Dedicated/dynamic user access code (default: "123456")

use satel_integra::{Config, ConnectionConfig, SatelIntegra};
use std::env;
use std::time::Duration;
use tokio::time::sleep;

/// ID of the output to test control on (e.g. relay, light, test output)
const TEST_OUTPUT_ID: u16 = 10;

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
    println!(" SATEL INTEGRA - Control Outputs (ON/OFF/TOGGLE)");
    println!("==================================================");
    println!("Target test output ID: #{:03}", TEST_OUTPUT_ID);
    println!("Global PIN (Config):   \"{}\"", user_code.as_deref().unwrap_or("[None]"));
    println!("Dedicated PIN:         \"{}\"\n", dedicated_code);

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code: user_code.clone(), // Global PIN configured here
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 2. Connect
    println!("Connecting to the panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    // ==========================================================
    // ROUND 1: Using globally configured user_code (passing None)
    // ==========================================================
    println!(
        "--- ROUND 1: Control with Global PIN (Config: \"{}\") ---",
        user_code.as_deref().unwrap_or("[None]")
    );

    println!("  1. Turning Output #{:03} ON...", TEST_OUTPUT_ID);
    satel.set_output_on(TEST_OUTPUT_ID, None).await?;
    println!("     -> Command received (ON)");
    sleep(Duration::from_secs(2)).await;

    println!("  2. Turning Output #{:03} OFF...", TEST_OUTPUT_ID);
    satel.set_output_off(TEST_OUTPUT_ID, None).await?;
    println!("     -> Command received (OFF)");
    sleep(Duration::from_secs(2)).await;

    println!("  3. Toggling Output #{:03} state (Switch)...", TEST_OUTPUT_ID);
    satel.set_output_toggle(TEST_OUTPUT_ID, None).await?;
    println!("     -> Command received (Toggled)");
    sleep(Duration::from_secs(2)).await;

    println!("  4. Toggling Output #{:03} back...", TEST_OUTPUT_ID);
    satel.set_output_toggle(TEST_OUTPUT_ID, None).await?;
    println!("     -> Command received (Toggled back)\n");
    sleep(Duration::from_secs(2)).await;

    // ==========================================================
    // ROUND 2: Using dedicated per-call PIN (passing Some(PIN))
    // Useful for UI/interactive applications where each action
    // is authorized on-the-fly with the user's personal PIN.
    // ==========================================================
    println!(
        "--- ROUND 2: Control with Dedicated PIN (Some(\"{}\")) ---",
        dedicated_code
    );

    println!("  1. Turning Output #{:03} ON...", TEST_OUTPUT_ID);
    satel.set_output_on(TEST_OUTPUT_ID, Some(&dedicated_code)).await?;
    println!("     -> Command received (ON)");
    sleep(Duration::from_secs(2)).await;

    println!("  2. Turning Output #{:03} OFF...", TEST_OUTPUT_ID);
    satel.set_output_off(TEST_OUTPUT_ID, Some(&dedicated_code)).await?;
    println!("     -> Command received (OFF)");
    sleep(Duration::from_secs(2)).await;

    println!("  3. Toggling Output #{:03} state (Switch)...", TEST_OUTPUT_ID);
    satel.set_output_toggle(TEST_OUTPUT_ID, Some(&dedicated_code)).await?;
    println!("     -> Command received (Toggled)");
    sleep(Duration::from_secs(2)).await;

    println!("  4. Toggling Output #{:03} back...", TEST_OUTPUT_ID);
    satel.set_output_toggle(TEST_OUTPUT_ID, Some(&dedicated_code)).await?;
    println!("     -> Command received (Toggled back)\n");

    // 3. Disconnect cleanly
    println!("Disconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}
