//! Example 3_01: Controlling outputs (ON, OFF, TOGGLE) using global and per-call user codes.
//!
//! ============================================================================
//! 1. OUTPUT CONTROL OVERVIEW & API METHODS:
//! ============================================================================
//! The Satel Integra panel allows authorized output control via three asynchronous methods:
//!
//!   Command | Async Method                                     | Description
//!   --------+--------------------------------------------------+---------------------------------------------------
//!   0x88    | `satel.set_output_on(output_id, pin).await`     | Turn specified output ON (Active)
//!   0x89    | `satel.set_output_off(output_id, pin).await`    | Turn specified output OFF (Inactive)
//!   0x91    | `satel.set_output_toggle(output_id, pin).await` | Toggle specified output state (Switch)
//!
//! PIN Authentication Modes:
//!   - Global PIN (`pin = None`):
//!       Uses the system code pre-configured at startup in `Config.user_code`.
//!       Ideal for automated background daemons and smart home integrations.
//!   - Dynamic / Per-call PIN (`pin = Some("123456")`):
//!       Allows passing a dynamic user PIN per operation.
//!       Ideal for web portals and multi-user mobile apps where each end-user
//!       authorizes actions with their own personal PIN.
//!
//! ============================================================================
//! 2. PROTOCOL CONFIRMATION & SECURITY BEHAVIOR:
//! ============================================================================
//! Receiving a successful response (`Ok(())` / "Command received") indicates that
//! the command packet was successfully received and queued by the ETHM-1 module.
//!
//! Important Satel protocol note: The panel does NOT return error frames when:
//!   - The user PIN is invalid ("Wrong PIN"),
//!   - The user lacks authority to switch outputs ("No Access"),
//!   - The target output is not configured for user switching in DLOADX.
//! In all unauthorized cases, the Integra panel silently drops the relay execution
//! and logs an unauthorized access attempt to its internal event log.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 3_01_control_outputs
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
