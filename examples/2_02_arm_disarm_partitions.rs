//! Example 2_02: Arming, disarming, and clearing alarms in partitions using global and per-call user codes.
//!
//! ============================================================================
//! 1. PARTITION CONTROL OVERVIEW & API METHODS:
//! ============================================================================
//! The Satel Integra panel provides flexible arming modes and alarm management:
//!
//!   Command | Async Method                                     | Mode | Description
//!   --------+--------------------------------------------------+------+---------------------------------------------
//!   0x80    | `satel.arm_full(partition_id, pin).await`        | 0    | Full arming (all zones armed)
//!   0x81    | `satel.arm_stay(partition_id, pin).await`        | 1    | Stay arming (interior zones bypassed)
//!   0x82    | `satel.arm_stay_delay0(partition_id, pin).await` | 2    | Stay arming without entry delay
//!   0x83    | `satel.arm_stay_no_exit(partition_id, pin).await`| 3    | Stay arming without exit delay
//!   0xA0    | `satel.force_arm_full(partition_id, pin).await`  | 0    | Force full arming (violating zones bypassed)
//!   0xA1    | `satel.force_arm_stay(partition_id, pin).await`  | 1    | Force stay arming
//!   0x84    | `satel.disarm(partition_id, pin).await`          | -    | Disarm partition
//!   0x85    | `satel.clear_alarm(partition_id, pin).await`     | -    | Clear active alarm / alarm memory
//!
//! PIN Authentication Modes:
//!   - Global PIN (`pin = None`):
//!       Uses pre-configured `Config.user_code` from startup (daemons, smart home).
//!   - Dynamic / Per-call PIN (`pin = Some("123456")`):
//!       Passes personal user code on-the-fly (mobile apps, web portals).
//!
//! ============================================================================
//! 2. PROTOCOL CONFIRMATION & SECURITY BEHAVIOR:
//! ============================================================================
//! Receiving a successful response (`Ok(())` / "Command received") indicates that
//! the command packet was successfully received and queued by the ETHM-1 module.
//!
//! Important Satel protocol note: Just like with output control, the panel does NOT return
//! error frames when:
//!   - The user PIN is invalid ("Wrong PIN"),
//!   - The user lacks authority to arm/disarm this partition ("No Access"),
//!   - The partition has violated zones preventing arming (unless force arming is used).
//! In all unauthorized cases, the Integra panel silently drops the physical execution
//! and logs an unauthorized access attempt to its internal event log.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 2_02_arm_disarm_partitions
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

/// ID of the partition to test arming/disarming on (Set to Partition #05)
const TEST_PARTITION_ID: u16 = 5;

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
    println!(" SATEL INTEGRA - Arm / Disarm Partitions");
    println!("==================================================");
    println!("Target partition ID: #{:02}", TEST_PARTITION_ID);
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

    // Check partition name
    if let Ok(partition) = satel.get_partition_name(TEST_PARTITION_ID).await {
        println!("Selected Partition #{:02}: \"{}\"\n", TEST_PARTITION_ID, partition.name);
    }

    // ==========================================================
    // ROUND 1: Arm & Disarm with Global PIN (passing None)
    // ==========================================================
    println!(
        "--- ROUND 1: Control with Global PIN (Config: \"{}\") ---",
        user_code.as_deref().unwrap_or("[None]")
    );

    println!("  1. Arming Partition #{:02} in Mode 0 (Full Arm)...", TEST_PARTITION_ID);
    satel.arm_full(TEST_PARTITION_ID, None).await?;
    println!("     -> Command received (Full Arm)");
    sleep(Duration::from_secs(3)).await;

    println!("  2. Disarming Partition #{:02}...", TEST_PARTITION_ID);
    satel.disarm(TEST_PARTITION_ID, None).await?;
    println!("     -> Command received (Disarm)\n");
    sleep(Duration::from_secs(2)).await;

    // ==========================================================
    // ROUND 2: Stay Arm, Disarm & Clear Alarm with Dedicated PIN
    // ==========================================================
    println!(
        "--- ROUND 2: Control with Dedicated PIN (Some(\"{}\")) ---",
        dedicated_code
    );

    println!("  1. Arming Partition #{:02} in Mode 1 (Stay / Home Arm)...", TEST_PARTITION_ID);
    satel.arm_stay(TEST_PARTITION_ID, Some(&dedicated_code)).await?;
    println!("     -> Command received (Stay Arm)");
    sleep(Duration::from_secs(3)).await;

    println!("  2. Disarming Partition #{:02}...", TEST_PARTITION_ID);
    satel.disarm(TEST_PARTITION_ID, Some(&dedicated_code)).await?;
    println!("     -> Command received (Disarm)");
    sleep(Duration::from_secs(2)).await;

    println!("  3. Clearing active alarms / alarm memory in Partition #{:02}...", TEST_PARTITION_ID);
    satel.clear_alarm(TEST_PARTITION_ID, Some(&dedicated_code)).await?;
    println!("     -> Command received (Clear Alarm)\n");

    // 3. Disconnect cleanly
    println!("Disconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}
