//! Example 5_01: Automated real-time state streaming via ETHM-1 Auto-Push (Command 0x7F).
//!
//! ============================================================================
//! 1. AUTO-PUSH / AUTO-READ OVERVIEW & HARDWARE REGISTRATION (0x7F):
//! ============================================================================
//! In default configuration (`Config::default()`), all auto-read flags are disabled (`false`).
//! The client operates purely in manual request-response mode.
//!
//! When one or more `auto_read_*` flags are enabled in `Config`:
//!   - During `satel.connect().await`, the library automatically sends a one-time
//!     registration frame (**Command `0x7F`**) with a bitmask of requested data groups.
//!   - The ETHM-1 / INT-RS module saves this subscription in its active session and
//!     returns an `AutoReadReport`.
//!   - From this point forward, the ETHM-1 module **autonomously pushes raw state frames**
//!     over TCP whenever a physical state changes in the panel (e.g. PIR triggered,
//!     relay activated, partition armed).
//!
//! Supported Auto-Push Groups:
//!   - `auto_read_zones_violation`:         Zones movement / open state (0x00)
//!   - `auto_read_zones_tamper`:            Zones line / casing sabotage (0x01)
//!   - `auto_read_zones_alarm`:             Zones active alarms (0x02)
//!   - `auto_read_partitions_armed_really`: Partitions full arming state (0x0A)
//!   - `auto_read_partitions_alarm`:        Partitions alarm status (0x13)
//!   - `auto_read_outputs_state`:           Outputs / relays ON/OFF states (0x17)
//!   - `auto_read_system_troubles`:         System troubles parts 1..8 (0x1B-0x30)
//!
//! ============================================================================
//! 2. 100% PASSIVE OPERATION (ZERO POLLING OVERHEAD):
//! ============================================================================
//! In this example, `main()` does NOT perform a single `satel.get_*()` query!
//! The client sits completely idle. All events received in the listener arrive
//! exclusively via asynchronous push notifications streamed by the ETHM-1 module.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 5_01_auto_read_push
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use chrono::Local;
use satel_integra::{Config, ConnectionConfig, SatelEvent, SatelIntegra};
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

    println!("==================================================");
    println!(" SATEL INTEGRA - Automated Push Streaming (4_01)");
    println!("==================================================");

    // 2. Configure Auto-Push flags
    // Enabling these flags instructs the ETHM-1 module to autonomously stream changes
    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,

        // --- Enable Auto-Push Subscriptions (All 18 Supported Groups) ---
        auto_read_zones_violation: true,              // Zone violations (0x00)
        auto_read_zones_tamper: true,                 // Zone tampers (0x01)
        auto_read_zones_alarm: true,                  // Zone alarms (0x02)
        auto_read_zones_tamper_alarm: true,           // Zone tamper alarms (0x03)
        auto_read_zones_alarm_memory: true,           // Zone alarm memory (0x04)
        auto_read_zones_tamper_alarm_memory: true,    // Zone tamper alarm memory (0x05)
        auto_read_zones_bypass: true,                 // Zone bypasses (0x06)
        auto_read_zones_no_violation_trouble: true,   // Zone 'no violation' trouble (0x07)
        auto_read_zones_long_violation_trouble: true, // Zone 'long violation' trouble (0x08)
        auto_read_partitions_armed_suppressed: true,  // Partitions armed suppressed (0x09)
        auto_read_partitions_armed_really: true,      // Partitions armed really (0x0A)
        auto_read_partitions_alarm: true,             // Partitions alarm (0x13)
        auto_read_partitions_alarm_memory: true,      // Partitions alarm memory (0x15)
        auto_read_partitions_entry_time: true,        // Partitions entry time (0x0E)
        auto_read_partitions_exit_time: true,         // Partitions exit time (0x0F, 0x10)
        auto_read_outputs_state: true,                // Outputs state (0x17)
        auto_read_system_troubles: true,              // System troubles (0x1B-0x30)
        auto_read_troubles_memory: true,              // Troubles memory (0x20-0x31)

        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 3. Subscribe to event broadcast BEFORE connecting
    let mut rx = satel.subscribe();

    // 4. Spawn background listener task to receive spontaneous push events
    let listener_handle = tokio::spawn(async move {
        println!("[Push Listener] Task active. Waiting for unsolicited panel frames...\n");

        while let Ok(event) = rx.recv().await {
            let ts = Local::now().format("%H:%M:%S%.3f");

            match event {
                // Auto-Push Registration Report from Handshake
                SatelEvent::AutoReadConfigured(report) => {
                    println!("------------------------------------------------------------------");
                    println!("[{}] [AUTO-PUSH REGISTRATION REPORT]", ts);
                    println!("Active items accepted by ETHM: {}/{}", report.success_count, report.total_requested);
                    for item in &report.items {
                        println!("  * {:<35} -> {:?}", item.name, item.state);
                    }
                    println!("------------------------------------------------------------------\n");
                }

                // Spontaneous State Push Notifications from ETHM-1
                SatelEvent::ZoneViolation { id, state } => {
                    println!("[{}] [PUSH: ZONE VIOLATION] Zone #{:03} -> {}", ts, id, if state { "VIOLATED (Movement)" } else { "NORMAL" });
                }
                SatelEvent::ZoneTamper { id, state } => {
                    println!("[{}] [PUSH: ZONE TAMPER]    Zone #{:03} -> {}", ts, id, if state { "TAMPERED!" } else { "NORMAL" });
                }
                SatelEvent::OutputChanged { id, state } => {
                    println!("[{}] [PUSH: OUTPUT CHANGED] Output #{:03} -> {}", ts, id, if state { "ON" } else { "OFF" });
                }
                SatelEvent::PartitionArmedReally { id, state } => {
                    println!("[{}] [PUSH: PARTITION ARM]  Partition #{:02} -> {}", ts, id, if state { "ARMED" } else { "DISARMED" });
                }
                SatelEvent::PartitionAlarm { id, state } => {
                    println!("[{}] [PUSH: PARTITION ALARM]Partition #{:02} -> {}", ts, id, if state { "ALARM ACTIVE!" } else { "CLEARED" });
                }
                SatelEvent::Trouble(kind, state) => {
                    println!("[{}] [PUSH: SYSTEM TROUBLE] {:?} -> {}", ts, kind, if state { "FAULT ACTIVE" } else { "RESTORED" });
                }
                SatelEvent::ConnectionChanged(state) => {
                    println!("[{}] [CONNECTION STATE]     State -> {:?}", ts, state);
                }

                // Ignore other metadata events in this demo
                _ => {}
            }
        }
        println!("[Push Listener] Event channel closed.");
    });

    // 5. Connect to the panel (executes handshake & registers 0x7F Push mask with ETHM-1)
    println!("Connecting to the panel (registering 0x7F push mask during handshake)...");
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 6. 100% Passive Listening Mode (NO manual queries!)
    println!("==================================================================");
    println!(" PASSIVE LISTENING ACTIVE (25 seconds)");
    println!(" Notice: Zero manual get_*() queries are executed by this script!");
    println!(" Walk in front of a PIR, open a door, or switch an output in DLOADX");
    println!(" to observe real-time spontaneous push frames streamed by ETHM-1.");
    println!("==================================================================\n");

    sleep(Duration::from_secs(25)).await;

    // 7. Disconnect cleanly
    println!("\nDisconnecting...");
    satel.disconnect().await?;
    sleep(Duration::from_millis(500)).await;

    // Abort background listener
    listener_handle.abort();
    println!("Disconnected cleanly. Done.");

    Ok(())
}
