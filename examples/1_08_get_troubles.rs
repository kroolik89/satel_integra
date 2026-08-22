//! Example 1_08: Query real-time system troubles, diagnostic faults, and trouble memory.
//!
//! ============================================================================
//! 1. 2-STEP TROUBLES WORKFLOW (NETWORK FETCH VS CACHE READ):
//! ============================================================================
//! The client operates with a 2-step data model:
//!
//! STEP 1: Network Queries (Fetches system status & trouble parts from panel):
//!   Command | Async Method                              | Description
//!   --------+-------------------------------------------+---------------------------------------------------
//!   0x1A    | `satel.get_system_status().await`         | Query RTC clock, service mode & general trouble flags
//!   0x1B-30 | `satel.get_system_troubles(cmd).await`    | Query active trouble groups 1..8 (0x1B, 0x1C, 0x1D, ...)
//!   0x20-31 | `satel.get_system_troubles(cmd).await`    | Query trouble memory groups 1..8 (0x20, 0x21, 0x22, ...)
//!
//! STEP 2: Instant Cache Inspection (Synchronous, zero network overhead):
//!   - `satel.state_handle()`:
//!       Returns an `RwLock` read handle to entire internal `SatelState`.
//!       Access:
//!         * `state.system_status`: `Option<SystemStatus>` (troubles_present, troubles_memory, service_mode)
//!         * `state.troubles`: `[Vec<bool>; 8]` (currently active troubles mapped via `map_trouble_part_bit`)
//!         * `state.troubles_memory`: `[Vec<bool>; 8]` (stored trouble memory flags)
//!
//! ============================================================================
//! 2. INTEGRA TROUBLE ARCHITECTURE & GROUPS:
//! ============================================================================
//! The Integra panel monitors diagnostic bits split across 8 parts:
//!   - Part 1 (0x1B): Main board AC loss, low battery, missing battery, output overloads, RTC loss, expanders 1-8.
//!   - Part 2 (0x1C): Expanders 9-32 AC loss and battery low.
//!   - Part 3 (0x1D): Expanders 1-32 missing battery.
//!   - Part 4 (0x1E): Expanders 1-32 output overloads & data bus errors.
//!   - Part 5 (0x1F): ETHM/GSM monitoring errors, server connection, GSM signal, zones 1-8 technical troubles.
//!   - Parts 6-8 (0x2C, 0x2D, 0x30): Zones 9-128 technical troubles.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 1_08_get_troubles
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelCommand, SatelIntegra, TroubleType};
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
    println!(" SATEL INTEGRA - System Troubles & Diagnostics");
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

    // 3. STEP 1: Query general system status (0x1A)
    println!("--- Step 1: Querying System Status & Troubles Over Network ---");
    let status = satel.get_system_status().await?;
    println!("System Status (0x1A):");
    println!("  Panel RTC Time:     {}", status.rtc.format("%Y-%m-%d %H:%M:%S"));
    println!("  Service Mode:       {}", if status.service_mode { "ACTIVE" } else { "Normal operation" });
    println!("  Troubles Present:   {}", if status.troubles_present { "YES (Active Faults!)" } else { "No (OK)" });
    println!("  Troubles in Memory: {}", if status.troubles_memory { "YES (Stored in Memory)" } else { "No" });
    println!();

    // Query active trouble parts 1..8
    let active_trouble_cmds = [
        SatelCommand::TroublesPart1,
        SatelCommand::TroublesPart2,
        SatelCommand::TroublesPart3,
        SatelCommand::TroublesPart4,
        SatelCommand::TroublesPart5,
        SatelCommand::TroublesPart6,
        SatelCommand::TroublesPart7,
        SatelCommand::TroublesPart8,
    ];

    println!("Fetching active troubles (Parts 1..8)...");
    for cmd in active_trouble_cmds {
        satel.get_system_troubles(cmd).await?;
    }

    // Query trouble memory parts 1..8
    let memory_trouble_cmds = [
        SatelCommand::TroublesMemoryPart1,
        SatelCommand::TroublesMemoryPart2,
        SatelCommand::TroublesMemoryPart3,
        SatelCommand::TroublesMemoryPart4,
        SatelCommand::TroublesMemoryPart5,
        SatelCommand::TroublesMemoryPart6,
        SatelCommand::TroublesMemoryPart7,
        SatelCommand::TroublesMemoryPart8,
    ];

    println!("Fetching trouble memory (Parts 1..8)...");
    for cmd in memory_trouble_cmds {
        satel.get_system_troubles(cmd).await?;
    }
    println!("All trouble parts updated in internal cache.\n");

    // 4. STEP 2: Inspect Local Cache (Zero Network I/O)
    println!("--- Step 2: Diagnostic Inspection from Local Cache ---");
    {
        let handle = satel.state_handle();
        let state = handle.read().unwrap();

        let mut active_trouble_count = 0;
        let mut memory_trouble_count = 0;

        println!("{:-<85}", "");
        println!(
            "{: <12} | {: <10} | {: <10} | {: <50}",
            "Part / Bit", "Active", "In Memory", "Description / Diagnostic Target"
        );
        println!("{:-<85}", "");

        for part_idx in 0..8 {
            let active_part = &state.troubles[part_idx];
            let memory_part = &state.troubles_memory[part_idx];
            let max_len = active_part.len().max(memory_part.len());

            for bit_idx in 0..max_len {
                let is_active = active_part.get(bit_idx).copied().unwrap_or(false);
                let in_memory = memory_part.get(bit_idx).copied().unwrap_or(false);

                if is_active {
                    active_trouble_count += 1;
                }
                if in_memory {
                    memory_trouble_count += 1;
                }

                // Display row only if trouble is currently active or latched in memory
                if is_active || in_memory {
                    let trouble_type = satel_integra::parsers::map_trouble_part_bit(
                        part_idx as u8,
                        bit_idx as u16,
                    );
                    let description = format_trouble_description(trouble_type);

                    println!(
                        "P{}:Bit #{:03} | {: <10} | {: <10} | {: <50}",
                        part_idx + 1,
                        bit_idx,
                        if is_active { "ACTIVE" } else { "-" },
                        if in_memory { "MEMORY" } else { "-" },
                        description
                    );
                }
            }
        }

        println!("{:-<85}", "");
        if active_trouble_count == 0 && memory_trouble_count == 0 {
            println!("Diagnostic Result: System is healthy. Zero active troubles or memory records.\n");
        } else {
            println!(
                "Diagnostic Summary: Active Faults: {} | Memory Records: {}\n",
                active_trouble_count, memory_trouble_count
            );
        }
    }

    // 5. Disconnect cleanly
    println!("Disconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}

/// Helper function to format `TroubleType` into human-readable diagnostic text
fn format_trouble_description(trouble: TroubleType) -> String {
    match trouble {
        TroubleType::OutTrouble(id) => format!("Output #{:02} Trouble", id),
        TroubleType::MainBoardAcLoss => "Main Board AC Power Loss (230V)".to_string(),
        TroubleType::MainBoardBatteryLow => "Main Board Battery Voltage Low".to_string(),
        TroubleType::MainBoardBatteryMissing => "Main Board Battery Missing / Disconnected".to_string(),
        TroubleType::MainBoardOutOverload => "Main Board Power Supply / Output Overload".to_string(),
        TroubleType::TelephoneLineTrouble => "Telephone Line Trouble (No dial tone / voltage)".to_string(),
        TroubleType::RtcLoss => "RTC Real-Time Clock Loss (Time not set)".to_string(),
        TroubleType::PrinterTrouble => "RS-232 / Printer Interface Error".to_string(),
        TroubleType::MainBoardDataBusError => "Main Board Communication Keypad/Expander Bus Error".to_string(),
        TroubleType::ExpanderAcLoss(id) => format!("Expander #{:02} AC Power Loss", id),
        TroubleType::ExpanderBatteryLow(id) => format!("Expander #{:02} Battery Voltage Low", id),
        TroubleType::ExpanderBatteryMissing(id) => format!("Expander #{:02} Battery Missing", id),
        TroubleType::ExpanderOutOverload(id) => format!("Expander #{:02} Power Output Overload", id),
        TroubleType::ExpanderDataBusError(id) => format!("Expander #{:02} Data Bus Communication Error", id),
        TroubleType::EthmMonitoringStation1Error => "ETHM Monitoring Station 1 Connection Error".to_string(),
        TroubleType::EthmMonitoringStation2Error => "ETHM Monitoring Station 2 Connection Error".to_string(),
        TroubleType::EthmDloadxConnectionError => "ETHM DLOADX Remote Connection Error".to_string(),
        TroubleType::EthmSatelServerConnectionError => "ETHM Satel Server Connection Error".to_string(),
        TroubleType::IntGsmSignalLoss => "INT-GSM Cellular Signal Loss".to_string(),
        TroubleType::GsmMonitoringStation1Error => "GSM Monitoring Station 1 Error".to_string(),
        TroubleType::GsmMonitoringStation2Error => "GSM Monitoring Station 2 Error".to_string(),
        TroubleType::ServiceAccessBlocked => "Service Access Blocked / Locked".to_string(),
        TroubleType::ZoneTrouble(id) => format!("Zone (Input) #{:03} Technical Trouble", id),
        TroubleType::GenericTrouble { part, bit } => format!("Diagnostic Trouble (Part {}, Bit {})", part + 1, bit),
    }
}
