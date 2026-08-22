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

use satel_integra::{Config, ConnectionConfig, SatelCommand, SatelIntegra};
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

    // 3.1 Direct 1:1 strongly-typed query for Part 1 (Main board, AC/DC, Expander power, ETHM)
    println!("--- Direct 1:1 Typed Query: Troubles Part 1 (0x1B) ---");
    let p1 = satel.get_troubles_part1().await?;
    println!("Main Board Power & System Status:");
    println!("  - AC Power Trouble:      {}", p1.main_board.ac_trouble);
    println!("  - Battery Trouble:       {}", p1.main_board.battery_trouble);
    println!("  - Battery Missing:       {}", p1.main_board.no_battery_present);
    println!("  - OUT1..4 Overload:      {}", p1.main_board.out1_trouble || p1.main_board.out2_trouble || p1.main_board.out3_trouble || p1.main_board.out4_trouble);
    println!("  - DT1/DT2 Data Bus:      {}", p1.main_board.dt1_trouble || p1.main_board.dt2_trouble);
    println!("  - Telephone Line:        {}", p1.main_board.tel_line_no_signal || p1.main_board.tel_line_no_voltage);
    println!("  - ETHM Ping / Server:    {}", p1.ethm_ptsa.ethm_ping_trouble || p1.ethm_ptsa.no_server_connection);
    println!();

    // Query all active trouble parts 1..8
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

    println!("Fetching all trouble parts (Parts 1..8)...");
    for cmd in active_trouble_cmds {
        let _ = satel.get_troubles(cmd).await?;
    }

    // Query all trouble memory parts 1..8
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

    println!("Fetching all trouble memory parts (Parts 1..8)...");
    for cmd in memory_trouble_cmds {
        let _ = satel.get_troubles(cmd).await?;
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
                    let description = trouble_type.to_description();

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
