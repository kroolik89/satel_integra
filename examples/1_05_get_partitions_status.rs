//! Example 1_05: Query real-time status of all partitions (arming, alarms, entry/exit times).
//!
//! ============================================================================
//! 1. 2-STEP PARTITIONS STATUS WORKFLOW (NETWORK FETCH VS CACHE READ):
//! ============================================================================
//! The client operates with a 2-step data model:
//!
//! STEP 1: Network Queries (Fetches latest partition data from panel & updates internal cache):
//!   Command | Async Method                               | Description
//!   --------+--------------------------------------------+---------------------------------------------------
//!   0x0A    | `satel.get_partitions_armed_really().await` | Actually armed partitions (full protection active)
//!   0x09    | `satel.get_partitions_armed_suppressed().await` | Armed suppressed partitions (e.g. stay/partial arming)
//!   0x13    | `satel.get_partitions_alarm().await`        | Partitions currently in active alarm state
//!   0x15    | `satel.get_partitions_alarm_memory().await` | Partitions with latched alarm memory
//!   0x0E/F  | `satel.get_partitions_times().await`       | Combined entry delay and exit delay timers (>10s / <10s)
//!
//! STEP 2: Instant Cache Inspection (Synchronous, zero network overhead):
//!   - `satel.get_cached_partition_name(partition_id)`:
//!       Returns `Option<PartitionName>` from local cache.
//!   - `satel.state_handle()`:
//!       Returns an `RwLock` read handle to entire internal `SatelState`.
//!       Access `state.partitions[0..max_partitions]` for `PartitionStatus` items containing:
//!         * `id`: Partition number (1..=32)
//!         * `armed_really`: Boolean (full arming status)
//!         * `armed_suppressed`: Boolean (suppressed/stay arming)
//!         * `alarm`: Boolean (active alarm)
//!         * `alarm_memory`: Boolean (stored alarm flag)
//!         * `entry_time`: Boolean (entry delay timer countdown in progress)
//!         * `exit_time_gt_10s`: Boolean (exit delay > 10 seconds remaining)
//!         * `exit_time_lt_10s`: Boolean (exit delay < 10 seconds remaining)
//!         * `partition_name`: Optional cached partition name
//!         * Individual `read_at` timestamps for each state component
//!
//! ============================================================================
//! 2. INTEGRA PARTITION ARCHITECTURE & CAPACITIES:
//! ============================================================================
//! - Partition capacity is dynamically derived from the panel model:
//!     * Integra 24:       up to 4 partitions
//!     * Integra 32 / 64:  up to 8 partitions
//!     * Integra 128:      up to 16 partitions
//!     * Integra 256 Plus: up to 32 partitions
//! - `Armed Really` indicates physical security is active.
//! - `Armed Suppressed` indicates arming modes where certain interior zones or alarms are bypassed.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 1_05_get_partitions_status
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
    println!(" SATEL INTEGRA - Query Partitions Real-Time Status");
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

    // 3. Query panel version to determine partition capacity
    let version = satel.get_integra_version().await?;
    let max_partitions = match version.io_count {
        24 => 4,
        32 | 64 => 8,
        128 => 16,
        _ => 32,
    };
    println!("Integra panel model: {}, supported partitions: {}\n", version.model, max_partitions);

    // 4. Fetch real-time partition statuses from panel
    println!("Fetching partition statuses from control panel...");
    satel.get_partitions_armed_really().await?;
    satel.get_partitions_armed_suppressed().await?;
    satel.get_partitions_alarm().await?;
    satel.get_partitions_alarm_memory().await?;
    satel.get_partitions_times().await?;
    println!("Partition statuses refreshed successfully.\n");

    // 5. Display ALL partitions status table
    println!("--- Partitions Status Table (1..={}) ---", max_partitions);
    println!("{:-<85}", "");
    println!(
        "{: <5} | {: <14} | {: <14} | {: <7} | {: <10} | {: <10} | {: <10}",
        "ID", "Armed (Really)", "Armed (Suppr)", "Alarm", "Alarm Mem", "Entry Time", "Exit Time"
    );
    println!("{:-<85}", "");

    let mut armed_count = 0;
    let mut alarm_count = 0;
    let mut timing_count = 0;

    // Scoped block ensures the RwLock read guard is dropped immediately after reading
    {
        let handle = satel.state_handle();
        let state = handle.read().unwrap();

        for partition in &state.partitions[0..max_partitions as usize] {
            if partition.armed_really || partition.armed_suppressed {
                armed_count += 1;
            }
            if partition.alarm {
                alarm_count += 1;
            }
            if partition.entry_time || partition.exit_time_gt_10s || partition.exit_time_lt_10s {
                timing_count += 1;
            }

            let exit_time_str = if partition.exit_time_lt_10s {
                "< 10s"
            } else if partition.exit_time_gt_10s {
                "> 10s"
            } else {
                "ok"
            };

            println!(
                "#{:02}   | {: <14} | {: <14} | {: <7} | {: <10} | {: <10} | {: <10}",
                partition.id,
                if partition.armed_really { "ARMED" } else { "disarmed" },
                if partition.armed_suppressed { "SUPPRESSED" } else { "no" },
                if partition.alarm { "ALARM" } else { "ok" },
                if partition.alarm_memory { "MEMORY" } else { "ok" },
                if partition.entry_time { "COUNTING" } else { "ok" },
                exit_time_str
            );
        }
    }

    println!("{:-<85}", "");
    println!(
        "Summary: Armed Partitions: {} | In Alarm: {} | In Entry/Exit Delay: {}\n",
        armed_count, alarm_count, timing_count
    );

    // 6. Disconnect cleanly
    println!("Disconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}
