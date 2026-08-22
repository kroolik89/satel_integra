//! Example 1_02: Dynamically query ALL names of partitions, zones, and outputs based on panel capacity.
//!
//! ============================================================================
//! 1. 2-STEP SYSTEM NAMES WORKFLOW (NETWORK FETCH VS CACHE READ):
//! ============================================================================
//! The client operates with a 2-step data model:
//!
//! STEP 1: Network Queries (Fetches 16-character names from panel & updates internal cache):
//!   Command | Async Method                        | Device Type | Description
//!   --------+-------------------------------------+-------------+-------------------------------------
//!   0xEE    | `satel.get_partition_name(id).await`| 0x00        | Query user-configured partition name
//!   0xEE    | `satel.get_zone_name(id).await`     | 0x01        | Query user-configured zone (input) name
//!   0xEE    | `satel.get_output_name(id).await`   | 0x02        | Query user-configured output name
//!
//! STEP 2: Instant Cache Inspection (Synchronous, zero network overhead):
//!   - `satel.get_cached_partition_name(id)`: Returns `Option<PartitionName>` from local cache.
//!   - `satel.get_cached_zone_name(id)`:      Returns `Option<ZoneName>` from local cache.
//!   - `satel.get_cached_output_name(id)`:    Returns `Option<OutputName>` from local cache.
//!   - `satel.state_handle()`:                Direct `RwLock` access to full `SatelState`.
//!
//! ============================================================================
//! 2. PROTOCOL SPECIFICS & CHARACTER ENCODING:
//! ============================================================================
//! - Names are stored in the panel as 16-byte fixed strings in national codepage (CP1250 / ISO-8859-2).
//! - The library automatically trims trailing whitespace and converts characters to UTF-8 Rust `String`.
//! - If an object is not configured or unassigned, the panel responds with `0xEF` (ResultCode),
//!   which the library transparently converts into an empty string name.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 1_02_get_names
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
    println!(" SATEL INTEGRA - Query All System Object Names");
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

    // 3. Query panel version to determine capacity
    let version = satel.get_integra_version().await?;
    let io_count = version.io_count;
    let max_partitions = match io_count {
        24 => 4,
        32 | 64 => 8,
        128 => 16,
        _ => 32,
    };

    println!(
        "Integra panel model: {} ({} zones, {} outputs, {} partitions)\n",
        version.model, io_count, io_count, max_partitions
    );

    // 4. STEP 1: Fetch Partition Names (1..=max_partitions)
    println!("--- Fetching Partition Names from Panel (1..={}) ---", max_partitions);
    for partition_id in 1..=max_partitions {
        match satel.get_partition_name(partition_id).await {
            Ok(partition) => {
                if partition.name.trim().is_empty() {
                    println!("  Partition #{:02}: [Empty / Unassigned]", partition_id);
                } else {
                    println!("  Partition #{:02}: \"{}\"", partition_id, partition.name);
                }
            }
            Err(e) => eprintln!("  Partition #{:02}: Error ({})", partition_id, e),
        }
    }
    println!();

    // 5. STEP 1: Fetch ALL Zone Names (1..=io_count)
    println!("--- Fetching Zone Names from Panel (1..={}) ---", io_count);
    for zone_id in 1..=io_count {
        match satel.get_zone_name(zone_id).await {
            Ok(zone) => {
                if zone.name.trim().is_empty() {
                    println!("  Zone #{:03}: [Empty / Unassigned]", zone_id);
                } else {
                    println!("  Zone #{:03}: \"{}\"", zone_id, zone.name);
                }
            }
            Err(e) => eprintln!("  Zone #{:03}: Error ({})", zone_id, e),
        }
    }
    println!();

    // 6. STEP 1: Fetch ALL Output Names (1..=io_count)
    println!("--- Fetching Output Names from Panel (1..={}) ---", io_count);
    for output_id in 1..=io_count {
        match satel.get_output_name(output_id).await {
            Ok(output) => {
                if output.name.trim().is_empty() {
                    println!("  Output #{:03}: [Empty / Unassigned]", output_id);
                } else {
                    println!("  Output #{:03}: \"{}\"", output_id, output.name);
                }
            }
            Err(e) => eprintln!("  Output #{:03}: Error ({})", output_id, e),
        }
    }
    println!();

    // 7. STEP 2: Inspect Local Cache (Synchronous read)
    println!("--- Step 2: Instant Cache Verification (Zero Network I/O) ---");
    println!("Quick verification of first 4 items in local memory cache:");
    for id in 1..=4.min(max_partitions) {
        if let Ok(Some(cached)) = satel.get_cached_partition_name(id) {
            println!("  Cached Partition #{:02}: \"{}\"", id, cached.name);
        }
    }
    for id in 1..=4.min(io_count) {
        if let Ok(Some(cached)) = satel.get_cached_zone_name(id) {
            println!("  Cached Zone      #{:03}: \"{}\"", id, cached.name);
        }
    }
    for id in 1..=4.min(io_count) {
        if let Ok(Some(cached)) = satel.get_cached_output_name(id) {
            println!("  Cached Output    #{:03}: \"{}\"", id, cached.name);
        }
    }
    println!();

    // 8. Disconnect cleanly
    println!("Disconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}
