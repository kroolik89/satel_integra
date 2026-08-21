//! Example 02: Dynamically query ALL names of partitions, zones, and outputs based on panel capacity.
//!
//! Run with default settings:
//!   cargo run --example 02_get_names
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

    // 3. Query panel version to determine the exact number of supported zones, outputs, and partitions
    let version = satel.get_integra_version().await?;
    let io_count = version.io_count;
    let max_partitions = match io_count {
        24 => 4,
        32 | 64 => 8,
        128 => 16,
        _ => 32,
    };

    println!(
        "Integra panel has {} zones, {} outputs, and {} partitions.\n",
        io_count, io_count, max_partitions
    );

    // 4. Query Partition Names (1..=max_partitions)
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

    // 5. Query ALL Zone Names (1..=io_count)
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

    // 6. Query ALL Output Names (1..=io_count)
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

    // 7. Inspect Local Cache
    println!("--- Inspecting Local Cache ---");
    println!("Checking cached zones in memory (first 16 as quick check):");
    for zone_id in 1..=16.min(io_count) {
        match satel.get_cached_zone_name(zone_id) {
            Ok(Some(cached)) => {
                if cached.name.trim().is_empty() {
                    println!(
                        "  Cached Zone #{:03}: [Empty / Unassigned] (read at: {})",
                        zone_id,
                        cached.read_at.format("%H:%M:%S")
                    );
                } else {
                    println!(
                        "  Cached Zone #{:03}: \"{}\" (read at: {})",
                        zone_id,
                        cached.name,
                        cached.read_at.format("%H:%M:%S")
                    );
                }
            }
            Ok(None) => {
                println!("  Cached Zone #{:03}: [Not in cache]", zone_id);
            }
            Err(e) => eprintln!("  Cached Zone #{:03}: Cache error ({})", zone_id, e),
        }
    }

    // 8. Disconnect cleanly
    println!("\nDisconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}
