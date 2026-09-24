//! Example 2_10: Extended query of names and detailed hardware parameters for zones, outputs, and partitions.
//!
//! ============================================================================
//! EXTENDED PARAMETER READING (Command 0xEE):
//! ============================================================================
//! When `extended_name_read: true` (default in 1.9.0+), 0xEE queries retrieve both
//! device names and internal operational parameters:
//!   - Zones: Reaction type (0..97), partition assignment (1..32)
//!   - Outputs: Function (0..123), operating duration (×0.1s), controllability & mode (timed/bistable)
//!   - Partitions: Type (0..3), object assignment, options bitmask, auto-arm defer timer, dependent partitions
//!
//! Run with:
//!   cargo run --example 2_10_get_extended_names_and_params
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, OutputControl, SatelIntegra};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("================================================================================");
    println!(" SATEL INTEGRA - Extended Names, Device Parameters & Type Catalogs");
    println!("================================================================================");

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,
        extended_name_read: true,
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    println!("Connecting to Integra panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    let version = satel.get_integra_version().await?;
    let io_count = version.io_count;
    let partition_count = version.partition_count;

    println!(
        "Integra Model: {} | Max I/O: {} | Partitions: {}\n",
        version.model, io_count, partition_count
    );

    // =========================================================================
    // 1. PARTITIONS
    // =========================================================================
    println!("=== 1. PARTITIONS (STREFY: 1..={}) ===", partition_count);
    for part_id in 1..=partition_count {
        let name_res = satel.get_partition_name(part_id).await;
        let params_res = satel.get_cached_partition_params(part_id)?;

        match (name_res, params_res) {
            (Ok(name), Some(params)) => {
                if name.name.trim().is_empty() {
                    println!("  #{:02} [Unassigned]", part_id);
                } else {
                    let type_code = params.partition_type.code();
                    let type_label = params.partition_type.label_en();
                    let type_key = params.partition_type.key();

                    let obj_str = params
                        .object_number
                        .map(|o| format!("Object: {}", o))
                        .unwrap_or_else(|| "Object: none".to_string());

                    let dep_str = params
                        .dependent_partitions
                        .map(|d| format!("Dependent: {:?}", d.list()))
                        .unwrap_or_else(|| "Dependent: none".to_string());

                    let defer_str = params
                        .auto_arm_defer
                        .map(|def| format!("AutoArmDefer: {:?} ({}s)", def.status, def.defer_time_s))
                        .unwrap_or_else(|| "AutoArmDefer: none".to_string());

                    println!(
                        "  #{:02} \"{}\" | Code: {} ({}) [{}] | {}, {}, {}",
                        part_id, name.name, type_code, type_label, type_key, obj_str, dep_str, defer_str
                    );
                }
            }
            (Ok(name), None) => {
                if name.name.trim().is_empty() {
                    println!("  #{:02} [Unassigned]", part_id);
                } else {
                    println!("  #{:02} \"{}\" [Params not available]", part_id, name.name);
                }
            }
            (Err(e), _) => eprintln!("  #{:02} Error: {}", part_id, e),
        }
    }
    println!();

    // =========================================================================
    // 2. ZONES (INPUTS / WEJŚCIA)
    // =========================================================================
    println!("=== 2. ZONES / INPUTS (WEJŚCIA: 1..={}) ===", io_count);
    for zone_id in 1..=io_count {
        let name_res = satel.get_zone_name(zone_id).await;
        let params_res = satel.get_cached_zone_params(zone_id)?;

        match (name_res, params_res) {
            (Ok(name), Some(params)) => {
                if name.name.trim().is_empty() {
                    println!("  #{:03} [Unassigned]", zone_id);
                } else {
                    let reaction_code = params.reaction.code();
                    let reaction_label = params.reaction.label_en();
                    let reaction_key = params.reaction.key();
                    let kind = params.reaction.kind();
                    let part_str = params
                        .partition
                        .map(|p| format!("Partition: {}", p))
                        .unwrap_or_else(|| "Partition: none".to_string());

                    println!(
                        "  #{:03} \"{}\" | Code: {:2} ({}) [{}] | Kind: {:?} | {}",
                        zone_id, name.name, reaction_code, reaction_label, reaction_key, kind, part_str
                    );
                }
            }
            (Ok(name), None) => {
                if name.name.trim().is_empty() {
                    println!("  #{:03} [Unassigned]", zone_id);
                } else {
                    println!("  #{:03} \"{}\" [Params not available]", zone_id, name.name);
                }
            }
            (Err(e), _) => eprintln!("  #{:03} Error: {}", zone_id, e),
        }
    }
    println!();

    // =========================================================================
    // 3. OUTPUTS (WYJŚCIA)
    // =========================================================================
    println!("=== 3. OUTPUTS (WYJŚCIA: 1..={}) ===", io_count);
    for out_id in 1..=io_count {
        let name_res = satel.get_output_name(out_id).await;
        let params_res = satel.get_cached_output_params(out_id)?;

        match (name_res, params_res) {
            (Ok(name), Some(params)) => {
                if name.name.trim().is_empty() {
                    println!("  #{:03} [Unassigned]", out_id);
                } else {
                    let func_code = params.function.code();
                    let func_label = params.function.label_en();
                    let func_key = params.function.key();
                    let controllable = if params.function.is_controllable() {
                        "Controllable (YES)"
                    } else {
                        "Controllable (NO)"
                    };

                    let (mode_str, duration_str) = match &params.control {
                        OutputControl::Timed { duration } => {
                            let dur = duration
                                .map(|d| format!("{:.1}s", d.as_secs_f64()))
                                .unwrap_or_else(|| "unknown".to_string());
                            ("Timed (Czasowe)", dur)
                        }
                        OutputControl::Bistable => ("Bistable (Bistabilne)", "n/a".to_string()),
                        OutputControl::Unknown => ("Unknown (Nieznane)", "none".to_string()),
                        OutputControl::None => ("None (Brak sterowania)", "none".to_string()),
                    };

                    println!(
                        "  #{:03} \"{}\" | Code: {:3} ({}) [{}] | {} | Mode: {} | Duration: {}",
                        out_id, name.name, func_code, func_label, func_key, controllable, mode_str, duration_str
                    );
                }
            }
            (Ok(name), None) => {
                if name.name.trim().is_empty() {
                    println!("  #{:03} [Unassigned]", out_id);
                } else {
                    println!("  #{:03} \"{}\" [Params not available]", out_id, name.name);
                }
            }
            (Err(e), _) => eprintln!("  #{:03} Error: {}", out_id, e),
        }
    }

    println!("\nExtended query finished successfully.");
    Ok(())
}
