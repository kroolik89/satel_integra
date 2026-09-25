//! Example 2_04: Query real-time status of all outputs (ON / OFF) from Satel Integra.
//!
//! ============================================================================
//! 1. 2-STEP OUTPUTS STATUS WORKFLOW (NETWORK FETCH VS CACHE READ):
//! ============================================================================
//! The client operates with a 2-step data model:
//!
//! STEP 1: Network Query (Fetches all outputs state from panel & updates internal cache):
//!   Command | Async Method                        | Description
//!   --------+-------------------------------------+---------------------------------------------------
//!   0x17    | `satel.get_outputs_state().await`   | Query real-time state of all outputs (ON / OFF)
//!
//! STEP 2: Instant Cache Inspection (Synchronous, zero network overhead):
//!   - `satel.get_cached_output_name(output_id)`:
//!       Returns `Option<OutputName>` from local cache.
//!   - `satel.state_handle()`:
//!       Returns an `RwLock` read handle to entire internal `SatelState`.
//!       Access `state.outputs[0..io_count]` for individual `OutputStatus` items containing:
//!         * `id`: Output number (1..=256)
//!         * `state`: Boolean status (`true` = ON/Active, `false` = OFF/Inactive)
//!         * `state_read_at`: Timestamp of the last successful state update
//!         * `output_name`: Optional cached output name
//!
//! ============================================================================
//! 2. HARDWARE & LOGICAL OUTPUT NOTES:
//! ============================================================================
//! - In Integra panels, outputs can represent sirens, strobe lights, locks, heating
//!   valves, lighting relays, or internal logic gates used by the automation system.
//! - The number of available outputs depends on the panel model (e.g. Integra 24: 20 outputs,
//!   Integra 64: 64 outputs, Integra 128: 128 outputs, Integra 256 Plus: 256 outputs).
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 2_04_get_outputs_status
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
    println!(" SATEL INTEGRA - Query Outputs Real-Time Status");
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

    // 3. Query panel version to determine IO capacity
    let version = satel.get_integra_version().await?;
    let io_count = version.io_count;
    println!("Integra panel model: {}, supported outputs: {}\n", version.model, io_count);

    // 4. Fetch real-time state of all outputs
    println!("Fetching real-time output states from panel...");
    satel.get_outputs_state().await?;
    println!("Output states refreshed successfully.\n");

    // 5. Display ALL outputs status table
    println!("--- Outputs Status Table (1..={}) ---", io_count);
    println!("{:-<45}", "");
    println!("{: <5} | {: <14} | {: <10}", "ID", "State", "Updated At");
    println!("{:-<45}", "");

    let mut active_count = 0;
    let mut inactive_count = 0;

    // Scoped block ensures the RwLock read guard is dropped immediately after reading
    {
        let handle = satel.state_handle();
        let state = handle.read().unwrap();

        for output in &state.outputs[0..io_count as usize] {
            if output.state {
                active_count += 1;
            } else {
                inactive_count += 1;
            }

            let state_display = if output.state {
                "ON  [ACTIVE]"
            } else {
                "OFF [idle]"
            };

            let read_at_display = output
                .state_read_at
                .map(|t| t.format("%H:%M:%S").to_string())
                .unwrap_or_else(|| "-".to_string());

            println!(
                "#{:03}  | {: <14} | {: <10}",
                output.id,
                state_display,
                read_at_display
            );
        }
    }

    println!("{:-<45}", "");
    println!(
        "Summary: Active (ON): {} | Inactive (OFF): {}\n",
        active_count, inactive_count
    );

    // 6. Disconnect cleanly
    println!("Disconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}
