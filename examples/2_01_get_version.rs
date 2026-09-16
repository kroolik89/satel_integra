//! Example 2_01: Connect to Satel Integra panel and query device & module versions.
//!
//! ============================================================================
//! 1. 2-STEP VERSION WORKFLOW (NETWORK FETCH VS CACHE READ):
//! ============================================================================
//! The client operates with a 2-step data model:
//!
//! STEP 1: Network Queries (Fetches version frames from panel & updates internal cache):
//!   Command | Async Method                        | Description
//!   --------+-------------------------------------+---------------------------------------------------
//!   0x7E    | `satel.get_integra_version().await` | Query Integra alarm panel version, model, I/O capacity
//!   0x7C    | `satel.get_ethm_version().await`    | Query ETHM-1 / INT-RS communication module version & caps
//!
//! STEP 2: Instant Cache Inspection (Synchronous, zero network overhead):
//!   - `satel.get_cached_version()`:
//!       Returns `Option<IntegraVersion>` from local memory cache without network latency.
//!   - `satel.state_handle()`:
//!       Access `state.integra_version` and `state.ethm_version` directly via `RwLock`.
//!
//! ============================================================================
//! 2. HARDWARE & CAPABILITIES NOTES:
//! ============================================================================
//! - `IntegraVersion` fields:
//!     * `model`: Derived panel model name (e.g. "INTEGRA 128", "INTEGRA 256 Plus").
//!     * `firmware_version`: Firmware release string (e.g. "1.22 2023-05-10").
//!     * `language`: Configured panel language.
//!     * `io_count`: Maximum supported inputs/outputs (24, 32, 64, 128, 256).
//!     * `partition_count`: Maximum supported partitions (4, 16, 32).
//!     * `stored_in_flash`: Whether the firmware runs from FLASH memory.
//! - `EthmCapabilities` features:
//!     * `support_32_byte_frames`: Module supports 256 I/O bitmasks (32-byte frames).
//!     * `support_8_troubles_groups`: Module supports expanded 8 trouble groups (0x7F).
//!     * `support_extended_arming_commands`: Module supports mode-specific partition arming.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 2_01_get_version
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel / ETHM-1 Plus module (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelIntegra};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Read connection parameters from environment variables (or fall back to defaults)
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Query Panel & Module Versions");
    println!("==================================================");
    println!("Connection parameters:");
    println!("  Host:      {}", host);
    println!("  Port:      {}", port);
    println!("  User code: {:?}", user_code);
    println!("--------------------------------------------------");

    // 2. Configure client
    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 3. Connect to the alarm panel
    println!("Connecting to the control panel...");
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 4. STEP 1: Query Integra alarm panel version over the network (0x7E)
    println!("--- Step 1: Querying Versions Over Network ---");
    println!("Querying Integra panel version (0x7E)...");
    match satel.get_integra_version().await {
        Ok(ver) => {
            println!("  Panel model:      {}", ver.model);
            println!("  Firmware version: {}", ver.firmware_version);
            println!("  Language:         {}", ver.language);
            println!("  I/O capacity:     {}", ver.io_count);
            println!("  Partition count:  {}", ver.partition_count);
            println!("  Stored in FLASH:  {}", if ver.stored_in_flash { "Yes" } else { "No" });
            println!("  Read timestamp:   {}", ver.read_at.format("%Y-%m-%d %H:%M:%S"));
        }
        Err(e) => {
            eprintln!("  Error reading panel version: {}", e);
        }
    }
    println!();

    // Query ETHM-1 Plus / INT-RS module version & capabilities (0x7C)
    println!("Querying ETHM-1 / INT-RS communication module version & capabilities (0x7C)...");
    match satel.get_ethm_version().await {
        Ok(ethm) => {
            println!("  Module version:             {}", ethm.version_raw);
            println!("  Capabilities:");
            println!("    - 32-byte frames (256 IO): {}", if ethm.capabilities.support_32_byte_frames { "Supported" } else { "No" });
            println!("    - 8 trouble groups:       {}", if ethm.capabilities.support_8_troubles_groups { "Supported" } else { "No" });
            println!("    - Extended arming commands: {}", if ethm.capabilities.support_extended_arming_commands { "Supported" } else { "No" });
            println!("  Read timestamp:             {}", ethm.read_at.format("%Y-%m-%d %H:%M:%S"));
        }
        Err(e) => {
            eprintln!("  Error reading module version: {}", e);
        }
    }
    println!();

    // 5. STEP 2: Instant Cache Read (Zero Network I/O)
    println!("--- Step 2: Instant Cache Verification (Zero Network I/O) ---");
    if let Ok(Some(cached_ver)) = satel.get_cached_version() {
        println!(
            "  Cached Panel: {} (Firmware: {}, Read at: {})",
            cached_ver.model,
            cached_ver.firmware_version,
            cached_ver.read_at.format("%H:%M:%S")
        );
    }
    println!();

    // 6. Disconnect from panel
    println!("Disconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}
