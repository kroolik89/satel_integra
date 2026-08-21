//! Example 01: Connect to Satel Integra panel and query device & module versions.
//!
//! Run with default settings:
//!   cargo run --example 01_get_version
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel / ETHM-1 Plus module (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")
//!
//! Example with custom IP on Windows (PowerShell):
//!   $env:SATEL_HOST="10.20.30.5"; cargo run --example 01_get_version

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

    // 4. Query Integra alarm panel version
    println!("Querying Integra panel version...");
    match satel.get_integra_version().await {
        Ok(ver) => {
            println!("  Panel model:      {}", ver.model);
            println!("  Firmware version: {}", ver.firmware_version);
            println!("  Language:         {}", ver.language);
            println!("  I/O capacity:     {}", ver.io_count);
            println!("  Stored in FLASH:  {}", if ver.stored_in_flash { "Yes" } else { "No" });
            println!("  Read timestamp:   {}", ver.read_at.format("%Y-%m-%d %H:%M:%S"));
        }
        Err(e) => {
            eprintln!("  Error reading panel version: {}", e);
        }
    }
    println!();

    // 5. Query ETHM-1 Plus / INT-RS module version & capabilities
    println!("Querying ETHM-1 / INT-RS communication module version & capabilities...");
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

    // 6. Disconnect from panel
    println!("Disconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}
