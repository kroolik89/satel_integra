//! Example 1_03: Connect to Satel Integra panel using encrypted communication (AES-192 ECB).
//!
//! ============================================================================
//! 1. ENCRYPTED INTEGRATION PROTOCOL OVERVIEW:
//! ============================================================================
//! The ETHM-1 Plus module supports transparent AES-192 ECB encrypted communication
//! over TCP/IP using an Integration Key configured in DLOADX:
//!
//! DLOADX Configuration:
//!   1. Open DLOADX -> Structure -> Hardware -> Modules -> ETHM-1 Plus.
//!   2. Enable "Integration (open protocol)".
//!   3. Enable "Encrypted integration".
//!   4. Set "Integration key" (up to 12 alphanumeric ASCII characters).
//!   5. Set TCP port (default: 7094).
//!
//! Client Configuration in Rust:
//!   ```rust
//!   let config = Config {
//!       connection: ConnectionConfig::Tcp {
//!           host: "192.168.1.100".to_string(),
//!           port: 7094,
//!       },
//!       encryption: true,
//!       integration_key: Some("MyKey123".to_string()),
//!       ..Config::default()
//!   };
//!   ```
//!
//! ============================================================================
//! 2. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with environment variables:
//!   cargo run --example 1_03_encrypted_connection
//!
//! Environment variables (optional):
//!   SATEL_HOST            - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT            - TCP port (default: 7094)
//!   SATEL_INTEGRATION_KEY - Integration key configured in DLOADX (default: "MyKey123")
//!   SATEL_CODE            - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelIntegra};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let integration_key = env::var("SATEL_INTEGRATION_KEY").unwrap_or_else(|_| "Jmtp".to_string());
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Encrypted Communication (AES-192)");
    println!("==================================================");
    println!("Host:            {}:{}", host, port);
    println!("Encryption:      ENABLED (AES-192 ECB)");
    println!("Integration key: {}", integration_key);
    println!("User PIN:        {}", user_code.as_deref().unwrap_or("None"));
    println!("--------------------------------------------------");

    // 1. Configure encrypted client
    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        encryption: true,
        integration_key: Some(integration_key),
        user_code,
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 2. Connect to the alarm panel over encrypted transport
    println!("Connecting over encrypted TCP socket...");
    satel.connect().await?;
    println!("Encrypted connection established successfully!\n");

    // 3. Query Integra panel version over encrypted connection
    println!("--- Querying Alarm Panel Version ---");
    let panel_version = satel.get_integra_version().await?;
    println!("Panel Model:     {}", panel_version.model);
    println!("Firmware:        {}", panel_version.firmware_version);
    println!("Language:        {}", panel_version.language);
    println!("Max I/O zones:   {}", panel_version.io_count);

    // 4. Query ETHM module version over encrypted connection
    println!("\n--- Querying ETHM-1 Module Version ---");
    let ethm_version = satel.get_ethm_version().await?;
    println!("ETHM Firmware:   {}", ethm_version.version_raw);
    println!("Capabilities:    {:?}", ethm_version.capabilities);

    println!("\n==================================================");
    println!(" Encrypted communication test completed successfully!");
    println!("==================================================");

    Ok(())
}
