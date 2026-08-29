//! Example: Discover all temperature sensors configured in Satel Integra panel.
//!
//! Demonstrates:
//! 1. 3-phase batch synchronization events (`SyncStarted`, `SyncProgress`, `SyncFinished`) with `SyncCategory::Temperatures`.
//! 2. Live event reception via `subscribe_events()`.
//! 3. Fast sensor discovery across all panel zones with configurable per-probe timeout (minimum recommended: 500ms).
//! 4. Comprehensive error discrimination (Working sensor vs Probe 0xFFFF error vs Timeout / No sensor).
//!
//! Run with:
//!   cargo run --example 8_various_tests
//!
//! Environment variables (optional):
//!   SATEL_HOST         - IP address (default: "192.168.1.100")
//!   SATEL_PORT         - TCP port (default: 7094)
//!   SATEL_CODE         - User access code (default: "1234")
//!   PROBE_TIMEOUT_MS   - Probe timeout per zone in ms (default: 500)

use satel_integra::{
    Config, ConnectionConfig, SatelEvent, SatelIntegra, SyncCategory, TemperatureSensorStatus,
};
use std::env;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));
    let probe_timeout_ms: u64 = env::var("PROBE_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500);

    tracing::info!("Initializing Satel Integra discovery at {}:{} (probe timeout: {} ms)", host, port, probe_timeout_ms);

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,
        temp_blocking_enabled: false, // Ensure raw discovery touches every zone
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 1. Subscribe to live events before connecting
    let mut event_rx = satel.subscribe_events();
    let event_logger = tokio::spawn(async move {
        while let Ok(event) = event_rx.recv().await {
            match event {
                SatelEvent::SyncStarted { category: SyncCategory::Temperatures, total } => {
                    tracing::info!("Temperature discovery started for {} zones", total);
                }
                SatelEvent::ZoneTemperatureChanged { id, temperature } => {
                    tracing::info!("Zone #{:03}: Valid reading {:.1} °C", id, temperature);
                }
                SatelEvent::SyncProgress { category: SyncCategory::Temperatures, current, total, name } => {
                    let status_str = if name.is_empty() {
                        "No response (Timeout)".to_string()
                    } else {
                        name
                    };
                    tracing::debug!("Discovery progress [{:03}/{:03}] Zone #{:03}: {}", current, total, current, status_str);
                }
                SatelEvent::SyncFinished { category: SyncCategory::Temperatures, total, success_count, error } => {
                    tracing::info!("Discovery completed: {}/{} sensors responding. Error: {:?}", 
                        success_count, total, error);
                }
                _ => {}
            }
        }
    });

    // 2. Connect to panel
    tracing::info!("Connecting to the panel...");
    satel.connect().await?;
    tracing::info!("Connected! Integra panel initialized.");

    // 3. Run full discovery
    let start = Instant::now();
    tracing::info!("Launching get_all_zone_temperatures (timeout: {} ms)...", probe_timeout_ms);

    satel.get_all_zone_temperatures(Some(Duration::from_millis(probe_timeout_ms))).await?;

    let elapsed = start.elapsed();

    // 4. Summarize diagnostic results from internal cache
    let total_zones = satel
        .get_cached_version()?
        .map(|v| v.io_count)
        .unwrap_or(128);

    let state_handle = satel.state_handle();
    let state = state_handle.read().map_err(|e| format!("State read error: {:?}", e))?;

    let mut working_count = 0;
    let mut probe_error_count = 0;
    let mut no_sensor_count = 0;

    for zone_id in 1..=total_zones {
        if let Some(zone) = state.zones.get((zone_id.wrapping_sub(1) % 256) as usize) {
            match zone.temperature_status {
                TemperatureSensorStatus::Ok => {
                    working_count += 1;
                    tracing::info!(
                        "Zone #{:03}: Active & OK ({:.1} °C)",
                        zone_id,
                        zone.temperature_value
                    );
                }
                TemperatureSensorStatus::CommunicationError | TemperatureSensorStatus::BlockCommunicationError => {
                    probe_error_count += 1;
                    tracing::warn!(
                        "Zone #{:03}: Probe hardware error (0xFFFF)",
                        zone_id
                    );
                }
                _ => {
                    no_sensor_count += 1;
                }
            }
        } else {
            no_sensor_count += 1;
        }
    }

    tracing::info!(
        "Discovery summary (Scan time: {:?}): OK: {}, Probe errors: {}, No sensor: {}, Total zones: {}",
        elapsed, working_count, probe_error_count, no_sensor_count, total_zones
    );

    // 5. Clean disconnect
    event_logger.abort();
    satel.disconnect().await?;
    tracing::info!("Disconnected cleanly.");

    Ok(())
}
