//! Example 6_01: Complete reference guide showcasing all configuration fields in `Config`.
//!
//! ============================================================================
//! 1. CONFIGURATION SYSTEM OVERVIEW & ARCHITECTURE:
//! ============================================================================
//! The `satel_integra::Config` struct controls all operational aspects of the library,
//! categorized into 7 core architectural areas:
//!
//!   1. Connection Transport (TCP vs UART / RS-232)
//!   2. Network Timeouts & Queue TTL management
//!   3. User PIN Authentication
//!   4. Automatic Reconnection & Resilience
//!   5. Smart Temperature Error Blocking & Queue Protection
//!   6. Software Zone State Inversions (Violated <-> Normal, Tamper <-> OK)
//!   7. Hardware Auto-Push Subscriptions (ETHM-1 / INT-RS Command 0x7F)
//!   8. Background Periodic Polling (Wireless Temperature & Telemetry)
//!   9. Event Emission & Deduplication Filtering (emit_unchanged_*)
//!
//! ============================================================================
//! 2. CONFIGURATION REFERENCE TABLE & DEFAULTS:
//! ============================================================================
//! Field Name                          | Type             | Default      | Description
//! ------------------------------------+------------------+--------------+---------------------------------------------------
//! connection                          | ConnectionConfig | Tcp (7094)   | Network TCP or Serial UART transport
//! read_timeout_ms                     | u64              | 2000 ms      | Max time to wait for a standard response frame
//! write_timeout_ms                    | u64              | 500 ms       | Max time to flush a command to the socket/serial
//! temp_read_timeout_ms                | u64              | 2000 ms      | Dedicated timeout for wireless ABAX temperature
//! buffer_timeout_ms                   | u64              | 10000 ms     | Command TTL: expired commands are dropped
//! user_code                           | Option<String>   | None         | Global user PIN for control commands
//! auto_reconnect                      | bool             | true         | Automatically recover lost TCP/serial sessions
//! temp_blocking_enabled               | bool             | true         | Protect queue by blocking missing/broken sensors
//! temp_max_timeout_errors             | u32              | 4 errors     | Timeout threshold before blocking (0 ms return)
//! temp_max_sensor_errors              | u32              | 10 errors    | 0xFFFF error threshold before blocking (0 ms return)
//! io_violation_invert                 | Vec<u16>         | [] (Empty)   | List of zone IDs to logically invert violation
//! io_tamper_invert                    | Vec<u16>         | [] (Empty)   | List of zone IDs to logically invert tamper
//! io_alarm_invert                     | Vec<u16>         | [] (Empty)   | List of zone IDs to logically invert alarm
//! io_tamper_alarm_invert              | Vec<u16>         | [] (Empty)   | List of zone IDs to invert tamper alarm
//! io_alarm_memory_invert              | Vec<u16>         | [] (Empty)   | List of zone IDs to invert alarm memory
//! io_tamper_alarm_memory_invert       | Vec<u16>         | [] (Empty)   | List of zone IDs to invert tamper alarm memory
//! io_bypass_invert                    | Vec<u16>         | [] (Empty)   | List of zone IDs to invert bypass status
//! io_no_violation_trouble_invert      | Vec<u16>         | [] (Empty)   | List of zone IDs to invert no-violation trouble
//! io_long_violation_trouble_invert    | Vec<u16>         | [] (Empty)   | List of zone IDs to invert long-violation trouble
//! auto_read_zones_violation           | bool             | false        | Auto-Push: Zone movement / open states (0x00)
//! auto_read_zones_tamper              | bool             | false        | Auto-Push: Zone line/casing sabotage (0x01)
//! auto_read_zones_alarm               | bool             | false        | Auto-Push: Zone active alarms (0x02)
//! auto_read_zones_tamper_alarm        | bool             | false        | Auto-Push: Zone tamper alarms (0x03)
//! auto_read_zones_alarm_memory        | bool             | false        | Auto-Push: Zone alarm memory (0x04)
//! auto_read_zones_tamper_alarm_memory | bool             | false        | Auto-Push: Zone tamper alarm memory (0x05)
//! auto_read_zones_bypass              | bool             | false        | Auto-Push: Zone bypass status (0x06)
//! auto_read_zones_no_violation_trouble| bool             | false        | Auto-Push: Zone 'no violation' trouble (0x07)
//! auto_read_zones_long_violation_tr...| bool             | false        | Auto-Push: Zone 'long violation' trouble (0x08)
//! auto_read_partitions_armed_suppr... | bool             | false        | Auto-Push: Partitions armed suppressed (0x09)
//! auto_read_partitions_armed_really  | bool             | false        | Auto-Push: Partitions armed really (0x0A)
//! auto_read_partitions_alarm         | bool             | false        | Auto-Push: Partitions active alarms (0x13)
//! auto_read_partitions_alarm_memory  | bool             | false        | Auto-Push: Partitions alarm memory (0x15)
//! auto_read_partitions_entry_time    | bool             | false        | Auto-Push: Partitions entry delay time (0x0E)
//! auto_read_partitions_exit_time     | bool             | false        | Auto-Push: Partitions exit delay time (0x0F, 0x10)
//! auto_read_outputs_state            | bool             | false        | Auto-Push: Outputs / relays ON/OFF (0x17)
//! auto_read_system_troubles          | bool             | false        | Auto-Push: Active troubles parts 1..8 (0x1B-0x30)
//! auto_read_troubles_memory          | bool             | false        | Auto-Push: Trouble memory parts 1..8 (0x20-0x31)
//! polling_temperatures               | bool             | false        | Enable background cyclic polling for temperatures
//! polling_temperatures_zones         | Vec<u16>         | [] (Empty)   | Background cyclic polling: Zone IDs with temp sensors
//! polling_temperatures_interval_m... | u64              | 1 minute     | Background cyclic polling: Interval in minutes (min: 1)
//! emit_unchanged_temperatures        | bool             | false        | Emit temp event on every read even if unchanged
//! emit_unchanged_zones               | bool             | false        | Emit zone events on every read even if unchanged
//! emit_unchanged_outputs             | bool             | false        | Emit output events on every read even if unchanged
//! emit_unchanged_partitions          | bool             | false        | Emit partition events on every read even if unchanged
//! emit_unchanged_troubles            | bool             | false        | Emit trouble events on every read even if unchanged
//! emit_unchanged_system_status       | bool             | false        | Emit system status on every read even if unchanged
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 6_01_config_all
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelEvent, SatelIntegra, config::TemperatureProbe};
use std::env;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Read environment overrides
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Complete Configuration Guide (5_01)");
    println!("==================================================");

    // ========================================================================
    // COMPLETE CONFIGURATION STRUCT (ALL FIELDS EXPLICITLY POPULATED)
    // ========================================================================
    let config = Config {
        // --------------------------------------------------------------------
        // 1. Connection Transport & Security
        // --------------------------------------------------------------------
        // TCP Connection (ETHM-1 / ETHM-1 Plus modules)
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        // Alternatively, for Serial RS-232 / INT-RS hardware modules:
        // connection: ConnectionConfig::Uart {
        //     path: "COM3".to_string(), // or "/dev/ttyUSB0" on Linux
        //     baud_rate: 19200,         // Standard Satel INT-RS baud rate
        // },

        // Enable AES-192 ECB encrypted communication with ETHM-1 Plus. Default: false.
        // Requires setting `integration_key` (up to 12 ASCII characters).
        encryption: false,

        // Integration encryption key configured in DLOADX (Structure -> Modules -> ETHM-1).
        // Required when `encryption = true`. Stored in memory in plaintext.
        integration_key: None,

        // --------------------------------------------------------------------
        // 2. Network Timeouts & Queue Expiry
        // --------------------------------------------------------------------
        // Maximum time (in ms) to wait for a standard response frame from the panel.
        // Default: 2000 ms. Increase for slow GSM/cellular connections.
        read_timeout_ms: 2000,

        // Maximum time (in ms) to flush a request frame into the socket buffer.
        // Default: 500 ms.
        write_timeout_ms: 500,

        // Dedicated timeout (in ms) for wireless ABAX temperature sensor queries.
        // Default: 2000 ms.
        temp_read_timeout_ms: 2000,

        // Message TTL (in ms) in the buffer queue. If an outgoing command spends
        // more than this duration waiting in queue due to connection delays,
        // it expires and returns `Err(SatelError::MessageExpired)` rather than
        // performing obsolete actions later. Default: 10000 ms.
        buffer_timeout_ms: 10000,

        // --------------------------------------------------------------------
        // 3. User PIN Authentication
        // --------------------------------------------------------------------
        // Global 4-8 digit user access code for output control, arming, and clock sync.
        // When set, passing `None` to control methods automatically resolves to this PIN.
        user_code: user_code.clone(),

        // --------------------------------------------------------------------
        // 4. Resilience & Reconnection
        // --------------------------------------------------------------------
        // When true, the client automatically attempts to reconnect upon unexpected
        // socket termination or network drops with exponential backoff. Default: true.
        auto_reconnect: true,

        // --------------------------------------------------------------------
        // 5. Smart Temperature Error Blocking
        // --------------------------------------------------------------------
        // Protects the communication queue from being blocked by non-existent,
        // broken, or battery-depleted wireless temperature sensors. Default: true.
        temp_blocking_enabled: true,

        // Consecutive timeout threshold before a missing sensor is blocked.
        // Subsequent queries return `Err(SatelError::TempTooManyErrors)` instantly (0 ms).
        // Default: 4.
        temp_max_timeout_errors: 4,

        // Consecutive 0xFFFF error threshold before a broken sensor is blocked.
        // Default: 10.
        temp_max_sensor_errors: 10,

        // --------------------------------------------------------------------
        // 6. Software Zone State Inversions (Violated <-> Normal, Tamper <-> OK)
        // --------------------------------------------------------------------
        // Zone IDs (1..256) where physical violation state should be inverted (`!state`).
        io_violation_invert: vec![1, 2],

        // Zone IDs (1..256) where tamper state should be inverted.
        io_tamper_invert: Vec::new(),

        // Zone IDs (1..256) where alarm state should be inverted.
        io_alarm_invert: Vec::new(),

        // Zone IDs (1..256) where tamper alarm state should be inverted.
        io_tamper_alarm_invert: Vec::new(),

        // Zone IDs (1..256) where alarm memory state should be inverted.
        io_alarm_memory_invert: Vec::new(),

        // Zone IDs (1..256) where tamper alarm memory state should be inverted.
        io_tamper_alarm_memory_invert: Vec::new(),

        // Zone IDs (1..256) where bypass state should be inverted.
        io_bypass_invert: Vec::new(),

        // Zone IDs (1..256) where 'no violation' trouble state should be inverted.
        io_no_violation_trouble_invert: Vec::new(),

        // Zone IDs (1..256) where 'long violation' trouble state should be inverted.
        io_long_violation_trouble_invert: Vec::new(),

        // --------------------------------------------------------------------
        // 7. Hardware Auto-Push Subscriptions (ETHM-1 Command 0x7F)
        // --------------------------------------------------------------------
        // Automatically stream zone movement / open state changes (0x00).
        auto_read_zones_violation: true,

        // Automatically stream zone line/casing sabotage changes (0x01).
        auto_read_zones_tamper: true,

        // Automatically stream zone active alarms (0x02).
        auto_read_zones_alarm: true,

        // Automatically stream zone tamper alarms (0x03).
        auto_read_zones_tamper_alarm: true,

        // Automatically stream zone alarm memory changes (0x04).
        auto_read_zones_alarm_memory: true,

        // Automatically stream zone tamper alarm memory changes (0x05).
        auto_read_zones_tamper_alarm_memory: true,

        // Automatically stream zone bypass / unbypass state changes (0x06).
        auto_read_zones_bypass: true,

        // Automatically stream zone 'no violation' trouble changes (0x07).
        auto_read_zones_no_violation_trouble: true,

        // Automatically stream zone 'long violation' trouble changes (0x08).
        auto_read_zones_long_violation_trouble: true,

        // Automatically stream partition suppressed arming state (0x09).
        auto_read_partitions_armed_suppressed: true,

        // Automatically stream partition real physical arming state (0x0A).
        auto_read_partitions_armed_really: true,

        // Automatically stream partition active alarms (0x13).
        auto_read_partitions_alarm: true,

        // Automatically stream partition alarm memory (0x15).
        auto_read_partitions_alarm_memory: true,

        // Automatically stream partition entry delay countdown (0x0E).
        auto_read_partitions_entry_time: true,

        // Automatically stream partition exit delay countdown (0x0F, 0x10).
        auto_read_partitions_exit_time: true,

        // Automatically stream output relay ON / OFF transitions (0x17).
        auto_read_outputs_state: true,

        // Automatically stream active system troubles across parts 1..8 (0x1B-0x30).
        auto_read_system_troubles: true,

        // Automatically stream trouble memory changes across parts 1..8 (0x20-0x31).
        auto_read_troubles_memory: true,

        // --------------------------------------------------------------------
        // 8. Background Periodic Polling (Wireless Temperature & Telemetry)
        // --------------------------------------------------------------------
        temperature_probes: vec![
            TemperatureProbe {
                zone_id: 21,
                interval_minutes: 1,
                max_timeout_errors: 3,
                max_sensor_errors: 3,
                unblock_enabled: true,
                unblock_after_cycles: 10,
            },
            TemperatureProbe {
                zone_id: 22,
                interval_minutes: 1,
                max_timeout_errors: 3,
                max_sensor_errors: 3,
                unblock_enabled: true,
                unblock_after_cycles: 10,
            },
            TemperatureProbe {
                zone_id: 23,
                interval_minutes: 1,
                max_timeout_errors: 3,
                max_sensor_errors: 3,
                unblock_enabled: true,
                unblock_after_cycles: 10,
            },
            TemperatureProbe {
                zone_id: 24,
                interval_minutes: 1,
                max_timeout_errors: 3,
                max_sensor_errors: 3,
                unblock_enabled: true,
                unblock_after_cycles: 10,
            },
        ],

        // --------------------------------------------------------------------
        // 9. Event Emission & Deduplication Filtering (emit_unchanged_*)
        // --------------------------------------------------------------------
        // Emit temperature events on every query even if value is unchanged. Default: false.
        emit_unchanged_temperatures: true,

        // Emit zone events on every read even if state is unchanged. Default: false.
        emit_unchanged_zones: false,

        // Emit output events on every read even if state is unchanged. Default: false.
        emit_unchanged_outputs: false,

        // Emit partition events on every read even if state is unchanged. Default: false.
        emit_unchanged_partitions: false,

        // Emit trouble events on every read even if state is unchanged. Default: false.
        emit_unchanged_troubles: false,

        // Emit system status events on every read even if state is unchanged. Default: false.
        emit_unchanged_system_status: false,

        // --------------------------------------------------------------------
        // 10. Extended Name & Parameter Reading (ETHM-1 Command 0xEE)
        // --------------------------------------------------------------------
        // Read device parameters (reaction types, output functions & durations, partition options)
        // alongside UTF-8 names using extended query types (5, 17, 19). Default: true.
        extended_name_read: true,
    };

    println!("Config initialized successfully.");
    println!("  - Target Transport:    {}:{}", host, port);
    println!("  - Read Timeout:        {} ms", config.read_timeout_ms);
    println!("  - Write Timeout:       {} ms", config.write_timeout_ms);
    println!("  - Buffer Queue TTL:    {} ms", config.buffer_timeout_ms);
    println!("  - Auto Reconnect:      {}", config.auto_reconnect);
    println!("  - Temp Smart Blocking: {}", config.temp_blocking_enabled);
    println!("  - Violated Inversions: {:?}", config.io_violation_invert);
    println!("  - Auto-Push Enabled:   {}", config.is_auto_read_enabled());
    println!("  - Polling Temp Active: {}", config.is_polling_enabled());
    println!("  - Temperature Probes:  {}", config.temperature_probes.len());
    println!("  - Emit Unchanged Temp: {}", config.emit_unchanged_temperatures);
    println!("  - Emit Unchanged Zones:{}", config.emit_unchanged_zones);
    println!("  - Emit Unchanged Out:  {}\n", config.emit_unchanged_outputs);

    let satel = SatelIntegra::new(config);

    // Subscribe to event stream before connect
    let mut rx = satel.subscribe();

    let listener_handle = tokio::spawn(async move {
        while let Ok(event) = rx.recv().await {
            if let SatelEvent::AutoReadConfigured(report) = event {
                println!("--- Auto-Push Hardware Registration Report ---");
                println!("Accepted by ETHM: {}/{} items active", report.success_count, report.total_requested);
                for item in &report.items {
                    println!("  * {:<35} -> {:?}", item.name, item.state);
                }
                println!("----------------------------------------------\n");
            }
        }
    });

    // Connect
    println!("Connecting to the panel...");
    satel.connect().await?;
    println!("Connected successfully!\n");

    // Wait a moment for handshake report
    sleep(Duration::from_secs(2)).await;

    // Display Connection Telemetry
    {
        let stats = satel.statistics();
        println!("Connection Telemetry Summary:");
        println!("  - Current State:    {:?}", stats.state);
        println!("  - Bytes Sent:       {}", stats.bytes_sent);
        println!("  - Bytes Received:   {}", stats.bytes_received);
        println!("  - Connections Est:  {}", stats.connections_established);
        println!("  - Reconnect Att:    {}", stats.reconnect_attempts);
        println!("  - Connections Lost: {}", stats.connections_lost);
        println!("  - Timeouts:         {}", stats.timeouts);
        println!("  - CRC Errors:       {}", stats.crc_errors);
        println!("  - Rejected By Panel:{}", stats.rejected_by_panel);
        println!("  - IO Errors:        {}", stats.io_errors);
        println!("  - Total Connected:  {:?}", stats.total_connected);
    }

    // Disconnect cleanly
    println!("\nDisconnecting...");
    satel.disconnect().await?;
    sleep(Duration::from_millis(500)).await;

    listener_handle.abort();
    println!("Disconnected cleanly. Done.");

    Ok(())
}
