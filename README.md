# satel_integra

[![Crates.io](https://img.shields.io/crates/v/satel_integra.svg)](https://crates.io/crates/satel_integra)
[![Documentation](https://docs.rs/satel_integra/badge.svg)](https://docs.rs/satel_integra)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)
[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)

An asynchronous, robust, and strongly-typed Rust client for **Satel Integra** alarm control panels. 

Communicates via **ETHM-1 Plus** (TCP/IP) or **UART / RS-232** (via `tokio-serial`). Designed for high-reliability home automation systems (Home Assistant, openHAB, custom daemons, IoT gateways).

---

## Features

- ⚡ **Asynchronous & Thread-Safe**: Built on top of `tokio`. The `SatelIntegra` handle is lightweight and cheaply cloneable (`Arc`-backed) across tasks and threads.
- 🌐 **Dual Transport**: Supports both **TCP/IP** (ETHM-1 / ETHM-1 Plus modules) and **Serial / UART** (`RS-232` communication at 19200/115200 baud).
- 📡 **Real-Time Event Stream**: Broadcasts live security updates via `broadcast::Receiver<SatelEvent>`:
  - Zone violations, tampers, alarms, bypasses, and faults.
  - Partition arming states (Full, Stay, Night), exit countdowns, and alarms.
  - Output state changes (Physical and Virtual relays).
  - Zone temperature readings and fault status transitions.
  - System trouble detections and RTC time sync events.
- 🔍 **Event Deduplication Control**: Fine-grained `emit_unchanged_*` flags allow fine-tuning event stream noise without compromising responsiveness.
- 🌡️ **Smart Temperature Polling**:
  - Background polling worker for zones configured as temperature sensors (e.g. ATD-100, INT-TSG/TSI/CR, etc.).
  - **Smart Error Blocking**: Automatically locks broken or missing sensors in RAM (`0ms` overhead) after $N$ consecutive failures to protect the command queue, auto-recovering when restored.
- 🚨 **100% Protocol-Accurate System Troubles**: Strongly-typed structures for all 8 trouble parts (`0x1B`..`0x1F`, `0x2C`..`0x2D`, `0x30`) with named diagnostic fields for main board power, expander fuses, ABAX wireless signal/batteries, keypads, and GSM modules.
- 🎛️ **Full Arming & Output Control**:
  - Arming partitions with modes: *Full*, *Stay*, *Stay with Delay*, plus optional *Force Arming*.
  - Disarming & alarm clearing.
  - Output switching (On, Off, Toggle, Pulsed duration).
- 📥 **Auto-Read (0x7F) Push Notification Protocol**: Supports ETHM-1 auto-notify streams with automatic state synchronization.
- 💾 **Zero-IO Local Cache**: Inspect current states (`state_handle()`) with `RwLock` without incurring network latency.

---

## Installation

Add `satel_integra` to your `Cargo.toml`:

```toml
[dependencies]
satel_integra = "1.0.1"
tokio = { version = "1", features = ["full"] }
```

---

## Quick Start

```rust
use satel_integra::{Config, ConnectionConfig, SatelIntegra};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Configure connection
    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: "192.168.1.100".to_string(),
            port: 7094,
        },
        user_code: Some("1234".to_string()),
        ..Config::default()
    };

    // 2. Initialize and connect
    let satel = SatelIntegra::new(config);
    satel.connect().await?;
    println!("Connected to Satel Integra panel!");

    // 3. Query panel model & firmware version
    let version = satel.get_integra_version().await?;
    println!("Model: {:?}, Firmware: {}", version.model, version.version);

    // 4. Query partition and zone states
    let armed = satel.get_partitions_armed_really().await?;
    println!("Armed partitions: {:?}", armed);

    let violations = satel.get_zones_violation().await?;
    println!("Violated zones (PIR): {:?}", violations);

    // 5. Clean disconnect
    satel.disconnect().await?;
    Ok(())
}
```

---

## Usage Examples

### 1. Real-Time Event Monitoring

Subscribe to the broadcast channel to receive real-time updates as they occur on the panel:

```rust
use satel_integra::{Config, ConnectionConfig, SatelEvent, SatelIntegra};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let satel = SatelIntegra::new(Config {
        connection: ConnectionConfig::Tcp {
            host: "192.168.1.100".to_string(),
            port: 7094,
        },
        ..Config::default()
    });

    // Subscribe before connecting
    let mut rx = satel.subscribe_events();

    tokio::spawn(async move {
        while let Ok(event) = rx.recv().await {
            match event {
                SatelEvent::ZoneViolation { id, violated } => {
                    println!("Zone #{:03} PIR Motion: {}", id, violated);
                }
                SatelEvent::PartitionArmed { id, mode } => {
                    println!("Partition #{:02} Armed -> Mode: {:?}", id, mode);
                }
                SatelEvent::OutputChanged { id, active } => {
                    println!("Output #{:02} State -> {}", id, if active { "ON" } else { "OFF" });
                }
                SatelEvent::ZoneTemperature { id, temperature } => {
                    println!("Zone #{:03} Temperature: {:.1}°C", id, temperature);
                }
                SatelEvent::TroubleChanged { trouble, active } => {
                    println!("System Fault: {} -> Active: {}", trouble.to_description(), active);
                }
                _ => {}
            }
        }
    });

    satel.connect().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
    Ok(())
}
```

---

### 2. Output & Partition Control

```rust
use satel_integra::{Config, ConnectionConfig, SatelIntegra};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let satel = SatelIntegra::new(Config {
        connection: ConnectionConfig::Tcp {
            host: "192.168.1.100".to_string(),
            port: 7094,
        },
        user_code: Some("1234".to_string()),
        ..Config::default()
    });

    satel.connect().await?;

    // Switch relay Output #1 ON
    satel.set_output_on(1, None).await?;

    // Pulse Output #2 for 3 seconds (e.g., garage door trigger)
    satel.set_output_pulse(2, Duration::from_secs(3), None).await?;

    // Arm Partition #1 in Stay mode with force arming enabled
    satel.arm_stay(1, true, None).await?;

    // Disarm Partition #1 and clear alarms
    satel.disarm(1, None).await?;

    Ok(())
}
```

---

### 3. Background Temperature Polling with Smart Blocking

Configure automated background temperature querying for zones assigned as temperature sensors in the Integra panel:

```rust
use satel_integra::{Config, ConnectionConfig, SatelIntegra};

let config = Config {
    connection: ConnectionConfig::Tcp {
        host: "192.168.1.100".to_string(),
        port: 7094,
    },
    // Zones configured as temperature sensors to poll automatically
    polling_temperatures_zones: vec![21, 22, 23, 24],
    polling_temperatures_interval_minutes: 1, // Poll every 1 minute
    
    // Smart error protection parameters
    polling_temp_error_threshold: 3,         // Block sensor after 3 consecutive read failures
    polling_temp_unblock_interval_minutes: 10, // Re-probe blocked sensor every 10 minutes
    
    ..Config::default()
};

let satel = SatelIntegra::new(config);
satel.connect().await?; // Background worker spawns automatically
```

---

### 4. Strongly-Typed System Troubles Diagnostic

Query detailed 1:1 hardware trouble status across the entire installation:

```rust
use satel_integra::{Config, ConnectionConfig, SatelIntegra};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let satel = SatelIntegra::new(Config {
        connection: ConnectionConfig::Tcp {
            host: "192.168.1.100".to_string(),
            port: 7094,
        },
        ..Config::default()
    });

    satel.connect().await?;

    // Query Part 1: Main Board Power, Expander Fuses, Bus Health, ETHM
    let p1 = satel.get_troubles_part1().await?;
    println!("Main Board AC Power Trouble:   {}", p1.main_board.ac_trouble);
    println!("Main Board Battery Trouble:    {}", p1.main_board.battery_trouble);
    println!("DT1/DT2 Data Bus Health:       {}", !p1.main_board.dt1_trouble && !p1.main_board.dt2_trouble);
    println!("ETHM SATEL Server Connection:  {}", !p1.ethm_ptsa.no_server_connection);

    // Query Part 3: ABAX Wireless Devices (Low battery & Lost communication)
    let p3 = satel.get_troubles_part3().await?;
    for (zone_idx, low_batt) in p3.wireless_devices_low_battery.iter().enumerate() {
        if *low_batt {
            println!("Wireless Sensor #{:03}: Low Battery!", zone_idx + 1);
        }
    }

    Ok(())
}
```

---

## Interactive Examples

The crate includes 20 comprehensive examples organized by functionality:

| Category | Example | Description |
|---|---|---|
| **Inspection (1.x)** | `1_01_get_version` | Panel model, firmware version, and ETHM-1 capabilities |
| | `1_02_get_names` | Zone, partition, and output UTF-8 names reading |
| | `1_03_get_zones_status` | Detailed zone status (violations, tampers, alarms, bypasses) |
| | `1_04_get_outputs_status` | Relay output states reading |
| | `1_05_get_partitions_status`| Partition states (armed, alarm, entry/exit time) |
| | `1_06_get_temperatures` | Direct zone temperature sensor querying |
| | `1_07_get_temperatures_smart_blocking` | Testing zone temperature sensor fault recovery and blocking |
| | `1_08_get_troubles` | Complete 8-part system diagnostic report |
| | `1_09_get_time` | Real-time clock (RTC) read |
| **Control (2.x)** | `2_01_control_outputs` | Output ON / OFF / TOGGLE / PULSE switching |
| | `2_02_arm_disarm_partitions` | Arming modes (Full, Stay, Night), disarm, clear alarms |
| | `2_03_control_time` | Panel RTC time synchronization |
| **Event Stream (3.x)** | `3_01_monitor_system_info` | System state change listener |
| | `3_02_monitor_security_states` | Security zones & partition alarms event stream |
| | `3_03_monitor_temperatures` | Temperature changes and fault stream |
| | `3_04_monitor_troubles` | Real-time hardware trouble alerts |
| | `3_05_monitor_all_events` | Complete multi-domain event monitor |
| **Push / Background (4.x)**| `4_01_auto_read_push` | ETHM-1 0x7F push notification receiver |
| | `4_02_auto_poll_temperatures` | Passive listening to background temperature worker |
| **Reference (5.x)** | `5_01_config_all` | Comprehensive reference guide for all config fields |

Run any example using `cargo`:

```bash
cargo run --example 1_08_get_troubles
```

---

## Protocol Specifications

Built according to official Satel documentation:
- *ETHM-1 Plus Protocol Description (Integration Protocol version 1.23+)*
- *INT-GSM & INT-GSM LTE Module Integration Protocol*
- *INT-RS / INT-RS Plus Integration Protocol*

---

## 🛠️ Testing & Hardware Verification

This library was written with AI assistance and tested by a human on physical hardware:
- **Verified on Hardware**: Tested with real Satel Integra panels connected via the **ETHM-1 Plus** network module (including TCP commands, auto-push `0x7F`, zone temperature readings, output switching, partition arming/disarming, and full 8-part trouble diagnostics).
- **INT-RS (Serial / RS-232) Notice**: The serial UART transport is implemented strictly according to the official Satel integration protocol specifications. However, because physical INT-RS hardware was not available during development, it has not been validated on a live INT-RS device. Feedback and test reports from users with INT-RS modules are warmly welcome!

---

## License

This project is licensed under either of:

- **MIT License** ([LICENSE-MIT](LICENSE-MIT))
- **Apache License, Version 2.0** ([LICENSE-APACHE](LICENSE-APACHE))

at your option.

