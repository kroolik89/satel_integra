# satel_integra

**Asynchronous Rust client for communicating with Satel Integra alarm control panels.**

The library enables full integration and control of the security system via ETHM-1 Plus (TCP/IP) modules and direct UART (RS-232/INT-RS) connections. It was created for embedded systems and building automation, built on the highly efficient, non-blocking `tokio` architecture. It relieves the developer from the most difficult tasks related to hardware protocols, allowing them to focus on the business application logic.

## 🛠 Key Features

*   **Automatic Connection Recovery (Auto-Reconnect):** A built-in *watchdog* mechanism actively monitors session stability. In the event of a connection drop or network errors, the client automatically attempts to reconnect using an *exponential backoff* strategy. This guarantees automatic communication recovery without the risk of "flooding" the network with too many requests.
*   **Two communication modes:** Support for network (TCP) and serial (UART/RS-232) connections.
*   **Native encryption:** Built-in support for AES-192 (ECB) hardware encryption, required by newer ETHM-1 Plus firmware.
*   **Event-Driven Architecture (Auto-Push):** Uses hardware Push notifications (command 0x7F configuring auto-read) to asynchronously receive state changes directly from the control panel. This completely eliminates the need for active polling and drastically reduces the bus load.
*   **Thread-safe Cache:** The entire system state (zones, inputs, outputs, troubles, RTC) is continuously updated and stored in an internal cache, fully secured by `RwLock` primitives. This allows synchronous, instant reads of any parameters by multiple threads simultaneously, without network latency.
*   **Smart Sensor Blocking:** A built-in self-diagnostic and query queue protection system (e.g., during failures of ABAX 2 wireless temperature sensors). Damaged probes are temporarily isolated to prevent blocking communication with the panel (Timeouts).
*   **Full system control:** Native API enabling arming and disarming partitions (including night modes and zero exit delay), output control (ON/OFF/TOGGLE), clearing alarm memory, and RTC system time synchronization.
*   **Flexible authorization management (PIN):** The ability to define a global PIN code for background operations (e.g., automations), as well as dynamic authorization of single commands with individual user passwords (ideal for web/mobile apps).
*   **Extended Device Parameters & Type Catalogs (1.9.0+):** Command 0xEE reads not only object names but also internal reaction types (`ZoneReaction`), output functions (`OutputFunction`), control modes (`OutputControl`), and partition parameters (`PartitionType`, `PartitionOptions`, `DependentPartitions`). An automatic fallback mechanism transparently handles older panel firmwares without failing sync scans.
*   **Software state inversion:** A unique option to configure logical inversion of readings (e.g., tamper, violation, troubles) at the client level, facilitating integration with unusually wired sensors without the need to change DLOADX settings.
*   **One-Time Full State Reads (`refresh_*`, v1.10.0+):** Dedicated `refresh_*` methods (`refresh_outputs_state`, `refresh_zones_violation`, `refresh_partitions_alarm`, etc.) execute the corresponding panel query, update the cache, and emit a `SatelEvent` for **every** position (even unchanged), exactly once. Unlike `get_*` (which emits only changed items), `refresh_*` guarantees a full state broadcast strictly for that single query response, without any persistent flags or side-effects on subsequent push or poll frames.
*   **Initial State Broadcast via `Option` Timestamps (v1.11.0+):** State read timestamps (`state_read_at`, `violation_read_at`, `alarm_at`, etc.) are typed as `Option<DateTime<Local>>` (initialized to `None`). The very first frame received after startup emits `SatelEvent` broadcasts for all positions (even inactive `false` states), ensuring consumers receive the full baseline without extra commands. Timestamps are retained across reconnections to emit only deltas after network drops.

## 🏷️ Extended Parameter Discovery & Catalogs (v1.9.0)

When `extended_name_read: true` (default), the client queries device parameters alongside UTF-8 names:
* **Zones (Inputs):** Reaction type (0..97) with `ZoneKind` categorization and partition assignment.
* **Outputs:** Function (0..123) with controllability assessment (`is_controllable()`), duration, and mode (`Timed`, `Bistable`, `Unknown`, `None`).
* **Partitions:** Partition type (0..3), object assignment, options bitmask, auto-arm defer timer, and dependent partitions mask.

If the connected panel firmware rejects extended frame queries, the client automatically downgrades to basic query types (5 → 1 for zones, 17 → 4 for outputs, 19 → 18 → 16 → 0 for partitions) and caches the highest supported level for the session.

See `examples/2_10_get_extended_names_and_params.rs` for a full demonstration.

## 🏗 Architecture

The library is divided into logical, independent layers running in the background:
1.  **SatelCommunicationWorker:** The main actor managing the connection (TCP/UART), reconnects, authorization, and byte stream encryption.
2.  **SatelAutoRequester:** An actor processing incoming notification frames (Push) and updating the shared state (Cache) in memory.
3.  **SatelPollingWorker:** An optional mechanism for scheduled, cyclical polling for data that does not support Push notifications (e.g., temperature readings from command 0x7D).
4.  **Event Broadcast:** An event stream based on `tokio::sync::broadcast`, propagating filtered domain objects (e.g., `ZoneViolation`, `PartitionArmed`, `Trouble`) to subscribers in real-time.

## 🚀 Quick Start

Build your first bridge between code and hardware in just a few lines:

```rust
use satel_integra::{Config, ConnectionConfig, SatelIntegra};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Connection configuration
    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: "192.168.1.100".to_string(),
            port: 7094,
        },
        user_code: Some("1234".to_string()),
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 2. Establish connection (automatic handshake)
    satel.connect().await?;

    // 3. Subscribe to real-time events
    let mut events = satel.subscribe_events();
    
    // Change output state (control example)
    satel.set_output_on(10, None).await?;

    // Listening loop
    while let Ok(event) = events.recv().await {
        println!("New event from control panel: {:?}", event);
    }

    Ok(())
}
```

## 📚 Usage Examples

The repository contains a rich set of interactive examples in the `examples/` directory, presenting all aspects of integration step by step:

*   **Fetching information:** `2_01_get_version.rs`, `2_02_get_names.rs`, `2_03_get_zones_status.rs`, `2_04_get_outputs_status.rs`, `2_05_get_partitions_status.rs`, `2_06_get_temperatures.rs`.
*   **System control:** `3_01_control_outputs.rs`, `3_02_arm_disarm_partitions.rs`, `3_03_control_time.rs`.
*   **Event monitoring and diagnostics:** A set of scripts from the `4_xx` series filtering specific types of events (System Info, alarm states, temperatures, hardware troubles).
*   **Automation (Auto-Push):** `5_01_auto_read_push.rs` demonstrating completely passive, unattended data streaming using the `0x7F` mask.
