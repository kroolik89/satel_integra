# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.9.0] - 2026-09-24

### Added
- Extended name & parameter reading (ETHM-1 command 0xEE):
  - Configuration option `extended_name_read: bool` (default: `true`), dynamic reloadable via `hot_reload_config` without connection reset.
  - Automatic fallback hierarchy when the panel rejects extended query types:
    - Zones: 5 -> 1 (falls back to basic zone read, retaining reaction type).
    - Outputs: 17 -> 4 (falls back to basic output read, retaining function).
    - Partitions: 19 -> 18 -> 16 -> 0 (falls back progressively, retaining partition type).
    - Session remembers the highest supported query type across queries without repeating rejected queries.
  - Parameter data structures: `ZoneParams`, `OutputParams`, `PartitionParams`.
  - Client parameter query methods and memory cache getters:
    - `get_zone_params(id)` / `get_cached_zone_params(id)`
    - `get_output_params(id)` / `get_cached_output_params(id)`
    - `get_partition_params(id)` / `get_cached_partition_params(id)`
    - Cached output control helpers: `output_control(id) -> Option<OutputControl>` and `is_output_controllable(id) -> Option<bool>`.
  - New broadcast events: `SatelEvent::ZoneParamsReceived`, `SatelEvent::OutputParamsReceived`, `SatelEvent::PartitionParamsReceived`.
  - Scans (`get_all_zone_names`, `get_all_output_names`, `get_all_partition_names`) emit both `*NameReceived` and `*ParamsReceived` per position when `extended_name_read` is enabled, while `SyncProgress` tracks scanned positions.
- Type Catalogs:
  - `ZoneReaction` (codes 0..97) with stable snake_case key, English label, and `ZoneKind` categorization.
  - `OutputFunction` (codes 0..123) with stable snake_case key, English label, controllability check (`is_controllable()`), and control classification (`control()` -> `OutputControl::Timed`, `Bistable`, `Unknown`, `None`).
  - `PartitionType` (codes 0..3) with stable snake_case key, English label, options bitmask (`PartitionOptions`), auto-arm defer timer (`AutoArmDeferTimer`), and dependent partitions mask (`DependentPartitions`).
- Example `examples/2_10_get_extended_names_and_params.rs` demonstrating full discovery of names, parameters, and control modes.

## [1.8.1] - 2026-09-18

### Fixed
- Wireless device/output trouble numbering starts at 17, not 1.

## [1.8.0] - 2026-09-18

### Fixed
- `hot_reload_config` re-sends 0x7F mask when auto-read categories change.

## [1.7.0] - 2026-09-17

### Added
- `ConnectionStatistics` snapshot model and `StatsMark` change detection struct (`STATS_MIN_NON_PING_FRAMES = 5`, `statistics_changed()`).
- `SatelIntegra::statistics()` and `SatelIntegra::reset_statistics()`.
- `SatelEvent::ConnectionStatistics(ConnectionStatistics)` emitted immediately after `ConnectionChanged` and periodically every 30 seconds when connected if non-ping frames increased by >= 5 or any error/connection counter changed.
- `CountingStream` (`AsyncRead` + `AsyncWrite`) wrapping physical transport for raw wire byte counting (`bytes_sent`, `bytes_received`) before encryption.
- CRC error tracking in `SatelCodec` incrementing `crc_errors` counter on checksum mismatch.

### Changed
- **Breaking**: Telemetry counters migrated to `Arc<AtomicU64>` in `ConnectionTelemetry`.
- **Breaking**: `reconnect_count` renamed to `reconnect_attempts`.
- **Breaking**: `bytes_sent` and `bytes_received` now count actual physical wire bytes (including frame envelope, stuffing, and encryption overhead) via `CountingStream` instead of payload lengths.
- **Breaking**: `disconnect()` does not reset connection statistics (statistics persist across disconnections until explicit `reset_statistics()`).
- Added internal `non_ping_frames` counter tracking frames exchanged outside periodic keep-alive pings.

## [1.6.0] - 2026-09-17

### Added
- `temperature_probes` configuration replacing `polling_temperatures*`.
- `TemperatureProbe` struct for per-sensor configuration.
- `reset_temperature_sensor` function in client.
- Status `RetryRead` and automatic unblocking functionality with configurable thresholds (`unblock_enabled`, `unblock_after_cycles`).
- Error `InvalidConfig` returned on invalid configuration validation.

### Changed
- Removed deprecated `polling_temperatures`, `polling_temperatures_zones`, `polling_temperatures_interval_minutes` from `Config`.
- Note: `temp_max_timeout_errors` and `temp_max_sensor_errors` remain as fallback thresholds for zones not explicitly defined in `temperature_probes`.

## [1.5.1] - 2026-09-16

### Fixed
- Decoding bitmasks limits for 0x22, 0x1C, 0x23 (B1, B2).
- Event emission for initial 0-values in map-based troubles (B3).
- Przywrócone testy dekodowania i walidacji katalogu.

## [1.5.0] - 2026-09-16

### Added
- Declarative trouble decoding via `decode_troubles`.
- New `TroubleType` variants and descriptors for memory-based parsing (GSM, temperature, restart, tamper).
- Support for trouble memory parsing across frames (0x21, 0x22, 0x24, 0x2F).


## [1.4.0] - 2026-09-16

### Added
- TroubleType::catalog/key/address, TroubleDescriptor.
- SatelIntegra::auto_read_report.

## [1.3.0] - 2026-09-16

### Added
- `IntegraVersion::partition_count` field.

### Changed
- `get_all_partition_names` queries `1..=partition_count` instead of constant 32.

## [1.2.0] - 2026-08-28

### Added
- **Hot-Reload Architecture**: Introduced real-time configuration reloading without process restart or thread recreation.
  - Added `hot_reload_config(&self, mut new_config: Config)` for zero-downtime logical parameter updates (polling intervals, logical inversion, etc.) which strictly preserves physical connection parameters.
  - Added async `reload_config(&self, new_config: Config)` which applies a full configuration update and automatically triggers a seamless connection restart (`disconnect().await` and `connect().await`) if the client is currently connected.
  - Emits `SatelEvent::ConfigUpdated` upon successful reload.
- **Batch Name Retrieval Functions & Lifecycle Events**: Added `get_all_zone_names()`, `get_all_output_names()`, and `get_all_partition_names()` to `SatelIntegra` client.
  - Automatically verifies detected panel version and uses `io_count` to query all relevant zones/outputs/partitions.
  - **Full 3-Phase Synchronization Lifecycle**: Added real-time event broadcasting for all 3 categories (`SyncCategory::Zones`, `SyncCategory::Outputs`, `SyncCategory::Partitions`):
    - `SatelEvent::SyncStarted { category, total }`: Emitted immediately upon initiation with expected total count.
    - `SatelEvent::SyncProgress { category, current, total, name }`: Emitted sequentially for each queried item with current index and read name.
    - `SatelEvent::SyncFinished { category, total, success_count, error }`: Emitted upon completion with total queried, successful count, and error details if any occurred.
  - **Error Resilience**: Sequential query pipeline aggregates successes and errors without abrupt thread interruption, ensuring downstream subscribers receive proper completion signals.
- **Error Handling**: Added `SatelError::PanelVersionUnknown` when batch name operations are invoked before panel version is retrieved.

## [1.1.0] - 2026-08-24

### Added
- **Full Integration Protocol Encryption (AES-192)**: Native encrypted communication with Satel Integra panels via ETHM-1 / ETHM-1 Plus modules.
- **Encryption Configuration**: Added `encryption: bool` and `integration_key: Option<String>` fields to `Config`.
- **Configuration Validation**: `Config::validate()` automatically validates integration key format (1–12 ASCII characters, required when encryption is enabled) and rejects unsupported encryption over UART connections.
- **Interactive Examples**: Added `examples/1_03_encrypted_connection.rs` and `examples/5_03_auto_poll_temperatures_encrypted.rs`.

### Changed
- Reorganized and renumbered interactive examples into structured categories (1.x to 6.x).

## [1.0.1] - 2026-08-24

### Added
- Background cyclic temperature auto-poller worker (`polling_temperatures`, `polling_temperatures_zones`, `polling_temperatures_interval_minutes`).
- Error event broadcast for broken/missing probes (`SatelEvent::ZoneTemperatureError`).
- Smart temperature error blocking protection (`temp_blocking_enabled`, `temp_max_timeout_errors`, `temp_max_sensor_errors`).
- Soft state inversion filters for inputs/outputs/partitions in `Config`.
- Comprehensive 8-part system trouble decoding and monitoring.
- Initial release of asynchronous Rust client for Satel Integra panels via TCP/IP (ETHM-1 Plus) and UART (RS-232 / INT-RS).
