# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
