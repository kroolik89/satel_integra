use crate::error::SatelError;
use serde::{Deserialize, Serialize};

/// Czujnik temperatury odpytywany cyklicznie, z własnymi parametrami.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TemperatureProbe {
    pub zone_id: u16,                 // 1..=256
    pub interval_minutes: u64,        // 0 traktowane jak 1
    pub max_timeout_errors: u32,      // próg blokady „brak czujnika / timeout”
    pub max_sensor_errors: u32,       // próg blokady „błąd sondy 0xFFFF”
    #[serde(default)]
    pub unblock_enabled: bool,        // domyślnie false (T2)
    #[serde(default = "default_unblock_after_cycles")]
    pub unblock_after_cycles: u32,    // domyślnie 10, minimum 10 (T2)
}

pub const MIN_UNBLOCK_AFTER_CYCLES: u32 = 10;

fn default_unblock_after_cycles() -> u32 {
    MIN_UNBLOCK_AFTER_CYCLES
}

/// Main client configuration structure.
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// Connection transport configuration (TCP/IP or UART/RS-232).
    pub connection: ConnectionConfig,

    /// Whether to enable AES-192 encrypted communication with ETHM-1 Plus.
    /// Requires `integration_key`. Only supported for TCP connections.
    /// Default: false.
    #[serde(default = "default_encryption")]
    pub encryption: bool,

    /// Integration encryption key (up to 12 ASCII characters).
    /// Configured in DLOADX (Structure -> Modules -> ETHM-1 -> Integration key).
    /// Required when `encryption = true`. Stored in memory in plaintext.
    #[serde(default)]
    pub integration_key: Option<String>,

    /// Network / stream read timeout in milliseconds.
    #[serde(default = "default_read_timeout_ms")]
    pub read_timeout_ms: u64,

    /// Network / stream write timeout in milliseconds.
    #[serde(default = "default_write_timeout_ms")]
    pub write_timeout_ms: u64,

    /// Zone temperature sensor query timeout in milliseconds.
    #[serde(default = "default_temp_read_timeout_ms")]
    pub temp_read_timeout_ms: u64,

    /// Maximum message lifetime in the buffer queue before expiration (ms).
    #[serde(default = "default_buffer_timeout_ms")]
    pub buffer_timeout_ms: u64,

    /// Optional user access code required for control commands.
    pub user_code: Option<String>,

    /// Whether to automatically reconnect when the connection drops.
    #[serde(default = "default_auto_reconnect")]
    pub auto_reconnect: bool,

    /// Whether smart blocking for faulty temperature sensors is enabled.
    #[serde(default = "default_temp_blocking_enabled")]
    pub temp_blocking_enabled: bool,

    /// Maximum consecutive timeout / missing errors before blocking a temperature sensor (global).
    #[serde(default = "default_temp_max_timeout_errors")]
    pub temp_max_timeout_errors: u32,

    /// Maximum consecutive sensor errors (0xFFFF) before blocking a temperature sensor (global).
    #[serde(default = "default_temp_max_sensor_errors")]
    pub temp_max_sensor_errors: u32,

    /// List of zone IDs (1..256) whose tamper state should be logically inverted.
    #[serde(default)]
    pub io_tamper_invert: Vec<u16>,

    /// List of zone IDs (1..256) whose alarm state should be logically inverted.
    #[serde(default)]
    pub io_alarm_invert: Vec<u16>,

    /// List of zone IDs (1..256) whose violation state should be logically inverted.
    #[serde(default)]
    pub io_violation_invert: Vec<u16>,

    /// List of zone IDs (1..256) whose tamper alarm state should be logically inverted.
    #[serde(default)]
    pub io_tamper_alarm_invert: Vec<u16>,

    /// List of zone IDs (1..256) whose alarm memory state should be logically inverted.
    #[serde(default)]
    pub io_alarm_memory_invert: Vec<u16>,

    /// List of zone IDs (1..256) whose tamper alarm memory state should be logically inverted.
    #[serde(default)]
    pub io_tamper_alarm_memory_invert: Vec<u16>,

    /// List of zone IDs (1..256) whose bypass state should be logically inverted.
    #[serde(default)]
    pub io_bypass_invert: Vec<u16>,

    /// List of zone IDs (1..256) whose 'no violation trouble' state should be logically inverted.
    #[serde(default)]
    pub io_no_violation_trouble_invert: Vec<u16>,

    /// List of zone IDs (1..256) whose 'long violation trouble' state should be logically inverted.
    #[serde(default)]
    pub io_long_violation_trouble_invert: Vec<u16>,

    /// Whether to auto-read zone violations (0x00) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_violation: bool,

    /// Whether to auto-read zone tampers (0x01) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_tamper: bool,

    /// Whether to auto-read zone alarms (0x02) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_alarm: bool,

    /// Whether to auto-read zone tamper alarms (0x03) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_tamper_alarm: bool,

    /// Whether to auto-read zone alarm memory (0x04) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_alarm_memory: bool,

    /// Whether to auto-read zone tamper alarm memory (0x05) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_tamper_alarm_memory: bool,

    /// Whether to auto-read zone bypasses (0x06) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_bypass: bool,

    /// Whether to auto-read zone 'no violation trouble' (0x07) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_no_violation_trouble: bool,

    /// Whether to auto-read zone 'long violation trouble' (0x08) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_long_violation_trouble: bool,

    /// Whether to auto-read partition suppressed arm state (0x09) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_partitions_armed_suppressed: bool,

    /// Whether to auto-read partition real arm state (0x0A) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_partitions_armed_really: bool,

    /// Whether to auto-read partition alarms (0x13) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_partitions_alarm: bool,

    /// Whether to auto-read partition alarm memory (0x15) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_partitions_alarm_memory: bool,

    /// Whether to auto-read partition entry countdown time (0x0E) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_partitions_entry_time: bool,

    /// Whether to auto-read partition exit countdown time (0x0F, 0x10) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_partitions_exit_time: bool,

    /// Whether to auto-read output states (0x17) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_outputs_state: bool,

    /// Whether to auto-read system hardware troubles (0x1B-0x30) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_system_troubles: bool,

    /// Whether to auto-read system troubles memory (0x20-0x31) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_troubles_memory: bool,

    #[serde(default)]
    pub temperature_probes: Vec<TemperatureProbe>,

    /// Whether to emit temperature events (`ZoneTemperature`) on every read cycle,
    /// even if the measured value has not changed. Default: false.
    #[serde(default = "default_emit_unchanged")]
    pub emit_unchanged_temperatures: bool,

    /// Whether to emit zone events (`ZoneViolation`, `ZoneTamper`, etc.) on every read cycle,
    /// even if the zone state has not changed. Default: false.
    #[serde(default = "default_emit_unchanged")]
    pub emit_unchanged_zones: bool,

    /// Whether to emit output events (`OutputChanged`) on every read cycle,
    /// even if the output state has not changed. Default: false.
    #[serde(default = "default_emit_unchanged")]
    pub emit_unchanged_outputs: bool,

    /// Whether to emit partition events (`PartitionArmed`, `PartitionAlarm`, etc.) on every read cycle,
    /// even if the partition state has not changed. Default: false.
    #[serde(default = "default_emit_unchanged")]
    pub emit_unchanged_partitions: bool,

    /// Whether to emit trouble events (`TroubleChanged`) on every read cycle,
    /// even if the hardware trouble state has not changed. Default: false.
    #[serde(default = "default_emit_unchanged")]
    pub emit_unchanged_troubles: bool,

    /// Whether to emit system status events (`SystemStatusChanged`) on every 0x1A read,
    /// even if status bits have not changed. Default: false.
    #[serde(default = "default_emit_unchanged")]
    pub emit_unchanged_system_status: bool,

    /// Whether to read extended device parameters along with names (0xEE types 5, 17, 19).
    /// Default: true.
    #[serde(default = "default_extended_name_read")]
    pub extended_name_read: bool,

    /// Maximum consecutive missed keep-alive pings (0x7E) before declaring connection lost.
    /// Default: 3. Set to 0 to disable missed ping detection.
    #[serde(default = "default_max_missed_pings")]
    pub max_missed_pings: u32,
}

impl Config {
    /// Validates the configuration consistency.
    ///
    /// Checks if encryption settings are valid:
    /// - If encryption is enabled, `integration_key` must be specified.
    /// - `integration_key` must be 1-12 ASCII characters.
    /// - Encryption is only supported for TCP connections, not UART.
    pub fn validate(&self) -> Result<(), SatelError> {
        if self.encryption {
            let key = self.integration_key.as_deref().ok_or_else(|| {
                SatelError::InvalidIntegrationKey(
                    "encryption is enabled but integration_key is not set".into(),
                )
            })?;
            if key.is_empty() || key.len() > 12 {
                return Err(SatelError::InvalidIntegrationKey(format!(
                    "integration_key must be 1-12 characters long, got {}",
                    key.len()
                )));
            }
            if !key.is_ascii() {
                return Err(SatelError::InvalidIntegrationKey(
                    "integration_key must contain only ASCII characters".into(),
                ));
            }
            if matches!(self.connection, ConnectionConfig::Uart { .. }) {
                return Err(SatelError::InvalidIntegrationKey(
                    "encryption is only supported for TCP connections (ETHM-1 Plus), not UART (INT-RS)".into(),
                ));
            }
        }
        
        let mut seen_zones = std::collections::HashSet::new();
        for probe in &self.temperature_probes {
            if probe.zone_id == 0 || probe.zone_id > 256 {
                return Err(SatelError::InvalidConfig(format!(
                    "invalid zone_id {} in temperature_probes (must be 1..=256)",
                    probe.zone_id
                )));
            }
            if !seen_zones.insert(probe.zone_id) {
                return Err(SatelError::InvalidConfig(format!(
                    "duplicate zone_id {} in temperature_probes",
                    probe.zone_id
                )));
            }
            if probe.unblock_enabled && probe.unblock_after_cycles < MIN_UNBLOCK_AFTER_CYCLES {
                return Err(SatelError::InvalidConfig(format!(
                    "unblock_after_cycles for zone {} is {}, minimum is {}",
                    probe.zone_id, probe.unblock_after_cycles, MIN_UNBLOCK_AFTER_CYCLES
                )));
            }
        }
        
        Ok(())
    }

    /// Returns true if any auto-read (0x7F push notification) category is enabled.
    pub fn is_auto_read_enabled(&self) -> bool {
        self.auto_read_zones_violation
            || self.auto_read_zones_tamper
            || self.auto_read_zones_alarm
            || self.auto_read_zones_tamper_alarm
            || self.auto_read_zones_alarm_memory
            || self.auto_read_zones_tamper_alarm_memory
            || self.auto_read_zones_bypass
            || self.auto_read_zones_no_violation_trouble
            || self.auto_read_zones_long_violation_trouble
            || self.auto_read_partitions_armed_suppressed
            || self.auto_read_partitions_armed_really
            || self.auto_read_partitions_alarm
            || self.auto_read_partitions_alarm_memory
            || self.auto_read_partitions_entry_time
            || self.auto_read_partitions_exit_time
            || self.auto_read_outputs_state
            || self.auto_read_system_troubles
            || self.auto_read_troubles_memory
    }

    /// Returns true if background temperature polling is configured and enabled.
    pub fn is_polling_enabled(&self) -> bool {
        !self.temperature_probes.is_empty()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            connection: ConnectionConfig::default(),
            encryption: default_encryption(),
            integration_key: None,
            read_timeout_ms: default_read_timeout_ms(),
            write_timeout_ms: default_write_timeout_ms(),
            temp_read_timeout_ms: default_temp_read_timeout_ms(),
            buffer_timeout_ms: default_buffer_timeout_ms(),
            user_code: None,
            auto_reconnect: default_auto_reconnect(),
            temp_blocking_enabled: default_temp_blocking_enabled(),
            temp_max_timeout_errors: default_temp_max_timeout_errors(),
            temp_max_sensor_errors: default_temp_max_sensor_errors(),
            io_tamper_invert: Vec::new(),
            io_alarm_invert: Vec::new(),
            io_violation_invert: Vec::new(),
            io_tamper_alarm_invert: Vec::new(),
            io_alarm_memory_invert: Vec::new(),
            io_tamper_alarm_memory_invert: Vec::new(),
            io_bypass_invert: Vec::new(),
            io_no_violation_trouble_invert: Vec::new(),
            io_long_violation_trouble_invert: Vec::new(),
            auto_read_zones_violation: default_auto_read(),
            auto_read_zones_tamper: default_auto_read(),
            auto_read_zones_alarm: default_auto_read(),
            auto_read_zones_tamper_alarm: default_auto_read(),
            auto_read_zones_alarm_memory: default_auto_read(),
            auto_read_zones_tamper_alarm_memory: default_auto_read(),
            auto_read_zones_bypass: default_auto_read(),
            auto_read_zones_no_violation_trouble: default_auto_read(),
            auto_read_zones_long_violation_trouble: default_auto_read(),
            auto_read_partitions_armed_suppressed: default_auto_read(),
            auto_read_partitions_armed_really: default_auto_read(),
            auto_read_partitions_alarm: default_auto_read(),
            auto_read_partitions_alarm_memory: default_auto_read(),
            auto_read_partitions_entry_time: default_auto_read(),
            auto_read_partitions_exit_time: default_auto_read(),
            auto_read_outputs_state: default_auto_read(),
            auto_read_system_troubles: default_auto_read(),
            auto_read_troubles_memory: default_auto_read(),
            temperature_probes: Vec::new(),
            emit_unchanged_temperatures: default_emit_unchanged(),
            emit_unchanged_zones: default_emit_unchanged(),
            emit_unchanged_outputs: default_emit_unchanged(),
            emit_unchanged_partitions: default_emit_unchanged(),
            emit_unchanged_troubles: default_emit_unchanged(),
            emit_unchanged_system_status: default_emit_unchanged(),
            extended_name_read: default_extended_name_read(),
            max_missed_pings: default_max_missed_pings(),
        }
    }
}

fn default_max_missed_pings() -> u32 { 3 }

fn default_extended_name_read() -> bool { true }
fn default_encryption() -> bool { false }
fn default_emit_unchanged() -> bool { false }
fn default_auto_read() -> bool { false }
fn default_auto_reconnect() -> bool { true }
fn default_temp_blocking_enabled() -> bool { true }
fn default_temp_max_timeout_errors() -> u32 { 4 }
fn default_temp_max_sensor_errors() -> u32 { 10 }
fn default_baud_rate() -> u32 { 19200 }
fn default_read_timeout_ms() -> u64 { 2000 }
fn default_write_timeout_ms() -> u64 { 500 }
fn default_temp_read_timeout_ms() -> u64 { 2000 }
fn default_buffer_timeout_ms() -> u64 { 10000 }

/// Transport connection parameters.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(tag = "type")]
pub enum ConnectionConfig {
    #[serde(rename = "tcp")]
    Tcp { host: String, port: u16 },
    #[serde(rename = "uart")]
    Uart {
        path: String,
        #[serde(default = "default_baud_rate")]
        baud_rate: u32,
    },
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self::Tcp {
            host: "localhost".to_string(),
            port: 7094,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default_validation() {
        let config = Config::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_encryption_without_key() {
        let mut config = Config::default();
        config.encryption = true;
        config.integration_key = None;
        assert!(matches!(
            config.validate(),
            Err(SatelError::InvalidIntegrationKey(_))
        ));
    }

    #[test]
    fn test_config_encryption_empty_key() {
        let mut config = Config::default();
        config.encryption = true;
        config.integration_key = Some("".to_string());
        assert!(matches!(
            config.validate(),
            Err(SatelError::InvalidIntegrationKey(_))
        ));
    }

    #[test]
    fn test_config_encryption_key_too_long() {
        let mut config = Config::default();
        config.encryption = true;
        config.integration_key = Some("1234567890123".to_string()); // 13 chars
        assert!(matches!(
            config.validate(),
            Err(SatelError::InvalidIntegrationKey(_))
        ));
    }

    #[test]
    fn test_config_encryption_key_non_ascii() {
        let mut config = Config::default();
        config.encryption = true;
        config.integration_key = Some("KluczZażółć".to_string());
        assert!(matches!(
            config.validate(),
            Err(SatelError::InvalidIntegrationKey(_))
        ));
    }

    #[test]
    fn test_config_encryption_uart_unsupported() {
        let mut config = Config::default();
        config.connection = ConnectionConfig::Uart {
            path: "COM1".to_string(),
            baud_rate: 19200,
        };
        config.encryption = true;
        config.integration_key = Some("MyKey123".to_string());
        assert!(matches!(
            config.validate(),
            Err(SatelError::InvalidIntegrationKey(_))
        ));
    }

    #[test]
    fn test_config_encryption_valid() {
        let mut config = Config::default();
        config.encryption = true;
        config.integration_key = Some("MyKey123".to_string());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_encryption_disabled_ignores_invalid_key() {
        let mut config = Config::default();
        config.encryption = false;
        config.integration_key = Some("This key is too long but encryption is off".to_string());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_zone_id_0() {
        let mut config = Config::default();
        config.temperature_probes = vec![TemperatureProbe {
            zone_id: 0,
            max_timeout_errors: 3,
            max_sensor_errors: 10,
            interval_minutes: 1,
            unblock_enabled: false,
            unblock_after_cycles: 10,
        }];
        assert!(matches!(config.validate(), Err(SatelError::InvalidConfig(_))));
    }

    #[test]
    fn test_validate_duplicate_zone_id() {
        let mut config = Config::default();
        config.temperature_probes = vec![
            TemperatureProbe {
                zone_id: 5,
                max_timeout_errors: 3,
                max_sensor_errors: 10,
                interval_minutes: 1,
                unblock_enabled: false,
                unblock_after_cycles: 10,
            },
            TemperatureProbe {
                zone_id: 5,
                max_timeout_errors: 3,
                max_sensor_errors: 10,
                interval_minutes: 1,
                unblock_enabled: false,
                unblock_after_cycles: 10,
            },
        ];
        assert!(matches!(config.validate(), Err(SatelError::InvalidConfig(_))));
    }

    #[test]
    fn test_validate_unblock_cycles() {
        let mut config = Config::default();
        config.temperature_probes = vec![TemperatureProbe {
            zone_id: 5,
            max_timeout_errors: 3,
            max_sensor_errors: 10,
            interval_minutes: 1,
            unblock_enabled: true,
            unblock_after_cycles: 9,
        }];
        assert!(matches!(config.validate(), Err(SatelError::InvalidConfig(_))));

        config.temperature_probes[0].unblock_after_cycles = 10;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_temperature_probe_deserialize_defaults() {
        let toml_str = r#"
            zone_id = 5
            max_timeout_errors = 4
            max_sensor_errors = 8
            interval_minutes = 2
        "#;
        let probe: TemperatureProbe = toml::from_str(toml_str).unwrap();
        assert_eq!(probe.zone_id, 5);
        assert_eq!(probe.unblock_enabled, false);
        assert_eq!(probe.unblock_after_cycles, 10);
    }
}
