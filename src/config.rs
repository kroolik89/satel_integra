use serde::Deserialize;

/// Main client configuration structure.
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// Connection transport configuration (TCP/IP or UART/RS-232).
    pub connection: ConnectionConfig,

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

    /// Maximum consecutive timeout / missing errors before blocking a temperature sensor.
    #[serde(default = "default_temp_max_timeout_errors")]
    pub temp_max_timeout_errors: u32,

    /// Maximum consecutive sensor errors (0xFFFF) before blocking a temperature sensor.
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

    /// Whether automated background cyclic polling for temperature sensors is enabled.
    /// Default: false.
    #[serde(default = "default_polling_temperatures")]
    pub polling_temperatures: bool,

    /// List of zone IDs (1..256) configured as temperature probes to poll cyclically.
    /// Requires `polling_temperatures: true`.
    #[serde(default)]
    pub polling_temperatures_zones: Vec<u16>,

    /// Interval (in minutes) between consecutive temperature polling cycles.
    /// Minimum: 1 minute.
    #[serde(default = "default_polling_temperatures_interval_minutes")]
    pub polling_temperatures_interval_minutes: u64,

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
}

impl Config {
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
        self.polling_temperatures && !self.polling_temperatures_zones.is_empty()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            connection: ConnectionConfig::default(),
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
            polling_temperatures: default_polling_temperatures(),
            polling_temperatures_zones: Vec::new(),
            polling_temperatures_interval_minutes: default_polling_temperatures_interval_minutes(),
            emit_unchanged_temperatures: default_emit_unchanged(),
            emit_unchanged_zones: default_emit_unchanged(),
            emit_unchanged_outputs: default_emit_unchanged(),
            emit_unchanged_partitions: default_emit_unchanged(),
            emit_unchanged_troubles: default_emit_unchanged(),
            emit_unchanged_system_status: default_emit_unchanged(),
        }
    }
}

fn default_emit_unchanged() -> bool { false }
fn default_polling_temperatures() -> bool { false }
fn default_polling_temperatures_interval_minutes() -> u64 { 1 }
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
#[derive(Clone, Debug, Deserialize)]
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
