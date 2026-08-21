use serde::Deserialize;

/// Główna, nadrzędna struktura konfiguracyjna.
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// Konfiguracja metody połączenia (TCP lub UART).
    pub connection: ConnectionConfig,

    /// Czas oczekiwania na odczyt danych w milisekundach.
    #[serde(default = "default_read_timeout_ms")]
    pub read_timeout_ms: u64,

    /// Czas oczekiwania na zapis danych w milisekundach.
    #[serde(default = "default_write_timeout_ms")]
    pub write_timeout_ms: u64,

    /// Czas oczekiwania na odczyt temperatury w milisekundach.
    #[serde(default = "default_temp_read_timeout_ms")]
    pub temp_read_timeout_ms: u64,

    /// Maksymalny czas oczekiwania wiadomości w buforze kolejki w milisekundach.
    #[serde(default = "default_buffer_timeout_ms")]
    pub buffer_timeout_ms: u64,

    /// Opcjonalny kod użytkownika potrzebny do niektórych operacji.
    pub user_code: Option<String>,

    /// Czy włączyć automatyczne ponowne połączenie.
    #[serde(default = "default_auto_reconnect")]
    pub auto_reconnect: bool,

    /// Czy blokować odczyty z wadliwych czujników temperatury.
    #[serde(default = "default_temp_blocking_enabled")]
    pub temp_blocking_enabled: bool,

    /// Maksymalna liczba błędów braku czujnika / timeout przed zablokowaniem.
    #[serde(default = "default_temp_max_timeout_errors")]
    pub temp_max_timeout_errors: u32,

    /// Maksymalna liczba błędów czujnika raportowanych przez Satel przed zablokowaniem.
    #[serde(default = "default_temp_max_sensor_errors")]
    pub temp_max_sensor_errors: u32,

    /// Lista ID wejść (1-256), których stan sabotażu ma być odwrócony.
    #[serde(default)]
    pub io_tamper_invert: Vec<u16>,

    /// Lista ID wejść (1-256), których stan alarmowy ma być odwrócony.
    #[serde(default)]
    pub io_alarm_invert: Vec<u16>,

    /// Lista ID wejść (1-256), których stan naruszenia (violation) ma być odwrócony.
    #[serde(default)]
    pub io_violation_invert: Vec<u16>,

    /// Lista ID wejść (1-256), których stan alarmu sabotażowego ma być odwrócony.
    #[serde(default)]
    pub io_tamper_alarm_invert: Vec<u16>,

    /// Lista ID wejść (1-256), których stan pamięci alarmu ma być odwrócony.
    #[serde(default)]
    pub io_alarm_memory_invert: Vec<u16>,

    /// Lista ID wejść (1-256), których stan pamięci alarmu sabotażowego ma być odwrócony.
    #[serde(default)]
    pub io_tamper_alarm_memory_invert: Vec<u16>,

    /// Lista ID wejść (1-256), których stan blokady (bypass) ma być odwrócony.
    #[serde(default)]
    pub io_bypass_invert: Vec<u16>,

    /// Lista ID wejść (1-256), których stan awarii "brak naruszenia" ma być odwrócony.
    #[serde(default)]
    pub io_no_violation_trouble_invert: Vec<u16>,

    /// Lista ID wejść (1-256), których stan awarii "długie naruszenie" ma być odwrócony.
    #[serde(default)]
    pub io_long_violation_trouble_invert: Vec<u16>,

    /// Czy automatycznie odpytywać o stan naruszeń wejść (0x00).
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_violation: bool,

    /// Czy automatycznie odpytywać o stan sabotaży wejść (0x01).
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_tamper: bool,

    /// Czy automatycznie odpytywać o stan alarmów wejść (0x02).
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_alarm: bool,

    /// Czy automatycznie odpytywać o stan alarmów sabotażowych wejść (0x03).
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_tamper_alarm: bool,

    /// Czy automatycznie odpytywać o stan pamięci alarmów wejść (0x04).
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_alarm_memory: bool,

    /// Czy automatycznie odpytywać o stan pamięci alarmów sabotażowych wejść (0x05).
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_tamper_alarm_memory: bool,

    /// Czy automatycznie odpytywać o stan blokad wejść (0x06).
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_bypass: bool,

    /// Czy automatycznie odpytywać o stan awarii "brak naruszenia" wejść (0x07).
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_no_violation_trouble: bool,

    /// Czy automatycznie odpytywać o stan awarii "długie naruszenie" wejść (0x08).
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_long_violation_trouble: bool,

    /// Czy automatycznie odpytywać o stan uzbrojenia stref (suppressed) (0x09).
    #[serde(default = "default_auto_read")]
    pub auto_read_partitions_armed_suppressed: bool,

    /// Czy automatycznie odpytywać o faktyczny stan uzbrojenia stref (0x0A).
    #[serde(default = "default_auto_read")]
    pub auto_read_partitions_armed_really: bool,

    /// Czy automatycznie odpytywać o stan alarmów stref (0x13).
    #[serde(default = "default_auto_read")]
    pub auto_read_partitions_alarm: bool,

    /// Czy automatycznie odpytywać o stan pamięci alarmów stref (0x15).
    #[serde(default = "default_auto_read")]
    pub auto_read_partitions_alarm_memory: bool,

    /// Czy automatycznie odpytywać o czas na wejście (0x0E).
    #[serde(default = "default_auto_read")]
    pub auto_read_partitions_entry_time: bool,

    /// Czy automatycznie odpytywać o czas na wyjście (0x0F, 0x10).
    #[serde(default = "default_auto_read")]
    pub auto_read_partitions_exit_time: bool,

    /// Czy automatycznie odpytywać o stan wyjść (0x17).
    #[serde(default = "default_auto_read")]
    pub auto_read_outputs_state: bool,

    /// Czy automatycznie odpytywać o stan awarii (0x1B-0x30).
    #[serde(default = "default_auto_read")]
    pub auto_read_system_troubles: bool,

    /// Czy automatycznie odpytywać o pamięć awarii (0x20-0x31).
    #[serde(default = "default_auto_read")]
    pub auto_read_troubles_memory: bool,
}

impl Config {
    /// Sprawdza czy jakakolwiek opcja automatycznego odczytu jest włączona.
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
        }
    }
}

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

/// Konfiguracja metody połączenia.
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
