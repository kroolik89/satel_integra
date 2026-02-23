use serde::Deserialize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Instant, SystemTime};
use chrono::{DateTime, Local};

// --- Nowe Struktury Konfiguracyjne ---

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
        }
    }
}

/// Domyślna wartość dla pól automatycznego odczytu.
fn default_auto_read() -> bool {
    false
}

/// Domyślna wartość dla automatycznego ponownego połączenia.
fn default_auto_reconnect() -> bool {
    true
}

/// Domyślna wartość dla blokowania wadliwych czujników.
fn default_temp_blocking_enabled() -> bool {
    true
}

/// Domyślna wartość dla max timeoutów.
fn default_temp_max_timeout_errors() -> u32 {
    4
}

/// Domyślna wartość dla max błędów czujnika.
fn default_temp_max_sensor_errors() -> u32 {
    10
}

/// Konfiguracja metody połączenia.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type")] // Użyj pola "type" w TOML do rozróżnienia wariantów
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

/// Domyślna wartość dla baud_rate.
fn default_baud_rate() -> u32 {
    19200
}

/// Domyślna wartość dla timeoutu odczytu (ms).
fn default_read_timeout_ms() -> u64 {
    2000
}

/// Domyślna wartość dla timeoutu zapisu (ms).
fn default_write_timeout_ms() -> u64 {
    500
}

/// Domyślna wartość dla timeoutu odczytu temperatury (ms).
fn default_temp_read_timeout_ms() -> u64 {
    2000
}

/// Domyślna wartość dla timeoutu bufora (ms).
fn default_buffer_timeout_ms() -> u64 {
    10000
}


// --- Istniejące Struktury Danych (bez zmian) ---

/// Stany połączenia z centralą.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConnectionState {
    Connected,
    #[default]
    Disconnected,
    ConnectionLost,
    Connecting,
    Handshake,
}

/// Status połączenia zawierający stan i metadane.
#[derive(Debug, Clone, Copy)]
pub struct ConnectionStatus {
    pub state: ConnectionState,
    pub failed_attempts: u32,
    pub last_event_at: Instant,
}

impl Default for ConnectionStatus {
    fn default() -> Self {
        Self {
            state: ConnectionState::Disconnected,
            failed_attempts: 0,
            last_event_at: Instant::now(),
        }
    }
}

/// Typ używanego połączenia.
#[derive(Debug, Clone)]
pub enum ConnectionType {
    Tcp(String, u16),
    Uart(String),
}

/// Telemetria i stan dotyczący transmisji danych.
#[derive(Debug)]
pub struct ConnectionTelemetry {
    pub status: ConnectionStatus,
    pub last_connected_at: Option<SystemTime>,
    pub last_send_at: Instant,
    pub bytes_sent: AtomicUsize,
    pub bytes_received: AtomicUsize,
    pub reconnect_count: AtomicUsize,
}

impl Default for ConnectionTelemetry {
    fn default() -> Self {
        Self {
            status: ConnectionStatus::default(),
            last_connected_at: None,
            last_send_at: Instant::now(),
            bytes_sent: AtomicUsize::new(0),
            bytes_received: AtomicUsize::new(0),
            reconnect_count: AtomicUsize::new(0),
        }
    }
}

impl ConnectionTelemetry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&self) {
        // Uwaga: status i last_connected_at nie są atomowe, 
        // ale reset() jest zazwyczaj wołany przy pełnym restarcie lub przez write locka na stanie.
        // W tej strukturze pola te będą modyfikowane przez write locka na SatelState.
        self.bytes_sent.store(0, Ordering::Relaxed);
        self.bytes_received.store(0, Ordering::Relaxed);
        self.reconnect_count.store(0, Ordering::Relaxed);
    }
}

/// Zdarzenia przesyłane przez system rozgłoszeniowy (broadcast).
#[derive(Debug, Clone)]
pub enum SatelEvent {
    /// Zmiana stanu połączenia.
    ConnectionChanged(ConnectionState),
    /// Zmiana stanu naruszenia wejścia (0x00).
    ZoneViolation { id: u16, state: bool },
    /// Zmiana stanu sabotażu wejścia (0x01).
    ZoneTamper { id: u16, state: bool },
    /// Zmiana stanu alarmu wejścia (0x02).
    ZoneAlarm { id: u16, state: bool },
    /// Zmiana stanu alarmu sabotażowego wejścia (0x03).
    ZoneTamperAlarm { id: u16, state: bool },
    /// Zmiana stanu pamięci alarmu wejścia (0x04).
    ZoneAlarmMemory { id: u16, state: bool },
    /// Zmiana stanu pamięci alarmu sabotażowego wejścia (0x05).
    ZoneTamperAlarmMemory { id: u16, state: bool },
    /// Zmiana stanu blokady wejścia (0x06).
    ZoneBypass { id: u16, state: bool },
    /// Zmiana stanu awarii "brak naruszenia" (0x07).
    ZoneNoViolationTrouble { id: u16, state: bool },
    /// Zmiana stanu awarii "długie naruszenie" (0x08).
    ZoneLongViolationTrouble { id: u16, state: bool },
    /// Zmiana stanu uzbrojenia strefy (0x09).
    PartitionArmed { id: u16, state: bool },
    /// Zmiana faktycznego stanu uzbrojenia strefy (0x0A).
    PartitionArmedReally { id: u16, state: bool },
    /// Zmiana stanu alarmu w strefie (0x13).
    PartitionAlarm { id: u16, state: bool },
    /// Zmiana stanu pamięci alarmu w strefie (0x15).
    PartitionAlarmMemory { id: u16, state: bool },
    /// Zmiana stanu czasu na wejście (0x0E).
    PartitionEntryTime { id: u16, state: bool },
    /// Zmiana stanu czasu na wyjście > 10s (0x0F).
    PartitionExitTimeGt10s { id: u16, state: bool },
    /// Zmiana stanu czasu na wyjście < 10s (0x10).
    PartitionExitTimeLt10s { id: u16, state: bool },
    /// Zmiana temperatury wejścia (0x7D).
    ZoneTemperatureChanged { id: u16, temperature: f32 },
    /// Odebrano nazwę wejścia (0xEE typ 1).
    ZoneNameReceived { id: u16, name: String },
    /// Odebrano nazwę wyjścia (0xEE typ 4).
    OutputNameReceived { id: u16, name: String },
    /// Odebrano nazwę strefy (0xEE typ 0).
    PartitionNameReceived { id: u16, name: String },
}

/// Informacje o wersji centrali Integra.
#[derive(Debug, Clone)]
pub struct IntegraVersion {
    pub model: String,
    pub firmware_version: String,
    pub language: String,
    pub stored_in_flash: bool,
    pub io_count: u16,
    pub read_at: DateTime<Local>,
}

/// Nazwa wejścia/wyjścia/strefy z datą odczytu.
#[derive(Debug, Clone)]
pub struct SatelName {
    pub name: String,
    pub read_at: DateTime<Local>,
}

/// Status czujnika temperatury.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TemperatureSensorStatus {
    #[default]
    NoRead,             // Brak odczytu
    Ok,                 // Sprawny
    SensorMissing,      // Brak czujnika (Timeout)
    CommunicationError, // Błąd komunikacji (0xFFFF)
}

/// Temperatura z wejścia z datą odczytu.
#[derive(Debug, Clone)]
pub struct ZoneTemperature {
    pub zone_id: u16,
    pub temperature: f32,
    pub read_at: DateTime<Local>,
}

/// Dane o sabotażu wejść odczytane z centrali (dla procesora).
#[derive(Debug, Clone)]
pub struct ZonesTamperData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Dane o alarmach wejść odczytane z centrali (dla procesora).
#[derive(Debug, Clone)]
pub struct ZonesAlarmData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Dane o naruszeniach wejść odczytane z centrali (dla procesora).
#[derive(Debug, Clone)]
pub struct ZonesViolationData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Dane o alarmach sabotażowych wejść odczytane z centrali (dla procesora).
#[derive(Debug, Clone)]
pub struct ZonesTamperAlarmData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Dane o pamięci alarmów wejść odczytane z centrali (dla procesora).
#[derive(Debug, Clone)]
pub struct ZonesAlarmMemoryData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Dane o pamięci alarmów sabotażowych wejść odczytane z centrali (dla procesora).
#[derive(Debug, Clone)]
pub struct ZonesTamperAlarmMemoryData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Dane o blokadach wejść odczytane z centrali (dla procesora).
#[derive(Debug, Clone)]
pub struct ZonesBypassData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Dane o awariach "brak naruszenia" wejść odczytane z centrali (dla procesora).
#[derive(Debug, Clone)]
pub struct ZonesNoViolationTroubleData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Dane o awariach "długie naruszenie" wejść odczytane z centrali (dla procesora).
#[derive(Debug, Clone)]
pub struct ZonesLongViolationTroubleData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Dane o stanie stref odczytane z centrali (dla procesora).
#[derive(Debug, Clone)]
pub struct PartitionsData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

// Zachowanie kompatybilności wstecznej aliasem
pub type PartitionsArmedData = PartitionsData;

/// Zagregowany status pojedynczego wejścia (dla użytkownika).
#[derive(Debug, Clone)]
pub struct ZoneStatus {
    pub id: u16,
    pub name: String,
    pub temperature: f32,
    pub violation_state: bool,
    pub violation_at: DateTime<Local>,
    pub tamper_state: bool,
    pub tamper_at: DateTime<Local>,
    pub alarm_state: bool,
    pub alarm_at: DateTime<Local>,
    pub tamper_alarm_state: bool,
    pub tamper_alarm_at: DateTime<Local>,
    pub alarm_memory_state: bool,
    pub alarm_memory_at: DateTime<Local>,
    pub tamper_alarm_memory_state: bool,
    pub tamper_alarm_memory_at: DateTime<Local>,
    pub bypass_state: bool,
    pub bypass_at: DateTime<Local>,
    pub no_violation_trouble_state: bool,
    pub no_violation_trouble_at: DateTime<Local>,
    pub long_violation_trouble_state: bool,
    pub long_violation_trouble_at: DateTime<Local>,
}

/// Aliasy dla czytelności (zachowanie kompatybilności wstecznej)
pub type ZoneName = SatelName;
pub type OutputName = SatelName;
pub type PartitionName = SatelName;

/// Ujednolicona struktura Wejścia (Zone).
#[derive(Debug, Clone)]
pub struct Zone {
    pub id: u16,
    // Dane z nazwy
    pub zone_name: String,
    pub zone_name_read_at: DateTime<Local>,
    // Dane z temperatury
    pub temperature_value: f32,
    pub temperature_read_at: DateTime<Local>,
    pub temperature_status: TemperatureSensorStatus,
    pub temperature_timeout_errors_total: u32,
    pub temperature_sensor_errors_total: u32,
    pub temperature_timeout_errors_current: u32,
    pub temperature_sensor_errors_current: u32,
    // Dane z sabotażu
    pub tamper_state: bool,
    pub tamper_read_at: DateTime<Local>,
    // Dane z alarmu
    pub alarm_state: bool,
    pub alarm_read_at: DateTime<Local>,
    // Dane z naruszenia
    pub violation_state: bool,
    pub violation_read_at: DateTime<Local>,
    // Dane z alarmu sabotażowego
    pub tamper_alarm_state: bool,
    pub tamper_alarm_read_at: DateTime<Local>,
    // Dane z pamięci alarmu
    pub alarm_memory_state: bool,
    pub alarm_memory_read_at: DateTime<Local>,
    // Dane z pamięci alarmu sabotażowego
    pub tamper_alarm_memory_state: bool,
    pub tamper_alarm_memory_read_at: DateTime<Local>,
    // Dane z blokady (bypass)
    pub bypass_state: bool,
    pub bypass_read_at: DateTime<Local>,
    // Dane z awarii "brak naruszenia"
    pub no_violation_trouble_state: bool,
    pub no_violation_trouble_read_at: DateTime<Local>,
    // Dane z awarii "długie naruszenie"
    pub long_violation_trouble_state: bool,
    pub long_violation_trouble_read_at: DateTime<Local>,
}

impl Zone {
    pub fn new(id: u16) -> Self {
        let now = Local::now();
        Self {
            id,
            zone_name: String::new(),
            zone_name_read_at: now,
            temperature_value: 0.0,
            temperature_read_at: now,
            temperature_status: TemperatureSensorStatus::NoRead,
            temperature_timeout_errors_total: 0,
            temperature_sensor_errors_total: 0,
            temperature_timeout_errors_current: 0,
            temperature_sensor_errors_current: 0,
            tamper_state: false,
            tamper_read_at: now,
            alarm_state: false,
            alarm_read_at: now,
            violation_state: false,
            violation_read_at: now,
            tamper_alarm_state: false,
            tamper_alarm_read_at: now,
            alarm_memory_state: false,
            alarm_memory_read_at: now,
            tamper_alarm_memory_state: false,
            tamper_alarm_memory_read_at: now,
            bypass_state: false,
            bypass_read_at: now,
            no_violation_trouble_state: false,
            no_violation_trouble_read_at: now,
            long_violation_trouble_state: false,
            long_violation_trouble_read_at: now,
        }
    }

    pub fn to_zone_name(&self) -> ZoneName {
        ZoneName {
            name: self.zone_name.clone(),
            read_at: self.zone_name_read_at,
        }
    }

    pub fn to_zone_temperature(&self) -> ZoneTemperature {
        ZoneTemperature {
            zone_id: self.id,
            temperature: self.temperature_value,
            read_at: self.temperature_read_at,
        }
    }
}

/// Ujednolicona struktura Strefy (Partition).
#[derive(Debug, Clone)]
pub struct Partition {
    pub id: u16,
    pub name: String,
    pub name_read_at: DateTime<Local>,
    // Stany logiczne
    pub armed_suppressed: bool, // 0x09
    pub armed_suppressed_at: DateTime<Local>,
    pub armed_really: bool, // 0x0A
    pub armed_really_at: DateTime<Local>,
    pub alarm: bool, // 0x13
    pub alarm_at: DateTime<Local>,
    pub alarm_memory: bool, // 0x15
    pub alarm_memory_at: DateTime<Local>,
    pub entry_time: bool, // 0x0E
    pub entry_time_at: DateTime<Local>,
    pub exit_time_gt_10s: bool, // 0x0F
    pub exit_time_gt_10s_at: DateTime<Local>,
    pub exit_time_lt_10s: bool, // 0x10
    pub exit_time_lt_10s_at: DateTime<Local>,
}

impl Partition {
    pub fn new(id: u16) -> Self {
        let now = Local::now();
        Self {
            id,
            name: String::new(),
            name_read_at: now,
            armed_suppressed: false,
            armed_suppressed_at: now,
            armed_really: false,
            armed_really_at: now,
            alarm: false,
            alarm_at: now,
            alarm_memory: false,
            alarm_memory_at: now,
            entry_time: false,
            entry_time_at: now,
            exit_time_gt_10s: false,
            exit_time_gt_10s_at: now,
            exit_time_lt_10s: false,
            exit_time_lt_10s_at: now,
        }
    }

    pub fn to_partition_name(&self) -> PartitionName {
        PartitionName {
            name: self.name.clone(),
            read_at: self.name_read_at,
        }
    }
}

/// Ujednolicona struktura Wyjścia (Output).
#[derive(Debug, Clone)]
pub struct Output {
    pub id: u16,
    pub name: String,
    pub name_read_at: DateTime<Local>,
}

impl Output {
    pub fn new(id: u16) -> Self {
        Self {
            id,
            name: String::new(),
            name_read_at: Local::now(),
        }
    }

    pub fn to_output_name(&self) -> OutputName {
        OutputName {
            name: self.name.clone(),
            read_at: self.name_read_at,
        }
    }
}

/// Struktura przechowująca współdzielony stan połączenia (zunifikowany cache).
#[derive(Debug)]
pub struct SatelState {
    pub connection_type: Option<ConnectionType>,
    pub telemetry: ConnectionTelemetry,
    pub integra_version: Option<IntegraVersion>,
    pub zones: Vec<Zone>,           // Tablica 256 wejść
    pub outputs: Vec<Output>,       // Tablica 256 wyjść
    pub partitions: Vec<Partition>, // Tablica 32 stref
}

impl SatelState {
    pub fn new() -> Self {
        let mut zones = Vec::with_capacity(256);
        let mut outputs = Vec::with_capacity(256);
        for i in 1..=256 {
            zones.push(Zone::new(i as u16));
            outputs.push(Output::new(i as u16));
        }

        let mut partitions = Vec::with_capacity(32);
        for i in 1..=32 {
            partitions.push(Partition::new(i as u16));
        }

        Self {
            connection_type: None,
            telemetry: ConnectionTelemetry::new(),
            integra_version: None,
            zones,
            outputs,
            partitions,
        }
    }
}

/// Wątkobezpieczny uchwyt do stanu `SatelState`.
pub type SatelStateHandle = Arc<RwLock<SatelState>>;

/// Enum reprezentujący komendy protokołu Satel Integra.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SatelCommand {
    // ... (reszta komend bez zmian)
    ZonesViolation = 0x00,
    ZonesTamper = 0x01,
    ZonesAlarm = 0x02,
    ZonesTamperAlarm = 0x03,
    ZonesAlarmMemory = 0x04,
    ZonesTamperAlarmMemory = 0x05,
    ZonesBypass = 0x06,
    ZonesNoViolationTrouble = 0x07,
    ZonesLongViolationTrouble = 0x08,
    ArmedPartitionsSuppressed = 0x09,
    ArmedPartitionsReally = 0x0A,
    PartitionsArmedMode2 = 0x0B,
    PartitionsArmedMode3 = 0x0C,
    PartitionsWith1stCodeEntered = 0x0D,
    PartitionsEntryTime = 0x0E,
    PartitionsExitTimeMore10s = 0x0F,
    PartitionsExitTimeLess10s = 0x10,
    PartitionsTemporaryBlocked = 0x11,
    PartitionsBlockedForGuardRound = 0x12,
    PartitionsAlarm = 0x13,
    PartitionsFireAlarm = 0x14,
    PartitionsAlarmMemory = 0x15,
    PartitionsFireAlarmMemory = 0x16,
    OutputsState = 0x17,
    DoorsOpened = 0x18,
    DoorsOpenedLong = 0x19,
    RtcAndBasicStatusBits = 0x1A,
    TroublesPart1 = 0x1B,
    TroublesPart2 = 0x1C,
    TroublesPart3 = 0x1D,
    TroublesPart4 = 0x1E,
    TroublesPart5 = 0x1F,
    TroublesMemoryPart1 = 0x20,
    TroublesMemoryPart2 = 0x21,
    TroublesMemoryPart3 = 0x22,
    TroublesMemoryPart4 = 0x23,
    TroublesMemoryPart5 = 0x24,
    PartitionsWithViolatedZones = 0x25,
    ZonesIsolate = 0x26,
    PartitionsWithVerifiedAlarms = 0x27,
    ZonesMasked = 0x28,
    ZonesMaskedMemory = 0x29,
    PartitionsArmedInMode1 = 0x2A,
    PartitionsWithWarningAlarms = 0x2B,
    TroublesPart6 = 0x2C,
    TroublesPart7 = 0x2D,
    TroublesMemoryPart6 = 0x2E,
    TroublesMemoryPart7 = 0x2F,
    TroublesPart8 = 0x30,
    TroublesMemoryPart8 = 0x31,
    ReadOutputPower = 0x7B,
    ModuleVersion = 0x7C,
    ReadZoneTemperature = 0x7D,
    IntegraVersion = 0x7E,
    ListOfNewData = 0x7F,
    ArmMode0 = 0x80,
    ArmMode1 = 0x81,
    ArmMode2 = 0x82,
    ArmMode3 = 0x83,
    Disarm = 0x84,
    ClearAlarm = 0x85,
    ZonesBypassCmd = 0x86,
    ZonesUnbypass = 0x87,
    OutputsOn = 0x88,
    OutputsOff = 0x89,
    OpenDoor = 0x8A,
    ClearTroubleMemory = 0x8B,
    ReadEvent = 0x8C,
    Enter1stCode = 0x8D,
    SetRtcClock = 0x8E,
    GetEventText = 0x8F,
    ZonesIsolateCmd = 0x90,
    OutputsSwitch = 0x91,
    ForceArmMode0 = 0xA0,
    ForceArmMode1 = 0xA1,
    ForceArmMode2 = 0xA2,
    ForceArmMode3 = 0xA3,
    ReadSelfInfo = 0xE0,
    ReadUser = 0xE1,
    ReadUsersList = 0xE2,
    ReadUserLocks = 0xE3,
    WriteUserLocks = 0xE4,
    RemoveUser = 0xE5,
    CreateUser = 0xE6,
    ChangeUser = 0xE7,
    UserDallasCardKeyFobMgmt = 0xE8,
    ChangeUserCode = 0xE9,
    ChangeUserTelCode = 0xEA,
    ReadDeviceName = 0xEE,
    ResultCode = 0xEF,
}

impl SatelCommand {
    pub fn to_byte(self) -> u8 {
        self as u8
    }

    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0x00 => Some(Self::ZonesViolation),
            0x01 => Some(Self::ZonesTamper),
            0x02 => Some(Self::ZonesAlarm),
            0x03 => Some(Self::ZonesTamperAlarm),
            0x04 => Some(Self::ZonesAlarmMemory),
            0x05 => Some(Self::ZonesTamperAlarmMemory),
            0x06 => Some(Self::ZonesBypass),
            0x07 => Some(Self::ZonesNoViolationTrouble),
            0x08 => Some(Self::ZonesLongViolationTrouble),
            0x09 => Some(Self::ArmedPartitionsSuppressed),
            0x0A => Some(Self::ArmedPartitionsReally),
            0x0B => Some(Self::PartitionsArmedMode2),
            0x0C => Some(Self::PartitionsArmedMode3),
            0x0D => Some(Self::PartitionsWith1stCodeEntered),
            0x0E => Some(Self::PartitionsEntryTime),
            0x0F => Some(Self::PartitionsExitTimeMore10s),
            0x10 => Some(Self::PartitionsExitTimeLess10s),
            0x11 => Some(Self::PartitionsTemporaryBlocked),
            0x12 => Some(Self::PartitionsBlockedForGuardRound),
            0x13 => Some(Self::PartitionsAlarm),
            0x14 => Some(Self::PartitionsFireAlarm),
            0x15 => Some(Self::PartitionsAlarmMemory),
            0x16 => Some(Self::PartitionsFireAlarmMemory),
            0x17 => Some(Self::OutputsState),
            0x18 => Some(Self::DoorsOpened),
            0x19 => Some(Self::DoorsOpenedLong),
            0x1A => Some(Self::RtcAndBasicStatusBits),
            0x1B => Some(Self::TroublesPart1),
            0x1C => Some(Self::TroublesPart2),
            0x1D => Some(Self::TroublesPart3),
            0x1E => Some(Self::TroublesPart4),
            0x1F => Some(Self::TroublesPart5),
            0x20 => Some(Self::TroublesMemoryPart1),
            0x21 => Some(Self::TroublesMemoryPart2),
            0x22 => Some(Self::TroublesMemoryPart3),
            0x23 => Some(Self::TroublesMemoryPart4),
            0x24 => Some(Self::TroublesMemoryPart5),
            0x25 => Some(Self::PartitionsWithViolatedZones),
            0x26 => Some(Self::ZonesIsolate),
            0x27 => Some(Self::PartitionsWithVerifiedAlarms),
            0x28 => Some(Self::ZonesMasked),
            0x29 => Some(Self::ZonesMaskedMemory),
            0x2A => Some(Self::PartitionsArmedInMode1),
            0x2B => Some(Self::PartitionsWithWarningAlarms),
            0x2C => Some(Self::TroublesPart6),
            0x2D => Some(Self::TroublesPart7),
            0x2E => Some(Self::TroublesMemoryPart6),
            0x2F => Some(Self::TroublesMemoryPart7),
            0x30 => Some(Self::TroublesPart8),
            0x31 => Some(Self::TroublesMemoryPart8),
            0x7B => Some(Self::ReadOutputPower),
            0x7C => Some(Self::ModuleVersion),
            0x7D => Some(Self::ReadZoneTemperature),
            0x7E => Some(Self::IntegraVersion),
            0x7F => Some(Self::ListOfNewData),
            0x80 => Some(Self::ArmMode0),
            0x81 => Some(Self::ArmMode1),
            0x82 => Some(Self::ArmMode2),
            0x83 => Some(Self::ArmMode3),
            0x84 => Some(Self::Disarm),
            0x85 => Some(Self::ClearAlarm),
            0x86 => Some(Self::ZonesBypassCmd),
            0x87 => Some(Self::ZonesUnbypass),
            0x88 => Some(Self::OutputsOn),
            0x89 => Some(Self::OutputsOff),
            0x8A => Some(Self::OpenDoor),
            0x8B => Some(Self::ClearTroubleMemory),
            0x8C => Some(Self::ReadEvent),
            0x8D => Some(Self::Enter1stCode),
            0x8E => Some(Self::SetRtcClock),
            0x8F => Some(Self::GetEventText),
            0x90 => Some(Self::ZonesIsolateCmd),
            0x91 => Some(Self::OutputsSwitch),
            0xA0 => Some(Self::ForceArmMode0),
            0xA1 => Some(Self::ForceArmMode1),
            0xA2 => Some(Self::ForceArmMode2),
            0xA3 => Some(Self::ForceArmMode3),
            0xE0 => Some(Self::ReadSelfInfo),
            0xE1 => Some(Self::ReadUser),
            0xE2 => Some(Self::ReadUsersList),
            0xE3 => Some(Self::ReadUserLocks),
            0xE4 => Some(Self::WriteUserLocks),
            0xE5 => Some(Self::RemoveUser),
            0xE6 => Some(Self::CreateUser),
            0xE7 => Some(Self::ChangeUser),
            0xE8 => Some(Self::UserDallasCardKeyFobMgmt),
            0xE9 => Some(Self::ChangeUserCode),
            0xEA => Some(Self::ChangeUserTelCode),
            0xEE => Some(Self::ReadDeviceName),
            0xEF => Some(Self::ResultCode),
            _ => None,
        }
    }
}
