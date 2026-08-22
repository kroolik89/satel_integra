use chrono::{DateTime, Local};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Instant, SystemTime};

// --- Połączenie ---

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
        self.bytes_sent.store(0, Ordering::Relaxed);
        self.bytes_received.store(0, Ordering::Relaxed);
        self.reconnect_count.store(0, Ordering::Relaxed);
    }
}

// --- Wersje ---

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

/// Możliwości i funkcje modułu ETHM (z ramki 0x7C).
#[derive(Debug, Clone, Copy)]
pub struct EthmCapabilities {
    /// Bit 0: Obsługa powiększonych ramek (32 bajty / 256 wejść/wyjść).
    pub support_32_byte_frames: bool,
    /// Bit 1: Obsługa 8 grup awarii i 14-bajtowej maski 0x7F.
    pub support_8_troubles_groups: bool,
    /// Bit 2: Obsługa rozszerzonych komend uzbrajania.
    pub support_extended_arming_commands: bool,
    pub reserved_bit3: bool,
    pub reserved_bit4: bool,
    pub reserved_bit5: bool,
    pub reserved_bit6: bool,
    pub reserved_bit7: bool,
}

/// Informacje o wersji modułu ETHM (z ramki 0x7C).
#[derive(Debug, Clone)]
pub struct EthmVersion {
    pub version_raw: String,
    pub capabilities: EthmCapabilities,
    pub read_at: DateTime<Local>,
}

// --- Nazwy ---

/// Nazwa wejścia/wyjścia/strefy z datą odczytu.
#[derive(Debug, Clone)]
pub struct SatelName {
    pub name: String,
    pub read_at: DateTime<Local>,
}

/// Alias nazwy wejścia.
pub type ZoneName = SatelName;
/// Alias nazwy wyjścia.
pub type OutputName = SatelName;
/// Alias nazwy strefy.
pub type PartitionName = SatelName;

// --- Temperatura ---

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

// --- Dane pośrednie parserów ---

/// Dane o sabotażu wejść odczytane z centrali.
#[derive(Debug, Clone)]
pub struct ZonesTamperData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Dane o alarmach wejść odczytane z centrali.
#[derive(Debug, Clone)]
pub struct ZonesAlarmData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Dane o naruszeniach wejść odczytane z centrali.
#[derive(Debug, Clone)]
pub struct ZonesViolationData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Dane o alarmach sabotażowych wejść odczytane z centrali.
#[derive(Debug, Clone)]
pub struct ZonesTamperAlarmData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Dane o pamięci alarmów wejść odczytane z centrali.
#[derive(Debug, Clone)]
pub struct ZonesAlarmMemoryData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Dane o pamięci alarmów sabotażowych wejść odczytane z centrali.
#[derive(Debug, Clone)]
pub struct ZonesTamperAlarmMemoryData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Dane o blokadach wejść odczytane z centrali.
#[derive(Debug, Clone)]
pub struct ZonesBypassData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Dane o awariach "brak naruszenia" wejść odczytane z centrali.
#[derive(Debug, Clone)]
pub struct ZonesNoViolationTroubleData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Dane o awariach "długie naruszenie" wejść odczytane z centrali.
#[derive(Debug, Clone)]
pub struct ZonesLongViolationTroubleData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Dane o stanie stref odczytane z centrali.
#[derive(Debug, Clone)]
pub struct PartitionsData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Alias dla zachowania kompatybilności.
pub type PartitionsArmedData = PartitionsData;

/// Dane o stanie wyjść odczytane z centrali.
#[derive(Debug, Clone)]
pub struct OutputsStateData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

// --- Zagregowane struktury dla użytkownika ---

/// Zagregowany status pojedynczego wejścia.
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

// --- Struktury wejść, stref i wyjść ---

/// Ujednolicona struktura Wejścia (Zone).
#[derive(Debug, Clone)]
pub struct Zone {
    pub id: u16,
    pub zone_name: String,
    pub zone_name_read_at: DateTime<Local>,
    pub temperature_value: f32,
    pub temperature_read_at: DateTime<Local>,
    pub temperature_status: TemperatureSensorStatus,
    pub temperature_timeout_errors_total: u32,
    pub temperature_sensor_errors_total: u32,
    pub temperature_timeout_errors_current: u32,
    pub temperature_sensor_errors_current: u32,
    pub tamper_state: bool,
    pub tamper_read_at: DateTime<Local>,
    pub alarm_state: bool,
    pub alarm_read_at: DateTime<Local>,
    pub violation_state: bool,
    pub violation_read_at: DateTime<Local>,
    pub tamper_alarm_state: bool,
    pub tamper_alarm_read_at: DateTime<Local>,
    pub alarm_memory_state: bool,
    pub alarm_memory_read_at: DateTime<Local>,
    pub tamper_alarm_memory_state: bool,
    pub tamper_alarm_memory_read_at: DateTime<Local>,
    pub bypass_state: bool,
    pub bypass_read_at: DateTime<Local>,
    pub no_violation_trouble_state: bool,
    pub no_violation_trouble_read_at: DateTime<Local>,
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
    pub armed_suppressed: bool,
    pub armed_suppressed_at: DateTime<Local>,
    pub armed_really: bool,
    pub armed_really_at: DateTime<Local>,
    pub alarm: bool,
    pub alarm_at: DateTime<Local>,
    pub alarm_memory: bool,
    pub alarm_memory_at: DateTime<Local>,
    pub entry_time: bool,
    pub entry_time_at: DateTime<Local>,
    pub exit_time_gt_10s: bool,
    pub exit_time_gt_10s_at: DateTime<Local>,
    pub exit_time_lt_10s: bool,
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
    pub state: bool,
    pub state_read_at: DateTime<Local>,
}

impl Output {
    pub fn new(id: u16) -> Self {
        let now = Local::now();
        Self {
            id,
            name: String::new(),
            name_read_at: now,
            state: false,
            state_read_at: now,
        }
    }

    pub fn to_output_name(&self) -> OutputName {
        OutputName {
            name: self.name.clone(),
            read_at: self.name_read_at,
        }
    }
}

// --- Awarie i status systemu ---

/// Typy awarii systemu Satel Integra.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TroubleType {
    OutTrouble(u8),
    MainBoardAcLoss,
    MainBoardBatteryLow,
    MainBoardBatteryMissing,
    MainBoardOutOverload,
    TelephoneLineTrouble,
    RtcLoss,
    PrinterTrouble,
    MainBoardDataBusError,
    ExpanderAcLoss(u8),
    ExpanderBatteryLow(u8),
    ExpanderBatteryMissing(u8),
    ExpanderOutOverload(u8),
    ExpanderDataBusError(u8),
    EthmMonitoringStation1Error,
    EthmMonitoringStation2Error,
    EthmDloadxConnectionError,
    EthmSatelServerConnectionError,
    IntGsmSignalLoss,
    GsmMonitoringStation1Error,
    GsmMonitoringStation2Error,
    ServiceAccessBlocked,
    ZoneTrouble(u16),
    GenericTrouble { part: u8, bit: u8 },
}

/// Ogólny status systemu (z ramki 0x1A).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SystemStatus {
    pub service_mode: bool,
    pub troubles_present: bool,
    pub troubles_memory: bool,
    pub rtc: DateTime<Local>,
}

// --- Autoodczyt ---

/// Stan poszczególnego elementu autoodczytu.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoReadItemState {
    /// Aktywny i działający.
    Active,
    /// Nieżądany w konfiguracji.
    NotRequested,
    /// Nieobsługiwany przez sprzęt (wymagane 14 bajtów, dostępne 12).
    UnsupportedByHardware,
    /// Odrzucony przez centralę (błąd 0xEF).
    RejectedByPanel(u8),
}

impl AutoReadItemState {
    pub fn to_description(&self) -> String {
        match self {
            Self::Active => "Aktywny".to_string(),
            Self::NotRequested => "Nie żądano".to_string(),
            Self::UnsupportedByHardware => "Brak wsparcia w ETHM (wymagane 14B)".to_string(),
            Self::RejectedByPanel(code) => {
                let desc = match code {
                    0x01 => "Błąd: Kod użytkownika nieznany",
                    0x02 => "Błąd: Brak dostępu",
                    0x03 => "Błąd: Użytkownik nie istnieje",
                    0x04 => "Błąd: Użytkownik już istnieje",
                    0x05 => "Błąd: Błędny kod",
                    0x08 => "Błąd: Inny błąd centrali",
                    _ => "Błąd: Odrzucono (0xEF)",
                };
                format!("{} ({:02X})", desc, code)
            }
        }
    }
}

/// Status konkretnej kategorii autoodczytu.
#[derive(Debug, Clone)]
pub struct AutoReadItemStatus {
    pub name: String,
    pub state: AutoReadItemState,
}

/// Raport zbiorczy z konfiguracji autoodczytu.
#[derive(Debug, Clone)]
pub struct AutoReadReport {
    pub items: Vec<AutoReadItemStatus>,
    pub success_count: usize,
    pub total_requested: usize,
}

// --- Główny cache stanu ---

/// Struktura przechowująca współdzielony stan połączenia (zunifikowany cache).
#[derive(Debug)]
pub struct SatelState {
    pub connection_type: Option<ConnectionType>,
    pub telemetry: ConnectionTelemetry,
    pub integra_version: Option<IntegraVersion>,
    pub ethm_version: Option<EthmVersion>,
    pub zones: Vec<Zone>,
    pub outputs: Vec<Output>,
    pub partitions: Vec<Partition>,
    pub system_status: Option<SystemStatus>,
    pub troubles: [Vec<bool>; 8],
    pub troubles_memory: [Vec<bool>; 8],
}

impl Default for SatelState {
    fn default() -> Self {
        Self::new()
    }
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
            ethm_version: None,
            zones,
            outputs,
            partitions,
            system_status: None,
            troubles: Default::default(),
            troubles_memory: Default::default(),
        }
    }
}

/// Wątkobezpieczny uchwyt do stanu `SatelState`.
pub type SatelStateHandle = Arc<RwLock<SatelState>>;
