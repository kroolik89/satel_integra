use serde::Deserialize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

// --- Nowe Struktury Konfiguracyjne ---

/// Główna, nadrzędna struktura konfiguracyjna.
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// Konfiguracja metody połączenia (TCP lub UART).
    pub connection: ConnectionConfig,

    /// Czas oczekiwania na odczyt danych w sekundach.
    #[serde(default = "default_read_timeout")]
    pub read_timeout: u64,

    /// Czas oczekiwania na zapis danych w sekundach.
    #[serde(default = "default_write_timeout")]
    pub write_timeout: u64,

    /// Opcjonalny kod użytkownika potrzebny do niektórych operacji.
    pub user_code: Option<String>,
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

/// Domyślna wartość dla baud_rate.
fn default_baud_rate() -> u32 {
    19200
}

/// Domyślna wartość dla timeoutu odczytu.
fn default_read_timeout() -> u64 {
    5
}

/// Domyślna wartość dla timeoutu zapisu.
fn default_write_timeout() -> u64 {
    5
}


// --- Istniejące Struktury Danych (bez zmian) ---

/// Status połączenia z centralą.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionStatus {
    Connected,
    Disconnected,
    ConnectionLost,
}

/// Typ używanego połączenia.
#[derive(Debug, Clone)]
pub enum ConnectionType {
    Tcp(String, u16),
    Uart(String),
}

/// Metryki dotyczące transmisji danych.
#[derive(Debug, Default)]
pub struct ConnectionMetrics {
    pub bytes_sent: AtomicUsize,
    pub bytes_received: AtomicUsize,
}

impl ConnectionMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&self) {
        self.bytes_sent.store(0, Ordering::Relaxed);
        self.bytes_received.store(0, Ordering::Relaxed);
    }
}

/// Struktura przechowująca współdzielony stan połączenia.
#[derive(Debug)]
pub struct SatelState {
    pub status: ConnectionStatus,
    pub connection_type: Option<ConnectionType>,
    pub metrics: ConnectionMetrics,
}

impl SatelState {
    pub fn new() -> Self {
        Self {
            status: ConnectionStatus::Disconnected,
            connection_type: None,
            metrics: ConnectionMetrics::new(),
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
        // ... (implementacja from_byte bez zmian)
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
