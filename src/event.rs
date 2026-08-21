use crate::command::SatelResult;
use crate::state::{
    AutoReadReport, ConnectionState, EthmVersion, IntegraVersion, SystemStatus, TroubleType,
};

/// Zdarzenia przesyłane przez system rozgłoszeniowy (broadcast).
/// Można je odbierać przez `SatelIntegra::subscribe()`.
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
    /// Zmiana stanu wyjścia (0x17).
    OutputChanged { id: u16, state: bool },
    /// Zmiana temperatury wejścia (0x7D).
    ZoneTemperatureChanged { id: u16, temperature: f32 },
    /// Odebrano nazwę wejścia (0xEE typ 1).
    ZoneNameReceived { id: u16, name: String },
    /// Odebrano nazwę wyjścia (0xEE typ 4).
    OutputNameReceived { id: u16, name: String },
    /// Odebrano nazwę strefy (0xEE typ 0).
    PartitionNameReceived { id: u16, name: String },
    /// Odebrano informacje o wersji centrali (0x7E).
    IntegraVersionReceived(IntegraVersion),
    /// Odebrano informacje o wersji modułu komunikacyjnego (0x7C).
    EthmVersionReceived(EthmVersion),
    /// Otrzymano kod wyniku operacji z panelu (0xEF).
    PanelMessage(SatelResult),
    /// Konfiguracja autoodczytu zakończona.
    AutoReadConfigured(AutoReadReport),
    /// Zmiana stanu awarii systemu.
    Trouble(TroubleType, bool),
    /// Zmiana stanu pamięci awarii systemu.
    TroubleMemory(TroubleType, bool),
    /// Zmiana ogólnego statusu systemu (0x1A).
    SystemStatusChanged(SystemStatus),
}
