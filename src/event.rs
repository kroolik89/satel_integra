use serde::{Deserialize, Serialize};

use crate::command::SatelResult;
use crate::state::{
    AutoReadReport, CmeSource, ConnectionState, ConnectionStatistics, EthmVersion, IntegraVersion,
    SystemStatus, TemperatureSensorStatus, TroubleType,
};

/// Category of panel elements being synchronized in batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SyncCategory {
    /// Alarm zones (inputs) 1..=io_count
    Zones,
    /// Programmable outputs 1..=io_count
    Outputs,
    /// Security partitions 1..=32
    Partitions,
    /// Temperature sensors 1..=io_count
    Temperatures,
}

impl std::fmt::Display for SyncCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncCategory::Zones => write!(f, "zones"),
            SyncCategory::Outputs => write!(f, "outputs"),
            SyncCategory::Partitions => write!(f, "partitions"),
            SyncCategory::Temperatures => write!(f, "temperatures"),
        }
    }
}

/// Events broadcasted in real time to subscribers.
/// Can be received via `SatelIntegra::subscribe_events()`.
#[derive(Debug, Clone)]
pub enum SatelEvent {
    /// Connection state transition.
    ConnectionChanged(ConnectionState),
    /// Connection telemetry and error statistics snapshot.
    ConnectionStatistics(ConnectionStatistics),
    /// Zone violation state changed (0x00).
    ZoneViolation { id: u16, state: bool },
    /// Zone tamper state changed (0x01).
    ZoneTamper { id: u16, state: bool },
    /// Zone alarm state changed (0x02).
    ZoneAlarm { id: u16, state: bool },
    /// Zone tamper alarm state changed (0x03).
    ZoneTamperAlarm { id: u16, state: bool },
    /// Zone alarm memory state changed (0x04).
    ZoneAlarmMemory { id: u16, state: bool },
    /// Zone tamper alarm memory state changed (0x05).
    ZoneTamperAlarmMemory { id: u16, state: bool },
    /// Zone bypass state changed (0x06).
    ZoneBypass { id: u16, state: bool },
    /// Zone 'no violation trouble' state changed (0x07).
    ZoneNoViolationTrouble { id: u16, state: bool },
    /// Zone 'long violation trouble' state changed (0x08).
    ZoneLongViolationTrouble { id: u16, state: bool },
    /// Partition suppressed arm state changed (0x09).
    PartitionArmed { id: u16, state: bool },
    /// Partition real arm state changed (0x0A).
    PartitionArmedReally { id: u16, state: bool },
    /// Partition alarm state changed (0x13).
    PartitionAlarm { id: u16, state: bool },
    /// Partition alarm memory state changed (0x15).
    PartitionAlarmMemory { id: u16, state: bool },
    /// Partition entry countdown time state changed (0x0E).
    PartitionEntryTime { id: u16, state: bool },
    /// Partition exit countdown time (>10s) state changed (0x0F).
    PartitionExitTimeGt10s { id: u16, state: bool },
    /// Partition exit countdown time (<10s) state changed (0x10).
    PartitionExitTimeLt10s { id: u16, state: bool },
    /// Output state changed (0x17).
    OutputChanged { id: u16, state: bool },
    /// Zone temperature sensor reading updated (0x7D).
    ZoneTemperatureChanged { id: u16, temperature: f32 },
    /// Zone temperature sensor fault error (timeout, missing, sensor error, blocked).
    ZoneTemperatureError { id: u16, status: TemperatureSensorStatus },
    /// Zone UTF-8 name received (0xEE type 1).
    ZoneNameReceived { id: u16, name: String },
    /// Output UTF-8 name received (0xEE type 4).
    OutputNameReceived { id: u16, name: String },
    /// Partition UTF-8 name received (0xEE type 0).
    PartitionNameReceived { id: u16, name: String },
    /// Integra panel model and firmware version received (0x7E).
    IntegraVersionReceived(IntegraVersion),
    /// ETHM/UART communication module version received (0x7C).
    EthmVersionReceived(EthmVersion),
    /// Panel command result code received (0xEF).
    PanelMessage(SatelResult),
    /// Auto-read push notification categories configured (0x7F).
    AutoReadConfigured(AutoReadReport),
    /// System hardware trouble state changed.
    Trouble(TroubleType, bool),
    /// System trouble memory state changed.
    TroubleMemory(TroubleType, bool),
    /// ACU-100 module jamming level updated.
    AcuJamLevel { module: u8, level: u8 },
    /// GSM modem CME error reported.
    /// `code` is the raw BCD value directly from the frame (e.g., 0x0123 means code 123).
    /// `code == 0` means no error.
    CmeError { source: CmeSource, sim: u8, code: u16, memory: bool },
    /// System status bits updated (0x1A).
    SystemStatusChanged(SystemStatus),
    /// Batch name sync started for a category (zones, outputs, partitions).
    SyncStarted {
        category: SyncCategory,
        total: u16,
    },
    /// Batch name sync progress for a single item.
    SyncProgress {
        category: SyncCategory,
        current: u16,
        total: u16,
        name: String,
    },
    /// Batch name sync finished for a category (with success count and optional error).
    SyncFinished {
        category: SyncCategory,
        total: u16,
        success_count: u16,
        error: Option<String>,
    },
    /// Configuration has been dynamically updated in place.
    ConfigUpdated,
}
