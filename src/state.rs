use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};

pub use crate::output_catalog::{OutputControl, OutputFunction};
pub use crate::partition_catalog::{
    AutoArmDeferStatus, AutoArmDeferTimer, DependentPartitions, PartitionOptions, PartitionType,
};
pub use crate::zone_catalog::{ZoneKind, ZoneReaction};

// --- Connection ---

/// Connection state machine variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConnectionState {
    Connected,
    #[default]
    Disconnected,
    ConnectionLost,
    Connecting,
    Handshake,
}

/// Connection status containing current state, retry count, and activity timestamps.
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

/// Active transport connection type.
#[derive(Debug, Clone)]
pub enum ConnectionType {
    Tcp(String, u16),
    Uart(String),
}

/// Public snapshot of connection statistics and telemetry counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionStatistics {
    pub state: ConnectionState,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub connections_established: u64,
    pub reconnect_attempts: u64,
    pub connections_lost: u64,
    pub timeouts: u64,
    pub crc_errors: u64,
    pub rejected_by_panel: u64,
    pub io_errors: u64,
    pub connected_since: Option<DateTime<Local>>,
    pub total_connected: Duration,
    pub taken_at: DateTime<Local>,
}

/// Threshold of non-ping frames required to trigger a periodic statistics event.
pub const STATS_MIN_NON_PING_FRAMES: u64 = 5;

/// Connection telemetry state mark used to decide whether periodic statistics should be emitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StatsMark {
    pub non_ping_frames: u64,
    pub connections_established: u64,
    pub reconnect_attempts: u64,
    pub connections_lost: u64,
    pub timeouts: u64,
    pub crc_errors: u64,
    pub rejected_by_panel: u64,
    pub io_errors: u64,
}

/// Evaluates whether connection statistics have meaningfully changed between two marks.
/// Returns true if non-ping frames increased by at least 5 or any other connection/error counter changed.
pub fn statistics_changed(prev: &StatsMark, now: &StatsMark) -> bool {
    now.non_ping_frames.saturating_sub(prev.non_ping_frames) >= STATS_MIN_NON_PING_FRAMES
        || prev.connections_established != now.connections_established
        || prev.reconnect_attempts != now.reconnect_attempts
        || prev.connections_lost != now.connections_lost
        || prev.timeouts != now.timeouts
        || prev.crc_errors != now.crc_errors
        || prev.rejected_by_panel != now.rejected_by_panel
        || prev.io_errors != now.io_errors
}

/// Data transmission telemetry and connection health counters.
#[derive(Debug, Clone)]
pub struct ConnectionTelemetry {
    pub status: ConnectionStatus,
    pub last_connected_at: Option<SystemTime>,
    pub last_send_at: Instant,
    pub bytes_sent: Arc<AtomicU64>,
    pub bytes_received: Arc<AtomicU64>,
    pub connections_established: Arc<AtomicU64>,
    pub reconnect_attempts: Arc<AtomicU64>,
    pub connections_lost: Arc<AtomicU64>,
    pub timeouts: Arc<AtomicU64>,
    pub crc_errors: Arc<AtomicU64>,
    pub rejected_by_panel: Arc<AtomicU64>,
    pub io_errors: Arc<AtomicU64>,
    pub non_ping_frames: Arc<AtomicU64>,
    pub connected_since: Option<DateTime<Local>>,
    pub total_connected_before: Duration,
    pub last_sent_stats_mark: StatsMark,
}

impl Default for ConnectionTelemetry {
    fn default() -> Self {
        Self {
            status: ConnectionStatus::default(),
            last_connected_at: None,
            last_send_at: Instant::now(),
            bytes_sent: Arc::new(AtomicU64::new(0)),
            bytes_received: Arc::new(AtomicU64::new(0)),
            connections_established: Arc::new(AtomicU64::new(0)),
            reconnect_attempts: Arc::new(AtomicU64::new(0)),
            connections_lost: Arc::new(AtomicU64::new(0)),
            timeouts: Arc::new(AtomicU64::new(0)),
            crc_errors: Arc::new(AtomicU64::new(0)),
            rejected_by_panel: Arc::new(AtomicU64::new(0)),
            io_errors: Arc::new(AtomicU64::new(0)),
            non_ping_frames: Arc::new(AtomicU64::new(0)),
            connected_since: None,
            total_connected_before: Duration::ZERO,
            last_sent_stats_mark: StatsMark::default(),
        }
    }
}

impl ConnectionTelemetry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn stats_mark(&self) -> StatsMark {
        StatsMark {
            non_ping_frames: self.non_ping_frames.load(Ordering::Relaxed),
            connections_established: self.connections_established.load(Ordering::Relaxed),
            reconnect_attempts: self.reconnect_attempts.load(Ordering::Relaxed),
            connections_lost: self.connections_lost.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
            crc_errors: self.crc_errors.load(Ordering::Relaxed),
            rejected_by_panel: self.rejected_by_panel.load(Ordering::Relaxed),
            io_errors: self.io_errors.load(Ordering::Relaxed),
        }
    }

    pub fn reset(&mut self) {
        self.bytes_sent.store(0, Ordering::Relaxed);
        self.bytes_received.store(0, Ordering::Relaxed);
        self.connections_established.store(0, Ordering::Relaxed);
        self.reconnect_attempts.store(0, Ordering::Relaxed);
        self.connections_lost.store(0, Ordering::Relaxed);
        self.timeouts.store(0, Ordering::Relaxed);
        self.crc_errors.store(0, Ordering::Relaxed);
        self.rejected_by_panel.store(0, Ordering::Relaxed);
        self.io_errors.store(0, Ordering::Relaxed);
        self.non_ping_frames.store(0, Ordering::Relaxed);
        self.total_connected_before = Duration::ZERO;
        self.last_sent_stats_mark = StatsMark::default();
        if self.status.state == ConnectionState::Connected {
            self.connected_since = Some(Local::now());
        } else {
            self.connected_since = None;
        }
    }

    pub fn statistics(&self) -> ConnectionStatistics {
        let taken_at = Local::now();
        let current_session = if self.status.state == ConnectionState::Connected {
            self.connected_since
                .map(|since| (taken_at - since).to_std().unwrap_or(Duration::ZERO))
                .unwrap_or(Duration::ZERO)
        } else {
            Duration::ZERO
        };
        let total_connected = self.total_connected_before.saturating_add(current_session);
        let connected_since = if self.status.state == ConnectionState::Connected {
            self.connected_since
        } else {
            None
        };

        ConnectionStatistics {
            state: self.status.state,
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            connections_established: self.connections_established.load(Ordering::Relaxed),
            reconnect_attempts: self.reconnect_attempts.load(Ordering::Relaxed),
            connections_lost: self.connections_lost.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
            crc_errors: self.crc_errors.load(Ordering::Relaxed),
            rejected_by_panel: self.rejected_by_panel.load(Ordering::Relaxed),
            io_errors: self.io_errors.load(Ordering::Relaxed),
            connected_since,
            total_connected,
            taken_at,
        }
    }
}

// --- Version info ---

/// Integra alarm panel version and model information.
#[derive(Debug, Clone)]
pub struct IntegraVersion {
    pub model: String,
    pub firmware_version: String,
    pub language: String,
    pub stored_in_flash: bool,
    pub io_count: u16,
    /// Maksymalna liczba stref (partycji) dla modelu; 0 = model nieznany.
    pub partition_count: u16,
    pub read_at: DateTime<Local>,
}

/// Capabilities and features supported by the ETHM module (from 0x7C frame).
#[derive(Debug, Clone, Copy)]
pub struct EthmCapabilities {
    /// Bit 0: Support for expanded 32-byte frames (256 zones/outputs).
    pub support_32_byte_frames: bool,
    /// Bit 1: Support for 8 trouble groups and 14-byte 0x7F mask.
    pub support_8_troubles_groups: bool,
    /// Bit 2: Support for extended arming commands.
    pub support_extended_arming_commands: bool,
    pub reserved_bit3: bool,
    pub reserved_bit4: bool,
    pub reserved_bit5: bool,
    pub reserved_bit6: bool,
    pub reserved_bit7: bool,
}

/// ETHM / UART communication module version information (from 0x7C frame).
#[derive(Debug, Clone)]
pub struct EthmVersion {
    pub version_raw: String,
    pub capabilities: EthmCapabilities,
    pub read_at: DateTime<Local>,
}

// --- Names ---

/// Name structure for zones, outputs, or partitions with retrieval timestamp.
#[derive(Debug, Clone)]
pub struct SatelName {
    pub name: String,
    pub read_at: DateTime<Local>,
}

/// Alias for zone name.
pub type ZoneName = SatelName;
/// Alias for output name.
pub type OutputName = SatelName;
/// Alias for partition name.
pub type PartitionName = SatelName;

// --- Parameters ---

/// Detailed parameters of a zone (reaction type, partition assignment).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZoneParams {
    pub zone_id: u16,
    pub reaction: ZoneReaction,
    pub partition: Option<u8>,
    pub read_at: DateTime<Local>,
}

/// Detailed parameters of an output (function, operating duration, control capability).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputParams {
    pub output_id: u16,
    pub function: OutputFunction,
    pub duration: Option<Duration>,
    pub control: OutputControl,
    pub read_at: DateTime<Local>,
}

/// Detailed parameters of a partition (partition type, object assignment, options, timers, dependencies).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionParams {
    pub partition_id: u16,
    pub partition_type: PartitionType,
    pub object_number: Option<u8>,
    pub options: Option<PartitionOptions>,
    pub auto_arm_defer: Option<AutoArmDeferTimer>,
    pub dependent_partitions: Option<DependentPartitions>,
    pub read_at: DateTime<Local>,
}

// --- Temperature ---

/// Status and health state of a zone temperature probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TemperatureSensorStatus {
    #[default]
    NoRead,                 // Initial state before first query
    Ok,                     // Healthy reading received
    SensorMissing,          // Missing sensor or query timeout (during retries)
    CommunicationError,     // Communication failure or 0xFFFF (during retries)
    BlockSensorMissing,     // Blocked in RAM after exceeding timeout threshold
    BlockCommunicationError,// Blocked in RAM after exceeding sensor error threshold
    RetryRead,              // Po odblokowaniu, czeka na próbę odczytu
}

/// Zone temperature reading with retrieval timestamp.
#[derive(Debug, Clone)]
pub struct ZoneTemperature {
    pub zone_id: u16,
    pub temperature: f32,
    pub read_at: DateTime<Local>,
}

// --- Parser intermediate raw data structures ---

/// Zone tamper data parsed from the panel.
#[derive(Debug, Clone)]
pub struct ZonesTamperData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Zone alarm data parsed from the panel.
#[derive(Debug, Clone)]
pub struct ZonesAlarmData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Zone violation data parsed from the panel.
#[derive(Debug, Clone)]
pub struct ZonesViolationData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Zone tamper alarm data parsed from the panel.
#[derive(Debug, Clone)]
pub struct ZonesTamperAlarmData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Zone alarm memory data parsed from the panel.
#[derive(Debug, Clone)]
pub struct ZonesAlarmMemoryData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Zone tamper alarm memory data parsed from the panel.
#[derive(Debug, Clone)]
pub struct ZonesTamperAlarmMemoryData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Zone bypass data parsed from the panel.
#[derive(Debug, Clone)]
pub struct ZonesBypassData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Zone 'no violation trouble' data parsed from the panel.
#[derive(Debug, Clone)]
pub struct ZonesNoViolationTroubleData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Zone 'long violation trouble' data parsed from the panel.
#[derive(Debug, Clone)]
pub struct ZonesLongViolationTroubleData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Partition states data parsed from the panel.
#[derive(Debug, Clone)]
pub struct PartitionsData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Alias for backwards compatibility.
pub type PartitionsArmedData = PartitionsData;

/// Output states data parsed from the panel.
#[derive(Debug, Clone)]
pub struct OutputsStateData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

// --- Aggregated public models ---

/// Aggregated diagnostic status for a single zone.
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

// --- Zone, Partition, Output unified entities ---

/// Unified Zone representation containing all cached states and telemetry.
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
    pub temperature_blocked_cycles: u32,
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
    pub reaction: Option<ZoneReaction>,
    pub partition: Option<u8>,
    pub params_read_at: Option<DateTime<Local>>,
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
            temperature_blocked_cycles: 0,
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
            reaction: None,
            partition: None,
            params_read_at: None,
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

    pub fn to_zone_params(&self) -> Option<ZoneParams> {
        let reaction = self.reaction?;
        Some(ZoneParams {
            zone_id: self.id,
            reaction,
            partition: self.partition,
            read_at: self.params_read_at.unwrap_or(self.zone_name_read_at),
        })
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
    pub partition_type: Option<PartitionType>,
    pub object_number: Option<u8>,
    pub options: Option<PartitionOptions>,
    pub auto_arm_defer: Option<AutoArmDeferTimer>,
    pub dependent_partitions: Option<DependentPartitions>,
    pub params_read_at: Option<DateTime<Local>>,
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
            partition_type: None,
            object_number: None,
            options: None,
            auto_arm_defer: None,
            dependent_partitions: None,
            params_read_at: None,
        }
    }

    pub fn to_partition_name(&self) -> PartitionName {
        PartitionName {
            name: self.name.clone(),
            read_at: self.name_read_at,
        }
    }

    pub fn to_partition_params(&self) -> Option<PartitionParams> {
        let partition_type = self.partition_type?;
        Some(PartitionParams {
            partition_id: self.id,
            partition_type,
            object_number: self.object_number,
            options: self.options,
            auto_arm_defer: self.auto_arm_defer,
            dependent_partitions: self.dependent_partitions,
            read_at: self.params_read_at.unwrap_or(self.name_read_at),
        })
    }
}

/// Unified Output structure.
#[derive(Debug, Clone)]
pub struct Output {
    pub id: u16,
    pub name: String,
    pub name_read_at: DateTime<Local>,
    pub state: bool,
    pub state_read_at: DateTime<Local>,
    pub function: Option<OutputFunction>,
    pub duration: Option<Duration>,
    pub control: Option<OutputControl>,
    pub params_read_at: Option<DateTime<Local>>,
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
            function: None,
            duration: None,
            control: None,
            params_read_at: None,
        }
    }

    pub fn to_output_name(&self) -> OutputName {
        OutputName {
            name: self.name.clone(),
            read_at: self.name_read_at,
        }
    }

    pub fn to_output_params(&self) -> Option<OutputParams> {
        let function = self.function?;
        let control = self.control.clone().unwrap_or_else(|| function.control_from_duration(self.duration));
        Some(OutputParams {
            output_id: self.id,
            function,
            duration: self.duration,
            control,
            read_at: self.params_read_at.unwrap_or(self.name_read_at),
        })
    }
}

// --- 1:1 Trouble Data Structures (Parts 1..8) ---

/// Main control panel board troubles (from Part 1 frame).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MainBoardTroubles {
    pub out1_trouble: bool,
    pub out2_trouble: bool,
    pub out3_trouble: bool,
    pub out4_trouble: bool,
    pub kpd_power_trouble: bool,
    pub ex1_ex2_power_trouble: bool,
    pub battery_trouble: bool,
    pub ac_trouble: bool,
    pub dt1_trouble: bool,
    pub dt2_trouble: bool,
    pub dtm_trouble: bool,
    pub rtc_trouble: bool,
    pub no_dtr_signal: bool,
    pub no_battery_present: bool,
    pub external_modem_init_trouble: bool,
    pub external_modem_cmd_trouble: bool,
    pub tel_line_no_voltage: bool,
    pub tel_line_bad_signal: bool,
    pub tel_line_no_signal: bool,
    pub monitoring_station_1_trouble: bool,
    pub monitoring_station_2_trouble: bool,
    pub eeprom_rtc_trouble: bool,
    pub ram_trouble: bool,
    pub main_panel_restart: bool,
}

/// ETHM-1 / INT-GSM / PTSA communication module troubles (from Part 1 frame).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EthmPtsaTroubles {
    pub ethm_ping_trouble: bool,
    pub server_id_error: bool,
    pub no_server_connection: bool,
    pub no_ethm_mon_station_1: bool,
    pub no_ethm_mon_station_2: bool,
    pub no_gprs_mon_station_1: bool,
    pub no_gprs_mon_station_2: bool,
    pub time_server_trouble: bool,
    pub gsm_init_error: bool,
    pub ip_mon_station_1_trouble: bool,
    pub ip_mon_station_2_trouble: bool,
}

/// Parsed trouble frame for Part 1 (0x1B / 0x20 - 47 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TroublesPart1Data {
    pub is_memory: bool,
    pub technical_zones: Vec<bool>,
    pub expanders_ac: Vec<bool>,
    pub expanders_battery: Vec<bool>,
    pub expanders_no_battery: Vec<bool>,
    pub main_board: MainBoardTroubles,
    pub ethm_ptsa: EthmPtsaTroubles,
    pub read_at: DateTime<Local>,
}

/// Parsed trouble frame for Part 2 (0x1C / 0x21 - 26 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TroublesPart2Data {
    pub is_memory: bool,
    pub card_readers_head_a_or_synchro: Vec<bool>,
    pub card_readers_head_b_or_charging: Vec<bool>,
    pub expanders_supply_overload: Vec<bool>,
    pub acu_jammed_or_short_circuit: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Parsed trouble memory frame for Part 2 (0x21 - 39 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TroublesMemoryPart2Data {
    pub card_readers_head_a_or_synchro: Vec<bool>,
    pub card_readers_head_b_or_charging: Vec<bool>,
    pub expanders_supply_overload: Vec<bool>,
    pub acu_jammed_or_short_circuit: Vec<bool>,
    pub keypad_restart: Vec<bool>,
    pub expander_restart: Vec<bool>,
    pub sim_cme_error: u16,
    pub sim_cme_error_memory: u16,
    pub read_at: DateTime<Local>,
}

/// Parsed trouble frame for Part 3 (0x1D / 0x22 - 60 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TroublesPart3Data {
    pub is_memory: bool,
    pub acu_jam_levels: Vec<u8>,
    pub wireless_devices_low_battery: Vec<bool>,
    pub wireless_devices_no_comm: Vec<bool>,
    pub wireless_outputs_no_comm: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Parsed trouble memory frame for Part 3 (0x22 - 60 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TroublesMemoryPart3Data {
    pub acu_jammed_or_short_circuit: Vec<bool>,
    pub acu_jammed_or_short_circuit_memory: Vec<bool>,
    pub wireless_devices_low_battery: Vec<bool>,
    pub wireless_devices_no_comm: Vec<bool>,
    pub wireless_outputs_no_comm: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Parsed trouble frame for Part 4 (0x1E / 0x23 - 30 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TroublesPart4Data {
    pub is_memory: bool,
    pub expanders_no_comm: Vec<bool>,
    pub expanders_substituted: Vec<bool>,
    pub keypads_no_comm: Vec<bool>,
    pub keypads_substituted: Vec<bool>,
    pub ethm_no_lan_or_intrs_no_dsr: Vec<bool>,
    pub expanders_tamper: Vec<bool>,
    pub keypads_tamper: Vec<bool>,
    pub keypad_init_errors: Vec<bool>,
    pub auxiliary_stm_troubles: u8,
    pub read_at: DateTime<Local>,
}

/// Parsed trouble frame for Part 5 (0x1F / 0x24 - 31 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TroublesPart5Data {
    pub is_memory: bool,
    pub masters_key_fobs_low_battery: Vec<bool>,
    pub users_key_fobs_low_battery: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Parsed trouble memory frame for Part 5 (0x24 - 48 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TroublesMemoryPart5Data {
    pub zone_long_violation: Vec<bool>,
    pub zone_no_violation: Vec<bool>,
    pub zone_tamper: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Parsed trouble frame for Part 6 (0x2C / 0x2E - 45 bytes - Integra 256).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TroublesPart6Data {
    pub is_memory: bool,
    pub wireless_devices_low_battery: Vec<bool>,
    pub wireless_devices_no_comm: Vec<bool>,
    pub wireless_outputs_no_comm: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Parsed trouble frame for Part 7 (0x2D / 0x2F - 47 bytes - Integra 256).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TroublesPart7Data {
    pub is_memory: bool,
    pub technical_zones: Vec<bool>,
    pub technical_zones_memory: Vec<bool>,
    pub acu_jam_levels: Vec<u8>,
    pub read_at: DateTime<Local>,
}

/// Parsed trouble memory frame for Part 7 (0x2F - 48 bytes - Integra 256).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TroublesMemoryPart7Data {
    pub zone_long_violation: Vec<bool>,
    pub zone_no_violation: Vec<bool>,
    pub zone_tamper: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Detailed troubles for an individual INT-GSM module (from Part 8 frame).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GsmModuleTroubles {
    pub module_address: u8,
    pub no_ethm_mon_station_1: bool,
    pub no_ethm_mon_station_2: bool,
    pub no_gprs_sim1_mon_station_1: bool,
    pub no_gprs_sim1_mon_station_2: bool,
    pub no_gprs_sim2_mon_station_1: bool,
    pub no_gprs_sim2_mon_station_2: bool,
    pub no_sms_sim1_mon_station_1: bool,
    pub no_sms_sim1_mon_station_2: bool,
    pub no_sms_sim2_mon_station_1: bool,
    pub no_sms_sim2_mon_station_2: bool,
    pub wrong_sim1_pin: bool,
    pub wrong_sim2_pin: bool,
    pub sim1_logging_error: bool,
    pub sim2_logging_error: bool,
    pub sim1_credit_low: bool,
    pub sim2_credit_low: bool,
    pub sim1_sms_error: bool,
    pub sim2_sms_error: bool,
    pub gsm_jamming: bool,
    pub settings_crc_error: bool,
    pub missing_module: bool,
    pub changed_module: bool,
    pub satel_server_conn_error: bool,
    pub mail_server_conn_error: bool,
    pub ntp_server_conn_error: bool,
    pub sim1_cme_error: u16,
    pub sim2_cme_error: u16,
}

/// Parsed trouble frame for Part 8 (0x30 / 0x31 - 64 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TroublesPart8Data {
    pub is_memory: bool,
    pub gsm_modules: Vec<GsmModuleTroubles>,
    pub read_at: DateTime<Local>,
}

/// Universal enum representing any decoded trouble command frame (Parts 1..8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TroublesData {
    Part1(TroublesPart1Data),
    Part2(TroublesPart2Data),
    MemoryPart2(TroublesMemoryPart2Data),
    Part3(TroublesPart3Data),
    MemoryPart3(TroublesMemoryPart3Data),
    Part4(TroublesPart4Data),
    Part5(TroublesPart5Data),
    MemoryPart5(TroublesMemoryPart5Data),
    Part6(TroublesPart6Data),
    Part7(TroublesPart7Data),
    MemoryPart7(TroublesMemoryPart7Data),
    Part8(TroublesPart8Data),
}

/// Source of CME error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CmeSource {
    GsmModule(u8),
    Panel,
}

/// System trouble variants for Satel Integra panels (matching 100% of protocol spec).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TroubleType {
    // --- Main panel board ---
    MainBoardAcLoss,
    MainBoardBatteryLow,
    MainBoardBatteryMissing,
    MainBoardOutOverload(u8),
    MainBoardKpdPowerOverload,
    MainBoardExPowerOverload,
    MainBoardDataBusDt1,
    MainBoardDataBusDt2,
    MainBoardDataBusDtm,
    RtcLoss,
    NoDtrSignal,
    ExternalModemInitTrouble,
    ExternalModemCmdTrouble,
    TelephoneLineNoVoltage,
    TelephoneLineBadSignal,
    TelephoneLineNoSignal,
    MonitoringStation1Trouble,
    MonitoringStation2Trouble,
    EepromRtcTrouble,
    RamMemoryError,
    MainPanelRestartMemory,

    // --- Technical zones ---
    TechnicalZoneTrouble(u16),

    // --- Expanders (1..64) ---
    ExpanderAcLoss(u8),
    ExpanderBatteryLow(u8),
    ExpanderBatteryMissing(u8),
    ExpanderSupplyOverload(u8),
    ExpanderNoComm(u8),
    ExpanderSubstituted(u8),
    ExpanderTamper(u8),
    ExpanderCardReaderHeadA(u8),
    ExpanderCardReaderHeadB(u8),
    ExpanderAcuJammedOrShortCircuit(u8),

    // --- Keypads (1..8) ---
    KeypadNoComm(u8),
    KeypadSubstituted(u8),
    KeypadTamper(u8),
    KeypadInitError(u8),
    KeypadRestart(u8),

    // --- Communication modules (ETHM-1 / INT-GSM / PTSA) ---
    EthmNoLanCable(u8),
    EthmPingTrouble(u8),
    EthmServerIdError(u8),
    EthmSatelServerConnectionError(u8),
    EthmMonitoringStation1Error,
    EthmMonitoringStation2Error,
    GprsMonitoringStation1Error,
    GprsMonitoringStation2Error,
    IpMonitoringStation1Trouble,
    IpMonitoringStation2Trouble,
    TimeServerTrouble,
    GsmInitError,
    GsmEthmStation1Error(u8),
    GsmEthmStation2Error(u8),
    GsmGprsSim1Station1Error(u8),
    GsmGprsSim1Station2Error(u8),
    GsmGprsSim2Station1Error(u8),
    GsmGprsSim2Station2Error(u8),
    GsmSmsSim1Station1Error(u8),
    GsmSmsSim1Station2Error(u8),
    GsmSmsSim2Station1Error(u8),
    GsmSmsSim2Station2Error(u8),
    GsmSimPinError { module: u8, sim: u8 },
    GsmSimLoggingError { module: u8, sim: u8 },
    GsmSimCreditLow { module: u8, sim: u8 },
    GsmSimSmsError { module: u8, sim: u8 },
    GsmJamming(u8),
    GsmSettingsCrcError(u8),
    GsmModuleMissing(u8),
    GsmModuleChanged(u8),
    GsmServerConnError(u8),
    GsmMailServerConnError(u8),
    GsmNtpServerConnError(u8),

    // --- Wireless devices (ABAX / ABAX 2) ---
    WirelessDeviceLowBattery { zone_id: u16 },
    WirelessDeviceNoComm { zone_id: u16 },
    WirelessOutputNoComm { output_id: u16 },

    // --- Key fobs ---
    MasterKeyFobLowBattery(u8),
    UserKeyFobLowBattery { user_id: u16 },

    // --- Zone specific (memory) ---
    ZoneLongViolationTrouble(u16),
    ZoneNoViolationTrouble(u16),
    ZoneTamperTrouble(u16),

    // --- Expander Restarts ---
    ExpanderRestart(u8),

    // --- Other ---
    AuxiliaryStmTroubles,
    GenericTrouble { part: u8, bit: u16 },
}

impl TroubleType {
    pub fn to_description(&self) -> String {
        match self {
            Self::MainBoardAcLoss => "Main Board: AC Power Loss (230V)".to_string(),
            Self::MainBoardBatteryLow => "Main Board: Battery Low Voltage".to_string(),
            Self::MainBoardBatteryMissing => "Main Board: Battery Missing / Disconnected".to_string(),
            Self::MainBoardOutOverload(out) => format!("Main Board: Supply Output #{} Overload", out),
            Self::MainBoardKpdPowerOverload => "Main Board: Keypad Power Supply (+KPD) Overload".to_string(),
            Self::MainBoardExPowerOverload => "Main Board: Expander Power Supply (+EX1/+EX2) Overload".to_string(),
            Self::MainBoardDataBusDt1 => "Main Board: Data Bus DT1 Communication Error".to_string(),
            Self::MainBoardDataBusDt2 => "Main Board: Data Bus DT2 Communication Error".to_string(),
            Self::MainBoardDataBusDtm => "Main Board: Data Bus DTM Communication Error".to_string(),
            Self::RtcLoss => "Main Board: Real-Time Clock (RTC) Loss / Not Set".to_string(),
            Self::NoDtrSignal => "Main Board: No DTR Signal on RS-232 Port".to_string(),
            Self::ExternalModemInitTrouble => "Main Board: External Modem Initialization Error".to_string(),
            Self::ExternalModemCmdTrouble => "Main Board: External Modem Command Error".to_string(),
            Self::TelephoneLineNoVoltage => "Telephone Line: No Voltage".to_string(),
            Self::TelephoneLineBadSignal => "Telephone Line: Bad Signal".to_string(),
            Self::TelephoneLineNoSignal => "Telephone Line: No Dial Tone".to_string(),
            Self::MonitoringStation1Trouble => "Monitoring: Station 1 Transmission Fault".to_string(),
            Self::MonitoringStation2Trouble => "Monitoring: Station 2 Transmission Fault".to_string(),
            Self::EepromRtcTrouble => "Main Board: EEPROM / RTC Access Trouble".to_string(),
            Self::RamMemoryError => "Main Board: RAM Memory Error".to_string(),
            Self::MainPanelRestartMemory => "Main Board: Panel Restart Latched in Memory".to_string(),

            Self::TechnicalZoneTrouble(zone) => format!("Technical Zone #{:03}: Trouble Detected", zone),

            Self::ExpanderAcLoss(exp) => format!("Expander #{:02}: AC Power Loss", exp),
            Self::ExpanderBatteryLow(exp) => format!("Expander #{:02}: Battery Low Voltage", exp),
            Self::ExpanderBatteryMissing(exp) => format!("Expander #{:02}: Battery Missing", exp),
            Self::ExpanderSupplyOverload(exp) => format!("Expander #{:02}: Power Supply Overload", exp),
            Self::ExpanderNoComm(exp) => format!("Expander #{:02}: No Communication", exp),
            Self::ExpanderSubstituted(exp) => format!("Expander #{:02}: Substituted / Unknown Hardware", exp),
            Self::ExpanderTamper(exp) => format!("Expander #{:02}: Tamper / Sabotage", exp),
            Self::ExpanderCardReaderHeadA(exp) => format!("Expander #{:02}: Card Reader Head A / Synchro Trouble", exp),
            Self::ExpanderCardReaderHeadB(exp) => format!("Expander #{:02}: Card Reader Head B / Charging Trouble", exp),
            Self::ExpanderAcuJammedOrShortCircuit(exp) => format!("Expander #{:02}: Jammed / Addressable Loop Short Circuit", exp),

            Self::KeypadNoComm(kpd) => format!("Keypad #{:02}: No Communication", kpd),
            Self::KeypadSubstituted(kpd) => format!("Keypad #{:02}: Substituted Keypad", kpd),
            Self::KeypadTamper(kpd) => format!("Keypad #{:02}: Tamper / Sabotage", kpd),
            Self::KeypadInitError(kpd) => format!("Keypad #{:02}: Initialization Error", kpd),
            Self::KeypadRestart(kpd) => format!("Keypad #{:02}: Restart Latched", kpd),

            Self::EthmNoLanCable(mod_id) => format!("ETHM-1 #{:02}: Ethernet LAN Cable Unplugged", mod_id),
            Self::EthmPingTrouble(mod_id) => format!("ETHM-1 #{:02}: Ping Network Test Failed", mod_id),
            Self::EthmServerIdError(mod_id) => format!("ETHM-1 #{:02}: SATEL Server MAC/ID Verification Error", mod_id),
            Self::EthmSatelServerConnectionError(mod_id) => format!("ETHM-1 #{:02}: No Connection to SATEL Server", mod_id),
            Self::EthmMonitoringStation1Error => "ETHM-1: Monitoring Station 1 Connection Error".to_string(),
            Self::EthmMonitoringStation2Error => "ETHM-1: Monitoring Station 2 Connection Error".to_string(),
            Self::GprsMonitoringStation1Error => "INT-GSM: GPRS Monitoring Station 1 Error".to_string(),
            Self::GprsMonitoringStation2Error => "INT-GSM: GPRS Monitoring Station 2 Error".to_string(),
            Self::IpMonitoringStation1Trouble => "IP Monitoring: Station 1 Communication Trouble".to_string(),
            Self::IpMonitoringStation2Trouble => "IP Monitoring: Station 2 Communication Trouble".to_string(),
            Self::TimeServerTrouble => "Network: NTP Time Synchronization Server Error".to_string(),
            Self::GsmInitError => "INT-GSM: GSM Module Initialization Error".to_string(),

            Self::GsmEthmStation1Error(addr) => format!("INT-GSM (Addr {}): No ETHM connection to monitoring station 1", addr),
            Self::GsmEthmStation2Error(addr) => format!("INT-GSM (Addr {}): No ETHM connection to monitoring station 2", addr),
            Self::GsmGprsSim1Station1Error(addr) => format!("INT-GSM (Addr {}): No GPRS SIM1 connection to monitoring station 1", addr),
            Self::GsmGprsSim1Station2Error(addr) => format!("INT-GSM (Addr {}): No GPRS SIM1 connection to monitoring station 2", addr),
            Self::GsmGprsSim2Station1Error(addr) => format!("INT-GSM (Addr {}): No GPRS SIM2 connection to monitoring station 1", addr),
            Self::GsmGprsSim2Station2Error(addr) => format!("INT-GSM (Addr {}): No GPRS SIM2 connection to monitoring station 2", addr),
            Self::GsmSmsSim1Station1Error(addr) => format!("INT-GSM (Addr {}): No SMS SIM1 connection to monitoring station 1", addr),
            Self::GsmSmsSim1Station2Error(addr) => format!("INT-GSM (Addr {}): No SMS SIM1 connection to monitoring station 2", addr),
            Self::GsmSmsSim2Station1Error(addr) => format!("INT-GSM (Addr {}): No SMS SIM2 connection to monitoring station 1", addr),
            Self::GsmSmsSim2Station2Error(addr) => format!("INT-GSM (Addr {}): No SMS SIM2 connection to monitoring station 2", addr),
            
            Self::GsmSimPinError { module, sim } => format!("INT-GSM (Addr {}): SIM{} Wrong PIN", module, sim),
            Self::GsmSimLoggingError { module, sim } => format!("INT-GSM (Addr {}): SIM{} Network Registration Error", module, sim),
            Self::GsmSimCreditLow { module, sim } => format!("INT-GSM (Addr {}): SIM{} Account Credit Low", module, sim),
            Self::GsmSimSmsError { module, sim } => format!("INT-GSM (Addr {}): SIM{} SMS Sending Error", module, sim),
            Self::GsmJamming(addr) => format!("INT-GSM (Addr {}): Cellular Jamming Detected", addr),
            Self::GsmSettingsCrcError(addr) => format!("INT-GSM (Addr {}): Settings CRC Checksum Error", addr),
            Self::GsmModuleMissing(addr) => format!("INT-GSM (Addr {}): Module Missing", addr),
            Self::GsmModuleChanged(addr) => format!("INT-GSM (Addr {}): Module Changed", addr),
            Self::GsmServerConnError(addr) => format!("INT-GSM (Addr {}): Server Connection Error", addr),
            Self::GsmMailServerConnError(addr) => format!("INT-GSM (Addr {}): Mail Server Error", addr),
            Self::GsmNtpServerConnError(addr) => format!("INT-GSM (Addr {}): NTP Server Error", addr),

            Self::WirelessDeviceLowBattery { zone_id } => format!("Wireless Sensor (Zone #{:03}): Low Battery", zone_id),
            Self::WirelessDeviceNoComm { zone_id } => format!("Wireless Sensor (Zone #{:03}): No Radio Communication", zone_id),
            Self::WirelessOutputNoComm { output_id } => format!("Wireless Output #{:03}: No Radio Communication", output_id),

            Self::MasterKeyFobLowBattery(master) => format!("Master User #{:02} Key Fob: Low Battery", master),
            Self::UserKeyFobLowBattery { user_id } => format!("User #{:03} Key Fob: Low Battery", user_id),

            Self::ZoneLongViolationTrouble(zone_id) => format!("Zone #{:03}: Long Violation", zone_id),
            Self::ZoneNoViolationTrouble(zone_id) => format!("Zone #{:03}: No Violation", zone_id),
            Self::ZoneTamperTrouble(zone_id) => format!("Zone #{:03}: Tamper", zone_id),
            
            Self::ExpanderRestart(exp) => format!("Expander #{:02}: Restart Latched", exp),

            Self::AuxiliaryStmTroubles => "Auxiliary Microprocessor (STM) Trouble".to_string(),
            Self::GenericTrouble { part, bit } => format!("Diagnostic Trouble (Part {}, Bit #{:03})", part + 1, bit),
        }
    }
}

/// General system status flags (from 0x1A frame).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SystemStatus {
    pub service_mode: bool,
    pub troubles_present: bool,
    pub troubles_memory: bool,
    pub rtc: DateTime<Local>,
}

// --- Auto-read (Push notifications) ---

/// State of an individual auto-read category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoReadItemState {
    /// Active and operational.
    Active,
    /// Not requested in configuration.
    NotRequested,
    /// Unsupported by hardware (requires 14-byte support, panel provides 12).
    UnsupportedByHardware,
    /// Rejected by panel (error code 0xEF).
    RejectedByPanel(u8),
}

impl AutoReadItemState {
    pub fn to_description(&self) -> String {
        match self {
            Self::Active => "Active".to_string(),
            Self::NotRequested => "Not requested".to_string(),
            Self::UnsupportedByHardware => "Unsupported by ETHM hardware (requires 14B mask)".to_string(),
            Self::RejectedByPanel(code) => {
                let desc = match code {
                    0x01 => "Error: Unknown user code",
                    0x02 => "Error: No access rights",
                    0x03 => "Error: User does not exist",
                    0x04 => "Error: User already exists",
                    0x05 => "Error: Wrong user code",
                    0x08 => "Error: Other panel error",
                    _ => "Error: Rejected (0xEF)",
                };
                format!("{} ({:02X})", desc, code)
            }
        }
    }
}

/// Status of a specific auto-read category.
#[derive(Debug, Clone)]
pub struct AutoReadItemStatus {
    pub name: String,
    pub state: AutoReadItemState,
}

/// Report summarizing auto-read configuration results.
#[derive(Debug, Clone)]
pub struct AutoReadReport {
    pub items: Vec<AutoReadItemStatus>,
    pub success_count: usize,
    pub total_requested: usize,
}

// --- Shared state cache ---

/// Thread-safe in-memory cache of the Integra panel state.
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
    pub trouble_flags: std::collections::HashMap<(TroubleType, bool), bool>,
    pub acu_jam_levels: std::collections::HashMap<u8, u8>,
    pub cme_errors: std::collections::HashMap<(CmeSource, u8, bool), u16>,
    pub auto_read_report: Option<AutoReadReport>,
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
            trouble_flags: std::collections::HashMap::new(),
            acu_jam_levels: std::collections::HashMap::new(),
            cme_errors: std::collections::HashMap::new(),
            auto_read_report: None,
        }
    }
}

/// Thread-safe shared handle to `SatelState`.
pub type SatelStateHandle = Arc<RwLock<SatelState>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_statistics_changed_logic() {
        let base = StatsMark {
            non_ping_frames: 10,
            connections_established: 1,
            reconnect_attempts: 0,
            connections_lost: 0,
            timeouts: 0,
            crc_errors: 0,
            rejected_by_panel: 0,
            io_errors: 0,
        };

        // 1. Brak zmian -> false
        assert!(!statistics_changed(&base, &base));

        // 2. +4 ramki -> false
        let mut mark_plus_4 = base;
        mark_plus_4.non_ping_frames = 14;
        assert!(!statistics_changed(&base, &mark_plus_4));

        // 3. +5 ramek -> true
        let mut mark_plus_5 = base;
        mark_plus_5.non_ping_frames = 15;
        assert!(statistics_changed(&base, &mark_plus_5));

        // 4. Zmiana timeouts przy 0 ramkach -> true
        let mut mark_timeout = base;
        mark_timeout.timeouts = 1;
        assert!(statistics_changed(&base, &mark_timeout));
    }
}
