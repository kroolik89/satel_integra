use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Control capability and nature of an Integra panel output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutputControl {
    /// Non-controllable output (status / indicator / automatic).
    None,
    /// Controllable timed output (active for a specified or configured duration).
    Timed {
        duration: Option<Duration>,
    },
    /// Controllable bistable output (toggles on/off state).
    Bistable,
    /// Controllable output whose exact mode (timed vs bistable) cannot be determined
    /// because operating duration was not read (e.g. telephone relay without extended read).
    Unknown,
}

impl OutputControl {
    /// Returns true if this output can be controlled by users/integrations.
    pub fn is_controllable(&self) -> bool {
        !matches!(self, Self::None)
    }

    /// Returns true if this output has a timed active duration.
    pub fn is_timed(&self) -> bool {
        matches!(self, Self::Timed { .. })
    }

    /// Returns true if this output is bistable (latched toggle).
    pub fn is_bistable(&self) -> bool {
        matches!(self, Self::Bistable)
    }
}

impl std::fmt::Display for OutputControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::Timed { duration: Some(d) } => write!(f, "Timed ({:.1}s)", d.as_secs_f32()),
            Self::Timed { duration: None } => write!(f, "Timed"),
            Self::Bistable => write!(f, "Bistable"),
            Self::Unknown => write!(f, "Unknown"),
        }
    }
}

/// Catalog entry descriptor for output functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputFunctionDescriptor {
    pub code: u8,
    pub key: &'static str,
    pub label_en: &'static str,
}

/// Output function type (Integra codes 0..123).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OutputFunction {
    Unused,
    BurglaryAlarm,
    FireAndBurglaryAlarm,
    FireAlarm,
    KeypadAlarm,
    KeypadFireAlarm,
    KeypadPanicAlarm,
    KeypadMedicalAlarm,
    TamperAlarm,
    AlarmDay,
    DuressAlarm,
    Chime,
    SilentAlarm,
    TechnicalAlarm,
    ZoneViolation,
    VideoDisarmed,
    VideoArmed,
    ReadyIndicator,
    BypassesIndicator,
    ExitDelayIndicator,
    EntryDelayIndicator,
    ArmedIndicator,
    AllArmedIndicator,
    ArmDisarmConfirmation,
    MonoSwitch,
    BiSwitch,
    Timer,
    TroubleIndicator,
    MainBoardAcTrouble,
    ZonesAcTrouble,
    ExpandersAcTrouble,
    MainBoardBatteryTrouble,
    ZonesBatteryTrouble,
    ExpanderBatteryTrouble,
    ZoneTrouble,
    DialerActive,
    GroundStart,
    MonitoringConfirmation,
    ServiceModeIndicator,
    VibrationSensorsTest,
    CashMachineBypassIndicator,
    PowerSupply,
    PowerSupplyArmed,
    ResettablePowerSupply,
    FireDetectorsPowerSupply,
    PartitionBlockedIndicator,
    OutputsLogicAnd,
    OutputsLogicOr,
    Synthesizer(u8), // 0..=15
    TelephoneRelayNumbered(u8), // 1..=16
    NoGuardRound,
    LongMainBoardAcTrouble,
    LongExpandersAcTrouble,
    OutputsSignalingEnd,
    CodeEnteredSignaling,
    CodeUsedSignaling,
    DoorOpenIndicator,
    DoorLongOpenIndicator,
    BurglaryAlarmNoTamperNoFire,
    EventsMemory50Percent,
    EventsMemory90Percent,
    AutoArmDelaySignaling,
    AutoArmDelayIndicator,
    UnauthorizedDoorOpening,
    UnauthorizedDoorOpeningAlarm,
    IpMonitoringTrouble,
    TelephoneLineTrouble,
    SynthesizerSelectable,
    TelephoneRelay,
    CardRead,
    CardHold,
    CardReadInModule,
    WirelessZoneNoComm,
    WirelessOutputNoComm,
    WirelessDeviceBatteryTrouble,
    RollerBlindUp,
    RollerBlindDown,
    ExpanderCardHeadA,
    ExpanderCardHeadB,
    ZonesLogicAnd,
    UnverifiedAlarm,
    VerifiedAlarm,
    VerificationWithoutAlarm,
    VerificationBypassIndicator,
    ZoneTestIndicator,
    ArmModeIndicator,
    InternalSiren,
    TamperIndicator,
    KeyFobsBatteryTrouble,
    WirelessJammingTrouble,
    Thermostat,
    ZoneMaskingSignaling,
    ZoneMaskingIndicator,
    ModuleLock,
    Unknown(u8),
}

macro_rules! out_desc {
    ($code:expr, $key:expr, $label:expr) => {
        OutputFunctionDescriptor {
            code: $code,
            key: $key,
            label_en: $label,
        }
    };
}

impl OutputFunction {
    /// Full catalog of all 124 standard Integra output functions (codes 0..123).
    pub fn catalog() -> &'static [OutputFunctionDescriptor] {
        &[
            out_desc!(0, "unused", "Unused"),
            out_desc!(1, "burglary_alarm", "Burglary Alarm"),
            out_desc!(2, "fire_and_burglary_alarm", "Fire and Burglary Alarm"),
            out_desc!(3, "fire_alarm", "Fire Alarm"),
            out_desc!(4, "keypad_alarm", "Keypad Alarm"),
            out_desc!(5, "keypad_fire_alarm", "Keypad Fire Alarm"),
            out_desc!(6, "keypad_panic_alarm", "Keypad Panic Alarm"),
            out_desc!(7, "keypad_medical_alarm", "Keypad Medical Alarm"),
            out_desc!(8, "tamper_alarm", "Tamper Alarm"),
            out_desc!(9, "alarm_day", "Day Alarm"),
            out_desc!(10, "duress_alarm", "Duress Alarm"),
            out_desc!(11, "chime", "Chime"),
            out_desc!(12, "silent_alarm", "Silent Alarm"),
            out_desc!(13, "technical_alarm", "Technical Alarm"),
            out_desc!(14, "zone_violation", "Zone Violation"),
            out_desc!(15, "video_disarmed", "Video Disarmed"),
            out_desc!(16, "video_armed", "Video Armed"),
            out_desc!(17, "ready_indicator", "Ready Indicator"),
            out_desc!(18, "bypasses_indicator", "Bypasses Indicator"),
            out_desc!(19, "exit_delay_indicator", "Exit Delay Indicator"),
            out_desc!(20, "entry_delay_indicator", "Entry Delay Indicator"),
            out_desc!(21, "armed_indicator", "Armed Indicator"),
            out_desc!(22, "all_armed_indicator", "All Armed Indicator"),
            out_desc!(23, "arm_disarm_confirmation", "Arm/Disarm Confirmation"),
            out_desc!(24, "mono_switch", "Mono Switch"),
            out_desc!(25, "bi_switch", "Bi Switch"),
            out_desc!(26, "timer", "Timer"),
            out_desc!(27, "trouble_indicator", "Trouble Indicator"),
            out_desc!(28, "main_board_ac_trouble", "Main Board AC Trouble"),
            out_desc!(29, "zones_ac_trouble", "Zones AC Trouble"),
            out_desc!(30, "expanders_ac_trouble", "Expanders AC Trouble"),
            out_desc!(31, "main_board_battery_trouble", "Main Board Battery Trouble"),
            out_desc!(32, "zones_battery_trouble", "Zones Battery Trouble"),
            out_desc!(33, "expander_battery_trouble", "Expander Battery Trouble"),
            out_desc!(34, "zone_trouble", "Zone Trouble"),
            out_desc!(35, "dialer_active", "Dialer Active"),
            out_desc!(36, "ground_start", "Ground Start"),
            out_desc!(37, "monitoring_confirmation", "Monitoring Confirmation"),
            out_desc!(38, "service_mode_indicator", "Service Mode Indicator"),
            out_desc!(39, "vibration_sensors_test", "Vibration Sensors Test"),
            out_desc!(40, "cash_machine_bypass_indicator", "Cash Machine Bypass Indicator"),
            out_desc!(41, "power_supply", "Power Supply"),
            out_desc!(42, "power_supply_armed", "Power Supply in Armed State"),
            out_desc!(43, "resettable_power_supply", "Resettable Power Supply"),
            out_desc!(44, "fire_detectors_power_supply", "Fire Detectors Power Supply"),
            out_desc!(45, "partition_blocked_indicator", "Partition Blocked Indicator"),
            out_desc!(46, "outputs_logic_and", "Outputs Logic AND"),
            out_desc!(47, "outputs_logic_or", "Outputs Logic OR"),
            out_desc!(48, "synthesizer_0", "Voice Synthesizer 0"),
            out_desc!(49, "synthesizer_1", "Voice Synthesizer 1"),
            out_desc!(50, "synthesizer_2", "Voice Synthesizer 2"),
            out_desc!(51, "synthesizer_3", "Voice Synthesizer 3"),
            out_desc!(52, "synthesizer_4", "Voice Synthesizer 4"),
            out_desc!(53, "synthesizer_5", "Voice Synthesizer 5"),
            out_desc!(54, "synthesizer_6", "Voice Synthesizer 6"),
            out_desc!(55, "synthesizer_7", "Voice Synthesizer 7"),
            out_desc!(56, "synthesizer_8", "Voice Synthesizer 8"),
            out_desc!(57, "synthesizer_9", "Voice Synthesizer 9"),
            out_desc!(58, "synthesizer_10", "Voice Synthesizer 10"),
            out_desc!(59, "synthesizer_11", "Voice Synthesizer 11"),
            out_desc!(60, "synthesizer_12", "Voice Synthesizer 12"),
            out_desc!(61, "synthesizer_13", "Voice Synthesizer 13"),
            out_desc!(62, "synthesizer_14", "Voice Synthesizer 14"),
            out_desc!(63, "synthesizer_15", "Voice Synthesizer 15"),
            out_desc!(64, "telephone_relay_1", "Telephone Relay 1"),
            out_desc!(65, "telephone_relay_2", "Telephone Relay 2"),
            out_desc!(66, "telephone_relay_3", "Telephone Relay 3"),
            out_desc!(67, "telephone_relay_4", "Telephone Relay 4"),
            out_desc!(68, "telephone_relay_5", "Telephone Relay 5"),
            out_desc!(69, "telephone_relay_6", "Telephone Relay 6"),
            out_desc!(70, "telephone_relay_7", "Telephone Relay 7"),
            out_desc!(71, "telephone_relay_8", "Telephone Relay 8"),
            out_desc!(72, "telephone_relay_9", "Telephone Relay 9"),
            out_desc!(73, "telephone_relay_10", "Telephone Relay 10"),
            out_desc!(74, "telephone_relay_11", "Telephone Relay 11"),
            out_desc!(75, "telephone_relay_12", "Telephone Relay 12"),
            out_desc!(76, "telephone_relay_13", "Telephone Relay 13"),
            out_desc!(77, "telephone_relay_14", "Telephone Relay 14"),
            out_desc!(78, "telephone_relay_15", "Telephone Relay 15"),
            out_desc!(79, "telephone_relay_16", "Telephone Relay 16"),
            out_desc!(80, "no_guard_round", "No Guard Round"),
            out_desc!(81, "long_main_board_ac_trouble", "Long Main Board AC Trouble"),
            out_desc!(82, "long_expanders_ac_trouble", "Long Expanders AC Trouble"),
            out_desc!(83, "outputs_signaling_end", "Outputs Signaling End"),
            out_desc!(84, "code_entered_signaling", "Code Entered Signaling"),
            out_desc!(85, "code_used_signaling", "Code Used Signaling"),
            out_desc!(86, "door_open_indicator", "Door Open Indicator"),
            out_desc!(87, "door_long_open_indicator", "Door Long Open Indicator"),
            out_desc!(88, "burglary_alarm_no_tamper_no_fire", "Burglary Alarm (No Tamper, No Fire)"),
            out_desc!(89, "events_memory_50_percent", "50% Event Log Full"),
            out_desc!(90, "events_memory_90_percent", "90% Event Log Full"),
            out_desc!(91, "auto_arm_delay_signaling", "Auto-Arm Delay Signaling"),
            out_desc!(92, "auto_arm_delay_indicator", "Auto-Arm Delay Indicator"),
            out_desc!(93, "unauthorized_door_opening", "Unauthorized Door Opening"),
            out_desc!(94, "unauthorized_door_opening_alarm", "Unauthorized Door Opening Alarm"),
            out_desc!(95, "ip_monitoring_trouble", "IP Monitoring Trouble"),
            out_desc!(96, "telephone_line_trouble", "Telephone Line Trouble"),
            out_desc!(97, "synthesizer", "Voice Synthesizer"),
            out_desc!(98, "telephone_relay", "Telephone Relay"),
            out_desc!(99, "card_read", "Card Read"),
            out_desc!(100, "card_hold", "Card Hold"),
            out_desc!(101, "card_read_in_module", "Card Read in Module"),
            out_desc!(102, "wireless_zone_no_comm", "Wireless Zone No Comm"),
            out_desc!(103, "wireless_output_no_comm", "Wireless Output No Comm"),
            out_desc!(104, "wireless_device_battery_trouble", "Wireless Device Low Battery"),
            out_desc!(105, "roller_blind_up", "Roller Blind Up"),
            out_desc!(106, "roller_blind_down", "Roller Blind Down"),
            out_desc!(107, "expander_card_head_a", "Card on Expander Head A"),
            out_desc!(108, "expander_card_head_b", "Card on Expander Head B"),
            out_desc!(109, "zones_logic_and", "Zones Logic AND"),
            out_desc!(110, "unverified_alarm", "Unverified Alarm"),
            out_desc!(111, "verified_alarm", "Verified Alarm"),
            out_desc!(112, "verification_without_alarm", "Verification Without Alarm"),
            out_desc!(113, "verification_bypass_indicator", "Verification Bypass Indicator"),
            out_desc!(114, "zone_test_indicator", "Zone Test Indicator"),
            out_desc!(115, "arm_mode_indicator", "Arm Mode Indicator"),
            out_desc!(116, "internal_siren", "Internal Siren"),
            out_desc!(117, "tamper_indicator", "Tamper Indicator"),
            out_desc!(118, "key_fobs_battery_trouble", "Key Fobs Low Battery"),
            out_desc!(119, "wireless_jamming_trouble", "Wireless Module Jamming"),
            out_desc!(120, "thermostat", "Thermostat"),
            out_desc!(121, "zone_masking_signaling", "Zone Masking Signaling"),
            out_desc!(122, "zone_masking_indicator", "Zone Masking Indicator"),
            out_desc!(123, "module_lock", "Module Lock"),
        ]
    }

    /// Constructs an `OutputFunction` from raw protocol byte code (0..123).
    pub fn from_code(code: u8) -> Self {
        match code {
            0 => Self::Unused,
            1 => Self::BurglaryAlarm,
            2 => Self::FireAndBurglaryAlarm,
            3 => Self::FireAlarm,
            4 => Self::KeypadAlarm,
            5 => Self::KeypadFireAlarm,
            6 => Self::KeypadPanicAlarm,
            7 => Self::KeypadMedicalAlarm,
            8 => Self::TamperAlarm,
            9 => Self::AlarmDay,
            10 => Self::DuressAlarm,
            11 => Self::Chime,
            12 => Self::SilentAlarm,
            13 => Self::TechnicalAlarm,
            14 => Self::ZoneViolation,
            15 => Self::VideoDisarmed,
            16 => Self::VideoArmed,
            17 => Self::ReadyIndicator,
            18 => Self::BypassesIndicator,
            19 => Self::ExitDelayIndicator,
            20 => Self::EntryDelayIndicator,
            21 => Self::ArmedIndicator,
            22 => Self::AllArmedIndicator,
            23 => Self::ArmDisarmConfirmation,
            24 => Self::MonoSwitch,
            25 => Self::BiSwitch,
            26 => Self::Timer,
            27 => Self::TroubleIndicator,
            28 => Self::MainBoardAcTrouble,
            29 => Self::ZonesAcTrouble,
            30 => Self::ExpandersAcTrouble,
            31 => Self::MainBoardBatteryTrouble,
            32 => Self::ZonesBatteryTrouble,
            33 => Self::ExpanderBatteryTrouble,
            34 => Self::ZoneTrouble,
            35 => Self::DialerActive,
            36 => Self::GroundStart,
            37 => Self::MonitoringConfirmation,
            38 => Self::ServiceModeIndicator,
            39 => Self::VibrationSensorsTest,
            40 => Self::CashMachineBypassIndicator,
            41 => Self::PowerSupply,
            42 => Self::PowerSupplyArmed,
            43 => Self::ResettablePowerSupply,
            44 => Self::FireDetectorsPowerSupply,
            45 => Self::PartitionBlockedIndicator,
            46 => Self::OutputsLogicAnd,
            47 => Self::OutputsLogicOr,
            48..=63 => Self::Synthesizer(code - 48),
            64..=79 => Self::TelephoneRelayNumbered(code - 64 + 1),
            80 => Self::NoGuardRound,
            81 => Self::LongMainBoardAcTrouble,
            82 => Self::LongExpandersAcTrouble,
            83 => Self::OutputsSignalingEnd,
            84 => Self::CodeEnteredSignaling,
            85 => Self::CodeUsedSignaling,
            86 => Self::DoorOpenIndicator,
            87 => Self::DoorLongOpenIndicator,
            88 => Self::BurglaryAlarmNoTamperNoFire,
            89 => Self::EventsMemory50Percent,
            90 => Self::EventsMemory90Percent,
            91 => Self::AutoArmDelaySignaling,
            92 => Self::AutoArmDelayIndicator,
            93 => Self::UnauthorizedDoorOpening,
            94 => Self::UnauthorizedDoorOpeningAlarm,
            95 => Self::IpMonitoringTrouble,
            96 => Self::TelephoneLineTrouble,
            97 => Self::SynthesizerSelectable,
            98 => Self::TelephoneRelay,
            99 => Self::CardRead,
            100 => Self::CardHold,
            101 => Self::CardReadInModule,
            102 => Self::WirelessZoneNoComm,
            103 => Self::WirelessOutputNoComm,
            104 => Self::WirelessDeviceBatteryTrouble,
            105 => Self::RollerBlindUp,
            106 => Self::RollerBlindDown,
            107 => Self::ExpanderCardHeadA,
            108 => Self::ExpanderCardHeadB,
            109 => Self::ZonesLogicAnd,
            110 => Self::UnverifiedAlarm,
            111 => Self::VerifiedAlarm,
            112 => Self::VerificationWithoutAlarm,
            113 => Self::VerificationBypassIndicator,
            114 => Self::ZoneTestIndicator,
            115 => Self::ArmModeIndicator,
            116 => Self::InternalSiren,
            117 => Self::TamperIndicator,
            118 => Self::KeyFobsBatteryTrouble,
            119 => Self::WirelessJammingTrouble,
            120 => Self::Thermostat,
            121 => Self::ZoneMaskingSignaling,
            122 => Self::ZoneMaskingIndicator,
            123 => Self::ModuleLock,
            other => Self::Unknown(other),
        }
    }

    /// Returns the raw protocol byte code (0..123) or unknown value.
    pub fn code(&self) -> u8 {
        match self {
            Self::Unused => 0,
            Self::BurglaryAlarm => 1,
            Self::FireAndBurglaryAlarm => 2,
            Self::FireAlarm => 3,
            Self::KeypadAlarm => 4,
            Self::KeypadFireAlarm => 5,
            Self::KeypadPanicAlarm => 6,
            Self::KeypadMedicalAlarm => 7,
            Self::TamperAlarm => 8,
            Self::AlarmDay => 9,
            Self::DuressAlarm => 10,
            Self::Chime => 11,
            Self::SilentAlarm => 12,
            Self::TechnicalAlarm => 13,
            Self::ZoneViolation => 14,
            Self::VideoDisarmed => 15,
            Self::VideoArmed => 16,
            Self::ReadyIndicator => 17,
            Self::BypassesIndicator => 18,
            Self::ExitDelayIndicator => 19,
            Self::EntryDelayIndicator => 20,
            Self::ArmedIndicator => 21,
            Self::AllArmedIndicator => 22,
            Self::ArmDisarmConfirmation => 23,
            Self::MonoSwitch => 24,
            Self::BiSwitch => 25,
            Self::Timer => 26,
            Self::TroubleIndicator => 27,
            Self::MainBoardAcTrouble => 28,
            Self::ZonesAcTrouble => 29,
            Self::ExpandersAcTrouble => 30,
            Self::MainBoardBatteryTrouble => 31,
            Self::ZonesBatteryTrouble => 32,
            Self::ExpanderBatteryTrouble => 33,
            Self::ZoneTrouble => 34,
            Self::DialerActive => 35,
            Self::GroundStart => 36,
            Self::MonitoringConfirmation => 37,
            Self::ServiceModeIndicator => 38,
            Self::VibrationSensorsTest => 39,
            Self::CashMachineBypassIndicator => 40,
            Self::PowerSupply => 41,
            Self::PowerSupplyArmed => 42,
            Self::ResettablePowerSupply => 43,
            Self::FireDetectorsPowerSupply => 44,
            Self::PartitionBlockedIndicator => 45,
            Self::OutputsLogicAnd => 46,
            Self::OutputsLogicOr => 47,
            Self::Synthesizer(n) => 48 + n.min(&15),
            Self::TelephoneRelayNumbered(n) => 64 + (n.clamp(&1, &16) - 1),
            Self::NoGuardRound => 80,
            Self::LongMainBoardAcTrouble => 81,
            Self::LongExpandersAcTrouble => 82,
            Self::OutputsSignalingEnd => 83,
            Self::CodeEnteredSignaling => 84,
            Self::CodeUsedSignaling => 85,
            Self::DoorOpenIndicator => 86,
            Self::DoorLongOpenIndicator => 87,
            Self::BurglaryAlarmNoTamperNoFire => 88,
            Self::EventsMemory50Percent => 89,
            Self::EventsMemory90Percent => 90,
            Self::AutoArmDelaySignaling => 91,
            Self::AutoArmDelayIndicator => 92,
            Self::UnauthorizedDoorOpening => 93,
            Self::UnauthorizedDoorOpeningAlarm => 94,
            Self::IpMonitoringTrouble => 95,
            Self::TelephoneLineTrouble => 96,
            Self::SynthesizerSelectable => 97,
            Self::TelephoneRelay => 98,
            Self::CardRead => 99,
            Self::CardHold => 100,
            Self::CardReadInModule => 101,
            Self::WirelessZoneNoComm => 102,
            Self::WirelessOutputNoComm => 103,
            Self::WirelessDeviceBatteryTrouble => 104,
            Self::RollerBlindUp => 105,
            Self::RollerBlindDown => 106,
            Self::ExpanderCardHeadA => 107,
            Self::ExpanderCardHeadB => 108,
            Self::ZonesLogicAnd => 109,
            Self::UnverifiedAlarm => 110,
            Self::VerifiedAlarm => 111,
            Self::VerificationWithoutAlarm => 112,
            Self::VerificationBypassIndicator => 113,
            Self::ZoneTestIndicator => 114,
            Self::ArmModeIndicator => 115,
            Self::InternalSiren => 116,
            Self::TamperIndicator => 117,
            Self::KeyFobsBatteryTrouble => 118,
            Self::WirelessJammingTrouble => 119,
            Self::Thermostat => 120,
            Self::ZoneMaskingSignaling => 121,
            Self::ZoneMaskingIndicator => 122,
            Self::ModuleLock => 123,
            Self::Unknown(c) => *c,
        }
    }

    /// Stable snake_case key for localization and identification.
    pub fn key(&self) -> &'static str {
        let c = self.code();
        if (c as usize) < Self::catalog().len() {
            Self::catalog()[c as usize].key
        } else {
            "unknown"
        }
    }

    /// English display label.
    pub fn label_en(&self) -> &'static str {
        let c = self.code();
        if (c as usize) < Self::catalog().len() {
            Self::catalog()[c as usize].label_en
        } else {
            "Unknown"
        }
    }

    /// Returns true if this output can be controlled by users/keypads/integrations (D-4b-4).
    ///
    /// Specifically:
    /// - 24: Mono Switch
    /// - 25: Bi Switch
    /// - 64..=79: Telephone Relay 1..16
    /// - 98: Telephone Relay
    /// - 105: Roller Blind Up
    /// - 106: Roller Blind Down
    pub fn is_controllable(&self) -> bool {
        matches!(
            self.code(),
            24 | 25 | 64..=79 | 98 | 105 | 106
        )
    }

    /// Determines the output control type given duration in tenths of a second (×0.1s) from 0xEE (type 17).
    pub fn control(&self, duration_tenths_sec: Option<u16>) -> OutputControl {
        let duration = duration_tenths_sec.map(|ds| Duration::from_millis(ds as u64 * 100));
        self.control_from_duration(duration)
    }

    /// Determines the output control type given an optional duration (D-4b-4).
    pub fn control_from_duration(&self, duration: Option<Duration>) -> OutputControl {
        match self.code() {
            24 | 105 | 106 => OutputControl::Timed { duration },
            25 => OutputControl::Bistable,
            64..=79 | 98 => match duration {
                None => OutputControl::Unknown,
                Some(d) if d.is_zero() => OutputControl::Bistable,
                Some(d) => OutputControl::Timed { duration: Some(d) },
            },
            _ => OutputControl::None,
        }
    }
}

impl std::fmt::Display for OutputFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label_en())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_output_function_catalog_completeness() {
        let catalog = OutputFunction::catalog();
        assert_eq!(catalog.len(), 124);
        for (i, entry) in catalog.iter().enumerate() {
            assert_eq!(entry.code as usize, i);
            assert!(!entry.key.is_empty());
            assert!(!entry.label_en.is_empty());

            let function = OutputFunction::from_code(entry.code);
            assert_eq!(function.code(), entry.code);
            assert_eq!(function.key(), entry.key);
            assert_eq!(function.label_en(), entry.label_en);
            assert_eq!(format!("{}", function), entry.label_en);
        }
    }

    #[test]
    fn test_output_controllability_exact_codes() {
        let controllable_codes = [24, 25, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 98, 105, 106];
        for code in 0..=123 {
            let func = OutputFunction::from_code(code);
            if controllable_codes.contains(&code) {
                assert!(func.is_controllable(), "Code {} should be controllable", code);
            } else {
                assert!(!func.is_controllable(), "Code {} should NOT be controllable", code);
            }
        }
    }

    #[test]
    fn test_output_control_modes() {
        // Mono Switch (24) -> Timed
        let mono = OutputFunction::from_code(24);
        assert_eq!(
            mono.control(Some(50)),
            OutputControl::Timed {
                duration: Some(Duration::from_millis(5000))
            }
        );
        assert_eq!(mono.control(None), OutputControl::Timed { duration: None });

        // Bi Switch (25) -> Bistable
        let bi = OutputFunction::from_code(25);
        assert_eq!(bi.control(Some(50)), OutputControl::Bistable);
        assert_eq!(bi.control(None), OutputControl::Bistable);

        // Roller Blinds (105, 106) -> Timed
        for code in [105, 106] {
            let blind = OutputFunction::from_code(code);
            assert_eq!(
                blind.control(Some(200)),
                OutputControl::Timed {
                    duration: Some(Duration::from_millis(20000))
                }
            );
            assert_eq!(blind.control(None), OutputControl::Timed { duration: None });
        }

        // Telephone Relays (64..=79 and 98)
        let mut relay_codes = Vec::new();
        for c in 64..=79 {
            relay_codes.push(c);
        }
        relay_codes.push(98);

        for code in relay_codes {
            let relay = OutputFunction::from_code(code);
            // With duration > 0 -> Timed
            assert_eq!(
                relay.control(Some(10)),
                OutputControl::Timed {
                    duration: Some(Duration::from_millis(1000))
                }
            );
            // With duration == 0 -> Bistable
            assert_eq!(relay.control(Some(0)), OutputControl::Bistable);
            // Without duration (None) -> Unknown
            assert_eq!(relay.control(None), OutputControl::Unknown);
        }

        // Non-controllable -> None
        let unused = OutputFunction::from_code(0);
        assert_eq!(unused.control(Some(10)), OutputControl::None);
        assert_eq!(unused.control(None), OutputControl::None);
    }

    #[test]
    fn test_output_numbered_ranges() {
        // 48..=63 Voice message / synthesizer 0..15
        for s in 0..=15 {
            let code = 48 + s;
            let func = OutputFunction::from_code(code);
            assert_eq!(func, OutputFunction::Synthesizer(s));
            assert_eq!(func.code(), code);
            assert_eq!(func.key(), format!("synthesizer_{}", s));
        }

        // 64..=79 Telephone relay 1..16
        for r in 1..=16 {
            let code = 64 + r - 1;
            let func = OutputFunction::from_code(code);
            assert_eq!(func, OutputFunction::TelephoneRelayNumbered(r));
            assert_eq!(func.code(), code);
            assert_eq!(func.key(), format!("telephone_relay_{}", r));
        }
    }
}

