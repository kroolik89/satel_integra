use serde::{Deserialize, Serialize};

/// High-level device class / kind for zones (useful for Home Assistant integration).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ZoneKind {
    Burglary,
    Fire,
    Smoke,
    Water,
    Gas,
    Freeze,
    Temperature,
    Panic,
    Medical,
    Tamper,
    Technical,
    Control,
    Other,
}

impl ZoneKind {
    pub fn key(&self) -> &'static str {
        match self {
            Self::Burglary => "burglary",
            Self::Fire => "fire",
            Self::Smoke => "smoke",
            Self::Water => "water",
            Self::Gas => "gas",
            Self::Freeze => "freeze",
            Self::Temperature => "temperature",
            Self::Panic => "panic",
            Self::Medical => "medical",
            Self::Tamper => "tamper",
            Self::Technical => "technical",
            Self::Control => "control",
            Self::Other => "other",
        }
    }

    pub fn label_en(&self) -> &'static str {
        match self {
            Self::Burglary => "Burglary",
            Self::Fire => "Fire",
            Self::Smoke => "Smoke",
            Self::Water => "Water",
            Self::Gas => "Gas",
            Self::Freeze => "Freeze",
            Self::Temperature => "Temperature",
            Self::Panic => "Panic",
            Self::Medical => "Medical",
            Self::Tamper => "Tamper",
            Self::Technical => "Technical",
            Self::Control => "Control",
            Self::Other => "Other",
        }
    }
}

impl std::fmt::Display for ZoneKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label_en())
    }
}

/// Catalog entry descriptor for zone reactions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ZoneReactionDescriptor {
    pub code: u8,
    pub key: &'static str,
    pub label_en: &'static str,
    pub kind: ZoneKind,
}

/// Zone reaction type (Integra codes 0..97).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ZoneReaction {
    EntryExit,
    Entry,
    DelayedWithSignaling,
    InteriorDelayed,
    Perimeter,
    Instant,
    Exit,
    AudibleSilent,
    Exterior,
    Tamper24h,
    Vibration24h,
    CashMachine24h,
    PanicAudible,
    PanicSilent,
    MedicalButton,
    MedicalRemote,
    Counting(u8), // 1..=16
    Fire24h,
    FireSmoke24h,
    FireCombustion24h,
    FireWater24h,
    FireTemperature24h,
    FireButton24h,
    FireDuct24h,
    FireFlame24h,
    FireCircuitsProtection24h,
    WaterPressure24h,
    Co2Pressure24h,
    ValveSensor24h,
    WaterLevel24h,
    PumpsStart24h,
    PumpsFailure24h,
    NoAlarmAction,
    AuxiliaryGeneral24h,
    AuxiliaryGas24h,
    AuxiliaryFreeze24h,
    AuxiliaryHeatLoss24h,
    AuxiliaryWaterLeak24h,
    AuxiliarySecurityNonBurglary24h,
    AuxiliaryLowCylinderGasPressure24h,
    AuxiliaryHighTemp24h,
    AuxiliaryLowTemp24h,
    TechnicalDoorControl,
    TechnicalDoorButton,
    TechnicalAcTrouble,
    TechnicalBatteryTrouble,
    TechnicalGsmTrouble,
    TechnicalPowerSupplyOverload,
    Trouble,
    BlockingGroup(u8), // 1..=16
    Arming,
    Disarming,
    ArmDisarm,
    AlarmClearing,
    GuardRound,
    EntryExitConditional,
    EntryExitFinal,
    ExitFinal,
    Burglary24h,
    ExitTimeTerminating,
    VerificationBlocking,
    MaskingSensor,
    OutputsGroupOff,
    OutputsGroupOn,
    EntryExitInterior,
    EntryInterior,
    MonitoringFire,
    MonitoringFirePanelTrouble,
    Unknown(u8),
}

macro_rules! zr_desc {
    ($code:expr, $key:expr, $label:expr, $kind:expr) => {
        ZoneReactionDescriptor {
            code: $code,
            key: $key,
            label_en: $label,
            kind: $kind,
        }
    };
}

impl ZoneReaction {
    /// Full catalog of all 98 standard Integra zone reactions (codes 0..97).
    pub fn catalog() -> &'static [ZoneReactionDescriptor] {
        &[
            zr_desc!(0, "entry_exit", "Entry/Exit", ZoneKind::Burglary),
            zr_desc!(1, "entry", "Entry", ZoneKind::Burglary),
            zr_desc!(2, "delayed_with_signaling", "Delayed with Delay Signaling", ZoneKind::Burglary),
            zr_desc!(3, "interior_delayed", "Interior Delayed", ZoneKind::Burglary),
            zr_desc!(4, "perimeter", "Perimeter", ZoneKind::Burglary),
            zr_desc!(5, "instant", "Instant", ZoneKind::Burglary),
            zr_desc!(6, "exit", "Exit", ZoneKind::Burglary),
            zr_desc!(7, "audible_silent", "Audible/Silent", ZoneKind::Burglary),
            zr_desc!(8, "exterior", "Exterior", ZoneKind::Burglary),
            zr_desc!(9, "tamper_24h", "24h Tamper", ZoneKind::Tamper),
            zr_desc!(10, "vibration_24h", "24h Vibration", ZoneKind::Burglary),
            zr_desc!(11, "cash_machine_24h", "24h Cash Machine", ZoneKind::Burglary),
            zr_desc!(12, "panic_audible", "Audible Panic", ZoneKind::Panic),
            zr_desc!(13, "panic_silent", "Silent Panic", ZoneKind::Panic),
            zr_desc!(14, "medical_button", "Medical - Button", ZoneKind::Medical),
            zr_desc!(15, "medical_remote", "Medical - Remote", ZoneKind::Medical),
            zr_desc!(16, "counting_l1", "Counting L1", ZoneKind::Burglary),
            zr_desc!(17, "counting_l2", "Counting L2", ZoneKind::Burglary),
            zr_desc!(18, "counting_l3", "Counting L3", ZoneKind::Burglary),
            zr_desc!(19, "counting_l4", "Counting L4", ZoneKind::Burglary),
            zr_desc!(20, "counting_l5", "Counting L5", ZoneKind::Burglary),
            zr_desc!(21, "counting_l6", "Counting L6", ZoneKind::Burglary),
            zr_desc!(22, "counting_l7", "Counting L7", ZoneKind::Burglary),
            zr_desc!(23, "counting_l8", "Counting L8", ZoneKind::Burglary),
            zr_desc!(24, "counting_l9", "Counting L9", ZoneKind::Burglary),
            zr_desc!(25, "counting_l10", "Counting L10", ZoneKind::Burglary),
            zr_desc!(26, "counting_l11", "Counting L11", ZoneKind::Burglary),
            zr_desc!(27, "counting_l12", "Counting L12", ZoneKind::Burglary),
            zr_desc!(28, "counting_l13", "Counting L13", ZoneKind::Burglary),
            zr_desc!(29, "counting_l14", "Counting L14", ZoneKind::Burglary),
            zr_desc!(30, "counting_l15", "Counting L15", ZoneKind::Burglary),
            zr_desc!(31, "counting_l16", "Counting L16", ZoneKind::Burglary),
            zr_desc!(32, "fire_24h", "24h Fire", ZoneKind::Fire),
            zr_desc!(33, "fire_smoke_24h", "24h Fire - Smoke Detector", ZoneKind::Smoke),
            zr_desc!(34, "fire_combustion_24h", "24h Fire - Combustion", ZoneKind::Fire),
            zr_desc!(35, "fire_water_sensor_24h", "24h Fire - Water Sensor", ZoneKind::Fire),
            zr_desc!(36, "fire_temperature_24h", "24h Fire - Temperature Sensor", ZoneKind::Temperature),
            zr_desc!(37, "fire_button_24h", "24h Fire - Button", ZoneKind::Fire),
            zr_desc!(38, "fire_duct_24h", "24h Fire - Duct Detector", ZoneKind::Smoke),
            zr_desc!(39, "fire_flame_24h", "24h Fire - Flame Detector", ZoneKind::Fire),
            zr_desc!(40, "fire_circuits_protection_24h", "24h Fire Circuits Protection", ZoneKind::Fire),
            zr_desc!(41, "water_pressure_24h", "24h Water Pressure Sensor", ZoneKind::Water),
            zr_desc!(42, "co2_pressure_24h", "24h CO2 Pressure Sensor", ZoneKind::Gas),
            zr_desc!(43, "valve_sensor_24h", "24h Valve Sensor", ZoneKind::Technical),
            zr_desc!(44, "water_level_24h", "24h Water Level Sensor", ZoneKind::Water),
            zr_desc!(45, "pumps_start_24h", "24h Pumps Start", ZoneKind::Technical),
            zr_desc!(46, "pumps_failure_24h", "24h Pumps Failure", ZoneKind::Technical),
            zr_desc!(47, "no_alarm_action", "No Alarm Action", ZoneKind::Other),
            zr_desc!(48, "auxiliary_general_24h", "24h Auxiliary - General", ZoneKind::Technical),
            zr_desc!(49, "auxiliary_gas_24h", "24h Auxiliary - Gas Sensor", ZoneKind::Gas),
            zr_desc!(50, "auxiliary_freeze_24h", "24h Auxiliary - Freeze", ZoneKind::Freeze),
            zr_desc!(51, "auxiliary_heat_loss_24h", "24h Auxiliary - Heating Loss", ZoneKind::Freeze),
            zr_desc!(52, "auxiliary_water_leak_24h", "24h Auxiliary - Water Leak", ZoneKind::Water),
            zr_desc!(53, "auxiliary_security_non_burglary_24h", "24h Auxiliary - Security (Non-Burglary)", ZoneKind::Technical),
            zr_desc!(54, "auxiliary_low_cylinder_gas_pressure_24h", "24h Auxiliary - Low Cylinder Gas Pressure", ZoneKind::Gas),
            zr_desc!(55, "auxiliary_high_temp_24h", "24h Auxiliary - High Temperature", ZoneKind::Temperature),
            zr_desc!(56, "auxiliary_low_temp_24h", "24h Auxiliary - Low Temperature", ZoneKind::Temperature),
            zr_desc!(57, "technical_door_control", "Technical - Door Control", ZoneKind::Technical),
            zr_desc!(58, "technical_door_button", "Technical - Door Button", ZoneKind::Control),
            zr_desc!(59, "technical_ac_trouble", "Technical - AC Loss", ZoneKind::Technical),
            zr_desc!(60, "technical_battery_trouble", "Technical - Battery Trouble", ZoneKind::Technical),
            zr_desc!(61, "technical_gsm_trouble", "Technical - GSM Trouble", ZoneKind::Technical),
            zr_desc!(62, "technical_power_supply_overload", "Technical - Power Supply Overload", ZoneKind::Technical),
            zr_desc!(63, "trouble", "Trouble", ZoneKind::Technical),
            zr_desc!(64, "blocking_group_1", "Blocking - Group 1", ZoneKind::Control),
            zr_desc!(65, "blocking_group_2", "Blocking - Group 2", ZoneKind::Control),
            zr_desc!(66, "blocking_group_3", "Blocking - Group 3", ZoneKind::Control),
            zr_desc!(67, "blocking_group_4", "Blocking - Group 4", ZoneKind::Control),
            zr_desc!(68, "blocking_group_5", "Blocking - Group 5", ZoneKind::Control),
            zr_desc!(69, "blocking_group_6", "Blocking - Group 6", ZoneKind::Control),
            zr_desc!(70, "blocking_group_7", "Blocking - Group 7", ZoneKind::Control),
            zr_desc!(71, "blocking_group_8", "Blocking - Group 8", ZoneKind::Control),
            zr_desc!(72, "blocking_group_9", "Blocking - Group 9", ZoneKind::Control),
            zr_desc!(73, "blocking_group_10", "Blocking - Group 10", ZoneKind::Control),
            zr_desc!(74, "blocking_group_11", "Blocking - Group 11", ZoneKind::Control),
            zr_desc!(75, "blocking_group_12", "Blocking - Group 12", ZoneKind::Control),
            zr_desc!(76, "blocking_group_13", "Blocking - Group 13", ZoneKind::Control),
            zr_desc!(77, "blocking_group_14", "Blocking - Group 14", ZoneKind::Control),
            zr_desc!(78, "blocking_group_15", "Blocking - Group 15", ZoneKind::Control),
            zr_desc!(79, "blocking_group_16", "Blocking - Group 16", ZoneKind::Control),
            zr_desc!(80, "arming", "Arming", ZoneKind::Control),
            zr_desc!(81, "disarming", "Disarming", ZoneKind::Control),
            zr_desc!(82, "arm_disarm", "Arm/Disarm", ZoneKind::Control),
            zr_desc!(83, "alarm_clearing", "Alarm Clearing", ZoneKind::Control),
            zr_desc!(84, "guard_round", "Guard Round", ZoneKind::Other),
            zr_desc!(85, "entry_exit_conditional", "Entry/Exit - Conditional", ZoneKind::Burglary),
            zr_desc!(86, "entry_exit_final", "Entry/Exit - Final", ZoneKind::Burglary),
            zr_desc!(87, "exit_final", "Exit - Final", ZoneKind::Burglary),
            zr_desc!(88, "burglary_24h", "24h Burglary", ZoneKind::Burglary),
            zr_desc!(89, "exit_time_terminating", "Exit Time Terminating", ZoneKind::Control),
            zr_desc!(90, "verification_blocking", "Verification Blocking", ZoneKind::Control),
            zr_desc!(91, "masking_sensor", "Masking Sensor", ZoneKind::Tamper),
            zr_desc!(92, "outputs_group_off", "Outputs Group Off", ZoneKind::Control),
            zr_desc!(93, "outputs_group_on", "Outputs Group On", ZoneKind::Control),
            zr_desc!(94, "entry_exit_interior", "Entry/Exit Interior", ZoneKind::Burglary),
            zr_desc!(95, "entry_interior", "Entry Interior", ZoneKind::Burglary),
            zr_desc!(96, "monitoring_fire", "Monitoring Fire", ZoneKind::Fire),
            zr_desc!(97, "monitoring_fire_panel_trouble", "Monitoring - Fire Panel Trouble", ZoneKind::Technical),
        ]
    }

    /// Constructs a `ZoneReaction` from raw protocol byte code (0..97).
    pub fn from_code(code: u8) -> Self {
        match code {
            0 => Self::EntryExit,
            1 => Self::Entry,
            2 => Self::DelayedWithSignaling,
            3 => Self::InteriorDelayed,
            4 => Self::Perimeter,
            5 => Self::Instant,
            6 => Self::Exit,
            7 => Self::AudibleSilent,
            8 => Self::Exterior,
            9 => Self::Tamper24h,
            10 => Self::Vibration24h,
            11 => Self::CashMachine24h,
            12 => Self::PanicAudible,
            13 => Self::PanicSilent,
            14 => Self::MedicalButton,
            15 => Self::MedicalRemote,
            16..=31 => Self::Counting(code - 16 + 1),
            32 => Self::Fire24h,
            33 => Self::FireSmoke24h,
            34 => Self::FireCombustion24h,
            35 => Self::FireWater24h,
            36 => Self::FireTemperature24h,
            37 => Self::FireButton24h,
            38 => Self::FireDuct24h,
            39 => Self::FireFlame24h,
            40 => Self::FireCircuitsProtection24h,
            41 => Self::WaterPressure24h,
            42 => Self::Co2Pressure24h,
            43 => Self::ValveSensor24h,
            44 => Self::WaterLevel24h,
            45 => Self::PumpsStart24h,
            46 => Self::PumpsFailure24h,
            47 => Self::NoAlarmAction,
            48 => Self::AuxiliaryGeneral24h,
            49 => Self::AuxiliaryGas24h,
            50 => Self::AuxiliaryFreeze24h,
            51 => Self::AuxiliaryHeatLoss24h,
            52 => Self::AuxiliaryWaterLeak24h,
            53 => Self::AuxiliarySecurityNonBurglary24h,
            54 => Self::AuxiliaryLowCylinderGasPressure24h,
            55 => Self::AuxiliaryHighTemp24h,
            56 => Self::AuxiliaryLowTemp24h,
            57 => Self::TechnicalDoorControl,
            58 => Self::TechnicalDoorButton,
            59 => Self::TechnicalAcTrouble,
            60 => Self::TechnicalBatteryTrouble,
            61 => Self::TechnicalGsmTrouble,
            62 => Self::TechnicalPowerSupplyOverload,
            63 => Self::Trouble,
            64..=79 => Self::BlockingGroup(code - 64 + 1),
            80 => Self::Arming,
            81 => Self::Disarming,
            82 => Self::ArmDisarm,
            83 => Self::AlarmClearing,
            84 => Self::GuardRound,
            85 => Self::EntryExitConditional,
            86 => Self::EntryExitFinal,
            87 => Self::ExitFinal,
            88 => Self::Burglary24h,
            89 => Self::ExitTimeTerminating,
            90 => Self::VerificationBlocking,
            91 => Self::MaskingSensor,
            92 => Self::OutputsGroupOff,
            93 => Self::OutputsGroupOn,
            94 => Self::EntryExitInterior,
            95 => Self::EntryInterior,
            96 => Self::MonitoringFire,
            97 => Self::MonitoringFirePanelTrouble,
            other => Self::Unknown(other),
        }
    }

    /// Returns the raw protocol byte code (0..97) or the unknown byte.
    pub fn code(&self) -> u8 {
        match self {
            Self::EntryExit => 0,
            Self::Entry => 1,
            Self::DelayedWithSignaling => 2,
            Self::InteriorDelayed => 3,
            Self::Perimeter => 4,
            Self::Instant => 5,
            Self::Exit => 6,
            Self::AudibleSilent => 7,
            Self::Exterior => 8,
            Self::Tamper24h => 9,
            Self::Vibration24h => 10,
            Self::CashMachine24h => 11,
            Self::PanicAudible => 12,
            Self::PanicSilent => 13,
            Self::MedicalButton => 14,
            Self::MedicalRemote => 15,
            Self::Counting(n) => 16 + (n.clamp(&1, &16) - 1),
            Self::Fire24h => 32,
            Self::FireSmoke24h => 33,
            Self::FireCombustion24h => 34,
            Self::FireWater24h => 35,
            Self::FireTemperature24h => 36,
            Self::FireButton24h => 37,
            Self::FireDuct24h => 38,
            Self::FireFlame24h => 39,
            Self::FireCircuitsProtection24h => 40,
            Self::WaterPressure24h => 41,
            Self::Co2Pressure24h => 42,
            Self::ValveSensor24h => 43,
            Self::WaterLevel24h => 44,
            Self::PumpsStart24h => 45,
            Self::PumpsFailure24h => 46,
            Self::NoAlarmAction => 47,
            Self::AuxiliaryGeneral24h => 48,
            Self::AuxiliaryGas24h => 49,
            Self::AuxiliaryFreeze24h => 50,
            Self::AuxiliaryHeatLoss24h => 51,
            Self::AuxiliaryWaterLeak24h => 52,
            Self::AuxiliarySecurityNonBurglary24h => 53,
            Self::AuxiliaryLowCylinderGasPressure24h => 54,
            Self::AuxiliaryHighTemp24h => 55,
            Self::AuxiliaryLowTemp24h => 56,
            Self::TechnicalDoorControl => 57,
            Self::TechnicalDoorButton => 58,
            Self::TechnicalAcTrouble => 59,
            Self::TechnicalBatteryTrouble => 60,
            Self::TechnicalGsmTrouble => 61,
            Self::TechnicalPowerSupplyOverload => 62,
            Self::Trouble => 63,
            Self::BlockingGroup(n) => 64 + (n.clamp(&1, &16) - 1),
            Self::Arming => 80,
            Self::Disarming => 81,
            Self::ArmDisarm => 82,
            Self::AlarmClearing => 83,
            Self::GuardRound => 84,
            Self::EntryExitConditional => 85,
            Self::EntryExitFinal => 86,
            Self::ExitFinal => 87,
            Self::Burglary24h => 88,
            Self::ExitTimeTerminating => 89,
            Self::VerificationBlocking => 90,
            Self::MaskingSensor => 91,
            Self::OutputsGroupOff => 92,
            Self::OutputsGroupOn => 93,
            Self::EntryExitInterior => 94,
            Self::EntryInterior => 95,
            Self::MonitoringFire => 96,
            Self::MonitoringFirePanelTrouble => 97,
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

    /// High-level device kind / category for this reaction type.
    pub fn kind(&self) -> ZoneKind {
        let c = self.code();
        if (c as usize) < Self::catalog().len() {
            Self::catalog()[c as usize].kind
        } else {
            ZoneKind::Other
        }
    }
}

impl std::fmt::Display for ZoneReaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label_en())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zone_reaction_catalog_completeness() {
        let catalog = ZoneReaction::catalog();
        assert_eq!(catalog.len(), 98);
        for (i, entry) in catalog.iter().enumerate() {
            assert_eq!(entry.code as usize, i);
            assert!(!entry.key.is_empty());
            assert!(!entry.label_en.is_empty());

            let reaction = ZoneReaction::from_code(entry.code);
            assert_eq!(reaction.code(), entry.code);
            assert_eq!(reaction.key(), entry.key);
            assert_eq!(reaction.label_en(), entry.label_en);
            assert_eq!(reaction.kind(), entry.kind);
            assert_eq!(format!("{}", reaction), entry.label_en);
        }
    }

    #[test]
    fn test_zone_reaction_unknown() {
        let reaction = ZoneReaction::from_code(250);
        assert_eq!(reaction, ZoneReaction::Unknown(250));
        assert_eq!(reaction.code(), 250);
        assert_eq!(reaction.key(), "unknown");
        assert_eq!(reaction.label_en(), "Unknown");
        assert_eq!(reaction.kind(), ZoneKind::Other);
    }

    #[test]
    fn test_zone_reaction_numbered_ranges() {
        // 16..=31 counting L1..L16
        for n in 1..=16 {
            let code = 16 + n - 1;
            let reaction = ZoneReaction::from_code(code);
            assert_eq!(reaction, ZoneReaction::Counting(n));
            assert_eq!(reaction.code(), code);
            assert_eq!(reaction.key(), format!("counting_l{}", n));
        }

        // 64..=79 blocking group 1..16
        for g in 1..=16 {
            let code = 64 + g - 1;
            let reaction = ZoneReaction::from_code(code);
            assert_eq!(reaction, ZoneReaction::BlockingGroup(g));
            assert_eq!(reaction.code(), code);
            assert_eq!(reaction.key(), format!("blocking_group_{}", g));
        }
    }
}

