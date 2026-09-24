use serde::{Deserialize, Serialize};

/// Catalog entry descriptor for partition types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionTypeDescriptor {
    pub code: u8,
    pub key: &'static str,
    pub label_en: &'static str,
}

/// Partition type (Integra codes 0..3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PartitionType {
    Normal,
    TimedBlocking,
    DependentAnd,
    DependentOr,
    Unknown(u8),
}

impl PartitionType {
    /// Full catalog of standard Integra partition types (codes 0..3).
    pub fn catalog() -> &'static [PartitionTypeDescriptor] {
        &[
            PartitionTypeDescriptor {
                code: 0,
                key: "normal",
                label_en: "Normal",
            },
            PartitionTypeDescriptor {
                code: 1,
                key: "timed_blocking",
                label_en: "With Timed Blocking",
            },
            PartitionTypeDescriptor {
                code: 2,
                key: "dependent_and",
                label_en: "Dependent (AND)",
            },
            PartitionTypeDescriptor {
                code: 3,
                key: "dependent_or",
                label_en: "Dependent (OR)",
            },
        ]
    }

    /// Constructs a `PartitionType` from raw protocol byte code (0..3).
    pub fn from_code(code: u8) -> Self {
        match code {
            0 => Self::Normal,
            1 => Self::TimedBlocking,
            2 => Self::DependentAnd,
            3 => Self::DependentOr,
            other => Self::Unknown(other),
        }
    }

    /// Returns the raw protocol byte code (0..3) or the unknown value.
    pub fn code(&self) -> u8 {
        match self {
            Self::Normal => 0,
            Self::TimedBlocking => 1,
            Self::DependentAnd => 2,
            Self::DependentOr => 3,
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
}

impl std::fmt::Display for PartitionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label_en())
    }
}

/// Bitwise options parsed from 0xEE response (device types 18 & 19).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PartitionOptions {
    /// .0: 1 = two codes to arm
    pub two_codes_to_arm: bool,
    /// .1: 1 = two codes to disarm
    pub two_codes_to_disarm: bool,
    /// .2: 1 = timer priority
    pub timer_priority: bool,
    /// .3: 1 = two codes on two devices
    pub two_codes_on_two_devices: bool,
    /// .4: 1 = alarm verification
    pub alarm_verification: bool,
    /// .5: 1 = exit delay can be shorten
    pub exit_delay_can_be_shortened: bool,
    /// .6: 1 = infinite exit delay
    pub infinite_exit_delay: bool,
    /// .7: 1 = constant (undefinable) 1st code validity period
    pub constant_first_code_validity_period: bool,

    /// Byte 2 .0: 1 = constant (uneditable) blocking time (if partition type is 1)
    pub constant_blocking_time: bool,
    /// Byte 2 .1: 1 = do not disarm in case of alarm (if partition type is 1)
    pub do_not_disarm_on_alarm: bool,
    /// Byte 2 .7: 1 = auto-arm can be deferred
    pub auto_arm_can_be_deferred: bool,

    /// Raw byte 1 of options.
    pub raw_byte_1: u8,
    /// Raw byte 2 of options.
    pub raw_byte_2: u8,
}

impl PartitionOptions {
    pub fn from_bytes(b1: u8, b2: u8) -> Self {
        Self {
            two_codes_to_arm: (b1 & 0x01) != 0,
            two_codes_to_disarm: (b1 & 0x02) != 0,
            timer_priority: (b1 & 0x04) != 0,
            two_codes_on_two_devices: (b1 & 0x08) != 0,
            alarm_verification: (b1 & 0x10) != 0,
            exit_delay_can_be_shortened: (b1 & 0x20) != 0,
            infinite_exit_delay: (b1 & 0x40) != 0,
            constant_first_code_validity_period: (b1 & 0x80) != 0,

            constant_blocking_time: (b2 & 0x01) != 0,
            do_not_disarm_on_alarm: (b2 & 0x02) != 0,
            auto_arm_can_be_deferred: (b2 & 0x80) != 0,

            raw_byte_1: b1,
            raw_byte_2: b2,
        }
    }
}

/// Auto-arm defer countdown status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutoArmDeferStatus {
    #[default]
    Inactive,
    Set,
    Running,
    Unknown(u8),
}

/// Auto-arm defer timer configuration and state (from 0xEE types 18 & 19).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AutoArmDeferTimer {
    pub status: AutoArmDeferStatus,
    /// Remaining or configured defer time in seconds (14 lsb).
    pub defer_time_s: u16,
    pub raw: u16,
}

impl AutoArmDeferTimer {
    pub fn from_raw(raw: u16) -> Self {
        let status_bits = (raw >> 14) as u8;
        let status = match status_bits {
            0 => AutoArmDeferStatus::Inactive,
            1 => AutoArmDeferStatus::Set,
            2 => AutoArmDeferStatus::Running,
            other => AutoArmDeferStatus::Unknown(other),
        };
        let defer_time_s = raw & 0x3FFF;
        Self {
            status,
            defer_time_s,
            raw,
        }
    }
}

/// Dependent partitions bitmask (32 bits, 1..=32) from 0xEE (type 19).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DependentPartitions {
    pub raw: u32,
}

impl DependentPartitions {
    pub fn from_bytes(bytes: [u8; 4]) -> Self {
        // Integers in Satel frames are typically little-endian for bitmasks
        Self {
            raw: u32::from_le_bytes(bytes),
        }
    }

    /// Checks if a 1-based partition ID (1..=32) is included in the dependent set.
    pub fn contains(&self, partition_id: u8) -> bool {
        if (1..=32).contains(&partition_id) {
            (self.raw & (1 << (partition_id - 1))) != 0
        } else {
            false
        }
    }

    /// Returns list of all 1-based partition IDs (1..=32) that this partition depends on.
    pub fn list(&self) -> Vec<u8> {
        (1..=32).filter(|&id| self.contains(id)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_type_catalog() {
        let catalog = PartitionType::catalog();
        assert_eq!(catalog.len(), 4);
        assert_eq!(catalog[0].key, "normal");
        assert_eq!(catalog[1].key, "timed_blocking");
        assert_eq!(catalog[2].key, "dependent_and");
        assert_eq!(catalog[3].key, "dependent_or");

        assert_eq!(PartitionType::from_code(0), PartitionType::Normal);
        assert_eq!(PartitionType::from_code(1), PartitionType::TimedBlocking);
        assert_eq!(PartitionType::from_code(2), PartitionType::DependentAnd);
        assert_eq!(PartitionType::from_code(3), PartitionType::DependentOr);
        assert_eq!(PartitionType::from_code(10), PartitionType::Unknown(10));

        assert_eq!(PartitionType::Normal.code(), 0);
        assert_eq!(PartitionType::TimedBlocking.code(), 1);
        assert_eq!(PartitionType::DependentAnd.code(), 2);
        assert_eq!(PartitionType::DependentOr.code(), 3);
        assert_eq!(PartitionType::Unknown(10).code(), 10);
    }

    #[test]
    fn test_partition_options_bitmask() {
        let opts = PartitionOptions::from_bytes(0b1010_0101, 0b1000_0011);
        assert!(opts.two_codes_to_arm); // bit 0
        assert!(!opts.two_codes_to_disarm); // bit 1
        assert!(opts.timer_priority); // bit 2
        assert!(!opts.two_codes_on_two_devices); // bit 3
        assert!(!opts.alarm_verification); // bit 4
        assert!(opts.exit_delay_can_be_shortened); // bit 5
        assert!(!opts.infinite_exit_delay); // bit 6
        assert!(opts.constant_first_code_validity_period); // bit 7

        assert!(opts.constant_blocking_time); // b2 bit 0
        assert!(opts.do_not_disarm_on_alarm); // b2 bit 1
        assert!(opts.auto_arm_can_be_deferred); // b2 bit 7
    }

    #[test]
    fn test_auto_arm_defer_timer() {
        // Raw: status = 2 (Running), time = 120s
        let raw = (2 << 14) | 120;
        let timer = AutoArmDeferTimer::from_raw(raw);
        assert_eq!(timer.status, AutoArmDeferStatus::Running);
        assert_eq!(timer.defer_time_s, 120);

        // Status = 1 (Set)
        let timer_set = AutoArmDeferTimer::from_raw((1 << 14) | 3600);
        assert_eq!(timer_set.status, AutoArmDeferStatus::Set);
        assert_eq!(timer_set.defer_time_s, 3600);

        // Status = 0 (Inactive)
        let timer_inact = AutoArmDeferTimer::from_raw(0);
        assert_eq!(timer_inact.status, AutoArmDeferStatus::Inactive);
        assert_eq!(timer_inact.defer_time_s, 0);
    }

    #[test]
    fn test_dependent_partitions_bitmask() {
        // Partitions 1, 3, 32 set
        let mask = (1 << 0) | (1 << 2) | (1 << 31);
        let bytes = (mask as u32).to_le_bytes();
        let dep = DependentPartitions::from_bytes(bytes);

        assert!(dep.contains(1));
        assert!(!dep.contains(2));
        assert!(dep.contains(3));
        assert!(dep.contains(32));
        assert!(!dep.contains(0)); // invalid
        assert!(!dep.contains(33)); // invalid

        assert_eq!(dep.list(), vec![1, 3, 32]);
    }
}

