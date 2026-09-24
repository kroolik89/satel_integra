use std::time::Duration;
use chrono::Local;

use crate::error::SatelError;
use crate::output_catalog::OutputFunction;
use crate::partition_catalog::{
    AutoArmDeferTimer, DependentPartitions, PartitionOptions, PartitionType,
};
use crate::state::{OutputParams, PartitionParams, SatelName, ZoneParams};
use crate::zone_catalog::ZoneReaction;

fn decode_name(raw: &[u8]) -> String {
    let (name, _, has_errors) = encoding_rs::WINDOWS_1250.decode(raw);
    if has_errors {
        String::from_utf8_lossy(raw).trim().to_string()
    } else {
        name.trim().to_string()
    }
}

/// Parses the complete response frame for command 0xEE for a zone (device types 1 and 5).
/// Returns `(zone_id, SatelName, ZoneParams)`.
pub fn process_zone_response(frame: &[u8]) -> Result<(u16, SatelName, ZoneParams), SatelError> {
    if frame.is_empty() || frame[0] != 0xEE {
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 19 {
        tracing::error!("Data too short for zone name: {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    let dev_type = data[0];
    if dev_type != 1 && dev_type != 5 {
        tracing::error!("Wrong device type for zone in response: got {}", dev_type);
        return Err(SatelError::InvalidFrame);
    }

    if dev_type == 5 && data.len() < 20 {
        tracing::error!("Data too short for extended zone (type 5): {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    let id = if data[1] == 0 { 256 } else { data[1] as u16 };
    let reaction = ZoneReaction::from_code(data[2]);
    let name = decode_name(&data[3..19]);
    let now = Local::now();

    let partition = if dev_type == 5 {
        let part_num = data[19];
        if (1..=32).contains(&part_num) {
            Some(part_num)
        } else {
            None
        }
    } else {
        None
    };

    let satel_name = SatelName {
        name,
        read_at: now,
    };

    let params = ZoneParams {
        zone_id: id,
        reaction,
        partition,
        read_at: now,
    };

    Ok((id, satel_name, params))
}

/// Parses the complete response frame for command 0xEE for an output (device types 4 and 17).
/// Returns `(output_id, SatelName, OutputParams)`.
pub fn process_output_response(frame: &[u8]) -> Result<(u16, SatelName, OutputParams), SatelError> {
    if frame.is_empty() || frame[0] != 0xEE {
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 19 {
        tracing::error!("Data too short for output name: {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    let dev_type = data[0];
    if dev_type != 4 && dev_type != 17 {
        tracing::error!("Wrong device type for output in response: got {}", dev_type);
        return Err(SatelError::InvalidFrame);
    }

    if dev_type == 17 && data.len() < 21 {
        tracing::error!("Data too short for extended output (type 17): {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    let id = if data[1] == 0 { 256 } else { data[1] as u16 };
    let function = OutputFunction::from_code(data[2]);
    let name = decode_name(&data[3..19]);
    let now = Local::now();

    let (duration, control) = if dev_type == 17 {
        let duration_ds = ((data[19] as u16) << 8) | (data[20] as u16);
        let duration = Some(Duration::from_millis(duration_ds as u64 * 100));
        let control = function.control(Some(duration_ds));
        (duration, control)
    } else {
        let control = function.control(None);
        (None, control)
    };

    let satel_name = SatelName {
        name,
        read_at: now,
    };

    let params = OutputParams {
        output_id: id,
        function,
        duration,
        control,
        read_at: now,
    };

    Ok((id, satel_name, params))
}

/// Parses the complete response frame for command 0xEE for a partition (device types 0, 16, 18, 19).
/// Returns `(partition_id, SatelName, PartitionParams)`.
pub fn process_partition_response(frame: &[u8]) -> Result<(u16, SatelName, PartitionParams), SatelError> {
    if frame.is_empty() || frame[0] != 0xEE {
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 19 {
        tracing::error!("Data too short for partition name: {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    let dev_type = data[0];
    if dev_type != 0 && dev_type != 16 && dev_type != 18 && dev_type != 19 {
        tracing::error!("Wrong device type for partition in response: got {}", dev_type);
        return Err(SatelError::InvalidFrame);
    }

    if dev_type == 16 && data.len() < 20 {
        return Err(SatelError::InvalidFrame);
    }
    if dev_type == 18 && data.len() < 24 {
        return Err(SatelError::InvalidFrame);
    }
    if dev_type == 19 && data.len() < 28 {
        return Err(SatelError::InvalidFrame);
    }

    let id = data[1] as u16;
    let partition_type = PartitionType::from_code(data[2]);
    let name = decode_name(&data[3..19]);
    let now = Local::now();

    let object_number = if dev_type >= 16 {
        Some(data[19])
    } else {
        None
    };

    let (options, auto_arm_defer) = if dev_type >= 18 {
        let opts = PartitionOptions::from_bytes(data[20], data[21]);
        let defer_raw = ((data[22] as u16) << 8) | (data[23] as u16);
        let defer = AutoArmDeferTimer::from_raw(defer_raw);
        (Some(opts), Some(defer))
    } else {
        (None, None)
    };

    let dependent_partitions = if dev_type == 19 {
        Some(DependentPartitions::from_bytes([
            data[24], data[25], data[26], data[27],
        ]))
    } else {
        None
    };

    let satel_name = SatelName {
        name,
        read_at: now,
    };

    let params = PartitionParams {
        partition_id: id,
        partition_type,
        object_number,
        options,
        auto_arm_defer,
        dependent_partitions,
        read_at: now,
    };

    Ok((id, satel_name, params))
}

/// Parses the name response for partitions (device types 0, 16, 18, 19).
pub fn process_partition_name(data: &[u8]) -> Result<(u16, SatelName), SatelError> {
    let (id, name, _) = process_partition_response(data)?;
    Ok((id, name))
}

/// Parses the name response for zones (device types 1 and 5).
pub fn process_zone_name(data: &[u8]) -> Result<(u16, SatelName), SatelError> {
    let (id, name, _) = process_zone_response(data)?;
    Ok((id, name))
}

/// Parses the name response for outputs (device types 4 and 17).
pub fn process_output_name(data: &[u8]) -> Result<(u16, SatelName), SatelError> {
    let (id, name, _) = process_output_response(data)?;
    Ok((id, name))
}

/// Parses parameters for zones (device types 1 and 5).
pub fn process_zone_params(data: &[u8]) -> Result<(u16, ZoneParams), SatelError> {
    let (id, _, params) = process_zone_response(data)?;
    Ok((id, params))
}

/// Parses parameters for outputs (device types 4 and 17).
pub fn process_output_params(data: &[u8]) -> Result<(u16, OutputParams), SatelError> {
    let (id, _, params) = process_output_response(data)?;
    Ok((id, params))
}

/// Parses parameters for partitions (device types 0, 16, 18, 19).
pub fn process_partition_params(data: &[u8]) -> Result<(u16, PartitionParams), SatelError> {
    let (id, _, params) = process_partition_response(data)?;
    Ok((id, params))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output_catalog::OutputControl;

    fn pad_name(s: &str) -> [u8; 16] {
        let (encoded, _, _) = encoding_rs::WINDOWS_1250.encode(s);
        let mut buf = [b' '; 16];
        let len = encoded.len().min(16);
        buf[..len].copy_from_slice(&encoded[..len]);
        buf
    }

    #[test]
    fn test_parse_zone_type_1() {
        let mut frame = vec![0xEE, 1, 5, 2]; // type 1, id 5, reaction 2 (DelayedWithSignaling)
        frame.extend_from_slice(&pad_name("Salon Pir"));
        let (id, name, params) = process_zone_response(&frame).unwrap();

        assert_eq!(id, 5);
        assert_eq!(name.name, "Salon Pir");
        assert_eq!(params.reaction, ZoneReaction::DelayedWithSignaling);
        assert_eq!(params.partition, None);
    }

    #[test]
    fn test_parse_zone_type_5() {
        let mut frame = vec![0xEE, 5, 12, 1]; // type 5, id 12, reaction 1 (Entry)
        frame.extend_from_slice(&pad_name("Wiatrołap"));
        frame.push(3); // partition 3

        let (id, name, params) = process_zone_response(&frame).unwrap();
        assert_eq!(id, 12);
        assert_eq!(name.name, "Wiatrołap");
        assert_eq!(params.reaction, ZoneReaction::Entry);
        assert_eq!(params.partition, Some(3));
    }

    #[test]
    fn test_parse_output_type_4() {
        let mut frame = vec![0xEE, 4, 8, 25]; // type 4, id 8, function 25 (BiSwitch)
        frame.extend_from_slice(&pad_name("Oświetlenie"));

        let (id, name, params) = process_output_response(&frame).unwrap();
        assert_eq!(id, 8);
        assert_eq!(name.name, "Oświetlenie");
        assert_eq!(params.function, OutputFunction::BiSwitch);
        assert_eq!(params.duration, None);
        assert_eq!(params.control, OutputControl::Bistable);
    }

    #[test]
    fn test_parse_output_type_17() {
        let mut frame = vec![0xEE, 17, 3, 24]; // type 17, id 3, function 24 (MonoSwitch)
        frame.extend_from_slice(&pad_name("Furtka"));
        frame.extend_from_slice(&[0x00, 0x32]); // 50 * 0.1s = 5s

        let (id, name, params) = process_output_response(&frame).unwrap();
        assert_eq!(id, 3);
        assert_eq!(name.name, "Furtka");
        assert_eq!(params.function, OutputFunction::MonoSwitch);
        assert_eq!(params.duration, Some(Duration::from_millis(5000)));
        assert_eq!(
            params.control,
            OutputControl::Timed {
                duration: Some(Duration::from_millis(5000))
            }
        );
    }

    #[test]
    fn test_parse_output_type_17_telephone_relay() {
        // Telephone relay with duration > 0 -> Timed
        let mut frame = vec![0xEE, 17, 10, 64]; // telephone relay 1
        frame.extend_from_slice(&pad_name("Przekaźnik 1"));
        frame.extend_from_slice(&[0x00, 0x14]); // 20 * 0.1s = 2s
        let (_, _, params) = process_output_response(&frame).unwrap();
        assert_eq!(
            params.control,
            OutputControl::Timed {
                duration: Some(Duration::from_millis(2000))
            }
        );

        // Telephone relay with duration == 0 -> Bistable
        let mut frame_bi = vec![0xEE, 17, 10, 64];
        frame_bi.extend_from_slice(&pad_name("Przekaźnik 1"));
        frame_bi.extend_from_slice(&[0x00, 0x00]); // 0s
        let (_, _, params_bi) = process_output_response(&frame_bi).unwrap();
        assert_eq!(params_bi.control, OutputControl::Bistable);

        // Telephone relay via type 4 (no duration) -> Unknown
        let mut frame_t4 = vec![0xEE, 4, 10, 64];
        frame_t4.extend_from_slice(&pad_name("Przekaźnik 1"));
        let (_, _, params_t4) = process_output_response(&frame_t4).unwrap();
        assert_eq!(params_t4.control, OutputControl::Unknown);
    }

    #[test]
    fn test_parse_partition_type_0() {
        let mut frame = vec![0xEE, 0, 1, 0]; // type 0, id 1, Normal
        frame.extend_from_slice(&pad_name("Dom"));

        let (id, name, params) = process_partition_response(&frame).unwrap();
        assert_eq!(id, 1);
        assert_eq!(name.name, "Dom");
        assert_eq!(params.partition_type, PartitionType::Normal);
        assert_eq!(params.object_number, None);
        assert_eq!(params.options, None);
        assert_eq!(params.auto_arm_defer, None);
        assert_eq!(params.dependent_partitions, None);
    }

    #[test]
    fn test_parse_partition_type_16() {
        let mut frame = vec![0xEE, 16, 2, 1]; // type 16, id 2, WithBlocking
        frame.extend_from_slice(&pad_name("Garaż"));
        frame.push(2); // object 2

        let (id, name, params) = process_partition_response(&frame).unwrap();
        assert_eq!(id, 2);
        assert_eq!(name.name, "Garaż");
        assert_eq!(params.partition_type, PartitionType::TimedBlocking);
        assert_eq!(params.object_number, Some(2));
        assert_eq!(params.options, None);
    }

    #[test]
    fn test_parse_partition_type_18() {
        let mut frame = vec![0xEE, 18, 3, 2]; // type 18, id 3, DependentAnd
        frame.extend_from_slice(&pad_name("Ogród"));
        frame.push(1); // object 1
        frame.push(0b0000_0001); // opt1: two_codes_to_arm
        frame.push(0b1000_0000); // opt2: auto_arm_can_be_deferred
        frame.extend_from_slice(&((2u16 << 14) | 300).to_be_bytes()); // defer: status=2 (Running), 300s

        let (id, name, params) = process_partition_response(&frame).unwrap();
        assert_eq!(id, 3);
        assert_eq!(name.name, "Ogród");
        assert_eq!(params.partition_type, PartitionType::DependentAnd);
        assert_eq!(params.object_number, Some(1));

        let opts = params.options.unwrap();
        assert!(opts.two_codes_to_arm);
        assert!(opts.auto_arm_can_be_deferred);

        let defer = params.auto_arm_defer.unwrap();
        assert_eq!(defer.status, crate::partition_catalog::AutoArmDeferStatus::Running);
        assert_eq!(defer.defer_time_s, 300);
        assert_eq!(params.dependent_partitions, None);
    }

    #[test]
    fn test_parse_partition_type_19() {
        let mut frame = vec![0xEE, 19, 4, 3]; // type 19, id 4, DependentOr
        frame.extend_from_slice(&pad_name("Magazyn"));
        frame.push(1); // object 1
        frame.push(0); // opt1
        frame.push(0); // opt2
        frame.extend_from_slice(&[0, 0]); // defer

        // Dependent partitions 1 and 2 (0b0000_0011)
        frame.extend_from_slice(&[0b0000_0011, 0, 0, 0]);

        let (id, name, params) = process_partition_response(&frame).unwrap();
        assert_eq!(id, 4);
        assert_eq!(name.name, "Magazyn");
        assert_eq!(params.partition_type, PartitionType::DependentOr);
        assert_eq!(params.object_number, Some(1));

        let dep = params.dependent_partitions.unwrap();
        assert!(dep.contains(1));
        assert!(dep.contains(2));
        assert!(!dep.contains(3));
        assert_eq!(dep.list(), vec![1, 2]);
    }
}

