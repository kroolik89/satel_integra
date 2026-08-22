use crate::error::SatelError;
use crate::state::{
    ZonesAlarmData, ZonesAlarmMemoryData, ZonesBypassData, ZonesLongViolationTroubleData,
    ZonesNoViolationTroubleData, ZonesTamperAlarmData, ZonesTamperAlarmMemoryData,
    ZonesTamperData, ZonesViolationData,
};
use chrono::Local;

/// Parses the complete response frame for command 0x7D (Read zone temperature).
/// Returns a tuple `(Zone ID, Temperature in °C)`.
pub fn process_zone_temperature(frame: &[u8]) -> Result<(u16, f32), SatelError> {
    if frame.is_empty() || frame[0] != 0x7D {
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 3 {
        return Err(SatelError::InvalidFrame);
    }

    let zone_id = if data[0] == 0 { 256 } else { data[0] as u16 };
    let temp_raw = u16::from_be_bytes([data[1], data[2]]);

    if temp_raw == 0xFFFF {
        return Err(SatelError::TemperatureSensorError);
    }

    // 0x0000 = -55.0°C, 0.5°C step
    let temperature = (temp_raw as f32 * 0.5) - 55.0;

    Ok((zone_id, temperature))
}

/// Parses the complete response frame for command 0x01 (Zones tamper).
/// Applies inversion for zones listed in `invert_list`.
pub fn process_zones_tamper(frame: &[u8], invert_list: &[u16]) -> Result<ZonesTamperData, SatelError> {
    if frame.is_empty() || frame[0] != 0x01 {
        tracing::error!("Invalid command byte in zone tamper frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Zone tamper data frame too short: {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesTamperData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x02 (Zones alarm).
/// Applies inversion for zones listed in `invert_list`.
pub fn process_zones_alarm(frame: &[u8], invert_list: &[u16]) -> Result<ZonesAlarmData, SatelError> {
    if frame.is_empty() || frame[0] != 0x02 {
        tracing::error!("Invalid command byte in zone alarm frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Zone alarm data frame too short: {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesAlarmData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x00 (Zones violation).
/// Applies inversion for zones listed in `invert_list`.
pub fn process_zones_violation(frame: &[u8], invert_list: &[u16]) -> Result<ZonesViolationData, SatelError> {
    if frame.is_empty() || frame[0] != 0x00 {
        tracing::error!("Invalid command byte in zone violation frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Zone violation data frame too short: {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesViolationData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x03 (Zones tamper alarm).
/// Applies inversion for zones listed in `invert_list`.
pub fn process_zones_tamper_alarm(frame: &[u8], invert_list: &[u16]) -> Result<ZonesTamperAlarmData, SatelError> {
    if frame.is_empty() || frame[0] != 0x03 {
        tracing::error!("Invalid command byte in zone tamper alarm frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Zone tamper alarm data frame too short: {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesTamperAlarmData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x04 (Zones alarm memory).
/// Applies inversion for zones listed in `invert_list`.
pub fn process_zones_alarm_memory(frame: &[u8], invert_list: &[u16]) -> Result<ZonesAlarmMemoryData, SatelError> {
    if frame.is_empty() || frame[0] != 0x04 {
        tracing::error!("Invalid command byte in zone alarm memory frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Zone alarm memory data frame too short: {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesAlarmMemoryData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x05 (Zones tamper alarm memory).
/// Applies inversion for zones listed in `invert_list`.
pub fn process_zones_tamper_alarm_memory(frame: &[u8], invert_list: &[u16]) -> Result<ZonesTamperAlarmMemoryData, SatelError> {
    if frame.is_empty() || frame[0] != 0x05 {
        tracing::error!("Invalid command byte in zone tamper alarm memory frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Zone tamper alarm memory data frame too short: {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesTamperAlarmMemoryData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x06 (Zones bypass).
/// Applies inversion for zones listed in `invert_list`.
pub fn process_zones_bypass(frame: &[u8], invert_list: &[u16]) -> Result<ZonesBypassData, SatelError> {
    if frame.is_empty() || frame[0] != 0x06 {
        tracing::error!("Invalid command byte in zone bypass frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Zone bypass data frame too short: {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesBypassData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x07 (Zones 'no violation' trouble).
/// Applies inversion for zones listed in `invert_list`.
pub fn process_zones_no_violation_trouble(frame: &[u8], invert_list: &[u16]) -> Result<ZonesNoViolationTroubleData, SatelError> {
    if frame.is_empty() || frame[0] != 0x07 {
        tracing::error!("Invalid command byte in zone 'no violation' trouble frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Zone 'no violation' trouble data frame too short: {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesNoViolationTroubleData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x08 (Zones 'long violation' trouble).
/// Applies inversion for zones listed in `invert_list`.
pub fn process_zones_long_violation_trouble(frame: &[u8], invert_list: &[u16]) -> Result<ZonesLongViolationTroubleData, SatelError> {
    if frame.is_empty() || frame[0] != 0x08 {
        tracing::error!("Invalid command byte in zone 'long violation' trouble frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Zone 'long violation' trouble data frame too short: {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesLongViolationTroubleData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Common helper: parses bitwise mask bytes into `Vec<bool>` with optional zone state inversion.
fn parse_bit_states(data: &[u8], invert_list: &[u16]) -> Vec<bool> {
    let mut states = Vec::with_capacity(data.len() * 8);
    for (byte_idx, &byte) in data.iter().enumerate() {
        for bit in 0..8 {
            let zone_id = (byte_idx * 8 + bit + 1) as u16;
            let raw_state = (byte & (1 << bit)) != 0;
            let final_state = if invert_list.contains(&zone_id) {
                !raw_state
            } else {
                raw_state
            };
            states.push(final_state);
        }
    }
    states
}
