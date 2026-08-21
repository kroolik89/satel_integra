use crate::error::SatelError;
use crate::state::{
    ZonesAlarmData, ZonesAlarmMemoryData, ZonesBypassData, ZonesLongViolationTroubleData,
    ZonesNoViolationTroubleData, ZonesTamperAlarmData, ZonesTamperAlarmMemoryData,
    ZonesTamperData, ZonesViolationData,
};
use chrono::Local;

/// Przetwarza całą ramkę odpowiedzi na komendę 0x7D (Read zone temperature).
/// Zwraca krotkę (Numer wejścia, Temperatura).
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

    // 0x0000 = -55.0°C, krok 0.5°C
    let temperature = (temp_raw as f32 * 0.5) - 55.0;

    Ok((zone_id, temperature))
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x01 (Zones tamper).
/// Uwzględnia listę wejść, których stan ma zostać odwrócony.
pub fn process_zones_tamper(frame: &[u8], invert_list: &[u16]) -> Result<ZonesTamperData, SatelError> {
    if frame.is_empty() || frame[0] != 0x01 {
        tracing::error!("Nieprawidłowy kod komendy w ramce sabotażu: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Dane sabotażu zbyt krótkie: {} bajtów", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesTamperData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x02 (Zones alarm).
/// Uwzględnia listę wejść, których stan ma zostać odwrócony.
pub fn process_zones_alarm(frame: &[u8], invert_list: &[u16]) -> Result<ZonesAlarmData, SatelError> {
    if frame.is_empty() || frame[0] != 0x02 {
        tracing::error!("Nieprawidłowy kod komendy w ramce alarmów: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Dane alarmów zbyt krótkie: {} bajtów", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesAlarmData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x00 (Zones violation).
/// Uwzględnia listę wejść, których stan ma zostać odwrócony.
pub fn process_zones_violation(frame: &[u8], invert_list: &[u16]) -> Result<ZonesViolationData, SatelError> {
    if frame.is_empty() || frame[0] != 0x00 {
        tracing::error!("Nieprawidłowy kod komendy w ramce naruszeń: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Dane naruszeń zbyt krótkie: {} bajtów", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesViolationData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x03 (Zones tamper alarm).
/// Uwzględnia listę wejść, których stan ma zostać odwrócony.
pub fn process_zones_tamper_alarm(frame: &[u8], invert_list: &[u16]) -> Result<ZonesTamperAlarmData, SatelError> {
    if frame.is_empty() || frame[0] != 0x03 {
        tracing::error!("Nieprawidłowy kod komendy w ramce alarmów sabotażowych: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Dane alarmów sabotażowych zbyt krótkie: {} bajtów", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesTamperAlarmData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x04 (Zones alarm memory).
/// Uwzględnia listę wejść, których stan ma zostać odwrócony.
pub fn process_zones_alarm_memory(frame: &[u8], invert_list: &[u16]) -> Result<ZonesAlarmMemoryData, SatelError> {
    if frame.is_empty() || frame[0] != 0x04 {
        tracing::error!("Nieprawidłowy kod komendy w ramce pamięci alarmów: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Dane pamięci alarmów zbyt krótkie: {} bajtów", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesAlarmMemoryData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x05 (Zones tamper alarm memory).
/// Uwzględnia listę wejść, których stan ma zostać odwrócony.
pub fn process_zones_tamper_alarm_memory(frame: &[u8], invert_list: &[u16]) -> Result<ZonesTamperAlarmMemoryData, SatelError> {
    if frame.is_empty() || frame[0] != 0x05 {
        tracing::error!("Nieprawidłowy kod komendy w ramce pamięci alarmów sabotażowych: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Dane pamięci alarmów sabotażowych zbyt krótkie: {} bajtów", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesTamperAlarmMemoryData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x06 (Zones bypass).
/// Uwzględnia listę wejść, których stan ma zostać odwrócony.
pub fn process_zones_bypass(frame: &[u8], invert_list: &[u16]) -> Result<ZonesBypassData, SatelError> {
    if frame.is_empty() || frame[0] != 0x06 {
        tracing::error!("Nieprawidłowy kod komendy w ramce blokad (bypass): {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Dane blokad (bypass) zbyt krótkie: {} bajtów", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesBypassData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x07 (Zones 'no violation' trouble).
/// Uwzględnia listę wejść, których stan ma zostać odwrócony.
pub fn process_zones_no_violation_trouble(frame: &[u8], invert_list: &[u16]) -> Result<ZonesNoViolationTroubleData, SatelError> {
    if frame.is_empty() || frame[0] != 0x07 {
        tracing::error!("Nieprawidłowy kod komendy w ramce awarii 'brak naruszenia': {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Dane awarii 'brak naruszenia' zbyt krótkie: {} bajtów", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesNoViolationTroubleData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x08 (Zones 'long violation' trouble).
/// Uwzględnia listę wejść, których stan ma zostać odwrócony.
pub fn process_zones_long_violation_trouble(frame: &[u8], invert_list: &[u16]) -> Result<ZonesLongViolationTroubleData, SatelError> {
    if frame.is_empty() || frame[0] != 0x08 {
        tracing::error!("Nieprawidłowy kod komendy w ramce awarii 'długie naruszenie': {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Dane awarii 'długie naruszenie' zbyt krótkie: {} bajtów", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesLongViolationTroubleData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Wspólny helper: parsuje bajty bitowe do Vec<bool> z opcjonalną inwersją.
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
