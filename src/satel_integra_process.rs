use crate::satel_integra_data::{
    IntegraVersion, OutputsStateData, PartitionsArmedData, PartitionsData, SatelName,
    ZonesAlarmData, ZonesAlarmMemoryData, ZonesBypassData, ZonesLongViolationTroubleData,
    ZonesNoViolationTroubleData, ZonesTamperAlarmData, ZonesTamperAlarmMemoryData, ZonesTamperData,
    ZonesViolationData,
};
use crate::satel_integra::SatelError;
use chrono::Local;

/// Przetwarza całą ramkę odpowiedzi na komendę 0x7E (Wersja centrali).
pub fn process_integra_version(frame: &[u8]) -> Result<IntegraVersion, SatelError> {
    if frame.is_empty() || frame[0] != 0x7E {
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 14 {
        return Err(SatelError::InvalidFrame);
    }

    // 1 bajt: Typ centrali
    let type_code = data[0];
    let (model, io_count) = match type_code {
        0 => ("INTEGRA 24", 24),
        1 => ("INTEGRA 32", 32),
        2 => ("INTEGRA 64", 64),
        3 => ("INTEGRA 128", 128),
        4 => ("INTEGRA 128-WRL SIM300", 128),
        132 => ("INTEGRA 128-WRL LEON", 128),
        66 => ("INTEGRA 64 Plus", 64),
        67 => ("INTEGRA 128 Plus", 128),
        72 => ("INTEGRA 256 Plus", 256),
        8 => ("INTEGRA 256 Plus", 256), // Według opisu 0x1A RTC status bits
        _ => ("Nieznana INTEGRA", 0),
    };

    // 11 bajtów: Wersja i data (ASCII)
    let version_raw = &data[1..12];
    let firmware_version = String::from_utf8_lossy(version_raw).trim().to_string();

    // 1 bajt: Język
    let lang_code = data[12];
    let language = match lang_code {
        0 => "PL",
        1 => "EN",
        _ => "Inny",
    }.to_string();

    // 1 bajt: Zapis we FLASH (255 = Tak, inaczej = Nie)
    let stored_in_flash = data[13] == 255;

    Ok(IntegraVersion {
        model: model.to_string(),
        firmware_version,
        language,
        stored_in_flash,
        io_count,
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0xEE (Read device name).
/// Zwraca krotkę (Numer urządzenia, Dane nazwy).
fn process_device_name(frame: &[u8], expected_type: u8) -> Result<(u16, SatelName), SatelError> {
    if frame.is_empty() || frame[0] != 0xEE {
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 19 {
        tracing::error!("Data too short for device name: {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    if data[0] != expected_type {
        tracing::error!("Wrong device type in response: expected {}, got {}", expected_type, data[0]);
        return Err(SatelError::InvalidFrame);
    }

    let id = if data[1] == 0 { 256 } else { data[1] as u16 };
    let name_raw = &data[3..19];
    
    let (name, _, has_errors) = encoding_rs::WINDOWS_1250.decode(name_raw);
    let name = if has_errors {
        String::from_utf8_lossy(name_raw).trim().to_string()
    } else {
        name.trim().to_string()
    };

    Ok((
        id,
        SatelName {
            name,
            read_at: Local::now(),
        },
    ))
}

/// Przetwarza odpowiedź dla stref/partycji (typ 0).
pub fn process_partition_name(data: &[u8]) -> Result<(u16, SatelName), SatelError> {
    process_device_name(data, 0)
}

/// Przetwarza odpowiedź dla wejść (typ 1).
pub fn process_zone_name(data: &[u8]) -> Result<(u16, SatelName), SatelError> {
    process_device_name(data, 1)
}

/// Przetwarza odpowiedź dla wyjść (typ 4).
pub fn process_output_name(data: &[u8]) -> Result<(u16, SatelName), SatelError> {
    process_device_name(data, 4)
}

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
        return Err(SatelError::TemperatureSensorError); // Undetermined temperature
    }

    // 0x0000 = -55.0°C, krok 0.5°C
    let temperature = (temp_raw as f32 * 0.5) - 55.0;

    Ok((zone_id, temperature))
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x01 (Zones tamper).
/// Uwzględnia listę wejść, których stan ma zostać odwrócony.
pub fn process_zones_tamper(frame: &[u8], invert_list: &[u16]) -> Result<ZonesTamperData, SatelError> {
    if frame.is_empty() || frame[0] != 0x01 {
        tracing::error!("Nieprawidłowy kod komendy w ramce sabotażu: {:02X?}", frame.get(0));
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    // Satel zwraca 16 (128 wejść) lub 32 (256 wejść) bajty danych
    if data.len() < 16 {
        tracing::error!("Dane sabotażu zbyt krótkie: {} bajtów", data.len());
        return Err(SatelError::InvalidFrame);
    }

    let mut states = Vec::with_capacity(data.len() * 8);
    for (byte_idx, &byte) in data.iter().enumerate() {
        for bit in 0..8 {
            let zone_id = (byte_idx * 8 + bit + 1) as u16;
            let raw_state = (byte & (1 << bit)) != 0;
            
            // Inwersja stanu jeśli zone_id znajduje się na liście invert_list
            let final_state = if invert_list.contains(&zone_id) {
                !raw_state
            } else {
                raw_state
            };
            states.push(final_state);
        }
    }

    Ok(ZonesTamperData {
        states,
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x02 (Zones alarm).
/// Uwzględnia listę wejść, których stan ma zostać odwrócony.
pub fn process_zones_alarm(frame: &[u8], invert_list: &[u16]) -> Result<ZonesAlarmData, SatelError> {
    if frame.is_empty() || frame[0] != 0x02 {
        tracing::error!("Nieprawidłowy kod komendy w ramce alarmów: {:02X?}", frame.get(0));
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    // Satel zwraca 16 (128 wejść) lub 32 (256 wejść) bajty danych
    if data.len() < 16 {
        tracing::error!("Dane alarmów zbyt krótkie: {} bajtów", data.len());
        return Err(SatelError::InvalidFrame);
    }

    let mut states = Vec::with_capacity(data.len() * 8);
    for (byte_idx, &byte) in data.iter().enumerate() {
        for bit in 0..8 {
            let zone_id = (byte_idx * 8 + bit + 1) as u16;
            let raw_state = (byte & (1 << bit)) != 0;
            
            // Inwersja stanu jeśli zone_id znajduje się na liście invert_list
            let final_state = if invert_list.contains(&zone_id) {
                !raw_state
            } else {
                raw_state
            };
            states.push(final_state);
        }
    }

    Ok(ZonesAlarmData {
        states,
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x00 (Zones violation).
/// Uwzględnia listę wejść, których stan ma zostać odwrócony.
pub fn process_zones_violation(frame: &[u8], invert_list: &[u16]) -> Result<ZonesViolationData, SatelError> {
    if frame.is_empty() || frame[0] != 0x00 {
        tracing::error!("Nieprawidłowy kod komendy w ramce naruszeń: {:02X?}", frame.get(0));
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    // Satel zwraca 16 (128 wejść) lub 32 (256 wejść) bajty danych
    if data.len() < 16 {
        tracing::error!("Dane naruszeń zbyt krótkie: {} bajtów", data.len());
        return Err(SatelError::InvalidFrame);
    }

    let mut states = Vec::with_capacity(data.len() * 8);
    for (byte_idx, &byte) in data.iter().enumerate() {
        for bit in 0..8 {
            let zone_id = (byte_idx * 8 + bit + 1) as u16;
            let raw_state = (byte & (1 << bit)) != 0;
            
            // Inwersja stanu jeśli zone_id znajduje się na liście invert_list
            let final_state = if invert_list.contains(&zone_id) {
                !raw_state
            } else {
                raw_state
            };
            states.push(final_state);
        }
    }

    Ok(ZonesViolationData {
        states,
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x03 (Zones tamper alarm).
/// Uwzględnia listę wejść, których stan ma zostać odwrócony.
pub fn process_zones_tamper_alarm(frame: &[u8], invert_list: &[u16]) -> Result<ZonesTamperAlarmData, SatelError> {
    if frame.is_empty() || frame[0] != 0x03 {
        tracing::error!("Nieprawidłowy kod komendy w ramce alarmów sabotażowych: {:02X?}", frame.get(0));
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    // Satel zwraca 16 (128 wejść) lub 32 (256 wejść) bajty danych
    if data.len() < 16 {
        tracing::error!("Dane alarmów sabotażowych zbyt krótkie: {} bajtów", data.len());
        return Err(SatelError::InvalidFrame);
    }

    let mut states = Vec::with_capacity(data.len() * 8);
    for (byte_idx, &byte) in data.iter().enumerate() {
        for bit in 0..8 {
            let zone_id = (byte_idx * 8 + bit + 1) as u16;
            let raw_state = (byte & (1 << bit)) != 0;
            
            // Inwersja stanu jeśli zone_id znajduje się na liście invert_list
            let final_state = if invert_list.contains(&zone_id) {
                !raw_state
            } else {
                raw_state
            };
            states.push(final_state);
        }
    }

    Ok(ZonesTamperAlarmData {
        states,
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x04 (Zones alarm memory).
/// Uwzględnia listę wejść, których stan ma zostać odwrócony.
pub fn process_zones_alarm_memory(frame: &[u8], invert_list: &[u16]) -> Result<ZonesAlarmMemoryData, SatelError> {
    if frame.is_empty() || frame[0] != 0x04 {
        tracing::error!("Nieprawidłowy kod komendy w ramce pamięci alarmów: {:02X?}", frame.get(0));
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    // Satel zwraca 16 (128 wejść) lub 32 (256 wejść) bajty danych
    if data.len() < 16 {
        tracing::error!("Dane pamięci alarmów zbyt krótkie: {} bajtów", data.len());
        return Err(SatelError::InvalidFrame);
    }

    let mut states = Vec::with_capacity(data.len() * 8);
    for (byte_idx, &byte) in data.iter().enumerate() {
        for bit in 0..8 {
            let zone_id = (byte_idx * 8 + bit + 1) as u16;
            let raw_state = (byte & (1 << bit)) != 0;
            
            // Inwersja stanu jeśli zone_id znajduje się na liście invert_list
            let final_state = if invert_list.contains(&zone_id) {
                !raw_state
            } else {
                raw_state
            };
            states.push(final_state);
        }
    }

    Ok(ZonesAlarmMemoryData {
        states,
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x05 (Zones tamper alarm memory).
/// Uwzględnia listę wejść, których stan ma zostać odwrócony.
pub fn process_zones_tamper_alarm_memory(frame: &[u8], invert_list: &[u16]) -> Result<ZonesTamperAlarmMemoryData, SatelError> {
    if frame.is_empty() || frame[0] != 0x05 {
        tracing::error!("Nieprawidłowy kod komendy w ramce pamięci alarmów sabotażowych: {:02X?}", frame.get(0));
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    // Satel zwraca 16 (128 wejść) lub 32 (256 wejść) bajty danych
    if data.len() < 16 {
        tracing::error!("Dane pamięci alarmów sabotażowych zbyt krótkie: {} bajtów", data.len());
        return Err(SatelError::InvalidFrame);
    }

    let mut states = Vec::with_capacity(data.len() * 8);
    for (byte_idx, &byte) in data.iter().enumerate() {
        for bit in 0..8 {
            let zone_id = (byte_idx * 8 + bit + 1) as u16;
            let raw_state = (byte & (1 << bit)) != 0;
            
            // Inwersja stanu jeśli zone_id znajduje się na liście invert_list
            let final_state = if invert_list.contains(&zone_id) {
                !raw_state
            } else {
                raw_state
            };
            states.push(final_state);
        }
    }

    Ok(ZonesTamperAlarmMemoryData {
        states,
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x07 (Zones 'no violation' trouble).
/// Uwzględnia listę wejść, których stan ma zostać odwrócony.
pub fn process_zones_no_violation_trouble(frame: &[u8], invert_list: &[u16]) -> Result<ZonesNoViolationTroubleData, SatelError> {
    if frame.is_empty() || frame[0] != 0x07 {
        tracing::error!("Nieprawidłowy kod komendy w ramce awarii 'brak naruszenia': {:02X?}", frame.get(0));
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    // Satel zwraca 16 (128 wejść) lub 32 (256 wejść) bajty danych
    if data.len() < 16 {
        tracing::error!("Dane awarii 'brak naruszenia' zbyt krótkie: {} bajtów", data.len());
        return Err(SatelError::InvalidFrame);
    }

    let mut states = Vec::with_capacity(data.len() * 8);
    for (byte_idx, &byte) in data.iter().enumerate() {
        for bit in 0..8 {
            let zone_id = (byte_idx * 8 + bit + 1) as u16;
            let raw_state = (byte & (1 << bit)) != 0;
            
            // Inwersja stanu jeśli zone_id znajduje się na liście invert_list
            let final_state = if invert_list.contains(&zone_id) {
                !raw_state
            } else {
                raw_state
            };
            states.push(final_state);
        }
    }

    Ok(ZonesNoViolationTroubleData {
        states,
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x06 (Zones bypass).
/// Uwzględnia listę wejść, których stan ma zostać odwrócony.
pub fn process_zones_bypass(frame: &[u8], invert_list: &[u16]) -> Result<ZonesBypassData, SatelError> {
    if frame.is_empty() || frame[0] != 0x06 {
        tracing::error!("Nieprawidłowy kod komendy w ramce blokad (bypass): {:02X?}", frame.get(0));
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    // Satel zwraca 16 (128 wejść) lub 32 (256 wejść) bajty danych
    if data.len() < 16 {
        tracing::error!("Dane blokad (bypass) zbyt krótkie: {} bajtów", data.len());
        return Err(SatelError::InvalidFrame);
    }

    let mut states = Vec::with_capacity(data.len() * 8);
    for (byte_idx, &byte) in data.iter().enumerate() {
        for bit in 0..8 {
            let zone_id = (byte_idx * 8 + bit + 1) as u16;
            let raw_state = (byte & (1 << bit)) != 0;
            
            // Inwersja stanu jeśli zone_id znajduje się na liście invert_list
            let final_state = if invert_list.contains(&zone_id) {
                !raw_state
            } else {
                raw_state
            };
            states.push(final_state);
        }
    }

    Ok(ZonesBypassData {
        states,
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x08 (Zones 'long violation' trouble).
/// Uwzględnia listę wejść, których stan ma zostać odwrócony.
pub fn process_zones_long_violation_trouble(frame: &[u8], invert_list: &[u16]) -> Result<ZonesLongViolationTroubleData, SatelError> {
    if frame.is_empty() || frame[0] != 0x08 {
        tracing::error!("Nieprawidłowy kod komendy w ramce awarii 'długie naruszenie': {:02X?}", frame.get(0));
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    // Satel zwraca 16 (128 wejść) lub 32 (256 wejść) bajty danych
    if data.len() < 16 {
        tracing::error!("Dane awarii 'długie naruszenie' zbyt krótkie: {} bajtów", data.len());
        return Err(SatelError::InvalidFrame);
    }

    let mut states = Vec::with_capacity(data.len() * 8);
    for (byte_idx, &byte) in data.iter().enumerate() {
        for bit in 0..8 {
            let zone_id = (byte_idx * 8 + bit + 1) as u16;
            let raw_state = (byte & (1 << bit)) != 0;
            
            // Inwersja stanu jeśli zone_id znajduje się na liście invert_list
            let final_state = if invert_list.contains(&zone_id) {
                !raw_state
            } else {
                raw_state
            };
            states.push(final_state);
        }
    }

    Ok(ZonesLongViolationTroubleData {
        states,
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x09 (Armed partitions suppressed).
pub fn process_partitions_armed_suppressed(frame: &[u8]) -> Result<PartitionsArmedData, SatelError> {
    if frame.is_empty() || frame[0] != 0x09 {
        tracing::error!("Nieprawidłowy kod komendy w ramce uzbrojenia stref (suppressed): {:02X?}", frame.get(0));
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    // Satel zwraca 4 bajty dla stref (32 strefy)
    if data.len() < 4 {
        tracing::error!("Dane uzbrojenia stref zbyt krótkie: {} bajtów", data.len());
        return Err(SatelError::InvalidFrame);
    }

    let mut states = Vec::with_capacity(32);
    for &byte in data.iter().take(4) {
        for bit in 0..8 {
            let state = (byte & (1 << bit)) != 0;
            states.push(state);
        }
    }

    Ok(PartitionsArmedData {
        states,
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x0A (Armed partitions really).
pub fn process_partitions_armed_really(frame: &[u8]) -> Result<PartitionsData, SatelError> {
    if frame.is_empty() || frame[0] != 0x0A {
        tracing::error!("Nieprawidłowy kod komendy w ramce uzbrojenia stref (really): {:02X?}", frame.get(0));
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 4 {
        return Err(SatelError::InvalidFrame);
    }

    let mut states = Vec::with_capacity(32);
    for &byte in data.iter().take(4) {
        for bit in 0..8 {
            let state = (byte & (1 << bit)) != 0;
            states.push(state);
        }
    }

    Ok(PartitionsData {
        states,
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x0E (Partitions entry time).
pub fn process_partitions_entry_time(frame: &[u8]) -> Result<PartitionsData, SatelError> {
    if frame.is_empty() || frame[0] != 0x0E {
        tracing::error!("Nieprawidłowy kod komendy w ramce czasu na wejście: {:02X?}", frame.get(0));
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 4 {
        return Err(SatelError::InvalidFrame);
    }

    let mut states = Vec::with_capacity(32);
    for &byte in data.iter().take(4) {
        for bit in 0..8 {
            let state = (byte & (1 << bit)) != 0;
            states.push(state);
        }
    }

    Ok(PartitionsData {
        states,
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x0F (Partitions exit time > 10s).
pub fn process_partitions_exit_time_gt_10s(frame: &[u8]) -> Result<PartitionsData, SatelError> {
    if frame.is_empty() || frame[0] != 0x0F {
        tracing::error!("Nieprawidłowy kod komendy w ramce czasu na wyjście > 10s: {:02X?}", frame.get(0));
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 4 {
        return Err(SatelError::InvalidFrame);
    }

    let mut states = Vec::with_capacity(32);
    for &byte in data.iter().take(4) {
        for bit in 0..8 {
            let state = (byte & (1 << bit)) != 0;
            states.push(state);
        }
    }

    Ok(PartitionsData {
        states,
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x10 (Partitions exit time < 10s).
pub fn process_partitions_exit_time_lt_10s(frame: &[u8]) -> Result<PartitionsData, SatelError> {
    if frame.is_empty() || frame[0] != 0x10 {
        tracing::error!("Nieprawidłowy kod komendy w ramce czasu na wyjście < 10s: {:02X?}", frame.get(0));
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 4 {
        return Err(SatelError::InvalidFrame);
    }

    let mut states = Vec::with_capacity(32);
    for &byte in data.iter().take(4) {
        for bit in 0..8 {
            let state = (byte & (1 << bit)) != 0;
            states.push(state);
        }
    }

    Ok(PartitionsData {
        states,
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x13 (Partitions alarm).
pub fn process_partitions_alarm(frame: &[u8]) -> Result<PartitionsData, SatelError> {
    if frame.is_empty() || frame[0] != 0x13 {
        tracing::error!("Nieprawidłowy kod komendy w ramce alarmów stref: {:02X?}", frame.get(0));
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 4 {
        return Err(SatelError::InvalidFrame);
    }

    let mut states = Vec::with_capacity(32);
    for &byte in data.iter().take(4) {
        for bit in 0..8 {
            let state = (byte & (1 << bit)) != 0;
            states.push(state);
        }
    }

    Ok(PartitionsData {
        states,
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x15 (Partitions alarm memory).
pub fn process_partitions_alarm_memory(frame: &[u8]) -> Result<PartitionsData, SatelError> {
    if frame.is_empty() || frame[0] != 0x15 {
        tracing::error!("Nieprawidłowy kod komendy w ramce pamięci alarmów stref: {:02X?}", frame.get(0));
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 4 {
        return Err(SatelError::InvalidFrame);
    }

    let mut states = Vec::with_capacity(32);
    for &byte in data.iter().take(4) {
        for bit in 0..8 {
            let state = (byte & (1 << bit)) != 0;
            states.push(state);
        }
    }

    Ok(PartitionsData {
        states,
        read_at: Local::now(),
    })
}

/// Przetwarza całą ramkę odpowiedzi na komendę 0x17 (Outputs state).
pub fn process_outputs_state(frame: &[u8]) -> Result<OutputsStateData, SatelError> {
    if frame.is_empty() || frame[0] != 0x17 {
        tracing::error!(
            "Nieprawidłowy kod komendy w ramce stanu wyjść: {:02X?}",
            frame.get(0)
        );
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    // Satel zwraca 16 (128 wejść) lub 32 (256 wejść) bajty danych
    if data.len() < 16 {
        tracing::error!("Dane stanu wyjść zbyt krótkie: {} bajtów", data.len());
        return Err(SatelError::InvalidFrame);
    }

    let mut states = Vec::with_capacity(data.len() * 8);
    for &byte in data {
        for bit in 0..8 {
            let state = (byte & (1 << bit)) != 0;
            states.push(state);
        }
    }

    Ok(OutputsStateData {
        states,
        read_at: Local::now(),
    })
}
