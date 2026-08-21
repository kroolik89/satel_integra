use crate::error::SatelError;
use crate::state::SatelName;
use chrono::Local;

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
