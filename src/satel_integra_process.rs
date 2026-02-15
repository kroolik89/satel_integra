use crate::satel_integra_data::{IntegraVersion, SatelName};
use crate::satel_integra::SatelError;
use chrono::Local;

/// Przetwarza dane otrzymane w odpowiedzi na komendę 0x7E (Wersja centrali).
/// Oczekuje 14 bajtów danych (bez komendy 0x7E na początku).
pub fn process_integra_version(data: &[u8]) -> Result<IntegraVersion, SatelError> {
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

/// Przetwarza odpowiedź na komendę 0xEE (Read device name).
/// Zwraca krotkę (Numer urządzenia, Dane nazwy).
fn process_device_name(data: &[u8], expected_type: u8) -> Result<(u16, SatelName), SatelError> {
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

/// Przetwarza odpowiedź na komendę 0x7D (Read zone temperature).
/// Zwraca krotkę (Numer wejścia, Temperatura).
pub fn process_zone_temperature(data: &[u8]) -> Result<(u16, f32), SatelError> {
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
