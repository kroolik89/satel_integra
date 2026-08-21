use crate::config::Config;
use crate::error::SatelError;
use crate::state::{
    AutoReadItemState, AutoReadItemStatus, AutoReadReport, EthmCapabilities, EthmVersion,
    IntegraVersion, SystemStatus, TroubleType,
};
use chrono::{Local, TimeZone};

/// Przetwarza całą ramkę odpowiedzi na komendę 0x7C (Wersja modułu ETHM/INT-RS).
pub fn process_ethm_version(frame: &[u8]) -> Result<EthmVersion, SatelError> {
    if frame.is_empty() || frame[0] != 0x7C {
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 12 {
        return Err(SatelError::InvalidFrame);
    }

    // 11 bajtów: Wersja i data (ASCII)
    let version_raw = String::from_utf8_lossy(&data[0..11]).trim().to_string();

    // 12-ty bajt (indeks 11 w 'data'): Bitmaska możliwości
    let caps_byte = data[11];
    let capabilities = EthmCapabilities {
        support_32_byte_frames: (caps_byte & 0x01) != 0,
        support_8_troubles_groups: (caps_byte & 0x02) != 0,
        support_extended_arming_commands: (caps_byte & 0x04) != 0,
        reserved_bit3: (caps_byte & 0x08) != 0,
        reserved_bit4: (caps_byte & 0x10) != 0,
        reserved_bit5: (caps_byte & 0x20) != 0,
        reserved_bit6: (caps_byte & 0x40) != 0,
        reserved_bit7: (caps_byte & 0x80) != 0,
    };

    Ok(EthmVersion {
        version_raw,
        capabilities,
        read_at: Local::now(),
    })
}

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
        8 => ("INTEGRA 256 Plus", 256),
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

/// Przetwarza całą ramkę odpowiedzi na komendę 0x1A (RTC and status bits).
pub fn process_rtc_and_status(frame: &[u8]) -> Result<SystemStatus, SatelError> {
    if frame.is_empty() || frame[0] != 0x1A {
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 7 {
        return Err(SatelError::InvalidFrame);
    }

    // Bajty 0-5: BCD RTC (YYYY MM DD HH MM SS)
    let year = 2000 + bcd_to_u8(data[0]) as i32;
    let month = bcd_to_u8(data[1]) as u32;
    let day = bcd_to_u8(data[2]) as u32;
    let hour = bcd_to_u8(data[3]) as u32;
    let min = bcd_to_u8(data[4]) as u32;
    let sec = bcd_to_u8(data[5]) as u32;

    let rtc = Local
        .with_ymd_and_hms(year, month, day, hour, min, sec)
        .single()
        .unwrap_or_else(Local::now);

    // Bajt 6: Status
    let status_byte = data[6];
    let service_mode = (status_byte & (1 << 7)) != 0;
    let troubles_present = (status_byte & (1 << 6)) != 0;
    let troubles_memory = (status_byte & (1 << 5)) != 0;

    Ok(SystemStatus {
        service_mode,
        troubles_present,
        troubles_memory,
        rtc,
    })
}

fn bcd_to_u8(bcd: u8) -> u8 {
    ((bcd >> 4) * 10) + (bcd & 0x0F)
}

/// Przetwarza całą ramkę odpowiedzi na komendy awarii (0x1B-0x31).
/// Zwraca wektor 40 bitów (5 bajtów danych).
pub fn process_troubles(frame: &[u8]) -> Result<Vec<bool>, SatelError> {
    if frame.is_empty() {
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 5 {
        return Err(SatelError::InvalidFrame);
    }

    let mut states = Vec::with_capacity(40);
    for &byte in data.iter().take(5) {
        for bit in 0..8 {
            states.push((byte & (1 << bit)) != 0);
        }
    }

    Ok(states)
}

/// Przetwarza odpowiedź na komendę 0x7F i generuje raport stanu autoodczytu.
pub fn process_auto_read_response(
    config: &Config,
    support_14_byte: bool,
    response: &[u8],
) -> AutoReadReport {
    let mask_len = if support_14_byte { 14 } else { 12 };

    let is_success = if response.is_empty() {
        false
    } else if response[0] == 0x7F {
        true
    } else {
        response[0] == 0xEF && response.get(1) == Some(&0xFF)
    };

    let error_code = if !is_success && response[0] == 0xEF {
        response.get(1).cloned()
    } else {
        None
    };

    let mut items = Vec::new();
    let mut success_count = 0;
    let mut total_requested = 0;

    let defs = vec![
        ("Naruszenia wejść (0x00)", 0, config.auto_read_zones_violation),
        ("Sabotaże wejść (0x01)", 0, config.auto_read_zones_tamper),
        ("Alarmy wejść (0x02)", 0, config.auto_read_zones_alarm),
        ("Alarmy sabotażowe wejść (0x03)", 0, config.auto_read_zones_tamper_alarm),
        ("Pamięć alarmów wejść (0x04)", 0, config.auto_read_zones_alarm_memory),
        ("Pamięć al. sabot. wejść (0x05)", 0, config.auto_read_zones_tamper_alarm_memory),
        ("Blokady wejść (0x06)", 0, config.auto_read_zones_bypass),
        ("Awarie 'brak naruszenia' (0x07)", 0, config.auto_read_zones_no_violation_trouble),
        ("Awarie 'długie narusz.' (0x08)", 1, config.auto_read_zones_long_violation_trouble),
        ("Uzbrojenie stref (0x09)", 1, config.auto_read_partitions_armed_suppressed),
        ("Fakt. uzbrojenie stref (0x0A)", 1, config.auto_read_partitions_armed_really),
        ("Alarmy stref (0x13)", 2, config.auto_read_partitions_alarm),
        ("Pamięć alarmów stref (0x15)", 2, config.auto_read_partitions_alarm_memory),
        ("Czas na wejście (0x0E)", 1, config.auto_read_partitions_entry_time),
        ("Czas na wyjście (0x0F, 0x10)", 1, config.auto_read_partitions_exit_time),
        ("Stan wyjść (0x17)", 2, config.auto_read_outputs_state),
        ("Awarie systemu (0x1A-0x30)", 3, config.auto_read_system_troubles),
        ("Pamięć awarii (0x20-0x31)", 4, config.auto_read_troubles_memory),
    ];

    for (name, byte_idx, requested) in defs {
        let state = if !requested {
            AutoReadItemState::NotRequested
        } else {
            total_requested += 1;
            if byte_idx >= mask_len {
                AutoReadItemState::UnsupportedByHardware
            } else if is_success {
                success_count += 1;
                AutoReadItemState::Active
            } else {
                AutoReadItemState::RejectedByPanel(error_code.unwrap_or(0x08))
            }
        };

        items.push(AutoReadItemStatus {
            name: name.to_string(),
            state,
        });
    }

    AutoReadReport {
        items,
        success_count,
        total_requested,
    }
}

/// Mapuje globalny indeks bitu awarii (0-319) na nazwany typ `TroubleType`.
pub fn map_trouble_bit(index: u16) -> TroubleType {
    let part = (index / 40) as u8;
    let bit = (index % 40) as u8;

    match (part, bit) {
        // Part 1 (0x1B)
        (0, 0..=15) => TroubleType::OutTrouble(bit + 1),
        (0, 16) => TroubleType::MainBoardAcLoss,
        (0, 17) => TroubleType::MainBoardBatteryLow,
        (0, 18) => TroubleType::MainBoardBatteryMissing,
        (0, 19) => TroubleType::MainBoardOutOverload,
        (0, 20) => TroubleType::TelephoneLineTrouble,
        (0, 21) => TroubleType::RtcLoss,
        (0, 22) => TroubleType::PrinterTrouble,
        (0, 23) => TroubleType::MainBoardDataBusError,
        (0, 24..=31) => TroubleType::ExpanderAcLoss(bit - 24 + 1),
        (0, 32..=39) => TroubleType::ExpanderBatteryLow(bit - 32 + 1),

        // Part 2 (0x1C)
        (1, 0..=7) => TroubleType::ExpanderAcLoss(bit + 9),
        (1, 8..=15) => TroubleType::ExpanderBatteryLow(bit - 8 + 9),
        (1, 16..=23) => TroubleType::ExpanderAcLoss(bit - 16 + 17),
        (1, 24..=31) => TroubleType::ExpanderBatteryLow(bit - 24 + 17),
        (1, 32..=39) => TroubleType::ExpanderAcLoss(bit - 32 + 25),

        // Part 3 (0x1D)
        (2, 0..=7) => TroubleType::ExpanderBatteryLow(bit + 25),
        (2, 8..=39) => TroubleType::ExpanderBatteryMissing(bit - 8 + 1),

        // Part 4 (0x1E)
        (3, 0..=31) => TroubleType::ExpanderOutOverload(bit + 1),
        (3, 32..=39) => TroubleType::ExpanderDataBusError(bit - 32 + 1),

        // Part 5 (0x1F)
        (4, 0..=23) => TroubleType::ExpanderDataBusError(bit + 9),
        (4, 24) => TroubleType::EthmMonitoringStation1Error,
        (4, 25) => TroubleType::EthmMonitoringStation2Error,
        (4, 26) => TroubleType::EthmDloadxConnectionError,
        (4, 27) => TroubleType::EthmSatelServerConnectionError,
        (4, 28) => TroubleType::IntGsmSignalLoss,
        (4, 29) => TroubleType::GsmMonitoringStation1Error,
        (4, 30) => TroubleType::GsmMonitoringStation2Error,
        (4, 31) => TroubleType::ServiceAccessBlocked,
        (4, 32..=39) => TroubleType::ZoneTrouble(bit as u16 - 32 + 1),

        // Part 6 (0x2C)
        (5, bit) => TroubleType::ZoneTrouble(bit as u16 + 9),

        // Part 7 (0x2D)
        (6, bit) => TroubleType::ZoneTrouble(bit as u16 + 49),

        // Part 8 (0x30)
        (7, bit) => TroubleType::ZoneTrouble(bit as u16 + 89),

        _ => TroubleType::GenericTrouble { part, bit },
    }
}
