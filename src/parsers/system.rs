use crate::config::Config;
use crate::error::SatelError;
use crate::state::{
    AutoReadItemState, AutoReadItemStatus, AutoReadReport, EthmCapabilities, EthmVersion,
    IntegraVersion, SystemStatus, TroubleType,
};
use chrono::{Local, TimeZone};

/// Parses the complete response frame for command 0x7C (ETHM/INT-RS module version).
pub fn process_ethm_version(frame: &[u8]) -> Result<EthmVersion, SatelError> {
    if frame.is_empty() || frame[0] != 0x7C {
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 12 {
        return Err(SatelError::InvalidFrame);
    }

    // 11 bytes: Version and build date (ASCII)
    let version_raw = String::from_utf8_lossy(&data[0..11]).trim().to_string();

    // 12th byte (index 11 in 'data'): Feature capabilities bitmask
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

/// Parses the complete response frame for command 0x7E (Integra panel version & model).
pub fn process_integra_version(frame: &[u8]) -> Result<IntegraVersion, SatelError> {
    if frame.is_empty() || frame[0] != 0x7E {
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 14 {
        return Err(SatelError::InvalidFrame);
    }

    // 1 byte: Panel model type
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
        _ => ("Unknown INTEGRA", 0),
    };

    // 11 bytes: Firmware version and compilation date (ASCII)
    let version_raw = &data[1..12];
    let firmware_version = String::from_utf8_lossy(version_raw).trim().to_string();

    // 1 byte: Language code
    let lang_code = data[12];
    let language = match lang_code {
        0 => "PL",
        1 => "EN",
        _ => "Other",
    }.to_string();

    // 1 byte: Stored in FLASH (255 = Yes, other = No)
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

/// Parses the complete response frame for command 0x1A (RTC and status bits).
pub fn process_rtc_and_status(frame: &[u8]) -> Result<SystemStatus, SatelError> {
    if frame.is_empty() || frame[0] != 0x1A {
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 7 {
        return Err(SatelError::InvalidFrame);
    }

    // Format 0x1A: [YYYY_hi, YYYY_lo, MM, DD, HH, MM, SS, Status/DayOfWeek, ...]
    let (year, month, day, hour, min, sec, status_byte) = if data.len() >= 8 {
        let y = (bcd_to_u8(data[0]) as i32 * 100) + bcd_to_u8(data[1]) as i32;
        let m = bcd_to_u8(data[2]) as u32;
        let d = bcd_to_u8(data[3]) as u32;
        let h = bcd_to_u8(data[4]) as u32;
        let min = bcd_to_u8(data[5]) as u32;
        let s = bcd_to_u8(data[6]) as u32;
        let status = data[7];
        (y, m, d, h, min, s, status)
    } else {
        let y = 2000 + bcd_to_u8(data[0]) as i32;
        let m = bcd_to_u8(data[1]) as u32;
        let d = bcd_to_u8(data[2]) as u32;
        let h = bcd_to_u8(data[3]) as u32;
        let min = bcd_to_u8(data[4]) as u32;
        let s = bcd_to_u8(data[5]) as u32;
        let status = data[6];
        (y, m, d, h, min, s, status)
    };

    let rtc = Local
        .with_ymd_and_hms(year, month, day, hour, min, sec)
        .single()
        .unwrap_or_else(Local::now);

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

fn extract_bits(bytes: &[u8]) -> Vec<bool> {
    let mut bits = Vec::with_capacity(bytes.len() * 8);
    for &byte in bytes {
        for bit in 0..8 {
            bits.push((byte & (1 << bit)) != 0);
        }
    }
    bits
}

/// Parses the complete response frame for trouble commands (0x1B-0x31).
/// Returns a bit vector across all payload bytes.
pub fn process_troubles(frame: &[u8]) -> Result<Vec<bool>, SatelError> {
    if frame.is_empty() {
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    let mut states = Vec::with_capacity(data.len() * 8);
    for &byte in data {
        for bit in 0..8 {
            states.push((byte & (1 << bit)) != 0);
        }
    }

    Ok(states)
}

/// Parses the complete response frame for command 0x1B / 0x20 (Troubles Part 1 - 47 data bytes).
pub fn process_troubles_part1(frame: &[u8]) -> Result<crate::state::TroublesPart1Data, SatelError> {
    if frame.is_empty() || (frame[0] != 0x1B && frame[0] != 0x20) {
        return Err(SatelError::InvalidFrame);
    }
    let is_memory = frame[0] == 0x20;
    let data = &frame[1..];
    if data.len() < 47 {
        return Err(SatelError::InvalidFrame);
    }

    let technical_zones = extract_bits(&data[0..16]);
    let expanders_ac = extract_bits(&data[16..24]);
    let expanders_battery = extract_bits(&data[24..32]);
    let expanders_no_battery = extract_bits(&data[32..40]);

    let b1 = data[40];
    let b2 = data[41];
    let b3 = data[42];
    let main_board = crate::state::MainBoardTroubles {
        out1_trouble: (b1 & (1 << 0)) != 0,
        out2_trouble: (b1 & (1 << 1)) != 0,
        out3_trouble: (b1 & (1 << 2)) != 0,
        out4_trouble: (b1 & (1 << 3)) != 0,
        kpd_power_trouble: (b1 & (1 << 4)) != 0,
        ex1_ex2_power_trouble: (b1 & (1 << 5)) != 0,
        battery_trouble: (b1 & (1 << 6)) != 0,
        ac_trouble: (b1 & (1 << 7)) != 0,

        dt1_trouble: (b2 & (1 << 0)) != 0,
        dt2_trouble: (b2 & (1 << 1)) != 0,
        dtm_trouble: (b2 & (1 << 2)) != 0,
        rtc_trouble: (b2 & (1 << 3)) != 0,
        no_dtr_signal: (b2 & (1 << 4)) != 0,
        no_battery_present: (b2 & (1 << 5)) != 0,
        external_modem_init_trouble: (b2 & (1 << 6)) != 0,
        external_modem_cmd_trouble: (b2 & (1 << 7)) != 0,

        tel_line_no_voltage: (b3 & (1 << 0)) != 0,
        tel_line_bad_signal: (b3 & (1 << 1)) != 0,
        tel_line_no_signal: (b3 & (1 << 2)) != 0,
        monitoring_station_1_trouble: (b3 & (1 << 3)) != 0,
        monitoring_station_2_trouble: (b3 & (1 << 4)) != 0,
        eeprom_rtc_trouble: (b3 & (1 << 5)) != 0,
        ram_trouble: (b3 & (1 << 6)) != 0,
        main_panel_restart: (b3 & (1 << 7)) != 0,
    };

    let p1 = data[43];
    let p2 = data[44];
    let p3 = data[45];
    let p4 = data[46];
    let ethm_ptsa = crate::state::EthmPtsaTroubles {
        ethm_ping_trouble: p1 != 0,
        server_id_error: p2 != 0,
        no_server_connection: p3 != 0,
        no_ethm_mon_station_1: (p4 & (1 << 0)) != 0,
        no_ethm_mon_station_2: (p4 & (1 << 1)) != 0,
        no_gprs_mon_station_1: (p4 & (1 << 2)) != 0,
        no_gprs_mon_station_2: (p4 & (1 << 3)) != 0,
        time_server_trouble: (p4 & (1 << 4)) != 0,
        gsm_init_error: (p4 & (1 << 5)) != 0,
        ip_mon_station_1_trouble: (p4 & (1 << 6)) != 0,
        ip_mon_station_2_trouble: (p4 & (1 << 7)) != 0,
    };

    Ok(crate::state::TroublesPart1Data {
        is_memory,
        technical_zones,
        expanders_ac,
        expanders_battery,
        expanders_no_battery,
        main_board,
        ethm_ptsa,
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x1C / 0x21 (Troubles Part 2 - 26 data bytes).
pub fn process_troubles_part2(frame: &[u8]) -> Result<crate::state::TroublesPart2Data, SatelError> {
    if frame.is_empty() || (frame[0] != 0x1C && frame[0] != 0x21) {
        return Err(SatelError::InvalidFrame);
    }
    let is_memory = frame[0] == 0x21;
    let data = &frame[1..];
    if data.len() < 26 {
        return Err(SatelError::InvalidFrame);
    }

    let card_readers_head_a_or_synchro = extract_bits(&data[0..8]);
    let card_readers_head_b_or_charging = extract_bits(&data[8..16]);
    let expanders_supply_overload = extract_bits(&data[16..24]);
    let acu_jammed_or_short_circuit = extract_bits(&data[24..26]);

    Ok(crate::state::TroublesPart2Data {
        is_memory,
        card_readers_head_a_or_synchro,
        card_readers_head_b_or_charging,
        expanders_supply_overload,
        acu_jammed_or_short_circuit,
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x1D / 0x22 (Troubles Part 3 - 60 data bytes).
pub fn process_troubles_part3(frame: &[u8]) -> Result<crate::state::TroublesPart3Data, SatelError> {
    if frame.is_empty() || (frame[0] != 0x1D && frame[0] != 0x22) {
        return Err(SatelError::InvalidFrame);
    }
    let is_memory = frame[0] == 0x22;
    let data = &frame[1..];
    if data.len() < 60 {
        return Err(SatelError::InvalidFrame);
    }

    let acu_jam_levels = data[0..15].to_vec();
    let wireless_devices_low_battery = extract_bits(&data[15..30]);
    let wireless_devices_no_comm = extract_bits(&data[30..45]);
    let wireless_outputs_no_comm = extract_bits(&data[45..60]);

    Ok(crate::state::TroublesPart3Data {
        is_memory,
        acu_jam_levels,
        wireless_devices_low_battery,
        wireless_devices_no_comm,
        wireless_outputs_no_comm,
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x1E / 0x23 (Troubles Part 4 - 30 data bytes).
pub fn process_troubles_part4(frame: &[u8]) -> Result<crate::state::TroublesPart4Data, SatelError> {
    if frame.is_empty() || (frame[0] != 0x1E && frame[0] != 0x23) {
        return Err(SatelError::InvalidFrame);
    }
    let is_memory = frame[0] == 0x23;
    let data = &frame[1..];
    if data.len() < 30 {
        return Err(SatelError::InvalidFrame);
    }

    let expanders_no_comm = extract_bits(&data[0..8]);
    let expanders_substituted = extract_bits(&data[8..16]);
    let keypads_no_comm = extract_bits(&data[16..17]);
    let keypads_substituted = extract_bits(&data[17..18]);
    let ethm_no_lan_or_intrs_no_dsr = extract_bits(&data[18..19]);
    let expanders_tamper = extract_bits(&data[19..27]);
    let keypads_tamper = extract_bits(&data[27..28]);
    let keypad_init_errors = extract_bits(&data[28..29]);
    let auxiliary_stm_troubles = data[29];

    Ok(crate::state::TroublesPart4Data {
        is_memory,
        expanders_no_comm,
        expanders_substituted,
        keypads_no_comm,
        keypads_substituted,
        ethm_no_lan_or_intrs_no_dsr,
        expanders_tamper,
        keypads_tamper,
        keypad_init_errors,
        auxiliary_stm_troubles,
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x1F / 0x24 (Troubles Part 5 - 31 data bytes).
pub fn process_troubles_part5(frame: &[u8]) -> Result<crate::state::TroublesPart5Data, SatelError> {
    if frame.is_empty() || (frame[0] != 0x1F && frame[0] != 0x24) {
        return Err(SatelError::InvalidFrame);
    }
    let is_memory = frame[0] == 0x24;
    let data = &frame[1..];
    if data.len() < 31 {
        return Err(SatelError::InvalidFrame);
    }

    let masters_key_fobs_low_battery = extract_bits(&data[0..1]);
    let users_key_fobs_low_battery = extract_bits(&data[1..31]);

    Ok(crate::state::TroublesPart5Data {
        is_memory,
        masters_key_fobs_low_battery,
        users_key_fobs_low_battery,
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x2C / 0x2E (Troubles Part 6 - 45 data bytes - Integra 256).
pub fn process_troubles_part6(frame: &[u8]) -> Result<crate::state::TroublesPart6Data, SatelError> {
    if frame.is_empty() || (frame[0] != 0x2C && frame[0] != 0x2E) {
        return Err(SatelError::InvalidFrame);
    }
    let is_memory = frame[0] == 0x2E;
    let data = &frame[1..];
    if data.len() < 45 {
        return Err(SatelError::InvalidFrame);
    }

    let wireless_devices_low_battery = extract_bits(&data[0..15]);
    let wireless_devices_no_comm = extract_bits(&data[15..30]);
    let wireless_outputs_no_comm = extract_bits(&data[30..45]);

    Ok(crate::state::TroublesPart6Data {
        is_memory,
        wireless_devices_low_battery,
        wireless_devices_no_comm,
        wireless_outputs_no_comm,
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x2D / 0x2F (Troubles Part 7 - 47 data bytes - Integra 256).
pub fn process_troubles_part7(frame: &[u8]) -> Result<crate::state::TroublesPart7Data, SatelError> {
    if frame.is_empty() || (frame[0] != 0x2D && frame[0] != 0x2F) {
        return Err(SatelError::InvalidFrame);
    }
    let is_memory = frame[0] == 0x2F;
    let data = &frame[1..];
    if data.len() < 47 {
        return Err(SatelError::InvalidFrame);
    }

    let technical_zones = extract_bits(&data[0..16]);
    let technical_zones_memory = extract_bits(&data[16..32]);
    let acu_jam_levels = data[32..47].to_vec();

    Ok(crate::state::TroublesPart7Data {
        is_memory,
        technical_zones,
        technical_zones_memory,
        acu_jam_levels,
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x30 / 0x31 (Troubles Part 8 - 64 data bytes).
pub fn process_troubles_part8(frame: &[u8]) -> Result<crate::state::TroublesPart8Data, SatelError> {
    if frame.is_empty() || (frame[0] != 0x30 && frame[0] != 0x31) {
        return Err(SatelError::InvalidFrame);
    }
    let is_memory = frame[0] == 0x31;
    let data = &frame[1..];
    if data.len() < 64 {
        return Err(SatelError::InvalidFrame);
    }

    let mut gsm_modules = Vec::with_capacity(8);
    for addr in 0..8 {
        let chunk = &data[addr * 8..(addr + 1) * 8];
        let b0 = chunk[0];
        let b1 = chunk[1];
        let b2 = chunk[2];
        let b3 = chunk[3];
        let sim1_cme = u16::from_be_bytes([chunk[4], chunk[5]]);
        let sim2_cme = u16::from_be_bytes([chunk[6], chunk[7]]);

        gsm_modules.push(crate::state::GsmModuleTroubles {
            module_address: addr as u8,
            no_ethm_mon_station_1: (b0 & (1 << 0)) != 0,
            no_ethm_mon_station_2: (b0 & (1 << 1)) != 0,
            no_gprs_sim1_mon_station_1: (b0 & (1 << 2)) != 0,
            no_gprs_sim1_mon_station_2: (b0 & (1 << 3)) != 0,
            no_gprs_sim2_mon_station_1: (b0 & (1 << 4)) != 0,
            no_gprs_sim2_mon_station_2: (b0 & (1 << 5)) != 0,
            no_sms_sim1_mon_station_1: (b0 & (1 << 6)) != 0,
            no_sms_sim1_mon_station_2: (b0 & (1 << 7)) != 0,

            no_sms_sim2_mon_station_1: (b1 & (1 << 0)) != 0,
            no_sms_sim2_mon_station_2: (b1 & (1 << 1)) != 0,
            wrong_sim1_pin: (b1 & (1 << 2)) != 0,
            wrong_sim2_pin: (b1 & (1 << 3)) != 0,
            sim1_logging_error: (b1 & (1 << 4)) != 0,
            sim2_logging_error: (b1 & (1 << 5)) != 0,
            sim1_credit_low: (b1 & (1 << 6)) != 0,
            sim2_credit_low: (b1 & (1 << 7)) != 0,

            sim1_sms_error: (b2 & (1 << 0)) != 0,
            sim2_sms_error: (b2 & (1 << 1)) != 0,
            gsm_jamming: (b2 & (1 << 2)) != 0,
            settings_crc_error: (b2 & (1 << 3)) != 0,
            missing_module: (b2 & (1 << 4)) != 0,
            changed_module: (b2 & (1 << 5)) != 0,
            satel_server_conn_error: (b2 & (1 << 6)) != 0,
            mail_server_conn_error: (b2 & (1 << 7)) != 0,

            ntp_server_conn_error: (b3 & (1 << 0)) != 0,
            sim1_cme_error: sim1_cme,
            sim2_cme_error: sim2_cme,
        });
    }

    Ok(crate::state::TroublesPart8Data {
        is_memory,
        gsm_modules,
        read_at: Local::now(),
    })
}

/// Generic dispatcher decoding any trouble frame (Parts 1..8).
pub fn process_troubles_frame(frame: &[u8]) -> Result<crate::state::TroublesData, SatelError> {
    if frame.is_empty() {
        return Err(SatelError::InvalidFrame);
    }
    match frame[0] {
        0x1B | 0x20 => Ok(crate::state::TroublesData::Part1(process_troubles_part1(frame)?)),
        0x1C | 0x21 => Ok(crate::state::TroublesData::Part2(process_troubles_part2(frame)?)),
        0x1D | 0x22 => Ok(crate::state::TroublesData::Part3(process_troubles_part3(frame)?)),
        0x1E | 0x23 => Ok(crate::state::TroublesData::Part4(process_troubles_part4(frame)?)),
        0x1F | 0x24 => Ok(crate::state::TroublesData::Part5(process_troubles_part5(frame)?)),
        0x2C | 0x2E => Ok(crate::state::TroublesData::Part6(process_troubles_part6(frame)?)),
        0x2D | 0x2F => Ok(crate::state::TroublesData::Part7(process_troubles_part7(frame)?)),
        0x30 | 0x31 => Ok(crate::state::TroublesData::Part8(process_troubles_part8(frame)?)),
        _ => Err(SatelError::InvalidFrame),
    }
}

/// Parses the 0x7F response and generates an auto-read configuration report.
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
        ("Zone violations (0x00)", 0, config.auto_read_zones_violation),
        ("Zone tampers (0x01)", 0, config.auto_read_zones_tamper),
        ("Zone alarms (0x02)", 0, config.auto_read_zones_alarm),
        ("Zone tamper alarms (0x03)", 0, config.auto_read_zones_tamper_alarm),
        ("Zone alarm memory (0x04)", 0, config.auto_read_zones_alarm_memory),
        ("Zone tamper alarm memory (0x05)", 0, config.auto_read_zones_tamper_alarm_memory),
        ("Zone bypasses (0x06)", 0, config.auto_read_zones_bypass),
        ("Zone 'no violation' trouble (0x07)", 0, config.auto_read_zones_no_violation_trouble),
        ("Zone 'long violation' trouble (0x08)", 1, config.auto_read_zones_long_violation_trouble),
        ("Partitions armed suppressed (0x09)", 1, config.auto_read_partitions_armed_suppressed),
        ("Partitions armed really (0x0A)", 1, config.auto_read_partitions_armed_really),
        ("Partitions alarm (0x13)", 2, config.auto_read_partitions_alarm),
        ("Partitions alarm memory (0x15)", 2, config.auto_read_partitions_alarm_memory),
        ("Partitions entry time (0x0E)", 1, config.auto_read_partitions_entry_time),
        ("Partitions exit time (0x0F, 0x10)", 1, config.auto_read_partitions_exit_time),
        ("Outputs state (0x17)", 2, config.auto_read_outputs_state),
        ("System troubles (0x1A-0x30)", 3, config.auto_read_system_troubles),
        ("Troubles memory (0x20-0x31)", 4, config.auto_read_troubles_memory),
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

/// Maps trouble part index (0..7 for Parts 1..8) and bit index within frame to a strongly-typed `TroubleType`.
pub fn map_trouble_part_bit(part: u8, bit: u16) -> TroubleType {
    match part {
        // Part 1 (0x1B / 0x20 - 47 bytes = 376 bits)
        0 => match bit {
            0..=127 => TroubleType::TechnicalZoneTrouble(bit + 1),
            128..=191 => TroubleType::ExpanderAcLoss((bit - 128 + 1) as u8),
            192..=255 => TroubleType::ExpanderBatteryLow((bit - 192 + 1) as u8),
            256..=319 => TroubleType::ExpanderBatteryMissing((bit - 256 + 1) as u8),
            320 => TroubleType::MainBoardOutOverload(1),
            321 => TroubleType::MainBoardOutOverload(2),
            322 => TroubleType::MainBoardOutOverload(3),
            323 => TroubleType::MainBoardOutOverload(4),
            324 => TroubleType::MainBoardKpdPowerOverload,
            325 => TroubleType::MainBoardExPowerOverload,
            326 => TroubleType::MainBoardBatteryLow,
            327 => TroubleType::MainBoardAcLoss,
            328 => TroubleType::MainBoardDataBusDt1,
            329 => TroubleType::MainBoardDataBusDt2,
            330 => TroubleType::MainBoardDataBusDtm,
            331 => TroubleType::RtcLoss,
            332 => TroubleType::NoDtrSignal,
            333 => TroubleType::MainBoardBatteryMissing,
            334 => TroubleType::ExternalModemInitTrouble,
            335 => TroubleType::ExternalModemCmdTrouble,
            336 => TroubleType::TelephoneLineNoVoltage,
            337 => TroubleType::TelephoneLineBadSignal,
            338 => TroubleType::TelephoneLineNoSignal,
            339 => TroubleType::MonitoringStation1Trouble,
            340 => TroubleType::MonitoringStation2Trouble,
            341 => TroubleType::EepromRtcTrouble,
            342 => TroubleType::RamMemoryError,
            343 => TroubleType::MainPanelRestartMemory,
            344..=351 => TroubleType::EthmPingTrouble,
            352..=359 => TroubleType::EthmServerIdError,
            360..=367 => TroubleType::EthmSatelServerConnectionError,
            368 => TroubleType::EthmMonitoringStation1Error,
            369 => TroubleType::EthmMonitoringStation2Error,
            370 => TroubleType::GprsMonitoringStation1Error,
            371 => TroubleType::GprsMonitoringStation2Error,
            372 => TroubleType::TimeServerTrouble,
            373 => TroubleType::GsmInitError,
            374 => TroubleType::IpMonitoringStation1Trouble,
            375 => TroubleType::IpMonitoringStation2Trouble,
            _ => TroubleType::GenericTrouble { part, bit },
        },

        // Part 2 (0x1C / 0x21 - 26 bytes = 208 bits)
        1 => match bit {
            0..=63 => TroubleType::ExpanderCardReaderHeadA((bit + 1) as u8),
            64..=127 => TroubleType::ExpanderCardReaderHeadB((bit - 64 + 1) as u8),
            128..=191 => TroubleType::ExpanderSupplyOverload((bit - 128 + 1) as u8),
            192..=207 => TroubleType::ExpanderAcuJammedOrShortCircuit((bit - 192 + 1) as u8),
            _ => TroubleType::GenericTrouble { part, bit },
        },

        // Part 3 (0x1D / 0x22 - 60 bytes = 480 bits)
        2 => match bit {
            0..=119 => TroubleType::AcuModuleJamLevel((bit / 8 + 1) as u8),
            120..=239 => TroubleType::WirelessDeviceLowBattery { zone_id: bit - 120 + 1 },
            240..=359 => TroubleType::WirelessDeviceNoComm { zone_id: bit - 240 + 1 },
            360..=479 => TroubleType::WirelessOutputNoComm { output_id: bit - 360 + 1 },
            _ => TroubleType::GenericTrouble { part, bit },
        },

        // Part 4 (0x1E / 0x23 - 30 bytes = 240 bits)
        3 => match bit {
            0..=63 => TroubleType::ExpanderNoComm((bit + 1) as u8),
            64..=127 => TroubleType::ExpanderSubstituted((bit - 64 + 1) as u8),
            128..=135 => TroubleType::KeypadNoComm((bit - 128 + 1) as u8),
            136..=143 => TroubleType::KeypadSubstituted((bit - 136 + 1) as u8),
            144..=151 => TroubleType::EthmNoLanCable((bit - 144 + 1) as u8),
            152..=215 => TroubleType::ExpanderTamper((bit - 152 + 1) as u8),
            216..=223 => TroubleType::KeypadTamper((bit - 216 + 1) as u8),
            224..=231 => TroubleType::KeypadInitError((bit - 224 + 1) as u8),
            232..=239 => TroubleType::AuxiliaryStmTroubles,
            _ => TroubleType::GenericTrouble { part, bit },
        },

        // Part 5 (0x1F / 0x24 - 31 bytes = 248 bits)
        4 => match bit {
            0..=7 => TroubleType::MasterKeyFobLowBattery((bit + 1) as u8),
            8..=247 => TroubleType::UserKeyFobLowBattery { user_id: bit - 8 + 1 },
            _ => TroubleType::GenericTrouble { part, bit },
        },

        // Part 6 (0x2C / 0x2E - 45 bytes = 360 bits - Integra 256)
        5 => match bit {
            0..=119 => TroubleType::WirelessDeviceLowBattery { zone_id: bit + 121 },
            120..=239 => TroubleType::WirelessDeviceNoComm { zone_id: bit - 120 + 121 },
            240..=359 => TroubleType::WirelessOutputNoComm { output_id: bit - 240 + 121 },
            _ => TroubleType::GenericTrouble { part, bit },
        },

        // Part 7 (0x2D / 0x2F - 47 bytes = 376 bits - Integra 256)
        6 => match bit {
            0..=127 => TroubleType::TechnicalZoneTrouble(bit + 129),
            128..=255 => TroubleType::TechnicalZoneTrouble(bit - 128 + 129),
            256..=375 => TroubleType::AcuModuleJamLevel(((bit - 256) / 8 + 16) as u8),
            _ => TroubleType::GenericTrouble { part, bit },
        },

        // Part 8 (0x30 / 0x31 - 64 bytes = 512 bits)
        7 => {
            let module = (bit / 64) as u8;
            let off = bit % 64;
            match off {
                0 => TroubleType::GsmTrouble { module_address: module, desc: "No ETHM Connection to Monitoring Station 1" },
                1 => TroubleType::GsmTrouble { module_address: module, desc: "No ETHM Connection to Monitoring Station 2" },
                2 => TroubleType::GsmTrouble { module_address: module, desc: "No GPRS SIM1 Connection to Monitoring Station 1" },
                3 => TroubleType::GsmTrouble { module_address: module, desc: "No GPRS SIM1 Connection to Monitoring Station 2" },
                4 => TroubleType::GsmTrouble { module_address: module, desc: "No GPRS SIM2 Connection to Monitoring Station 1" },
                5 => TroubleType::GsmTrouble { module_address: module, desc: "No GPRS SIM2 Connection to Monitoring Station 2" },
                6 => TroubleType::GsmTrouble { module_address: module, desc: "No SMS SIM1 Connection to Monitoring Station 1" },
                7 => TroubleType::GsmTrouble { module_address: module, desc: "No SMS SIM1 Connection to Monitoring Station 2" },
                8 => TroubleType::GsmTrouble { module_address: module, desc: "No SMS SIM2 Connection to Monitoring Station 1" },
                9 => TroubleType::GsmTrouble { module_address: module, desc: "No SMS SIM2 Connection to Monitoring Station 2" },
                10 => TroubleType::GsmSimPinError { module, sim: 1 },
                11 => TroubleType::GsmSimPinError { module, sim: 2 },
                12 => TroubleType::GsmSimLoggingError { module, sim: 1 },
                13 => TroubleType::GsmSimLoggingError { module, sim: 2 },
                14 => TroubleType::GsmSimCreditLow { module, sim: 1 },
                15 => TroubleType::GsmSimCreditLow { module, sim: 2 },
                16 => TroubleType::GsmSimSmsError { module, sim: 1 },
                17 => TroubleType::GsmSimSmsError { module, sim: 2 },
                18 => TroubleType::GsmJamming(module),
                19 => TroubleType::GsmSettingsCrcError(module),
                20 => TroubleType::GsmModuleMissing(module),
                21 => TroubleType::GsmModuleChanged(module),
                22 => TroubleType::GsmServerConnError(module),
                23 => TroubleType::GsmMailServerConnError(module),
                24 => TroubleType::GsmNtpServerConnError(module),
                _ => TroubleType::GenericTrouble { part, bit },
            }
        }

        _ => TroubleType::GenericTrouble { part, bit },
    }
}

/// Maps global trouble bit index to a named `TroubleType`.
pub fn map_trouble_bit(index: u16) -> TroubleType {
    let part = (index / 40) as u8;
    let bit = index % 40;
    map_trouble_part_bit(part, bit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_troubles_part1() {
        let mut frame = vec![0x1B];
        frame.extend(vec![0u8; 47]);
        // Set technical zone 1 active
        frame[1] = 0x01;
        // Set expander 1 AC loss active
        frame[17] = 0x01;
        // Set Main board AC trouble (byte 40 bit 7) and Battery trouble (byte 40 bit 6)
        frame[41] = 0b11000000;

        let result = process_troubles_part1(&frame).expect("Should parse part 1");
        assert!(!result.is_memory);
        assert!(result.technical_zones[0]);
        assert!(!result.technical_zones[1]);
        assert!(result.expanders_ac[0]);
        assert!(!result.expanders_ac[1]);
        assert!(result.main_board.ac_trouble);
        assert!(result.main_board.battery_trouble);
        assert!(!result.main_board.out1_trouble);
    }

    #[test]
    fn test_process_troubles_part8() {
        let mut frame = vec![0x30];
        frame.extend(vec![0u8; 64]);
        // Module 0: wrong PIN on SIM1 (byte 1 bit 2), GSM jamming (byte 2 bit 2)
        frame[2] = 0b00000100;
        frame[3] = 0b00000100;
        // SIM1 CME error = 0x0021 (error 33)
        frame[5] = 0x00;
        frame[6] = 0x21;

        let result = process_troubles_part8(&frame).expect("Should parse part 8");
        assert_eq!(result.gsm_modules.len(), 8);
        assert!(result.gsm_modules[0].wrong_sim1_pin);
        assert!(result.gsm_modules[0].gsm_jamming);
        assert_eq!(result.gsm_modules[0].sim1_cme_error, 0x0021);
        assert!(!result.gsm_modules[0].wrong_sim2_pin);
    }

    #[test]
    fn test_process_troubles_frame_dispatcher() {
        let mut frame = vec![0x1C];
        frame.extend(vec![0u8; 26]);
        let result = process_troubles_frame(&frame).expect("Should dispatch part 2");
        match result {
            crate::state::TroublesData::Part2(p2) => {
                assert!(!p2.is_memory);
                assert_eq!(p2.card_readers_head_a_or_synchro.len(), 64);
            }
            _ => panic!("Expected Part2"),
        }
    }
}

