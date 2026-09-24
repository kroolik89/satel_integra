use crate::config::Config;
use crate::error::SatelError;
use crate::state::{
    AutoReadItemState, AutoReadItemStatus, AutoReadReport, EthmCapabilities, EthmVersion,
    IntegraVersion, SystemStatus, TroubleType, CmeSource, TroublesMemoryPart2Data,
    TroublesMemoryPart3Data, TroublesMemoryPart5Data, TroublesMemoryPart7Data,
};
use chrono::{Local, TimeZone};

/// A single decoded trouble item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TroubleItem {
    Flag {
        trouble: TroubleType,
        memory: bool,
        active: bool,
    },
    AcuJamLevel {
        module: u8,
        level: u8,
    },
    CmeError {
        source: CmeSource,
        sim: u8,
        memory: bool,
        code: u16,
    },
}

/// Parses the complete response frame for command 0x7C (ETHM/INT-RS module version).
pub fn process_ethm_version(frame: &[u8]) -> Result<EthmVersion, SatelError> {
    if frame.is_empty() || frame[0] != 0x7C {
        return Err(SatelError::InvalidFrame);
    }
    let data = &frame[1..];
    if data.len() < 12 {
        return Err(SatelError::InvalidFrame);
    }
    let version_raw = String::from_utf8_lossy(&data[0..11]).trim().to_string();
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
    let type_code = data[0];
    let (model, io_count, partition_count) = match type_code {
        0 => ("INTEGRA 24", 24, 4),
        1 => ("INTEGRA 32", 32, 16),
        2 => ("INTEGRA 64", 64, 32),
        3 => ("INTEGRA 128", 128, 32),
        4 => ("INTEGRA 128-WRL SIM300", 128, 32),
        132 => ("INTEGRA 128-WRL LEON", 128, 32),
        66 => ("INTEGRA 64 Plus", 64, 32),
        67 => ("INTEGRA 128 Plus", 128, 32),
        72 => ("INTEGRA 256 Plus", 256, 32),
        8 => ("INTEGRA 256 Plus", 256, 32),
        _ => ("Unknown INTEGRA", 0, 0),
    };
    let version_raw = &data[1..12];
    let firmware_version = String::from_utf8_lossy(version_raw).trim().to_string();
    let lang_code = data[12];
    let language = match lang_code {
        0 => "PL",
        1 => "EN",
        _ => "Other",
    }.to_string();
    let stored_in_flash = data[13] == 255;
    Ok(IntegraVersion {
        model: model.to_string(),
        firmware_version,
        language,
        stored_in_flash,
        io_count,
        partition_count,
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

/// Helper enum for tabular decoding
enum FieldRule {
    /// Bit vector field where each bit maps to `constructor(start_num + bit_index)`
    Bitmask {
        offset: usize,
        length: usize,
        start_num: u16,
        memory: bool,
        constructor: fn(u16) -> TroubleType,
    },
    /// Single byte where each bit represents a specific module 1..8
    ModuleMask {
        offset: usize,
        memory: bool,
        constructor: fn(u8) -> TroubleType,
    },
    /// Custom extractor returning a list of TroubleItem
    Custom(bool, fn(&[u8], bool) -> Vec<TroubleItem>),
    /// Bit vector field with a limit on how many bits to process
    BitmaskLimit {
        offset: usize,
        length: usize,
        limit: usize,
        start_num: u16,
        memory: bool,
        constructor: fn(u16) -> TroubleType,
    },
}

fn bitmask(offset: usize, length: usize, start_num: u16, memory: bool, constructor: fn(u16) -> TroubleType) -> FieldRule {
    FieldRule::Bitmask { offset, length, start_num, memory, constructor }
}

fn module_mask(offset: usize, memory: bool, constructor: fn(u8) -> TroubleType) -> FieldRule {
    FieldRule::ModuleMask { offset, memory, constructor }
}

fn custom(memory: bool, extractor: fn(&[u8], bool) -> Vec<TroubleItem>) -> FieldRule {
    FieldRule::Custom(memory, extractor)
}

fn bitmask_limit(offset: usize, length: usize, limit: usize, start_num: u16, memory: bool, constructor: fn(u16) -> TroubleType) -> FieldRule {
    FieldRule::BitmaskLimit { offset, length, limit, start_num, memory, constructor }
}

fn decode_system_troubles(data: &[u8], memory: bool) -> Vec<TroubleItem> {
    let mut items = Vec::new();
    if data.len() < 43 { return items; }
    let b1 = data[40];
    let b2 = data[41];
    let b3 = data[42];
    
    let rules = vec![
        (TroubleType::MainBoardOutOverload(1), b1 & (1<<0) != 0),
        (TroubleType::MainBoardOutOverload(2), b1 & (1<<1) != 0),
        (TroubleType::MainBoardOutOverload(3), b1 & (1<<2) != 0),
        (TroubleType::MainBoardOutOverload(4), b1 & (1<<3) != 0),
        (TroubleType::MainBoardKpdPowerOverload, b1 & (1<<4) != 0),
        (TroubleType::MainBoardExPowerOverload, b1 & (1<<5) != 0),
        (TroubleType::MainBoardBatteryLow, b1 & (1<<6) != 0),
        (TroubleType::MainBoardAcLoss, b1 & (1<<7) != 0),
        
        (TroubleType::MainBoardDataBusDt1, b2 & (1<<0) != 0),
        (TroubleType::MainBoardDataBusDt2, b2 & (1<<1) != 0),
        (TroubleType::MainBoardDataBusDtm, b2 & (1<<2) != 0),
        (TroubleType::RtcLoss, b2 & (1<<3) != 0),
        (TroubleType::NoDtrSignal, b2 & (1<<4) != 0),
        (TroubleType::MainBoardBatteryMissing, b2 & (1<<5) != 0),
        (TroubleType::ExternalModemInitTrouble, b2 & (1<<6) != 0),
        (TroubleType::ExternalModemCmdTrouble, b2 & (1<<7) != 0),
        
        (TroubleType::TelephoneLineNoVoltage, b3 & (1<<0) != 0),
        (TroubleType::TelephoneLineBadSignal, b3 & (1<<1) != 0),
        (TroubleType::TelephoneLineNoSignal, b3 & (1<<2) != 0),
        (TroubleType::MonitoringStation1Trouble, b3 & (1<<3) != 0),
        (TroubleType::MonitoringStation2Trouble, b3 & (1<<4) != 0),
        (TroubleType::EepromRtcTrouble, b3 & (1<<5) != 0),
        (TroubleType::RamMemoryError, b3 & (1<<6) != 0),
        (TroubleType::MainPanelRestartMemory, b3 & (1<<7) != 0),
    ];
    for (t, active) in rules {
        items.push(TroubleItem::Flag { trouble: t, memory, active });
    }
    items
}

fn decode_ethm_ptsa_status(data: &[u8], memory: bool) -> Vec<TroubleItem> {
    let mut items = Vec::new();
    if data.len() < 47 { return items; }
    let p4 = data[46];
    let rules = vec![
        (TroubleType::EthmMonitoringStation1Error, p4 & (1<<0) != 0),
        (TroubleType::EthmMonitoringStation2Error, p4 & (1<<1) != 0),
        (TroubleType::GprsMonitoringStation1Error, p4 & (1<<2) != 0),
        (TroubleType::GprsMonitoringStation2Error, p4 & (1<<3) != 0),
        (TroubleType::TimeServerTrouble, p4 & (1<<4) != 0),
        (TroubleType::GsmInitError, p4 & (1<<5) != 0),
        (TroubleType::IpMonitoringStation1Trouble, p4 & (1<<6) != 0),
        (TroubleType::IpMonitoringStation2Trouble, p4 & (1<<7) != 0),
    ];
    for (t, active) in rules {
        items.push(TroubleItem::Flag { trouble: t, memory, active });
    }
    items
}

fn decode_aux_stm(data: &[u8], memory: bool) -> Vec<TroubleItem> {
    let mut items = Vec::new();
    if data.len() > 29 {
        let active = data[29] != 0;
        items.push(TroubleItem::Flag { trouble: TroubleType::AuxiliaryStmTroubles, memory, active });
    }
    items
}

fn decode_acu_jam_level(data: &[u8], _memory: bool) -> Vec<TroubleItem> {
    let mut items = Vec::new();
    let end = std::cmp::min(15, data.len());
    for i in 0..end {
        items.push(TroubleItem::AcuJamLevel { module: (i + 1) as u8, level: data[i] });
    }
    items
}

fn decode_acu_jam_level_16_30(data: &[u8], _memory: bool) -> Vec<TroubleItem> {
    let mut items = Vec::new();
    if data.len() < 47 { return items; }
    for i in 0..15 {
        items.push(TroubleItem::AcuJamLevel { module: (i + 16) as u8, level: data[32 + i] });
    }
    items
}

fn decode_gsm_block(data: &[u8], memory: bool) -> Vec<TroubleItem> {
    let mut items = Vec::new();
    for module in 0..8 {
        let off = module * 8;
        if data.len() < off + 8 { break; }
        let b0 = data[off];
        let b1 = data[off+1];
        let b2 = data[off+2];
        let b3 = data[off+3];
        let m = module as u8;
        
        let mut flags = vec![
            (TroubleType::GsmEthmStation1Error(m), b0 & (1<<0) != 0),
            (TroubleType::GsmEthmStation2Error(m), b0 & (1<<1) != 0),
            (TroubleType::GsmGprsSim1Station1Error(m), b0 & (1<<2) != 0),
            (TroubleType::GsmGprsSim1Station2Error(m), b0 & (1<<3) != 0),
            (TroubleType::GsmGprsSim2Station1Error(m), b0 & (1<<4) != 0),
            (TroubleType::GsmGprsSim2Station2Error(m), b0 & (1<<5) != 0),
            (TroubleType::GsmSmsSim1Station1Error(m), b0 & (1<<6) != 0),
            (TroubleType::GsmSmsSim1Station2Error(m), b0 & (1<<7) != 0),
            
            (TroubleType::GsmSmsSim2Station1Error(m), b1 & (1<<0) != 0),
            (TroubleType::GsmSmsSim2Station2Error(m), b1 & (1<<1) != 0),
            (TroubleType::GsmSimPinError { module: m, sim: 1 }, b1 & (1<<2) != 0),
            (TroubleType::GsmSimPinError { module: m, sim: 2 }, b1 & (1<<3) != 0),
            (TroubleType::GsmSimLoggingError { module: m, sim: 1 }, b1 & (1<<4) != 0),
            (TroubleType::GsmSimLoggingError { module: m, sim: 2 }, b1 & (1<<5) != 0),
            (TroubleType::GsmSimCreditLow { module: m, sim: 1 }, b1 & (1<<6) != 0),
            (TroubleType::GsmSimCreditLow { module: m, sim: 2 }, b1 & (1<<7) != 0),
            
            (TroubleType::GsmSimSmsError { module: m, sim: 1 }, b2 & (1<<0) != 0),
            (TroubleType::GsmSimSmsError { module: m, sim: 2 }, b2 & (1<<1) != 0),
            (TroubleType::GsmJamming(m), b2 & (1<<2) != 0),
            (TroubleType::GsmSettingsCrcError(m), b2 & (1<<3) != 0),
            (TroubleType::GsmModuleMissing(m), b2 & (1<<4) != 0),
            (TroubleType::GsmModuleChanged(m), b2 & (1<<5) != 0),
            (TroubleType::GsmServerConnError(m), b2 & (1<<6) != 0),
            (TroubleType::GsmMailServerConnError(m), b2 & (1<<7) != 0),
            
            (TroubleType::GsmNtpServerConnError(m), b3 & (1<<0) != 0),
        ];
        
        // generic reserved bits
        for i in 1..8 {
            flags.push((TroubleType::GenericTrouble { part: 7, bit: (off * 8 + 3 * 8 + i) as u16 }, b3 & (1<<i) != 0));
        }

        for (t, active) in flags {
            // GenericTrouble only yielded if active
            if let TroubleType::GenericTrouble { .. } = t {
                if !active { continue; }
            }
            items.push(TroubleItem::Flag { trouble: t, memory, active });
        }
        
        let sim1_cme = u16::from_be_bytes([data[off+4], data[off+5]]);
        let sim2_cme = u16::from_be_bytes([data[off+6], data[off+7]]);
        items.push(TroubleItem::CmeError { source: CmeSource::GsmModule(m), sim: 1, memory, code: sim1_cme });
        items.push(TroubleItem::CmeError { source: CmeSource::GsmModule(m), sim: 2, memory, code: sim2_cme });
    }
    items
}

/// Returns the rules for a specific command.
fn get_rules(cmd: u8) -> Option<(usize, Vec<FieldRule>)> {
    match cmd {
        0x1B => Some((47, vec![
            bitmask(0, 16, 1, false, TroubleType::TechnicalZoneTrouble),
            bitmask(16, 8, 1, false, |x| TroubleType::ExpanderAcLoss(x as u8)),
            bitmask(24, 8, 1, false, |x| TroubleType::ExpanderBatteryLow(x as u8)),
            bitmask(32, 8, 1, false, |x| TroubleType::ExpanderBatteryMissing(x as u8)),
            custom(false, |data, memory| decode_system_troubles(data, memory)),
            module_mask(43, false, |x| TroubleType::EthmPingTrouble(x as u8)),
            module_mask(44, false, |x| TroubleType::EthmServerIdError(x as u8)),
            module_mask(45, false, |x| TroubleType::EthmSatelServerConnectionError(x as u8)),
            custom(false, |data, memory| decode_ethm_ptsa_status(data, memory)),
        ])),
        0x20 => Some((47, vec![
            bitmask(0, 16, 1, true, TroubleType::TechnicalZoneTrouble),
            bitmask(16, 8, 1, true, |x| TroubleType::ExpanderAcLoss(x as u8)),
            bitmask(24, 8, 1, true, |x| TroubleType::ExpanderBatteryLow(x as u8)),
            bitmask(32, 8, 1, true, |x| TroubleType::ExpanderBatteryMissing(x as u8)),
            custom(true, |data, memory| decode_system_troubles(data, memory)),
            module_mask(43, true, |x| TroubleType::EthmPingTrouble(x as u8)),
            module_mask(44, true, |x| TroubleType::EthmServerIdError(x as u8)),
            module_mask(45, true, |x| TroubleType::EthmSatelServerConnectionError(x as u8)),
            custom(true, |data, memory| decode_ethm_ptsa_status(data, memory)),
        ])),
        0x1C => Some((26, vec![
            bitmask(0, 8, 1, false, |x| TroubleType::ExpanderCardReaderHeadA(x as u8)),
            bitmask(8, 8, 1, false, |x| TroubleType::ExpanderCardReaderHeadB(x as u8)),
            bitmask(16, 8, 1, false, |x| TroubleType::ExpanderSupplyOverload(x as u8)),
            bitmask(24, 2, 1, false, |x| TroubleType::ExpanderAcuJammedOrShortCircuit(x as u8)),
        ])),
        0x21 => Some((39, vec![
            bitmask(0, 8, 1, true, |x| TroubleType::ExpanderCardReaderHeadA(x as u8)),
            bitmask(8, 8, 1, true, |x| TroubleType::ExpanderCardReaderHeadB(x as u8)),
            bitmask(16, 8, 1, true, |x| TroubleType::ExpanderSupplyOverload(x as u8)),
            bitmask(24, 2, 1, true, |x| TroubleType::ExpanderAcuJammedOrShortCircuit(x as u8)),
            module_mask(26, true, |x| TroubleType::KeypadRestart(x as u8)),
            bitmask(27, 8, 1, true, |x| TroubleType::ExpanderRestart(x as u8)),
            custom(true, |data, _memory| {
                let mut items = Vec::new();
                if data.len() >= 39 {
                    let cme1 = u16::from_be_bytes([data[35], data[36]]);
                    let cme2 = u16::from_be_bytes([data[37], data[38]]);
                    items.push(TroubleItem::CmeError { source: CmeSource::Panel, sim: 1, memory: false, code: cme1 });
                    items.push(TroubleItem::CmeError { source: CmeSource::Panel, sim: 1, memory: true, code: cme2 });
                }
                items
            }),
        ])),
        0x1D => Some((60, vec![
            custom(false, |data, memory| decode_acu_jam_level(data, memory)),
            bitmask(15, 15, 17, false, |z| TroubleType::WirelessDeviceLowBattery { zone_id: z }),
            bitmask(30, 15, 17, false, |z| TroubleType::WirelessDeviceNoComm { zone_id: z }),
            bitmask(45, 15, 17, false, |o| TroubleType::WirelessOutputNoComm { output_id: o }),
        ])),
        0x22 => Some((60, vec![
            bitmask_limit(0, 2, 14, 17, false, |x| TroubleType::ExpanderAcuJammedOrShortCircuit(x as u8)),
            bitmask_limit(2, 2, 14, 17, true, |x| TroubleType::ExpanderAcuJammedOrShortCircuit(x as u8)),
            bitmask(15, 15, 17, true, |z| TroubleType::WirelessDeviceLowBattery { zone_id: z }),
            bitmask(30, 15, 17, true, |z| TroubleType::WirelessDeviceNoComm { zone_id: z }),
            bitmask(45, 15, 17, true, |o| TroubleType::WirelessOutputNoComm { output_id: o }),
        ])),
        0x1E => Some((30, vec![
            bitmask(0, 8, 1, false, |x| TroubleType::ExpanderNoComm(x as u8)),
            bitmask(8, 8, 1, false, |x| TroubleType::ExpanderSubstituted(x as u8)),
            module_mask(16, false, |x| TroubleType::KeypadNoComm(x as u8)),
            module_mask(17, false, |x| TroubleType::KeypadSubstituted(x as u8)),
            module_mask(18, false, |x| TroubleType::EthmNoLanCable(x as u8)),
            bitmask(19, 8, 1, false, |x| TroubleType::ExpanderTamper(x as u8)),
            module_mask(27, false, |x| TroubleType::KeypadTamper(x as u8)),
            module_mask(28, false, |x| TroubleType::KeypadInitError(x as u8)),
            custom(false, |data, memory| decode_aux_stm(data, memory)),
        ])),
        0x23 => Some((30, vec![
            bitmask(0, 8, 1, true, |x| TroubleType::ExpanderNoComm(x as u8)),
            bitmask(8, 8, 1, true, |x| TroubleType::ExpanderSubstituted(x as u8)),
            module_mask(16, true, |x| TroubleType::KeypadNoComm(x as u8)),
            module_mask(17, true, |x| TroubleType::KeypadSubstituted(x as u8)),
            module_mask(18, true, |x| TroubleType::EthmNoLanCable(x as u8)),
            bitmask(19, 8, 1, true, |x| TroubleType::ExpanderTamper(x as u8)),
            module_mask(27, true, |x| TroubleType::KeypadTamper(x as u8)),
            module_mask(28, true, |x| TroubleType::KeypadInitError(x as u8)),
            custom(true, |data, memory| decode_aux_stm(data, memory)),
        ])),
        0x1F => Some((31, vec![
            module_mask(0, false, |x| TroubleType::MasterKeyFobLowBattery(x as u8)),
            bitmask(1, 30, 1, false, |u| TroubleType::UserKeyFobLowBattery { user_id: u }),
        ])),
        0x24 => Some((48, vec![
            bitmask(0, 16, 1, true, TroubleType::ZoneLongViolationTrouble),
            bitmask(16, 16, 1, true, TroubleType::ZoneNoViolationTrouble),
            bitmask(32, 16, 1, true, TroubleType::ZoneTamperTrouble),
        ])),
        0x2C => Some((45, vec![
            bitmask(0, 15, 137, false, |z| TroubleType::WirelessDeviceLowBattery { zone_id: z }),
            bitmask(15, 15, 137, false, |z| TroubleType::WirelessDeviceNoComm { zone_id: z }),
            bitmask(30, 15, 137, false, |o| TroubleType::WirelessOutputNoComm { output_id: o }),
        ])),
        0x2E => Some((45, vec![
            bitmask(0, 15, 137, true, |z| TroubleType::WirelessDeviceLowBattery { zone_id: z }),
            bitmask(15, 15, 137, true, |z| TroubleType::WirelessDeviceNoComm { zone_id: z }),
            bitmask(30, 15, 137, true, |o| TroubleType::WirelessOutputNoComm { output_id: o }),
        ])),
        0x2D => Some((47, vec![
            bitmask(0, 16, 129, false, TroubleType::TechnicalZoneTrouble),
            bitmask(16, 16, 129, true, TroubleType::TechnicalZoneTrouble),
            custom(false, |data, memory| decode_acu_jam_level_16_30(data, memory)),
        ])),
        0x2F => Some((48, vec![
            bitmask(0, 16, 129, true, TroubleType::ZoneLongViolationTrouble),
            bitmask(16, 16, 129, true, TroubleType::ZoneNoViolationTrouble),
            bitmask(32, 16, 129, true, TroubleType::ZoneTamperTrouble),
        ])),
        0x30 => Some((64, vec![
            custom(false, |data, memory| decode_gsm_block(data, memory)),
        ])),
        0x31 => Some((64, vec![
            custom(true, |data, memory| decode_gsm_block(data, memory)),
        ])),
        _ => None,
    }
}

/// Core decoder for all trouble/trouble memory frames (0x1B-0x1F, 0x20-0x24, 0x2C-0x2F, 0x30-0x31).
pub fn decode_troubles(cmd: u8, data: &[u8]) -> Result<Vec<TroubleItem>, SatelError> {
    let (expected_len, rules) = get_rules(cmd).ok_or(SatelError::InvalidFrame)?;
    if data.len() < expected_len {
        return Err(SatelError::InvalidFrame);
    }
    
    let mut items = Vec::new();
    
    for rule in rules {
        match rule {
            FieldRule::Bitmask { offset, length, start_num, memory, constructor } => {
                let slice = &data[offset..offset+length];
                let bits = extract_bits(slice);
                for (i, &active) in bits.iter().enumerate() {
                    items.push(TroubleItem::Flag {
                        trouble: constructor(start_num + i as u16),
                        memory,
                        active,
                    });
                }
            }
            FieldRule::ModuleMask { offset, memory, constructor } => {
                let byte = data[offset];
                for bit in 0..8 {
                    items.push(TroubleItem::Flag {
                        trouble: constructor((bit + 1) as u8),
                        memory,
                        active: (byte & (1 << bit)) != 0,
                    });
                }
            }
            FieldRule::BitmaskLimit { offset, length, limit, start_num, memory, constructor } => {
                let slice = &data[offset..offset+length];
                let bits = extract_bits(slice);
                for (i, &active) in bits.iter().enumerate() {
                    if i >= limit {
                        break;
                    }
                    items.push(TroubleItem::Flag {
                        trouble: constructor(start_num + i as u16),
                        memory,
                        active,
                    });
                }
            }
            FieldRule::Custom(memory, extractor) => {
                items.extend(extractor(data, memory));
            }
        }
    }
    
    Ok(items)
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

/// Parses the complete response frame for command 0x1C (Troubles Part 2 state - 26 data bytes).
pub fn process_troubles_part2(frame: &[u8]) -> Result<crate::state::TroublesPart2Data, SatelError> {
    if frame.is_empty() || frame[0] != 0x1C {
        return Err(SatelError::InvalidFrame);
    }
    let data = &frame[1..];
    if data.len() < 26 {
        return Err(SatelError::InvalidFrame);
    }

    let card_readers_head_a_or_synchro = extract_bits(&data[0..8]);
    let card_readers_head_b_or_charging = extract_bits(&data[8..16]);
    let expanders_supply_overload = extract_bits(&data[16..24]);
    let acu_jammed_or_short_circuit = extract_bits(&data[24..26]);

    Ok(crate::state::TroublesPart2Data {
        is_memory: false,
        card_readers_head_a_or_synchro,
        card_readers_head_b_or_charging,
        expanders_supply_overload,
        acu_jammed_or_short_circuit,
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x21 (Troubles Part 2 memory - 39 data bytes).
pub fn process_troubles_memory_part2(frame: &[u8]) -> Result<TroublesMemoryPart2Data, SatelError> {
    if frame.is_empty() || frame[0] != 0x21 {
        return Err(SatelError::InvalidFrame);
    }
    let data = &frame[1..];
    if data.len() < 39 {
        return Err(SatelError::InvalidFrame);
    }
    
    Ok(TroublesMemoryPart2Data {
        card_readers_head_a_or_synchro: extract_bits(&data[0..8]),
        card_readers_head_b_or_charging: extract_bits(&data[8..16]),
        expanders_supply_overload: extract_bits(&data[16..24]),
        acu_jammed_or_short_circuit: extract_bits(&data[24..26]),
        keypad_restart: extract_bits(&data[26..27]),
        expander_restart: extract_bits(&data[27..35]),
        sim_cme_error: u16::from_be_bytes([data[35], data[36]]),
        sim_cme_error_memory: u16::from_be_bytes([data[37], data[38]]),
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x1D (Troubles Part 3 - 60 data bytes).
pub fn process_troubles_part3(frame: &[u8]) -> Result<crate::state::TroublesPart3Data, SatelError> {
    if frame.is_empty() || frame[0] != 0x1D {
        return Err(SatelError::InvalidFrame);
    }
    let data = &frame[1..];
    if data.len() < 60 {
        return Err(SatelError::InvalidFrame);
    }

    let acu_jam_levels = data[0..15].to_vec();
    let wireless_devices_low_battery = extract_bits(&data[15..30]);
    let wireless_devices_no_comm = extract_bits(&data[30..45]);
    let wireless_outputs_no_comm = extract_bits(&data[45..60]);

    Ok(crate::state::TroublesPart3Data {
        is_memory: false,
        acu_jam_levels,
        wireless_devices_low_battery,
        wireless_devices_no_comm,
        wireless_outputs_no_comm,
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x22 (Troubles Part 3 memory - 60 data bytes).
pub fn process_troubles_memory_part3(frame: &[u8]) -> Result<TroublesMemoryPart3Data, SatelError> {
    if frame.is_empty() || frame[0] != 0x22 {
        return Err(SatelError::InvalidFrame);
    }
    let data = &frame[1..];
    if data.len() < 60 {
        return Err(SatelError::InvalidFrame);
    }
    
    Ok(TroublesMemoryPart3Data {
        acu_jammed_or_short_circuit: extract_bits(&data[0..2]),
        acu_jammed_or_short_circuit_memory: extract_bits(&data[2..4]),
        wireless_devices_low_battery: extract_bits(&data[15..30]),
        wireless_devices_no_comm: extract_bits(&data[30..45]),
        wireless_outputs_no_comm: extract_bits(&data[45..60]),
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

/// Parses the complete response frame for command 0x1F (Troubles Part 5 - 31 data bytes).
pub fn process_troubles_part5(frame: &[u8]) -> Result<crate::state::TroublesPart5Data, SatelError> {
    if frame.is_empty() || frame[0] != 0x1F {
        return Err(SatelError::InvalidFrame);
    }
    let data = &frame[1..];
    if data.len() < 31 {
        return Err(SatelError::InvalidFrame);
    }

    let masters_key_fobs_low_battery = extract_bits(&data[0..1]);
    let users_key_fobs_low_battery = extract_bits(&data[1..31]);

    Ok(crate::state::TroublesPart5Data {
        is_memory: false,
        masters_key_fobs_low_battery,
        users_key_fobs_low_battery,
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x24 (Troubles Part 5 memory - 48 data bytes).
pub fn process_troubles_memory_part5(frame: &[u8]) -> Result<TroublesMemoryPart5Data, SatelError> {
    if frame.is_empty() || frame[0] != 0x24 {
        return Err(SatelError::InvalidFrame);
    }
    let data = &frame[1..];
    if data.len() < 48 {
        return Err(SatelError::InvalidFrame);
    }
    
    Ok(TroublesMemoryPart5Data {
        zone_long_violation: extract_bits(&data[0..16]),
        zone_no_violation: extract_bits(&data[16..32]),
        zone_tamper: extract_bits(&data[32..48]),
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

/// Parses the complete response frame for command 0x2D (Troubles Part 7 - 47 data bytes - Integra 256).
pub fn process_troubles_part7(frame: &[u8]) -> Result<crate::state::TroublesPart7Data, SatelError> {
    if frame.is_empty() || frame[0] != 0x2D {
        return Err(SatelError::InvalidFrame);
    }
    let data = &frame[1..];
    if data.len() < 47 {
        return Err(SatelError::InvalidFrame);
    }

    let technical_zones = extract_bits(&data[0..16]);
    let technical_zones_memory = extract_bits(&data[16..32]);
    let acu_jam_levels = data[32..47].to_vec();

    Ok(crate::state::TroublesPart7Data {
        is_memory: false,
        technical_zones,
        technical_zones_memory,
        acu_jam_levels,
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x2F (Troubles Part 7 memory - 48 data bytes).
pub fn process_troubles_memory_part7(frame: &[u8]) -> Result<TroublesMemoryPart7Data, SatelError> {
    if frame.is_empty() || frame[0] != 0x2F {
        return Err(SatelError::InvalidFrame);
    }
    let data = &frame[1..];
    if data.len() < 48 {
        return Err(SatelError::InvalidFrame);
    }
    
    Ok(TroublesMemoryPart7Data {
        zone_long_violation: extract_bits(&data[0..16]),
        zone_no_violation: extract_bits(&data[16..32]),
        zone_tamper: extract_bits(&data[32..48]),
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
        0x1C => Ok(crate::state::TroublesData::Part2(process_troubles_part2(frame)?)),
        0x21 => Ok(crate::state::TroublesData::MemoryPart2(process_troubles_memory_part2(frame)?)),
        0x1D => Ok(crate::state::TroublesData::Part3(process_troubles_part3(frame)?)),
        0x22 => Ok(crate::state::TroublesData::MemoryPart3(process_troubles_memory_part3(frame)?)),
        0x1E | 0x23 => Ok(crate::state::TroublesData::Part4(process_troubles_part4(frame)?)),
        0x1F => Ok(crate::state::TroublesData::Part5(process_troubles_part5(frame)?)),
        0x24 => Ok(crate::state::TroublesData::MemoryPart5(process_troubles_memory_part5(frame)?)),
        0x2C | 0x2E => Ok(crate::state::TroublesData::Part6(process_troubles_part6(frame)?)),
        0x2D => Ok(crate::state::TroublesData::Part7(process_troubles_part7(frame)?)),
        0x2F => Ok(crate::state::TroublesData::MemoryPart7(process_troubles_memory_part7(frame)?)),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{TroubleType, CmeSource};

    #[test]
    fn test_process_integra_version() {
        let mut frame = vec![0x7E, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        
        frame[1] = 0;
        let v = process_integra_version(&frame).unwrap();
        assert_eq!(v.model, "INTEGRA 24");
        assert_eq!(v.io_count, 24);
        assert_eq!(v.partition_count, 4);

        frame[1] = 1;
        let v = process_integra_version(&frame).unwrap();
        assert_eq!(v.model, "INTEGRA 32");
        assert_eq!(v.io_count, 32);
        assert_eq!(v.partition_count, 16);

        frame[1] = 2;
        let v = process_integra_version(&frame).unwrap();
        assert_eq!(v.model, "INTEGRA 64");
        assert_eq!(v.io_count, 64);
        assert_eq!(v.partition_count, 32);

        frame[1] = 72;
        let v = process_integra_version(&frame).unwrap();
        assert_eq!(v.model, "INTEGRA 256 Plus");
        assert_eq!(v.io_count, 256);
        assert_eq!(v.partition_count, 32);

        frame[1] = 200;
        let v = process_integra_version(&frame).unwrap();
        assert_eq!(v.model, "Unknown INTEGRA");
        assert_eq!(v.io_count, 0);
        assert_eq!(v.partition_count, 0);
    }

    fn check_single_active(cmd: u8, data: &[u8], expected_item: TroubleItem) {
        let items = decode_troubles(cmd, data).unwrap();
        let active_items: Vec<_> = items.into_iter().filter(|i| match i {
            TroubleItem::Flag { active, .. } => *active,
            TroubleItem::AcuJamLevel { level, .. } => *level != 0,
            TroubleItem::CmeError { code, .. } => *code != 0,
        }).collect();
        
        assert_eq!(active_items.len(), 1, "Expected exactly 1 active item, got {:?}", active_items);
        assert_eq!(active_items[0], expected_item);
    }

    fn check_zero_active(cmd: u8, data: &[u8]) {
        let items = decode_troubles(cmd, data).unwrap();
        let active_items: Vec<_> = items.into_iter().filter(|i| match i {
            TroubleItem::Flag { active, .. } => *active,
            TroubleItem::AcuJamLevel { level, .. } => *level != 0,
            TroubleItem::CmeError { code, .. } => *code != 0,
        }).collect();
        assert_eq!(active_items.len(), 0, "Expected 0 active items, got {:?}", active_items);
    }

    #[test]
    fn test_t1_0x1b_byte_0_bit_0() {
        let mut data = vec![0; 47];
        data[0] |= 1 << 0;
        check_single_active(0x1B, &data, TroubleItem::Flag { trouble: TroubleType::TechnicalZoneTrouble(1), memory: false, active: true });
    }

    #[test]
    fn test_t1_0x1b_byte_15_bit_7() {
        let mut data = vec![0; 47];
        data[15] |= 1 << 7;
        check_single_active(0x1B, &data, TroubleItem::Flag { trouble: TroubleType::TechnicalZoneTrouble(128), memory: false, active: true });
    }

    #[test]
    fn test_t1_0x1b_byte_43_bit_0() {
        let mut data = vec![0; 47];
        data[43] |= 1 << 0;
        check_single_active(0x1B, &data, TroubleItem::Flag { trouble: TroubleType::EthmPingTrouble(1), memory: false, active: true });
    }

    #[test]
    fn test_t1_0x1b_byte_43_bit_7() {
        let mut data = vec![0; 47];
        data[43] |= 1 << 7;
        check_single_active(0x1B, &data, TroubleItem::Flag { trouble: TroubleType::EthmPingTrouble(8), memory: false, active: true });
    }

    #[test]
    fn test_t1_0x1b_byte_40_bit_7() {
        let mut data = vec![0; 47];
        data[40] |= 1 << 7;
        check_single_active(0x1B, &data, TroubleItem::Flag { trouble: TroubleType::MainBoardAcLoss, memory: false, active: true });
    }

    #[test]
    fn test_t1_0x20_byte_45_bit_2() {
        let mut data = vec![0; 47];
        data[45] |= 1 << 2;
        check_single_active(0x20, &data, TroubleItem::Flag { trouble: TroubleType::EthmSatelServerConnectionError(3), memory: true, active: true });
    }

    #[test]
    fn test_t1_0x1c_byte_16_bit_0() {
        let mut data = vec![0; 26];
        data[16] |= 1 << 0;
        check_single_active(0x1C, &data, TroubleItem::Flag { trouble: TroubleType::ExpanderSupplyOverload(1), memory: false, active: true });
    }

    #[test]
    fn test_t1_0x1d_byte_0_val_5() {
        let mut data = vec![0; 60];
        data[0] = 5;
        check_single_active(0x1D, &data, TroubleItem::AcuJamLevel { module: 1, level: 5 });
    }

    #[test]
    fn test_t1_0x1d_byte_14_val_9() {
        let mut data = vec![0; 60];
        data[14] = 9;
        check_single_active(0x1D, &data, TroubleItem::AcuJamLevel { module: 15, level: 9 });
    }

    #[test]
    fn test_t1_0x1d_byte_59_bit_7() {
        let mut data = vec![0; 60];
        data[59] |= 1 << 7;
        check_single_active(0x1D, &data, TroubleItem::Flag { trouble: TroubleType::WirelessOutputNoComm { output_id: 136 }, memory: false, active: true });
    }

    #[test]
    fn test_t1_0x1e_byte_29_val_80() {
        let mut data = vec![0; 30];
        data[29] = 0x80;
        check_single_active(0x1E, &data, TroubleItem::Flag { trouble: TroubleType::AuxiliaryStmTroubles, memory: false, active: true });
    }

    #[test]
    fn test_t1_0x23_byte_29_val_01() {
        let mut data = vec![0; 30];
        data[29] = 0x01;
        check_single_active(0x23, &data, TroubleItem::Flag { trouble: TroubleType::AuxiliaryStmTroubles, memory: true, active: true });
    }

    #[test]
    fn test_t1_0x1f_byte_30_bit_7() {
        let mut data = vec![0; 31];
        data[30] |= 1 << 7;
        check_single_active(0x1F, &data, TroubleItem::Flag { trouble: TroubleType::UserKeyFobLowBattery { user_id: 240 }, memory: false, active: true });
    }

    #[test]
    fn test_t1_0x21_byte_26_bit_7() {
        let mut data = vec![0; 39];
        data[26] |= 1 << 7;
        check_single_active(0x21, &data, TroubleItem::Flag { trouble: TroubleType::KeypadRestart(8), memory: true, active: true });
    }

    #[test]
    fn test_t1_0x21_byte_34_bit_7() {
        let mut data = vec![0; 39];
        data[34] |= 1 << 7;
        check_single_active(0x21, &data, TroubleItem::Flag { trouble: TroubleType::ExpanderRestart(64), memory: true, active: true });
    }

    #[test]
    fn test_t1_0x21_byte_35_36() {
        let mut data = vec![0; 39];
        data[35] = 0x01;
        data[36] = 0x23;
        check_single_active(0x21, &data, TroubleItem::CmeError { source: CmeSource::Panel, sim: 1, memory: false, code: 0x0123 });
    }

    #[test]
    fn test_t1_0x21_byte_37_38() {
        let mut data = vec![0; 39];
        data[37] = 0x00;
        data[38] = 0x05;
        check_single_active(0x21, &data, TroubleItem::CmeError { source: CmeSource::Panel, sim: 1, memory: true, code: 0x0005 });
    }

    #[test]
    fn test_t1_0x22_byte_0_bit_0() {
        let mut data = vec![0; 60];
        data[0] |= 1 << 0;
        check_single_active(0x22, &data, TroubleItem::Flag { trouble: TroubleType::ExpanderAcuJammedOrShortCircuit(17), memory: false, active: true });
    }

    #[test]
    fn test_t1_0x22_byte_1_bit_5() {
        let mut data = vec![0; 60];
        data[1] |= 1 << 5;
        check_single_active(0x22, &data, TroubleItem::Flag { trouble: TroubleType::ExpanderAcuJammedOrShortCircuit(30), memory: false, active: true });
    }

    #[test]
    fn test_t1_0x22_byte_3_bit_5() {
        let mut data = vec![0; 60];
        data[3] |= 1 << 5;
        check_single_active(0x22, &data, TroubleItem::Flag { trouble: TroubleType::ExpanderAcuJammedOrShortCircuit(30), memory: true, active: true });
    }

    #[test]
    fn test_t1_0x22_byte_1_bit_6() {
        let mut data = vec![0; 60];
        data[1] |= 1 << 6;
        check_zero_active(0x22, &data);
    }

    #[test]
    fn test_t1_0x22_byte_5_val_ff() {
        let mut data = vec![0; 60];
        data[5] = 0xFF;
        check_zero_active(0x22, &data);
    }

    #[test]
    fn test_t1_0x22_byte_15_bit_0() {
        let mut data = vec![0; 60];
        data[15] |= 1 << 0;
        check_single_active(0x22, &data, TroubleItem::Flag { trouble: TroubleType::WirelessDeviceLowBattery { zone_id: 17 }, memory: true, active: true });
    }

    #[test]
    fn test_t1_0x24_byte_32_bit_0() {
        let mut data = vec![0; 48];
        data[32] |= 1 << 0;
        check_single_active(0x24, &data, TroubleItem::Flag { trouble: TroubleType::ZoneTamperTrouble(1), memory: true, active: true });
    }

    #[test]
    fn test_t1_0x24_byte_0_bit_0() {
        let mut data = vec![0; 48];
        data[0] |= 1 << 0;
        check_single_active(0x24, &data, TroubleItem::Flag { trouble: TroubleType::ZoneLongViolationTrouble(1), memory: true, active: true });
    }

    #[test]
    fn test_t1_0x24_byte_16_bit_0() {
        let mut data = vec![0; 48];
        data[16] |= 1 << 0;
        check_single_active(0x24, &data, TroubleItem::Flag { trouble: TroubleType::ZoneNoViolationTrouble(1), memory: true, active: true });
    }

    #[test]
    fn test_t1_0x2c_byte_0_bit_0() {
        let mut data = vec![0; 45];
        data[0] |= 1 << 0;
        check_single_active(0x2C, &data, TroubleItem::Flag { trouble: TroubleType::WirelessDeviceLowBattery { zone_id: 137 }, memory: false, active: true });
    }

    #[test]
    fn test_t1_0x2c_byte_14_bit_7() {
        let mut data = vec![0; 45];
        data[14] |= 1 << 7;
        check_single_active(0x2C, &data, TroubleItem::Flag { trouble: TroubleType::WirelessDeviceLowBattery { zone_id: 256 }, memory: false, active: true });
    }

    #[test]
    fn test_t1_0x2d_byte_0_bit_0() {
        let mut data = vec![0; 47];
        data[0] |= 1 << 0;
        check_single_active(0x2D, &data, TroubleItem::Flag { trouble: TroubleType::TechnicalZoneTrouble(129), memory: false, active: true });
    }

    #[test]
    fn test_t1_0x2d_byte_16_bit_0() {
        let mut data = vec![0; 47];
        data[16] |= 1 << 0;
        check_single_active(0x2D, &data, TroubleItem::Flag { trouble: TroubleType::TechnicalZoneTrouble(129), memory: true, active: true });
    }

    #[test]
    fn test_t1_0x2d_byte_31_bit_7() {
        let mut data = vec![0; 47];
        data[31] |= 1 << 7;
        check_single_active(0x2D, &data, TroubleItem::Flag { trouble: TroubleType::TechnicalZoneTrouble(256), memory: true, active: true });
    }

    #[test]
    fn test_t1_0x2d_byte_32_val_7() {
        let mut data = vec![0; 47];
        data[32] = 7;
        check_single_active(0x2D, &data, TroubleItem::AcuJamLevel { module: 16, level: 7 });
    }

    #[test]
    fn test_t1_0x2d_byte_46_val_3() {
        let mut data = vec![0; 47];
        data[46] = 3;
        check_single_active(0x2D, &data, TroubleItem::AcuJamLevel { module: 30, level: 3 });
    }

    #[test]
    fn test_t1_0x2f_byte_47_bit_7() {
        let mut data = vec![0; 48];
        data[47] |= 1 << 7;
        check_single_active(0x2F, &data, TroubleItem::Flag { trouble: TroubleType::ZoneTamperTrouble(256), memory: true, active: true });
    }

    #[test]
    fn test_t1_0x30_byte_0_bit_0() {
        let mut data = vec![0; 64];
        data[0] |= 1 << 0;
        check_single_active(0x30, &data, TroubleItem::Flag { trouble: TroubleType::GsmEthmStation1Error(0), memory: false, active: true });
    }

    #[test]
    fn test_t1_0x30_byte_57_bit_3() {
        let mut data = vec![0; 64];
        data[57] |= 1 << 3;
        check_single_active(0x30, &data, TroubleItem::Flag { trouble: TroubleType::GsmSimPinError { module: 7, sim: 2 }, memory: false, active: true });
    }

    #[test]
    fn test_t1_0x30_byte_62_63() {
        let mut data = vec![0; 64];
        data[62] = 0x00;
        data[63] = 0x10;
        check_single_active(0x30, &data, TroubleItem::CmeError { source: CmeSource::GsmModule(7), sim: 2, memory: false, code: 0x0010 });
    }

    #[test]
    fn test_t1_0x30_byte_3_bit_1() {
        let mut data = vec![0; 64];
        data[3] |= 1 << 1;
        check_single_active(0x30, &data, TroubleItem::Flag { trouble: TroubleType::GenericTrouble { part: 7, bit: 25 }, memory: false, active: true });
    }

    #[test]
    fn test_t1_0x31_byte_10_bit_2() {
        let mut data = vec![0; 64];
        data[10] |= 1 << 2;
        check_single_active(0x31, &data, TroubleItem::Flag { trouble: TroubleType::GsmJamming(1), memory: true, active: true });
    }

    #[test]
    fn test_t2_lengths() {
        let lengths = [
            (0x1B, 47), (0x20, 47), (0x1C, 26), (0x21, 39),
            (0x1D, 60), (0x22, 60), (0x1E, 30), (0x23, 30),
            (0x1F, 31), (0x24, 48), (0x2C, 45), (0x2E, 45),
            (0x2D, 47), (0x2F, 48), (0x30, 64), (0x31, 64),
        ];
        
        for (cmd, exp) in lengths {
            let data_ok = vec![0; exp];
            assert!(decode_troubles(cmd, &data_ok).is_ok());
            
            let data_err = vec![0; exp - 1];
            assert!(matches!(decode_troubles(cmd, &data_err), Err(crate::error::SatelError::InvalidFrame)));
        }
        
        let data_unk = vec![0; 100];
        assert!(matches!(decode_troubles(0x25, &data_unk), Err(crate::error::SatelError::InvalidFrame)));
    }

    #[test]
    fn test_t3_catalog_consistency() {
        let lengths = [
            (0x1B, 47), (0x20, 47), (0x1C, 26), (0x21, 39),
            (0x1D, 60), (0x22, 60), (0x1E, 30), (0x23, 30),
            (0x1F, 31), (0x24, 48), (0x2C, 45), (0x2E, 45),
            (0x2D, 47), (0x2F, 48), (0x30, 64), (0x31, 64),
        ];

        let mut states_found = std::collections::HashMap::new();
        let mut memories_found = std::collections::HashMap::new();

        for (cmd, exp) in lengths {
            let data = vec![0xFF; exp];
            let items = decode_troubles(cmd, &data).unwrap();
            
            for item in items {
                if let TroubleItem::Flag { trouble, memory, active } = item {
                    if !active { continue; }
                    if matches!(trouble, TroubleType::GenericTrouble { .. }) {
                        continue;
                    }
                    
                    let key = trouble.key().expect("Missing key");
                    let address = trouble.address();
                    
                    let desc = TroubleType::catalog().iter().find(|d| d.key == key).expect(&format!("Missing descriptor for {}", key));
                    
                    if memory {
                        assert!(desc.has_memory, "{} expects has_memory", key);
                        memories_found.entry(key).or_insert_with(Vec::new).push(address);
                    } else {
                        assert!(desc.has_state, "{} expects has_state", key);
                        states_found.entry(key).or_insert_with(Vec::new).push(address);
                    }
                    
                    match (desc.addressing, address) {
                        (crate::trouble_catalog::TroubleAddressing::Single, _) => {}
                        (crate::trouble_catalog::TroubleAddressing::Range { from, to }, Some(crate::trouble_catalog::TroubleAddress::One(n))) => {
                            assert!(n >= from && n <= to, "{} address {} out of bounds", key, n);
                        }
                        (crate::trouble_catalog::TroubleAddressing::Pair { a_from, a_to, b_from, b_to }, Some(crate::trouble_catalog::TroubleAddress::Pair(a, b))) => {
                            assert!(a >= a_from && a <= a_to, "{} addr A {} out of bounds", key, a);
                            assert!(b >= b_from && b <= b_to, "{} addr B {} out of bounds", key, b);
                        }
                        _ => panic!("Address mismatch for {}", key),
                    }
                }
            }
        }
        
        for desc in TroubleType::catalog() {
            if desc.has_state {
                assert!(states_found.contains_key(desc.key), "State for {} never found", desc.key);
            }
            if desc.has_memory {
                assert!(memories_found.contains_key(desc.key), "Memory for {} never found", desc.key);
            }
            
            if let crate::trouble_catalog::TroubleAddressing::Range { from, to } = desc.addressing {
                if desc.has_state {
                    let mut nums: Vec<_> = states_found[desc.key].iter().map(|a| match a { Some(crate::trouble_catalog::TroubleAddress::One(n)) => *n, _ => unreachable!() }).collect();
                    nums.sort();
                    nums.dedup();
                    assert_eq!(nums, (from..=to).collect::<Vec<_>>(), "State range incomplete for {}", desc.key);
                }
                if desc.has_memory {
                    let mut nums: Vec<_> = memories_found[desc.key].iter().map(|a| match a { Some(crate::trouble_catalog::TroubleAddress::One(n)) => *n, _ => unreachable!() }).collect();
                    nums.sort();
                    nums.dedup();
                    assert_eq!(nums, (from..=to).collect::<Vec<_>>(), "Memory range incomplete for {}", desc.key);
                }
            }
        }
    }

    #[test]
    fn test_0x1d_real_panel_abax_troubles() {
        let mut data = vec![0u8; 60];
        // low-battery dla wejść 23, 24 (bit = numer - 17):
        // 23 - 17 = 6 -> byte 15, bit 6
        data[15] |= 1 << 6;
        // 24 - 17 = 7 -> byte 15, bit 7
        data[15] |= 1 << 7;

        // no-comm dla wejść 23, 24, 29, 30:
        // 23 - 17 = 6 -> byte 30, bit 6
        data[30] |= 1 << 6;
        // 24 - 17 = 7 -> byte 30, bit 7
        data[30] |= 1 << 7;
        // 29 - 17 = 12 -> byte 31, bit 4
        data[31] |= 1 << 4;
        // 30 - 17 = 13 -> byte 31, bit 5
        data[31] |= 1 << 5;

        let items = decode_troubles(0x1D, &data).unwrap();
        let active_wireless: Vec<TroubleType> = items.iter().filter_map(|item| {
            match item {
                TroubleItem::Flag { trouble, active: true, memory: false } => {
                    match trouble {
                        TroubleType::WirelessDeviceLowBattery { .. }
                        | TroubleType::WirelessDeviceNoComm { .. }
                        | TroubleType::WirelessOutputNoComm { .. } => Some(trouble.clone()),
                        _ => None,
                    }
                }
                _ => None,
            }
        }).collect();

        assert_eq!(active_wireless.len(), 6);
        assert!(active_wireless.contains(&TroubleType::WirelessDeviceLowBattery { zone_id: 23 }));
        assert!(active_wireless.contains(&TroubleType::WirelessDeviceLowBattery { zone_id: 24 }));
        assert!(active_wireless.contains(&TroubleType::WirelessDeviceNoComm { zone_id: 23 }));
        assert!(active_wireless.contains(&TroubleType::WirelessDeviceNoComm { zone_id: 24 }));
        assert!(active_wireless.contains(&TroubleType::WirelessDeviceNoComm { zone_id: 29 }));
        assert!(active_wireless.contains(&TroubleType::WirelessDeviceNoComm { zone_id: 30 }));
    }

    #[test]
    fn test_wireless_device_bit0_part3_and_part6() {
        // bit 0 w części 3 -> zone_id 17
        let mut data_part3 = vec![0u8; 60];
        data_part3[15] |= 1 << 0;
        check_single_active(0x1D, &data_part3, TroubleItem::Flag {
            trouble: TroubleType::WirelessDeviceLowBattery { zone_id: 17 },
            memory: false,
            active: true,
        });

        // bit 0 w części 6 (0x2C) -> zone_id 137
        let mut data_part6 = vec![0u8; 45];
        data_part6[0] |= 1 << 0;
        check_single_active(0x2C, &data_part6, TroubleItem::Flag {
            trouble: TroubleType::WirelessDeviceLowBattery { zone_id: 137 },
            memory: false,
            active: true,
        });
    }
}
