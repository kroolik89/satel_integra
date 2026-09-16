use crate::state::TroubleType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TroubleDomain {
    Zone,
    Output,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TroubleAddressing {
    Single,
    Range {
        from: u16,
        to: u16,
    },
    Pair {
        a_from: u16,
        a_to: u16,
        b_from: u16,
        b_to: u16,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TroubleAddress {
    One(u16),
    Pair(u16, u16),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TroubleDescriptor {
    pub key: &'static str,
    pub label_en: &'static str,
    pub addressing: TroubleAddressing,
    pub domain: Option<TroubleDomain>,
    pub has_memory: bool,
    pub has_state: bool,
}

macro_rules! desc {
    ($key:expr, $label:expr, $addr:expr, $domain:expr) => {
        TroubleDescriptor {
            key: $key,
            label_en: $label,
            addressing: $addr,
            domain: $domain,
            has_memory: true,
            has_state: true,
        }
    };
}

macro_rules! desc_custom {
    ($key:expr, $label:expr, $addr:expr, $domain:expr, $state:expr, $memory:expr) => {
        TroubleDescriptor {
            key: $key,
            label_en: $label,
            addressing: $addr,
            domain: $domain,
            has_state: $state,
            has_memory: $memory,
        }
    };
}


impl TroubleType {
    pub fn catalog() -> &'static [TroubleDescriptor] {
        &[
            // --- Main panel board ---
            desc!("main_board_ac_loss", "Main Board: AC Power Loss (230V)", TroubleAddressing::Single, None),
            desc!("main_board_battery_low", "Main Board: Battery Low Voltage", TroubleAddressing::Single, None),
            desc!("main_board_battery_missing", "Main Board: Battery Missing / Disconnected", TroubleAddressing::Single, None),
            desc!("main_board_out_overload", "Main Board: Supply Output Overload", TroubleAddressing::Range { from: 1, to: 4 }, None),
            desc!("main_board_kpd_power_overload", "Main Board: Keypad Power Supply (+KPD) Overload", TroubleAddressing::Single, None),
            desc!("main_board_ex_power_overload", "Main Board: Expander Power Supply (+EX1/+EX2) Overload", TroubleAddressing::Single, None),
            desc!("main_board_data_bus_dt1", "Main Board: Data Bus DT1 Communication Error", TroubleAddressing::Single, None),
            desc!("main_board_data_bus_dt2", "Main Board: Data Bus DT2 Communication Error", TroubleAddressing::Single, None),
            desc!("main_board_data_bus_dtm", "Main Board: Data Bus DTM Communication Error", TroubleAddressing::Single, None),
            desc!("rtc_loss", "Main Board: Real-Time Clock (RTC) Loss / Not Set", TroubleAddressing::Single, None),
            desc!("no_dtr_signal", "Main Board: No DTR Signal on RS-232 Port", TroubleAddressing::Single, None),
            desc!("external_modem_init_trouble", "Main Board: External Modem Initialization Error", TroubleAddressing::Single, None),
            desc!("external_modem_cmd_trouble", "Main Board: External Modem Command Error", TroubleAddressing::Single, None),
            desc!("telephone_line_no_voltage", "Telephone Line: No Voltage", TroubleAddressing::Single, None),
            desc!("telephone_line_bad_signal", "Telephone Line: Bad Signal", TroubleAddressing::Single, None),
            desc!("telephone_line_no_signal", "Telephone Line: No Dial Tone", TroubleAddressing::Single, None),
            desc!("monitoring_station1_trouble", "Monitoring: Station 1 Transmission Fault", TroubleAddressing::Single, None),
            desc!("monitoring_station2_trouble", "Monitoring: Station 2 Transmission Fault", TroubleAddressing::Single, None),
            desc!("eeprom_rtc_trouble", "Main Board: EEPROM / RTC Access Trouble", TroubleAddressing::Single, None),
            desc!("ram_memory_error", "Main Board: RAM Memory Error", TroubleAddressing::Single, None),
            desc!("main_panel_restart", "Main Board: Panel Restart Latched", TroubleAddressing::Single, None),

            // --- Technical zones ---
            desc!("technical_zone_trouble", "Technical Zone: Trouble Detected", TroubleAddressing::Range { from: 1, to: 256 }, Some(TroubleDomain::Zone)),

            // --- Expanders ---
            desc!("expander_ac_loss", "Expander: AC Power Loss", TroubleAddressing::Range { from: 1, to: 64 }, None),
            desc!("expander_battery_low", "Expander: Battery Low Voltage", TroubleAddressing::Range { from: 1, to: 64 }, None),
            desc!("expander_battery_missing", "Expander: Battery Missing", TroubleAddressing::Range { from: 1, to: 64 }, None),
            desc!("expander_supply_overload", "Expander: Power Supply Overload", TroubleAddressing::Range { from: 1, to: 64 }, None),
            desc!("expander_no_comm", "Expander: No Communication", TroubleAddressing::Range { from: 1, to: 64 }, None),
            desc!("expander_substituted", "Expander: Substituted / Unknown Hardware", TroubleAddressing::Range { from: 1, to: 64 }, None),
            desc!("expander_tamper", "Expander: Tamper / Sabotage", TroubleAddressing::Range { from: 1, to: 64 }, None),
            desc!("expander_card_reader_head_a", "Expander: Card Reader Head A / Synchro Trouble", TroubleAddressing::Range { from: 1, to: 64 }, None),
            desc!("expander_card_reader_head_b", "Expander: Card Reader Head B / Charging Trouble", TroubleAddressing::Range { from: 1, to: 64 }, None),
            desc!("expander_acu_jammed_or_short_circuit", "Expander: Jammed / Addressable Loop Short Circuit", TroubleAddressing::Range { from: 1, to: 30 }, None),

            // --- Keypads ---
            desc!("keypad_no_comm", "Keypad: No Communication", TroubleAddressing::Range { from: 1, to: 8 }, None),
            desc!("keypad_substituted", "Keypad: Substituted", TroubleAddressing::Range { from: 1, to: 8 }, None),
            desc!("keypad_tamper", "Keypad: Tamper / Sabotage", TroubleAddressing::Range { from: 1, to: 8 }, None),
            desc!("keypad_init_error", "Keypad: Initialization Error", TroubleAddressing::Range { from: 1, to: 8 }, None),

            // --- Communication modules ---
            desc!("ethm_no_lan_cable", "ETHM-1: Ethernet LAN Cable Unplugged", TroubleAddressing::Range { from: 1, to: 8 }, None),
            
            desc!("gsm_ethm_station_1_error", "INT-GSM/ETHM-1: Station 1 Transmission Error", TroubleAddressing::Range { from: 0, to: 7 }, None),
            desc!("gsm_ethm_station_2_error", "INT-GSM/ETHM-1: Station 2 Transmission Error", TroubleAddressing::Range { from: 0, to: 7 }, None),
            desc!("gsm_gprs_sim1_station_1_error", "INT-GSM: GPRS SIM 1 Station 1 Error", TroubleAddressing::Range { from: 0, to: 7 }, None),
            desc!("gsm_gprs_sim1_station_2_error", "INT-GSM: GPRS SIM 1 Station 2 Error", TroubleAddressing::Range { from: 0, to: 7 }, None),
            desc!("gsm_gprs_sim2_station_1_error", "INT-GSM: GPRS SIM 2 Station 1 Error", TroubleAddressing::Range { from: 0, to: 7 }, None),
            desc!("gsm_gprs_sim2_station_2_error", "INT-GSM: GPRS SIM 2 Station 2 Error", TroubleAddressing::Range { from: 0, to: 7 }, None),
            desc!("gsm_sms_sim1_station_1_error", "INT-GSM: SMS SIM 1 Station 1 Error", TroubleAddressing::Range { from: 0, to: 7 }, None),
            desc!("gsm_sms_sim1_station_2_error", "INT-GSM: SMS SIM 1 Station 2 Error", TroubleAddressing::Range { from: 0, to: 7 }, None),
            desc!("gsm_sms_sim2_station_1_error", "INT-GSM: SMS SIM 2 Station 1 Error", TroubleAddressing::Range { from: 0, to: 7 }, None),
            desc!("gsm_sms_sim2_station_2_error", "INT-GSM: SMS SIM 2 Station 2 Error", TroubleAddressing::Range { from: 0, to: 7 }, None),
            
            desc_custom!("zone_long_violation_trouble", "Zone: Long Violation", TroubleAddressing::Range { from: 1, to: 256 }, Some(TroubleDomain::Zone), false, true),
            desc_custom!("zone_no_violation_trouble", "Zone: No Violation", TroubleAddressing::Range { from: 1, to: 256 }, Some(TroubleDomain::Zone), false, true),
            desc_custom!("zone_tamper_trouble", "Zone: Tamper", TroubleAddressing::Range { from: 1, to: 256 }, Some(TroubleDomain::Zone), false, true),
            desc_custom!("expander_restart", "Expander: Restart", TroubleAddressing::Range { from: 1, to: 64 }, None, false, true),
            desc_custom!("keypad_restart", "Keypad: Restart", TroubleAddressing::Range { from: 1, to: 8 }, None, false, true),

            desc!("ethm_ping_trouble", "ETHM-1: Ping Network Test Failed", TroubleAddressing::Range { from: 1, to: 8 }, None),
            desc!("ethm_server_id_error", "ETHM-1: SATEL Server MAC/ID Verification Error", TroubleAddressing::Range { from: 1, to: 8 }, None),
            desc!("ethm_satel_server_connection_error", "ETHM-1: No Connection to SATEL Server", TroubleAddressing::Range { from: 1, to: 8 }, None),
            desc!("ethm_monitoring_station1_error", "ETHM-1: Monitoring Station 1 Connection Error", TroubleAddressing::Single, None),
            desc!("ethm_monitoring_station2_error", "ETHM-1: Monitoring Station 2 Connection Error", TroubleAddressing::Single, None),
            desc!("gprs_monitoring_station1_error", "INT-GSM: GPRS Monitoring Station 1 Error", TroubleAddressing::Single, None),
            desc!("gprs_monitoring_station2_error", "INT-GSM: GPRS Monitoring Station 2 Error", TroubleAddressing::Single, None),
            desc!("ip_monitoring_station1_trouble", "IP Monitoring: Station 1 Communication Trouble", TroubleAddressing::Single, None),
            desc!("ip_monitoring_station2_trouble", "IP Monitoring: Station 2 Communication Trouble", TroubleAddressing::Single, None),
            desc!("time_server_trouble", "Network: NTP Time Synchronization Server Error", TroubleAddressing::Single, None),
            desc!("gsm_init_error", "INT-GSM: Module Initialization Error", TroubleAddressing::Single, None),
                        desc!("gsm_jamming", "INT-GSM: Cellular Jamming Detected", TroubleAddressing::Range { from: 0, to: 7 }, None),
            desc!("gsm_sim_pin_error", "INT-GSM: SIM Wrong PIN", TroubleAddressing::Pair { a_from: 0, a_to: 7, b_from: 1, b_to: 2 }, None),
            desc!("gsm_sim_logging_error", "INT-GSM: SIM Network Registration Error", TroubleAddressing::Pair { a_from: 0, a_to: 7, b_from: 1, b_to: 2 }, None),
            desc!("gsm_sim_credit_low", "INT-GSM: SIM Account Credit Low", TroubleAddressing::Pair { a_from: 0, a_to: 7, b_from: 1, b_to: 2 }, None),
            desc!("gsm_sim_sms_error", "INT-GSM: SIM SMS Sending Error", TroubleAddressing::Pair { a_from: 0, a_to: 7, b_from: 1, b_to: 2 }, None),
            desc!("gsm_settings_crc_error", "INT-GSM: Settings CRC Checksum Error", TroubleAddressing::Range { from: 0, to: 7 }, None),
            desc!("gsm_module_missing", "INT-GSM: Module Missing", TroubleAddressing::Range { from: 0, to: 7 }, None),
            desc!("gsm_module_changed", "INT-GSM: Module Changed", TroubleAddressing::Range { from: 0, to: 7 }, None),
            desc!("gsm_server_conn_error", "INT-GSM: Server Connection Error", TroubleAddressing::Range { from: 0, to: 7 }, None),
            desc!("gsm_mail_server_conn_error", "INT-GSM: Mail Server Error", TroubleAddressing::Range { from: 0, to: 7 }, None),
            desc!("gsm_ntp_server_conn_error", "INT-GSM: NTP Server Error", TroubleAddressing::Range { from: 0, to: 7 }, None),

            // --- Wireless devices ---
            desc!("wireless_device_low_battery", "Wireless Sensor: Low Battery", TroubleAddressing::Range { from: 1, to: 240 }, Some(TroubleDomain::Zone)),
            desc!("wireless_device_no_comm", "Wireless Sensor: No Radio Communication", TroubleAddressing::Range { from: 1, to: 240 }, Some(TroubleDomain::Zone)),
            desc!("wireless_output_no_comm", "Wireless Output: No Radio Communication", TroubleAddressing::Range { from: 1, to: 240 }, Some(TroubleDomain::Output)),
            
            // --- Key fobs ---
            desc_custom!("master_key_fob_low_battery", "Master User Key Fob: Low Battery", TroubleAddressing::Range { from: 1, to: 8 }, None, true, false),
            desc_custom!("user_key_fob_low_battery", "User Key Fob: Low Battery", TroubleAddressing::Range { from: 1, to: 240 }, None, true, false),

            // --- Other ---
            desc!("auxiliary_stm_troubles", "Auxiliary Microprocessor (STM) Trouble", TroubleAddressing::Single, None),
        ]
    }

    pub fn key(&self) -> Option<&'static str> {
        match self {
            Self::MainBoardAcLoss => Some("main_board_ac_loss"),
            Self::MainBoardBatteryLow => Some("main_board_battery_low"),
            Self::MainBoardBatteryMissing => Some("main_board_battery_missing"),
            Self::MainBoardOutOverload(_) => Some("main_board_out_overload"),
            Self::MainBoardKpdPowerOverload => Some("main_board_kpd_power_overload"),
            Self::MainBoardExPowerOverload => Some("main_board_ex_power_overload"),
            Self::MainBoardDataBusDt1 => Some("main_board_data_bus_dt1"),
            Self::MainBoardDataBusDt2 => Some("main_board_data_bus_dt2"),
            Self::MainBoardDataBusDtm => Some("main_board_data_bus_dtm"),
            Self::RtcLoss => Some("rtc_loss"),
            Self::NoDtrSignal => Some("no_dtr_signal"),
            Self::ExternalModemInitTrouble => Some("external_modem_init_trouble"),
            Self::ExternalModemCmdTrouble => Some("external_modem_cmd_trouble"),
            Self::TelephoneLineNoVoltage => Some("telephone_line_no_voltage"),
            Self::TelephoneLineBadSignal => Some("telephone_line_bad_signal"),
            Self::TelephoneLineNoSignal => Some("telephone_line_no_signal"),
            Self::MonitoringStation1Trouble => Some("monitoring_station1_trouble"),
            Self::MonitoringStation2Trouble => Some("monitoring_station2_trouble"),
            Self::EepromRtcTrouble => Some("eeprom_rtc_trouble"),
            Self::RamMemoryError => Some("ram_memory_error"),
            Self::MainPanelRestartMemory => Some("main_panel_restart"),
            Self::TechnicalZoneTrouble(_) => Some("technical_zone_trouble"),
            Self::ExpanderAcLoss(_) => Some("expander_ac_loss"),
            Self::ExpanderBatteryLow(_) => Some("expander_battery_low"),
            Self::ExpanderBatteryMissing(_) => Some("expander_battery_missing"),
            Self::ExpanderSupplyOverload(_) => Some("expander_supply_overload"),
            Self::ExpanderNoComm(_) => Some("expander_no_comm"),
            Self::ExpanderSubstituted(_) => Some("expander_substituted"),
            Self::ExpanderTamper(_) => Some("expander_tamper"),
            Self::ExpanderCardReaderHeadA(_) => Some("expander_card_reader_head_a"),
            Self::ExpanderCardReaderHeadB(_) => Some("expander_card_reader_head_b"),
            Self::ExpanderAcuJammedOrShortCircuit(_) => Some("expander_acu_jammed_or_short_circuit"),
            Self::KeypadNoComm(_) => Some("keypad_no_comm"),
            Self::KeypadSubstituted(_) => Some("keypad_substituted"),
            Self::KeypadTamper(_) => Some("keypad_tamper"),
            Self::KeypadInitError(_) => Some("keypad_init_error"),
            Self::EthmNoLanCable(_) => Some("ethm_no_lan_cable"),
            Self::EthmPingTrouble(_) => Some("ethm_ping_trouble"),
            Self::EthmServerIdError(_) => Some("ethm_server_id_error"),
            Self::EthmSatelServerConnectionError(_) => Some("ethm_satel_server_connection_error"),
            Self::EthmMonitoringStation1Error => Some("ethm_monitoring_station1_error"),
            Self::EthmMonitoringStation2Error => Some("ethm_monitoring_station2_error"),
            Self::GprsMonitoringStation1Error => Some("gprs_monitoring_station1_error"),
            Self::GprsMonitoringStation2Error => Some("gprs_monitoring_station2_error"),
            Self::IpMonitoringStation1Trouble => Some("ip_monitoring_station1_trouble"),
            Self::IpMonitoringStation2Trouble => Some("ip_monitoring_station2_trouble"),
            Self::TimeServerTrouble => Some("time_server_trouble"),
            Self::GsmInitError => Some("gsm_init_error"),
            Self::GsmJamming(_) => Some("gsm_jamming"),
            Self::GsmSimPinError { .. } => Some("gsm_sim_pin_error"),
            Self::GsmSimLoggingError { .. } => Some("gsm_sim_logging_error"),
            Self::GsmSimCreditLow { .. } => Some("gsm_sim_credit_low"),
            Self::GsmSimSmsError { .. } => Some("gsm_sim_sms_error"),
            Self::GsmSettingsCrcError(_) => Some("gsm_settings_crc_error"),
            Self::GsmModuleMissing(_) => Some("gsm_module_missing"),
            Self::GsmModuleChanged(_) => Some("gsm_module_changed"),
            Self::GsmServerConnError(_) => Some("gsm_server_conn_error"),
            Self::GsmMailServerConnError(_) => Some("gsm_mail_server_conn_error"),
            Self::GsmNtpServerConnError(_) => Some("gsm_ntp_server_conn_error"),
            Self::WirelessDeviceLowBattery { .. } => Some("wireless_device_low_battery"),
            Self::WirelessDeviceNoComm { .. } => Some("wireless_device_no_comm"),
            Self::WirelessOutputNoComm { .. } => Some("wireless_output_no_comm"),
            Self::MasterKeyFobLowBattery(_) => Some("master_key_fob_low_battery"),
            Self::UserKeyFobLowBattery { .. } => Some("user_key_fob_low_battery"),

            Self::GsmEthmStation1Error(_) => Some("gsm_ethm_station_1_error"),
            Self::GsmEthmStation2Error(_) => Some("gsm_ethm_station_2_error"),
            Self::GsmGprsSim1Station1Error(_) => Some("gsm_gprs_sim1_station_1_error"),
            Self::GsmGprsSim1Station2Error(_) => Some("gsm_gprs_sim1_station_2_error"),
            Self::GsmGprsSim2Station1Error(_) => Some("gsm_gprs_sim2_station_1_error"),
            Self::GsmGprsSim2Station2Error(_) => Some("gsm_gprs_sim2_station_2_error"),
            Self::GsmSmsSim1Station1Error(_) => Some("gsm_sms_sim1_station_1_error"),
            Self::GsmSmsSim1Station2Error(_) => Some("gsm_sms_sim1_station_2_error"),
            Self::GsmSmsSim2Station1Error(_) => Some("gsm_sms_sim2_station_1_error"),
            Self::GsmSmsSim2Station2Error(_) => Some("gsm_sms_sim2_station_2_error"),
            Self::ZoneLongViolationTrouble(_) => Some("zone_long_violation_trouble"),
            Self::ZoneNoViolationTrouble(_) => Some("zone_no_violation_trouble"),
            Self::ZoneTamperTrouble(_) => Some("zone_tamper_trouble"),
            Self::ExpanderRestart(_) => Some("expander_restart"),
            Self::KeypadRestart(_) => Some("keypad_restart"),
            Self::AuxiliaryStmTroubles => Some("auxiliary_stm_troubles"),
            Self::GenericTrouble { .. } => None,
        }
    }

    pub fn address(&self) -> Option<TroubleAddress> {
        match self {
            Self::MainBoardAcLoss => Some(TroubleAddress::One(1)),
            Self::MainBoardBatteryLow => Some(TroubleAddress::One(1)),
            Self::MainBoardBatteryMissing => Some(TroubleAddress::One(1)),
            Self::MainBoardOutOverload(id) => Some(TroubleAddress::One(*id as u16)),
            Self::MainBoardKpdPowerOverload => Some(TroubleAddress::One(1)),
            Self::MainBoardExPowerOverload => Some(TroubleAddress::One(1)),
            Self::MainBoardDataBusDt1 => Some(TroubleAddress::One(1)),
            Self::MainBoardDataBusDt2 => Some(TroubleAddress::One(1)),
            Self::MainBoardDataBusDtm => Some(TroubleAddress::One(1)),
            Self::RtcLoss => Some(TroubleAddress::One(1)),
            Self::NoDtrSignal => Some(TroubleAddress::One(1)),
            Self::ExternalModemInitTrouble => Some(TroubleAddress::One(1)),
            Self::ExternalModemCmdTrouble => Some(TroubleAddress::One(1)),
            Self::TelephoneLineNoVoltage => Some(TroubleAddress::One(1)),
            Self::TelephoneLineBadSignal => Some(TroubleAddress::One(1)),
            Self::TelephoneLineNoSignal => Some(TroubleAddress::One(1)),
            Self::MonitoringStation1Trouble => Some(TroubleAddress::One(1)),
            Self::MonitoringStation2Trouble => Some(TroubleAddress::One(1)),
            Self::EepromRtcTrouble => Some(TroubleAddress::One(1)),
            Self::RamMemoryError => Some(TroubleAddress::One(1)),
            Self::MainPanelRestartMemory => Some(TroubleAddress::One(1)),
            Self::TechnicalZoneTrouble(id) => Some(TroubleAddress::One(*id)),
            Self::ExpanderAcLoss(id) => Some(TroubleAddress::One(*id as u16)),
            Self::ExpanderBatteryLow(id) => Some(TroubleAddress::One(*id as u16)),
            Self::ExpanderBatteryMissing(id) => Some(TroubleAddress::One(*id as u16)),
            Self::ExpanderSupplyOverload(id) => Some(TroubleAddress::One(*id as u16)),
            Self::ExpanderNoComm(id) => Some(TroubleAddress::One(*id as u16)),
            Self::ExpanderSubstituted(id) => Some(TroubleAddress::One(*id as u16)),
            Self::ExpanderTamper(id) => Some(TroubleAddress::One(*id as u16)),
            Self::ExpanderCardReaderHeadA(id) => Some(TroubleAddress::One(*id as u16)),
            Self::ExpanderCardReaderHeadB(id) => Some(TroubleAddress::One(*id as u16)),
            Self::ExpanderAcuJammedOrShortCircuit(id) => Some(TroubleAddress::One(*id as u16)),
            Self::KeypadNoComm(id) => Some(TroubleAddress::One(*id as u16)),
            Self::KeypadSubstituted(id) => Some(TroubleAddress::One(*id as u16)),
            Self::KeypadTamper(id) => Some(TroubleAddress::One(*id as u16)),
            Self::KeypadInitError(id) => Some(TroubleAddress::One(*id as u16)),
            Self::EthmNoLanCable(id) => Some(TroubleAddress::One(*id as u16)),
            Self::EthmPingTrouble(_) => Some(TroubleAddress::One(1)),
            Self::EthmServerIdError(_) => Some(TroubleAddress::One(1)),
            Self::EthmSatelServerConnectionError(_) => Some(TroubleAddress::One(1)),
            Self::EthmMonitoringStation1Error => Some(TroubleAddress::One(1)),
            Self::EthmMonitoringStation2Error => Some(TroubleAddress::One(1)),
            Self::GprsMonitoringStation1Error => Some(TroubleAddress::One(1)),
            Self::GprsMonitoringStation2Error => Some(TroubleAddress::One(1)),
            Self::IpMonitoringStation1Trouble => Some(TroubleAddress::One(1)),
            Self::IpMonitoringStation2Trouble => Some(TroubleAddress::One(1)),
            Self::TimeServerTrouble => Some(TroubleAddress::One(1)),
            Self::GsmInitError => Some(TroubleAddress::One(1)),
            Self::GsmJamming(id) => Some(TroubleAddress::One(*id as u16)),
            Self::GsmSimPinError { module, sim } => Some(TroubleAddress::Pair(*module as u16, *sim as u16)),
            Self::GsmSimLoggingError { module, sim } => Some(TroubleAddress::Pair(*module as u16, *sim as u16)),
            Self::GsmSimCreditLow { module, sim } => Some(TroubleAddress::Pair(*module as u16, *sim as u16)),
            Self::GsmSimSmsError { module, sim } => Some(TroubleAddress::Pair(*module as u16, *sim as u16)),
            Self::GsmSettingsCrcError(id) => Some(TroubleAddress::One(*id as u16)),
            Self::GsmModuleMissing(id) => Some(TroubleAddress::One(*id as u16)),
            Self::GsmModuleChanged(id) => Some(TroubleAddress::One(*id as u16)),
            Self::GsmServerConnError(id) => Some(TroubleAddress::One(*id as u16)),
            Self::GsmMailServerConnError(id) => Some(TroubleAddress::One(*id as u16)),
            Self::GsmNtpServerConnError(id) => Some(TroubleAddress::One(*id as u16)),
            Self::WirelessDeviceLowBattery { zone_id } => Some(TroubleAddress::One(*zone_id)),
            Self::WirelessDeviceNoComm { zone_id } => Some(TroubleAddress::One(*zone_id)),
            Self::WirelessOutputNoComm { output_id } => Some(TroubleAddress::One(*output_id)),
            Self::MasterKeyFobLowBattery(id) => Some(TroubleAddress::One(*id as u16)),
            Self::UserKeyFobLowBattery { user_id } => Some(TroubleAddress::One(*user_id)),

            Self::GsmEthmStation1Error(id) | Self::GsmEthmStation2Error(id) 
            | Self::GsmGprsSim1Station1Error(id) | Self::GsmGprsSim1Station2Error(id)
            | Self::GsmGprsSim2Station1Error(id) | Self::GsmGprsSim2Station2Error(id)
            | Self::GsmSmsSim1Station1Error(id) | Self::GsmSmsSim1Station2Error(id)
            | Self::GsmSmsSim2Station1Error(id) | Self::GsmSmsSim2Station2Error(id) 
            => Some(TroubleAddress::One(*id as u16)),
            
            Self::ZoneLongViolationTrouble(id) 
            | Self::ZoneNoViolationTrouble(id) 
            | Self::ZoneTamperTrouble(id) 
            => Some(TroubleAddress::One(*id)),
            
            Self::ExpanderRestart(id) => Some(TroubleAddress::One(*id as u16)),
            Self::KeypadRestart(id) => Some(TroubleAddress::One(*id as u16)),
            Self::AuxiliaryStmTroubles => Some(TroubleAddress::One(1)),
            Self::GenericTrouble { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_trouble_type_catalog() {
        let catalog = TroubleType::catalog();
        let mut keys = HashSet::new();

        for desc in catalog {
            assert!(
                desc.key.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
                "Key {} contains invalid characters", desc.key
            );
            assert!(
                !desc.key.ends_with("_memory"),
                "Key {} ends with _memory", desc.key
            );
            assert!(
                keys.insert(desc.key),
                "Duplicate key {}", desc.key
            );
        }

        // Exhaustive match to ensure every variant is covered
        // Note: GenericTrouble, GsmTrouble, GsmCmeError will return None.
        let check = |t: TroubleType| {
            let key_opt = t.key();
            if let Some(key) = key_opt {
                let desc = catalog.iter().find(|d| d.key == key).expect(&format!("Missing descriptor for key {}", key));
                
                let addr_opt = t.address();
                if let Some(addr) = addr_opt {
                    match (addr, &desc.addressing) {
                        (TroubleAddress::One(v), TroubleAddressing::Single) => assert_eq!(v, 1),
                        (TroubleAddress::One(v), TroubleAddressing::Range { from, to }) => {
                            assert!(v >= *from && v <= *to, "Value {} out of range {}..={} for {}", v, from, to, key);
                        }
                        (TroubleAddress::Pair(a, b), TroubleAddressing::Pair { a_from, a_to, b_from, b_to }) => {
                            assert!(a >= *a_from && a <= *a_to, "Value {} out of range {}..={} for {}", a, a_from, a_to, key);
                            assert!(b >= *b_from && b <= *b_to, "Value {} out of range {}..={} for {}", b, b_from, b_to, key);
                        }
                        _ => panic!("Address mismatch for key {}", key),
                    }
                }
            } else {
                match t {
                    TroubleType::GenericTrouble { .. } => {}
                    _ => panic!("Unexpected None key for {:?}", t),
                }
            }
        };

        check(TroubleType::MainBoardAcLoss);
        check(TroubleType::MainBoardBatteryLow);
        check(TroubleType::MainBoardBatteryMissing);
        check(TroubleType::MainBoardOutOverload(1));
        check(TroubleType::MainBoardOutOverload(4));
        check(TroubleType::MainBoardKpdPowerOverload);
        check(TroubleType::MainBoardExPowerOverload);
        check(TroubleType::MainBoardDataBusDt1);
        check(TroubleType::MainBoardDataBusDt2);
        check(TroubleType::MainBoardDataBusDtm);
        check(TroubleType::RtcLoss);
        check(TroubleType::NoDtrSignal);
        check(TroubleType::ExternalModemInitTrouble);
        check(TroubleType::ExternalModemCmdTrouble);
        check(TroubleType::TelephoneLineNoVoltage);
        check(TroubleType::TelephoneLineBadSignal);
        check(TroubleType::TelephoneLineNoSignal);
        check(TroubleType::MonitoringStation1Trouble);
        check(TroubleType::MonitoringStation2Trouble);
        check(TroubleType::EepromRtcTrouble);
        check(TroubleType::RamMemoryError);
        check(TroubleType::MainPanelRestartMemory);
        check(TroubleType::TechnicalZoneTrouble(1));
        check(TroubleType::TechnicalZoneTrouble(256));
        check(TroubleType::ExpanderAcLoss(1));
        check(TroubleType::ExpanderAcLoss(64));
        check(TroubleType::ExpanderBatteryLow(1));
        check(TroubleType::ExpanderBatteryLow(64));
        check(TroubleType::ExpanderBatteryMissing(1));
        check(TroubleType::ExpanderBatteryMissing(64));
        check(TroubleType::ExpanderSupplyOverload(1));
        check(TroubleType::ExpanderSupplyOverload(64));
        check(TroubleType::ExpanderNoComm(1));
        check(TroubleType::ExpanderNoComm(64));
        check(TroubleType::ExpanderSubstituted(1));
        check(TroubleType::ExpanderSubstituted(64));
        check(TroubleType::ExpanderTamper(1));
        check(TroubleType::ExpanderTamper(64));
        check(TroubleType::ExpanderCardReaderHeadA(1));
        check(TroubleType::ExpanderCardReaderHeadA(64));
        check(TroubleType::ExpanderCardReaderHeadB(1));
        check(TroubleType::ExpanderCardReaderHeadB(64));
        check(TroubleType::ExpanderAcuJammedOrShortCircuit(1));
        check(TroubleType::ExpanderAcuJammedOrShortCircuit(16));
        check(TroubleType::KeypadNoComm(1));
        check(TroubleType::KeypadNoComm(8));
        check(TroubleType::KeypadSubstituted(1));
        check(TroubleType::KeypadSubstituted(8));
        check(TroubleType::KeypadTamper(1));
        check(TroubleType::KeypadTamper(8));
        check(TroubleType::KeypadInitError(1));
        check(TroubleType::KeypadInitError(8));
        check(TroubleType::EthmNoLanCable(1));
        check(TroubleType::EthmNoLanCable(8));
        check(TroubleType::EthmPingTrouble(1));
        check(TroubleType::EthmServerIdError(1));
        check(TroubleType::EthmSatelServerConnectionError(1));
        check(TroubleType::EthmMonitoringStation1Error);
        check(TroubleType::EthmMonitoringStation2Error);
        check(TroubleType::GprsMonitoringStation1Error);
        check(TroubleType::GprsMonitoringStation2Error);
        check(TroubleType::IpMonitoringStation1Trouble);
        check(TroubleType::IpMonitoringStation2Trouble);
        check(TroubleType::TimeServerTrouble);
        check(TroubleType::GsmInitError);
        check(TroubleType::GsmEthmStation1Error(1));
        check(TroubleType::ZoneLongViolationTrouble(1));
        check(TroubleType::GsmJamming(0));
        check(TroubleType::GsmJamming(7));
        check(TroubleType::GsmSimPinError { module: 0, sim: 1 });
        check(TroubleType::GsmSimPinError { module: 7, sim: 2 });
        check(TroubleType::GsmSimLoggingError { module: 0, sim: 1 });
        check(TroubleType::GsmSimLoggingError { module: 7, sim: 2 });
        check(TroubleType::GsmSimCreditLow { module: 0, sim: 1 });
        check(TroubleType::GsmSimCreditLow { module: 7, sim: 2 });
        check(TroubleType::GsmSimSmsError { module: 0, sim: 1 });
        check(TroubleType::GsmSimSmsError { module: 7, sim: 2 });
        check(TroubleType::GsmSettingsCrcError(0));
        check(TroubleType::GsmSettingsCrcError(7));
        check(TroubleType::GsmModuleMissing(0));
        check(TroubleType::GsmModuleMissing(7));
        check(TroubleType::GsmModuleChanged(0));
        check(TroubleType::GsmModuleChanged(7));
        check(TroubleType::GsmServerConnError(0));
        check(TroubleType::GsmServerConnError(7));
        check(TroubleType::GsmMailServerConnError(0));
        check(TroubleType::GsmMailServerConnError(7));
        check(TroubleType::GsmNtpServerConnError(0));
        check(TroubleType::GsmNtpServerConnError(7));
        check(TroubleType::WirelessDeviceLowBattery { zone_id: 1 });
        check(TroubleType::WirelessDeviceLowBattery { zone_id: 240 });
        check(TroubleType::WirelessDeviceNoComm { zone_id: 1 });
        check(TroubleType::WirelessDeviceNoComm { zone_id: 240 });
        check(TroubleType::WirelessOutputNoComm { output_id: 1 });
        check(TroubleType::WirelessOutputNoComm { output_id: 240 });
                        check(TroubleType::MasterKeyFobLowBattery(1));
        check(TroubleType::MasterKeyFobLowBattery(8));
        check(TroubleType::UserKeyFobLowBattery { user_id: 1 });
        check(TroubleType::UserKeyFobLowBattery { user_id: 240 });
        check(TroubleType::AuxiliaryStmTroubles);
        check(TroubleType::GenericTrouble { part: 0, bit: 0 });
        check(TroubleType::ExpanderRestart(1));
        check(TroubleType::KeypadRestart(1));

        let variant_count = 83;
        let expected_desc_count = variant_count - 3;
        assert_eq!(catalog.len(), expected_desc_count, "Expected {} descriptors, got {}", expected_desc_count, catalog.len());
    }
}
