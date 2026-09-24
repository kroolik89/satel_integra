//! Internal state update methods for in-memory cache synchronization.
//! All methods are `pub(crate)` and used by `SatelIntegra` and `SatelAutoRequester`.
use crate::client::SatelIntegra;
use crate::command::SatelCommand;
use crate::error::SatelError;
use crate::event::SatelEvent;

use crate::state::{
    EthmVersion, IntegraVersion, OutputsStateData, PartitionsArmedData, PartitionsData,
    SystemStatus, ZonesAlarmData, ZonesAlarmMemoryData, ZonesBypassData,
    ZonesLongViolationTroubleData, ZonesNoViolationTroubleData, ZonesTamperAlarmData,
    ZonesTamperAlarmMemoryData, ZonesTamperData, ZonesViolationData,
};
use chrono::Local;

pub(crate) struct TempLimits {
    pub max_timeout_errors: u32,
    pub max_sensor_errors: u32,
    pub unblock_enabled: bool,
    pub unblock_after_cycles: u32,
}

impl SatelIntegra {
    pub(crate) fn temp_limits(&self, zone_id: u16) -> TempLimits {
        let config = self.config.read().unwrap();
        if let Some(probe) = config.temperature_probes.iter().find(|p| p.zone_id == zone_id) {
            TempLimits {
                max_timeout_errors: probe.max_timeout_errors,
                max_sensor_errors: probe.max_sensor_errors,
                unblock_enabled: probe.unblock_enabled,
                unblock_after_cycles: probe.unblock_after_cycles,
            }
        } else {
            TempLimits {
                max_timeout_errors: config.temp_max_timeout_errors,
                max_sensor_errors: config.temp_max_sensor_errors,
                unblock_enabled: false,
                unblock_after_cycles: crate::config::MIN_UNBLOCK_AFTER_CYCLES,
            }
        }
    }
    pub(crate) fn update_integra_version_internal(&self, version: IntegraVersion) -> Result<(), SatelError> {
        {
            let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
            state.integra_version = Some(version.clone());
        }
        let _ = self.event_tx.send(SatelEvent::IntegraVersionReceived(version));
        Ok(())
    }

    pub(crate) fn update_ethm_version_internal(&self, version: EthmVersion) -> Result<(), SatelError> {
        {
            let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
            state.ethm_version = Some(version.clone());
        }
        let _ = self.event_tx.send(SatelEvent::EthmVersionReceived(version));
        Ok(())
    }

    pub(crate) fn update_temp_error(&self, zone_id: u16, error: &SatelError) -> Result<(), SatelError> {
        let limits = self.temp_limits(zone_id);
        
        let (zone_id, reported_status) = {
            let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
            let zone = state
                .zones
                .get_mut((zone_id.wrapping_sub(1) % 256) as usize)
                .ok_or(SatelError::InvalidFrame)?;

            let status = match error {
                SatelError::Timeout | SatelError::TemperatureNotSupportedOrTimeOut => {
                    zone.temperature_timeout_errors_total += 1;
                    zone.temperature_timeout_errors_current += 1;
                    if zone.temperature_timeout_errors_current >= limits.max_timeout_errors {
                        zone.temperature_status = crate::state::TemperatureSensorStatus::BlockSensorMissing;
                        zone.temperature_blocked_cycles = 0;
                    } else {
                        zone.temperature_status = crate::state::TemperatureSensorStatus::SensorMissing;
                    }
                    // Real network attempt was a timeout
                    crate::state::TemperatureSensorStatus::SensorMissing
                }
                SatelError::TemperatureSensorError => {
                    zone.temperature_sensor_errors_total += 1;
                    zone.temperature_sensor_errors_current += 1;
                    if zone.temperature_sensor_errors_current >= limits.max_sensor_errors {
                        zone.temperature_status = crate::state::TemperatureSensorStatus::BlockCommunicationError;
                        zone.temperature_blocked_cycles = 0;
                    } else {
                        zone.temperature_status = crate::state::TemperatureSensorStatus::CommunicationError;
                    }
                    // Real network attempt was a communication fault (0xFFFF)
                    crate::state::TemperatureSensorStatus::CommunicationError
                }
                _ => zone.temperature_status,
            };
            zone.temperature_read_at = Local::now();
            (zone.id, status)
        };

        let _ = self.event_tx.send(SatelEvent::ZoneTemperatureError {
            id: zone_id,
            status: reported_status,
        });
        Ok(())
    }

    pub(crate) fn update_zones_tamper_internal(&self, result: ZonesTamperData) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(zone) = state.zones.get_mut(i) {
                let changed = zone.tamper_state != new_state;
                if changed || self.config.read().unwrap().emit_unchanged_zones {
                    zone.tamper_state = new_state;
                    zone.tamper_read_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::ZoneTamper { id: zone.id, state: new_state });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn update_zones_alarm_internal(&self, result: ZonesAlarmData) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(zone) = state.zones.get_mut(i) {
                let changed = zone.alarm_state != new_state;
                if changed || self.config.read().unwrap().emit_unchanged_zones {
                    zone.alarm_state = new_state;
                    zone.alarm_read_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::ZoneAlarm { id: zone.id, state: new_state });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn update_zones_violation_internal(&self, result: ZonesViolationData) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(zone) = state.zones.get_mut(i) {
                let changed = zone.violation_state != new_state;
                if changed || self.config.read().unwrap().emit_unchanged_zones {
                    zone.violation_state = new_state;
                    zone.violation_read_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::ZoneViolation { id: zone.id, state: new_state });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn update_zones_tamper_alarm_internal(&self, result: ZonesTamperAlarmData) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(zone) = state.zones.get_mut(i) {
                let changed = zone.tamper_alarm_state != new_state;
                if changed || self.config.read().unwrap().emit_unchanged_zones {
                    zone.tamper_alarm_state = new_state;
                    zone.tamper_alarm_read_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::ZoneTamperAlarm { id: zone.id, state: new_state });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn update_zones_alarm_memory_internal(&self, result: ZonesAlarmMemoryData) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(zone) = state.zones.get_mut(i) {
                let changed = zone.alarm_memory_state != new_state;
                if changed || self.config.read().unwrap().emit_unchanged_zones {
                    zone.alarm_memory_state = new_state;
                    zone.alarm_memory_read_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::ZoneAlarmMemory { id: zone.id, state: new_state });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn update_zones_tamper_alarm_memory_internal(&self, result: ZonesTamperAlarmMemoryData) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(zone) = state.zones.get_mut(i) {
                let changed = zone.tamper_alarm_memory_state != new_state;
                if changed || self.config.read().unwrap().emit_unchanged_zones {
                    zone.tamper_alarm_memory_state = new_state;
                    zone.tamper_alarm_memory_read_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::ZoneTamperAlarmMemory { id: zone.id, state: new_state });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn update_zones_bypass_internal(&self, result: ZonesBypassData) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(zone) = state.zones.get_mut(i) {
                let changed = zone.bypass_state != new_state;
                if changed || self.config.read().unwrap().emit_unchanged_zones {
                    zone.bypass_state = new_state;
                    zone.bypass_read_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::ZoneBypass { id: zone.id, state: new_state });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn update_zones_no_violation_trouble_internal(&self, result: ZonesNoViolationTroubleData) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(zone) = state.zones.get_mut(i) {
                let changed = zone.no_violation_trouble_state != new_state;
                if changed || self.config.read().unwrap().emit_unchanged_zones {
                    zone.no_violation_trouble_state = new_state;
                    zone.no_violation_trouble_read_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::ZoneNoViolationTrouble { id: zone.id, state: new_state });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn update_zones_long_violation_trouble_internal(&self, result: ZonesLongViolationTroubleData) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(zone) = state.zones.get_mut(i) {
                let changed = zone.long_violation_trouble_state != new_state;
                if changed || self.config.read().unwrap().emit_unchanged_zones {
                    zone.long_violation_trouble_state = new_state;
                    zone.long_violation_trouble_read_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::ZoneLongViolationTrouble { id: zone.id, state: new_state });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn update_partitions_armed_internal(&self, result: PartitionsArmedData) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(partition) = state.partitions.get_mut(i) {
                let changed = partition.armed_suppressed != new_state;
                if changed || self.config.read().unwrap().emit_unchanged_partitions {
                    partition.armed_suppressed = new_state;
                    partition.armed_suppressed_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::PartitionArmed { id: partition.id, state: new_state });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn update_partitions_armed_really_internal(&self, result: PartitionsData) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(partition) = state.partitions.get_mut(i) {
                let changed = partition.armed_really != new_state;
                if changed || self.config.read().unwrap().emit_unchanged_partitions {
                    partition.armed_really = new_state;
                    partition.armed_really_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::PartitionArmedReally { id: partition.id, state: new_state });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn update_partitions_alarm_internal(&self, result: PartitionsData) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(partition) = state.partitions.get_mut(i) {
                let changed = partition.alarm != new_state;
                if changed || self.config.read().unwrap().emit_unchanged_partitions {
                    partition.alarm = new_state;
                    partition.alarm_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::PartitionAlarm { id: partition.id, state: new_state });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn update_partitions_alarm_memory_internal(&self, result: PartitionsData) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(partition) = state.partitions.get_mut(i) {
                let changed = partition.alarm_memory != new_state;
                if changed || self.config.read().unwrap().emit_unchanged_partitions {
                    partition.alarm_memory = new_state;
                    partition.alarm_memory_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::PartitionAlarmMemory { id: partition.id, state: new_state });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn update_partitions_entry_time_internal(&self, result: PartitionsData) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(partition) = state.partitions.get_mut(i) {
                let changed = partition.entry_time != new_state;
                if changed || self.config.read().unwrap().emit_unchanged_partitions {
                    partition.entry_time = new_state;
                    partition.entry_time_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::PartitionEntryTime { id: partition.id, state: new_state });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn update_partitions_exit_time_gt_10s_internal(&self, result: PartitionsData) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(partition) = state.partitions.get_mut(i) {
                let changed = partition.exit_time_gt_10s != new_state;
                if changed || self.config.read().unwrap().emit_unchanged_partitions {
                    partition.exit_time_gt_10s = new_state;
                    partition.exit_time_gt_10s_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::PartitionExitTimeGt10s { id: partition.id, state: new_state });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn update_partitions_exit_time_lt_10s_internal(&self, result: PartitionsData) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(partition) = state.partitions.get_mut(i) {
                let changed = partition.exit_time_lt_10s != new_state;
                if changed || self.config.read().unwrap().emit_unchanged_partitions {
                    partition.exit_time_lt_10s = new_state;
                    partition.exit_time_lt_10s_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::PartitionExitTimeLt10s { id: partition.id, state: new_state });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn update_outputs_state_internal(&self, result: OutputsStateData) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(output) = state.outputs.get_mut(i) {
                let changed = output.state != new_state;
                if changed || self.config.read().unwrap().emit_unchanged_outputs {
                    output.state = new_state;
                    output.state_read_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::OutputChanged { id: output.id, state: new_state });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn update_system_status_internal(&self, status: SystemStatus) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        let changed = state.system_status != Some(status);
        if changed || self.config.read().unwrap().emit_unchanged_system_status {
            state.system_status = Some(status);
            let _ = self.event_tx.send(SatelEvent::SystemStatusChanged(status));
        }
        Ok(())
    }

    pub(crate) fn update_troubles_internal(&self, cmd: SatelCommand, data: &[u8]) -> Result<(), SatelError> {
        let items = crate::parsers::decode_troubles(cmd.to_byte(), data)?;
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        let emit_unchanged = self.config.read().unwrap().emit_unchanged_troubles;

        for item in items {
            match item {
                crate::parsers::TroubleItem::Flag { trouble, memory, active } => {
                    let prev = state.trouble_flags.insert((trouble, memory), active).unwrap_or(false);
                    let changed = prev != active;
                    if changed || emit_unchanged {
                        if memory {
                            let _ = self.event_tx.send(SatelEvent::TroubleMemory(trouble, active));
                        } else {
                            let _ = self.event_tx.send(SatelEvent::Trouble(trouble, active));
                        }
                    }
                }
                crate::parsers::TroubleItem::AcuJamLevel { module, level } => {
                    let prev = state.acu_jam_levels.insert(module, level).unwrap_or(0);
                    let changed = prev != level;
                    if changed || emit_unchanged {
                        let _ = self.event_tx.send(SatelEvent::AcuJamLevel { module, level });
                    }
                }
                crate::parsers::TroubleItem::CmeError { source, sim, memory, code } => {
                    let prev = state.cme_errors.insert((source, sim, memory), code).unwrap_or(0);
                    let changed = prev != code;
                    if changed || emit_unchanged {
                        let _ = self.event_tx.send(SatelEvent::CmeError { source, sim, code, memory });
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, RwLock, Mutex};
    use tokio::sync::broadcast;
    use crate::state::SatelState;
    use crate::config::Config;

    fn setup_client() -> (SatelIntegra, broadcast::Receiver<SatelEvent>) {
        let (tx, rx) = broadcast::channel(1000);
        let state = Arc::new(RwLock::new(SatelState::new()));
        let config = Arc::new(RwLock::new(Config::default()));
        
        let (internal_tx, _) = tokio::sync::mpsc::channel(1);
        let client = SatelIntegra {
            state,
            config,
            event_tx: tx,
            tx: internal_tx,
            worker: Arc::new(Mutex::new(None)),
            auto_read_dirty: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            session_zone_type: Arc::new(std::sync::atomic::AtomicU8::new(5)),
            session_output_type: Arc::new(std::sync::atomic::AtomicU8::new(17)),
            session_partition_type: Arc::new(std::sync::atomic::AtomicU8::new(19)),
            session_zone_confirmed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            session_output_confirmed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            session_partition_confirmed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        (client, rx)
    }

    #[test]
    fn test_t4_trouble_flag_initial_inactive() {
        let (client, mut rx) = setup_client();
        let data = vec![0; 47];
        client.update_troubles_internal(SatelCommand::TroublesPart1, &data).unwrap();
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_t4_trouble_flag_initial_active() {
        let (client, mut rx) = setup_client();
        let mut data = vec![0; 47];
        data[0] = 0x01;
        client.update_troubles_internal(SatelCommand::TroublesPart1, &data).unwrap();
        let event = rx.try_recv().unwrap();
        match event {
            SatelEvent::Trouble(crate::state::TroubleType::TechnicalZoneTrouble(1), true) => {}
            _ => panic!("Expected Trouble for TechnicalZoneTrouble(1)"),
        }
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_t4_trouble_flag_deactivation() {
        let (client, mut rx) = setup_client();
        let mut data = vec![0; 47];
        data[0] = 0x01;
        client.update_troubles_internal(SatelCommand::TroublesPart1, &data).unwrap();
        let _ = rx.try_recv().unwrap();

        data[0] = 0x00;
        client.update_troubles_internal(SatelCommand::TroublesPart1, &data).unwrap();
        let event = rx.try_recv().unwrap();
        match event {
            SatelEvent::Trouble(crate::state::TroubleType::TechnicalZoneTrouble(1), false) => {}
            _ => panic!("Expected restore Trouble event"),
        }
    }

    #[test]
    fn test_t4_acu_jam_level_initial_zero() {
        let (client, mut rx) = setup_client();
        let data = vec![0; 60];
        client.update_troubles_internal(SatelCommand::TroublesPart3, &data).unwrap();
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_t4_acu_jam_level_change() {
        let (client, mut rx) = setup_client();
        let mut data = vec![0; 60];
        data[0] = 5;
        client.update_troubles_internal(SatelCommand::TroublesPart3, &data).unwrap();
        match rx.try_recv().unwrap() {
            SatelEvent::AcuJamLevel { module: 1, level: 5 } => {}
            _ => panic!("Expected AcuJamLevel"),
        }

        data[0] = 0;
        client.update_troubles_internal(SatelCommand::TroublesPart3, &data).unwrap();
        match rx.try_recv().unwrap() {
            SatelEvent::AcuJamLevel { module: 1, level: 0 } => {}
            _ => panic!("Expected AcuJamLevel restore"),
        }
    }

    #[test]
    fn test_t4_cme_error_initial_zero() {
        let (client, mut rx) = setup_client();
        let data = vec![0; 64];
        client.update_troubles_internal(SatelCommand::TroublesPart8, &data).unwrap();
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_t4_trouble_flag_duplicate() {
        let (client, mut rx) = setup_client();
        let mut data = vec![0; 47];
        data[0] = 0x01;
        client.update_troubles_internal(SatelCommand::TroublesPart1, &data).unwrap();
        let _ = rx.try_recv().unwrap(); // first event
        assert!(rx.try_recv().is_err());

        // duplicate frame
        client.update_troubles_internal(SatelCommand::TroublesPart1, &data).unwrap();
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_t4_acu_jam_level_duplicate() {
        let (client, mut rx) = setup_client();
        let mut data = vec![0; 60];
        
        // 0 -> 0
        client.update_troubles_internal(SatelCommand::TroublesPart3, &data).unwrap();
        assert!(rx.try_recv().is_err());

        // 0 -> 5
        data[0] = 5;
        client.update_troubles_internal(SatelCommand::TroublesPart3, &data).unwrap();
        let _ = rx.try_recv().unwrap();
        
        // 5 -> 5
        client.update_troubles_internal(SatelCommand::TroublesPart3, &data).unwrap();
        assert!(rx.try_recv().is_err());

        // 5 -> 0
        data[0] = 0;
        client.update_troubles_internal(SatelCommand::TroublesPart3, &data).unwrap();
        let _ = rx.try_recv().unwrap();
    }

    #[test]
    fn test_t4_cme_error_flow() {
        let (client, mut rx) = setup_client();
        let mut data = vec![0; 64];
        
        // 0x00 0x10
        data[62] = 0x00;
        data[63] = 0x10;
        client.update_troubles_internal(SatelCommand::TroublesPart8, &data).unwrap();
        match rx.try_recv().unwrap() {
            SatelEvent::CmeError { code: 0x0010, .. } => {}
            _ => panic!("Expected CmeError with 0x0010"),
        }
        assert!(rx.try_recv().is_err());

        // duplicate
        client.update_troubles_internal(SatelCommand::TroublesPart8, &data).unwrap();
        assert!(rx.try_recv().is_err());

        // zeros -> event with code 0
        data[62] = 0x00;
        data[63] = 0x00;
        client.update_troubles_internal(SatelCommand::TroublesPart8, &data).unwrap();
        match rx.try_recv().unwrap() {
            SatelEvent::CmeError { code: 0, .. } => {}
            _ => panic!("Expected CmeError with 0"),
        }
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_t4_emit_unchanged_troubles() {
        let (client, mut rx) = setup_client();
        client.config.write().unwrap().emit_unchanged_troubles = true;
        let data = vec![0; 47];
        
        // first read
        client.update_troubles_internal(SatelCommand::TroublesPart1, &data).unwrap();
        let mut first_count = 0;
        while let Ok(_) = rx.try_recv() {
            first_count += 1;
        }

        // second read
        client.update_troubles_internal(SatelCommand::TroublesPart1, &data).unwrap();
        let mut second_count = 0;
        while let Ok(_) = rx.try_recv() {
            second_count += 1;
        }

        let decoded = crate::parsers::system::decode_troubles(0x1B, &data).unwrap();
        let expected_count = decoded.len();

        assert_eq!(first_count, expected_count);
        assert_eq!(second_count, expected_count);
    }
}
