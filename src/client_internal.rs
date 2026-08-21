/// Implementacja metod wewnętrznych aktualizacji cache.
/// Wszystkie metody są pub(crate) i przeznaczone do użycia wyłącznie przez:
///   - SatelIntegra (client.rs)
///   - SatelAutoRequester (auto_requester.rs)
use crate::client::SatelIntegra;
use crate::command::SatelCommand;
use crate::error::SatelError;
use crate::event::SatelEvent;
use crate::parsers::map_trouble_bit;
use crate::state::{
    EthmVersion, IntegraVersion, OutputsStateData, PartitionsArmedData, PartitionsData,
    SystemStatus, ZonesAlarmData, ZonesAlarmMemoryData, ZonesBypassData,
    ZonesLongViolationTroubleData, ZonesNoViolationTroubleData, ZonesTamperAlarmData,
    ZonesTamperAlarmMemoryData, ZonesTamperData, ZonesViolationData,
};
use chrono::Local;

impl SatelIntegra {
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
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        let zone = state
            .zones
            .get_mut((zone_id.wrapping_sub(1) % 256) as usize)
            .ok_or(SatelError::InvalidFrame)?;

        match error {
            SatelError::Timeout | SatelError::TemperatureNotSupportedOrTimeOut => {
                zone.temperature_timeout_errors_total += 1;
                zone.temperature_timeout_errors_current += 1;
            }
            SatelError::TemperatureSensorError => {
                zone.temperature_sensor_errors_total += 1;
                zone.temperature_sensor_errors_current += 1;
            }
            _ => {}
        }
        zone.temperature_read_at = Local::now();
        Ok(())
    }

    pub(crate) fn update_zones_tamper_internal(&self, result: ZonesTamperData) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(zone) = state.zones.get_mut(i) {
                if zone.tamper_state != new_state {
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
                if zone.alarm_state != new_state {
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
                if zone.violation_state != new_state {
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
                if zone.tamper_alarm_state != new_state {
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
                if zone.alarm_memory_state != new_state {
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
                if zone.tamper_alarm_memory_state != new_state {
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
                if zone.bypass_state != new_state {
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
                if zone.no_violation_trouble_state != new_state {
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
                if zone.long_violation_trouble_state != new_state {
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
                if partition.armed_suppressed != new_state {
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
                if partition.armed_really != new_state {
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
                if partition.alarm != new_state {
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
                if partition.alarm_memory != new_state {
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
                if partition.entry_time != new_state {
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
                if partition.exit_time_gt_10s != new_state {
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
                if partition.exit_time_lt_10s != new_state {
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
                if output.state != new_state {
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
        if state.system_status != Some(status) {
            state.system_status = Some(status);
            let _ = self.event_tx.send(SatelEvent::SystemStatusChanged(status));
        }
        Ok(())
    }

    pub(crate) fn update_troubles_internal(&self, cmd: SatelCommand, states: Vec<bool>) -> Result<(), SatelError> {
        let byte_cmd = cmd.to_byte();
        let is_memory = (0x20..=0x24).contains(&byte_cmd)
            || (0x2E..=0x2F).contains(&byte_cmd)
            || byte_cmd == 0x31;

        let base_index = match byte_cmd {
            0x1B => 0,
            0x1C => 40,
            0x1D => 80,
            0x1E => 120,
            0x1F => 160,
            0x2C => 200,
            0x2D => 240,
            0x30 => 280,
            0x20 => 0,
            0x21 => 40,
            0x22 => 80,
            0x23 => 120,
            0x24 => 160,
            0x2E => 200,
            0x2F => 240,
            0x31 => 280,
            _ => return Ok(()),
        };

        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        let target = if is_memory {
            &mut state.troubles_memory
        } else {
            &mut state.troubles
        };

        for (i, &new_val) in states.iter().enumerate() {
            let global_idx = base_index + i;
            if global_idx < target.len() && target[global_idx] != new_val {
                target[global_idx] = new_val;
                let trouble_type = map_trouble_bit(global_idx as u16);
                let _ = if is_memory {
                    self.event_tx.send(SatelEvent::TroubleMemory(trouble_type, new_val))
                } else {
                    self.event_tx.send(SatelEvent::Trouble(trouble_type, new_val))
                };
            }
        }

        Ok(())
    }
}
