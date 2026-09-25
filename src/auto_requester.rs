use crate::client::SatelIntegra;
use crate::command::{SatelCommand, SatelResult};
use crate::event::SatelEvent;
use crate::parsers::{
    process_outputs_state, process_partitions_alarm, process_partitions_alarm_memory,
    process_partitions_armed_really, process_partitions_armed_suppressed,
    process_partitions_entry_time, process_partitions_exit_time_gt_10s,
    process_partitions_exit_time_lt_10s, process_rtc_and_status,
    process_zones_alarm, process_zones_alarm_memory, process_zones_bypass,
    process_zones_long_violation_trouble, process_zones_no_violation_trouble, process_zones_tamper,
    process_zones_tamper_alarm, process_zones_tamper_alarm_memory, process_zones_violation,
};
use crate::worker::StateWorkerMessage;
use tokio::sync::mpsc;

/// `SatelAutoRequester` handles incoming Push notification frames
/// and updates the shared in-memory state cache.
pub(crate) struct SatelAutoRequester {
    pub integra: SatelIntegra,
    pub rx: mpsc::Receiver<StateWorkerMessage>,
}

impl SatelAutoRequester {
    pub async fn run(&mut self) {
        tracing::info!("SatelAutoRequester started");

        loop {
            tokio::select! {
                maybe_msg = self.rx.recv() => {
                    if let Some(msg) = maybe_msg {
                        match msg {
                            StateWorkerMessage::Frame(frame) => {
                                self.handle_auto_frame(&frame);
                            }
                            StateWorkerMessage::StatusChanged(state) => {
                                tracing::info!("SatelAutoRequester: Connection state transition -> {:?}", state);
                                let _ = self.integra.event_tx.send(SatelEvent::ConnectionChanged(state));
                                let stats = if let Ok(mut s) = self.integra.state.write() {
                                    s.telemetry.last_sent_stats_mark = s.telemetry.stats_mark();
                                    Some(s.telemetry.statistics())
                                } else {
                                    None
                                };
                                if let Some(stats) = stats {
                                    let _ = self.integra.event_tx.send(SatelEvent::ConnectionStatistics(stats));
                                }
                            }
                            StateWorkerMessage::IntegraVersion(v) => {
                                let _ = self.integra.update_integra_version_internal(v);
                            }
                            StateWorkerMessage::EthmVersion(v) => {
                                let _ = self.integra.update_ethm_version_internal(v);
                            }
                            StateWorkerMessage::AutoReadReport(report) => {
                                {
                                    if let Ok(mut state) = self.integra.state.write() {
                                        state.auto_read_report = Some(report.clone());
                                    }
                                }
                                let _ = self.integra.event_tx.send(SatelEvent::AutoReadConfigured(report));
                            }
                        }
                    } else {
                        break;
                    }
                }
            }
        }
        tracing::info!("SatelAutoRequester stopped");
    }

    fn handle_auto_frame(&mut self, frame: &[u8]) {
        if frame.is_empty() {
            return;
        }

        match frame[0] {
            0x00 => {
                if let Ok(d) = process_zones_violation(frame, &self.integra.config.read().unwrap().io_violation_invert) {
                    let _ = self.integra.update_zones_violation_internal(d, false);
                }
            }
            0x01 => {
                if let Ok(d) = process_zones_tamper(frame, &self.integra.config.read().unwrap().io_tamper_invert) {
                    let _ = self.integra.update_zones_tamper_internal(d, false);
                }
            }
            0x02 => {
                if let Ok(d) = process_zones_alarm(frame, &self.integra.config.read().unwrap().io_alarm_invert) {
                    let _ = self.integra.update_zones_alarm_internal(d, false);
                }
            }
            0x03 => {
                if let Ok(d) = process_zones_tamper_alarm(frame, &self.integra.config.read().unwrap().io_tamper_alarm_invert) {
                    let _ = self.integra.update_zones_tamper_alarm_internal(d, false);
                }
            }
            0x04 => {
                if let Ok(d) = process_zones_alarm_memory(frame, &self.integra.config.read().unwrap().io_alarm_memory_invert) {
                    let _ = self.integra.update_zones_alarm_memory_internal(d, false);
                }
            }
            0x05 => {
                if let Ok(d) = process_zones_tamper_alarm_memory(frame, &self.integra.config.read().unwrap().io_tamper_alarm_memory_invert) {
                    let _ = self.integra.update_zones_tamper_alarm_memory_internal(d, false);
                }
            }
            0x06 => {
                if let Ok(d) = process_zones_bypass(frame, &self.integra.config.read().unwrap().io_bypass_invert) {
                    let _ = self.integra.update_zones_bypass_internal(d, false);
                }
            }
            0x07 => {
                if let Ok(d) = process_zones_no_violation_trouble(frame, &self.integra.config.read().unwrap().io_no_violation_trouble_invert) {
                    let _ = self.integra.update_zones_no_violation_trouble_internal(d, false);
                }
            }
            0x08 => {
                if let Ok(d) = process_zones_long_violation_trouble(frame, &self.integra.config.read().unwrap().io_long_violation_trouble_invert) {
                    let _ = self.integra.update_zones_long_violation_trouble_internal(d, false);
                }
            }
            0x09 => {
                if let Ok(d) = process_partitions_armed_suppressed(frame) {
                    let _ = self.integra.update_partitions_armed_internal(d, false);
                }
            }
            0x0A => {
                if let Ok(d) = process_partitions_armed_really(frame) {
                    let _ = self.integra.update_partitions_armed_really_internal(d, false);
                }
            }
            0x13 => {
                if let Ok(d) = process_partitions_alarm(frame) {
                    let _ = self.integra.update_partitions_alarm_internal(d, false);
                }
            }
            0x0E => {
                if let Ok(d) = process_partitions_entry_time(frame) {
                    let _ = self.integra.update_partitions_entry_time_internal(d, false);
                }
            }
            0x0F => {
                if let Ok(d) = process_partitions_exit_time_gt_10s(frame) {
                    let _ = self.integra.update_partitions_exit_time_gt_10s_internal(d, false);
                }
            }
            0x10 => {
                if let Ok(d) = process_partitions_exit_time_lt_10s(frame) {
                    let _ = self.integra.update_partitions_exit_time_lt_10s_internal(d, false);
                }
            }
            0x15 => {
                if let Ok(d) = process_partitions_alarm_memory(frame) {
                    let _ = self.integra.update_partitions_alarm_memory_internal(d, false);
                }
            }
            0x17 => {
                if let Ok(d) = process_outputs_state(frame) {
                    let _ = self.integra.update_outputs_state_internal(d, false);
                }
            }
            0x1A => {
                if let Ok(s) = process_rtc_and_status(frame) {
                    let _ = self.integra.update_system_status_internal(s);
                }
            }
            0x1B..=0x1F | 0x2C | 0x2D | 0x30 | 0x20..=0x24 | 0x2E | 0x2F | 0x31 => {
                if let Some(cmd) = SatelCommand::from_byte(frame[0]) {
                    let _ = self.integra.update_troubles_internal(cmd, &frame[1..]);
                }
            }
            0xEF => {
                let code = frame.get(1).cloned().unwrap_or(0xFF);
                if code != 0xFF {
                    let result = SatelResult::from_byte(code);
                    let _ = self.integra.event_tx.send(SatelEvent::PanelMessage(result));
                }
            }
            _ => {}
        }
    }
}

