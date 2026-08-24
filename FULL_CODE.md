# Satel Integra Rust Library - Full Source Code & Examples

> Single consolidated Markdown file containing the complete codebase, configurations, and all interactive examples for `satel_integra`.

## Table of Contents

- [Project Configuration](#project-configuration)
  - [Cargo.toml](#cargotoml)
- [Source Code (src/)](#source-code-src)
  - [src/auto_requester.rs](#srcauto-requesterrs)
  - [src/client.rs](#srcclientrs)
  - [src/client_internal.rs](#srcclient-internalrs)
  - [src/codec.rs](#srccodecrs)
  - [src/command.rs](#srccommandrs)
  - [src/config.rs](#srcconfigrs)
  - [src/encryption.rs](#srcencryptionrs)
  - [src/error.rs](#srcerrorrs)
  - [src/event.rs](#srceventrs)
  - [src/lib.rs](#srclibrs)
  - [src/parsers/mod.rs](#srcparsersmodrs)
  - [src/parsers/names.rs](#srcparsersnamesrs)
  - [src/parsers/outputs.rs](#srcparsersoutputsrs)
  - [src/parsers/partitions.rs](#srcparserspartitionsrs)
  - [src/parsers/system.rs](#srcparserssystemrs)
  - [src/parsers/zones.rs](#srcparserszonesrs)
  - [src/polling_worker.rs](#srcpolling-workerrs)
  - [src/state.rs](#srcstaters)
  - [src/worker.rs](#srcworkerrs)
- [Interactive Examples (examples/)](#interactive-examples-examples)
  - [examples/1_03_encrypted_connection.rs](#examples1-03-encrypted-connectionrs)
  - [examples/2_01_get_version.rs](#examples2-01-get-versionrs)
  - [examples/2_02_get_names.rs](#examples2-02-get-namesrs)
  - [examples/2_03_get_zones_status.rs](#examples2-03-get-zones-statusrs)
  - [examples/2_04_get_outputs_status.rs](#examples2-04-get-outputs-statusrs)
  - [examples/2_05_get_partitions_status.rs](#examples2-05-get-partitions-statusrs)
  - [examples/2_06_get_temperatures.rs](#examples2-06-get-temperaturesrs)
  - [examples/2_07_get_temperatures_smart_blocking.rs](#examples2-07-get-temperatures-smart-blockingrs)
  - [examples/2_08_get_troubles.rs](#examples2-08-get-troublesrs)
  - [examples/2_09_get_time.rs](#examples2-09-get-timers)
  - [examples/3_01_control_outputs.rs](#examples3-01-control-outputsrs)
  - [examples/3_02_arm_disarm_partitions.rs](#examples3-02-arm-disarm-partitionsrs)
  - [examples/3_03_control_time.rs](#examples3-03-control-timers)
  - [examples/4_01_monitor_system_info.rs](#examples4-01-monitor-system-infors)
  - [examples/4_02_monitor_security_states.rs](#examples4-02-monitor-security-statesrs)
  - [examples/4_03_monitor_temperatures.rs](#examples4-03-monitor-temperaturesrs)
  - [examples/4_04_monitor_troubles.rs](#examples4-04-monitor-troublesrs)
  - [examples/4_05_monitor_all_events.rs](#examples4-05-monitor-all-eventsrs)
  - [examples/5_01_auto_read_push.rs](#examples5-01-auto-read-pushrs)
  - [examples/5_02_auto_poll_temperatures.rs](#examples5-02-auto-poll-temperaturesrs)
  - [examples/5_03_auto_poll_temperatures_encrypted.rs](#examples5-03-auto-poll-temperatures-encryptedrs)
  - [examples/6_01_config_all.rs](#examples6-01-config-allrs)

---

## Project Configuration

### Cargo.toml

```toml
[package]
name = "satel_integra"
version = "1.0.1"
edition = "2021"
authors = ["Marcin Król <krol.marcin1989@gmail.com>"]
description = "Asynchronous Rust client for Satel Integra alarm control panels via ETHM-1 Plus (TCP/IP) and UART (RS-232)."
license = "MIT OR Apache-2.0"
readme = "README.md"
repository = "https://github.com/kroolik89/satel_integra"
keywords = ["satel", "integra", "alarm", "home-automation", "security"]
categories = ["hardware-support", "asynchronous", "network-programming"]
exclude = [
    ".clinerules",
    "opis dla AI.txt",
    "opis protokołu/*",
    "create/*",
]

[dependencies]
tokio = { version = "1", features = ["full"] }
tokio-serial = "5.4.4"
tokio-util = { version = "0.7.10", features = ["codec"] }
bytes = "1.6.0"
futures = "0.3.30"
thiserror = "1.0.58"
serde = { version = "1.0", features = ["derive"] }
toml = "0.8"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["chrono"] }
chrono = "0.4"
encoding_rs = "0.8"
aes = "0.8"
```

---

## Source Code (src/)

### src/auto_requester.rs

```rust
use crate::client::SatelIntegra;
use crate::command::{SatelCommand, SatelResult};
use crate::event::SatelEvent;
use crate::parsers::{
    process_outputs_state, process_partitions_alarm, process_partitions_alarm_memory,
    process_partitions_armed_really, process_partitions_armed_suppressed,
    process_partitions_entry_time, process_partitions_exit_time_gt_10s,
    process_partitions_exit_time_lt_10s, process_rtc_and_status, process_troubles,
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
                            }
                            StateWorkerMessage::IntegraVersion(v) => {
                                let _ = self.integra.update_integra_version_internal(v);
                            }
                            StateWorkerMessage::EthmVersion(v) => {
                                let _ = self.integra.update_ethm_version_internal(v);
                            }
                            StateWorkerMessage::AutoReadReport(report) => {
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
                if let Ok(d) = process_zones_violation(frame, &self.integra.config.io_violation_invert) {
                    let _ = self.integra.update_zones_violation_internal(d);
                }
            }
            0x01 => {
                if let Ok(d) = process_zones_tamper(frame, &self.integra.config.io_tamper_invert) {
                    let _ = self.integra.update_zones_tamper_internal(d);
                }
            }
            0x02 => {
                if let Ok(d) = process_zones_alarm(frame, &self.integra.config.io_alarm_invert) {
                    let _ = self.integra.update_zones_alarm_internal(d);
                }
            }
            0x03 => {
                if let Ok(d) = process_zones_tamper_alarm(frame, &self.integra.config.io_tamper_alarm_invert) {
                    let _ = self.integra.update_zones_tamper_alarm_internal(d);
                }
            }
            0x04 => {
                if let Ok(d) = process_zones_alarm_memory(frame, &self.integra.config.io_alarm_memory_invert) {
                    let _ = self.integra.update_zones_alarm_memory_internal(d);
                }
            }
            0x05 => {
                if let Ok(d) = process_zones_tamper_alarm_memory(frame, &self.integra.config.io_tamper_alarm_memory_invert) {
                    let _ = self.integra.update_zones_tamper_alarm_memory_internal(d);
                }
            }
            0x06 => {
                if let Ok(d) = process_zones_bypass(frame, &self.integra.config.io_bypass_invert) {
                    let _ = self.integra.update_zones_bypass_internal(d);
                }
            }
            0x07 => {
                if let Ok(d) = process_zones_no_violation_trouble(frame, &self.integra.config.io_no_violation_trouble_invert) {
                    let _ = self.integra.update_zones_no_violation_trouble_internal(d);
                }
            }
            0x08 => {
                if let Ok(d) = process_zones_long_violation_trouble(frame, &self.integra.config.io_long_violation_trouble_invert) {
                    let _ = self.integra.update_zones_long_violation_trouble_internal(d);
                }
            }
            0x09 => {
                if let Ok(d) = process_partitions_armed_suppressed(frame) {
                    let _ = self.integra.update_partitions_armed_internal(d);
                }
            }
            0x0A => {
                if let Ok(d) = process_partitions_armed_really(frame) {
                    let _ = self.integra.update_partitions_armed_really_internal(d);
                }
            }
            0x13 => {
                if let Ok(d) = process_partitions_alarm(frame) {
                    let _ = self.integra.update_partitions_alarm_internal(d);
                }
            }
            0x0E => {
                if let Ok(d) = process_partitions_entry_time(frame) {
                    let _ = self.integra.update_partitions_entry_time_internal(d);
                }
            }
            0x0F => {
                if let Ok(d) = process_partitions_exit_time_gt_10s(frame) {
                    let _ = self.integra.update_partitions_exit_time_gt_10s_internal(d);
                }
            }
            0x10 => {
                if let Ok(d) = process_partitions_exit_time_lt_10s(frame) {
                    let _ = self.integra.update_partitions_exit_time_lt_10s_internal(d);
                }
            }
            0x15 => {
                if let Ok(d) = process_partitions_alarm_memory(frame) {
                    let _ = self.integra.update_partitions_alarm_memory_internal(d);
                }
            }
            0x17 => {
                if let Ok(d) = process_outputs_state(frame) {
                    let _ = self.integra.update_outputs_state_internal(d);
                }
            }
            0x1A => {
                if let Ok(s) = process_rtc_and_status(frame) {
                    let _ = self.integra.update_system_status_internal(s);
                }
            }
            0x1B..=0x1F | 0x2C | 0x2D | 0x30 | 0x20..=0x24 | 0x2E | 0x2F | 0x31 => {
                if let Some(cmd) = SatelCommand::from_byte(frame[0]) {
                    if let Ok(states) = process_troubles(frame) {
                        let _ = self.integra.update_troubles_internal(cmd, states);
                    }
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
```

### src/client.rs

```rust
use crate::auto_requester::SatelAutoRequester;
use crate::command::SatelCommand;
use crate::config::Config;
use crate::error::SatelError;
use crate::event::SatelEvent;
use crate::parsers::{
    process_ethm_version, process_integra_version, process_output_name,
    process_outputs_state, process_partition_name, process_partitions_alarm,
    process_partitions_alarm_memory, process_partitions_armed_really,
    process_partitions_armed_suppressed, process_partitions_entry_time,
    process_partitions_exit_time_gt_10s, process_partitions_exit_time_lt_10s,
    process_rtc_and_status, process_troubles, process_troubles_frame, process_troubles_part1,
    process_troubles_part2, process_troubles_part3, process_troubles_part4, process_troubles_part5,
    process_troubles_part6, process_troubles_part7, process_troubles_part8, process_zone_name,
    process_zone_temperature, process_zones_alarm, process_zones_alarm_memory,
    process_zones_bypass, process_zones_long_violation_trouble, process_zones_no_violation_trouble,
    process_zones_tamper, process_zones_tamper_alarm, process_zones_tamper_alarm_memory,
    process_zones_violation,
};
use crate::polling_worker::{SatelPollingWorker, TemperaturePollingTask};
use crate::state::{
    EthmVersion, IntegraVersion, OutputName, PartitionName, SatelState, SatelStateHandle,
    SystemStatus, TemperatureSensorStatus, TroublesData, TroublesPart1Data, TroublesPart2Data,
    TroublesPart3Data, TroublesPart4Data, TroublesPart5Data, TroublesPart6Data, TroublesPart7Data,
    TroublesPart8Data, ZoneName, ZoneStatus, ZoneTemperature,
};
use crate::worker::{InternalMessage, SatelCommunicationWorker};
use chrono::Local;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, oneshot};

/// Primary client handle for communicating with the Satel Integra alarm control panel.
/// Cheaply cloneable (`Arc`-backed) — all clones share the same underlying connection.
#[derive(Clone)]
pub struct SatelIntegra {
    pub(crate) tx: mpsc::Sender<InternalMessage>,
    pub(crate) state: SatelStateHandle,
    pub(crate) config: Config,
    pub(crate) worker: Arc<Mutex<Option<SatelCommunicationWorker>>>,
    pub(crate) event_tx: broadcast::Sender<SatelEvent>,
}

impl SatelIntegra {
    /// Creates a new `SatelIntegra` instance with the given configuration.
    pub fn new(config: Config) -> Self {
        crate::init_logging();
        let state = Arc::new(std::sync::RwLock::new(SatelState::new()));
        let (tx, rx) = mpsc::channel(100);
        let (event_tx, _) = broadcast::channel(1024);

        let worker = SatelCommunicationWorker {
            config: config.clone(),
            state: state.clone(),
            rx,
            stream: None,
            state_worker_tx: None,
        };

        Self {
            tx,
            state,
            config,
            worker: Arc::new(Mutex::new(Some(worker))),
            event_tx,
        }
    }

    /// Connects to the panel and spawns background tasks (actor worker, auto-requester, poller).
    pub async fn connect(&self) -> Result<(), SatelError> {
        self.config.validate()?;

        let maybe_worker = {
            let mut worker_lock = self.worker.lock().unwrap();
            worker_lock.take()
        };

        if let Some(mut worker) = maybe_worker {
            let (state_worker_tx, state_worker_rx) = mpsc::channel(100);
            worker.state_worker_tx = Some(state_worker_tx);

            let mut state_worker = SatelAutoRequester {
                integra: self.clone(),
                rx: state_worker_rx,
            };

            let (connect_tx, connect_rx) = oneshot::channel();

            tokio::spawn(async move {
                state_worker.run().await;
            });

            if self.config.is_polling_enabled() {
                let mut poller = SatelPollingWorker::new(self.clone());

                if !self.config.polling_temperatures_zones.is_empty() {
                    let interval = Duration::from_secs(
                        self.config
                            .polling_temperatures_interval_minutes
                            .max(1)
                            * 60,
                    );
                    poller.register_task(Box::new(TemperaturePollingTask::new(
                        self.config.polling_temperatures_zones.clone(),
                        interval,
                    )));
                }

                tokio::spawn(async move {
                    poller.run().await;
                });
            }

            tokio::spawn(async move {
                worker.run(connect_tx).await;
            });

            return connect_rx.await.map_err(|_| SatelError::WorkerDropped)?;
        }

        let (tx, rx) = oneshot::channel();
        if self
            .tx
            .send(InternalMessage::Connect { response_tx: tx })
            .await
            .is_err()
        {
            return Err(SatelError::WorkerDropped);
        }

        rx.await.map_err(|_| SatelError::WorkerDropped)?
    }

    /// Disconnects from the panel and halts automatic reconnect attempts.
    pub async fn disconnect(&self) -> Result<(), SatelError> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send(InternalMessage::Disconnect { response_tx: tx })
            .await
            .map_err(|_| SatelError::WorkerDropped)?;

        rx.await.map_err(|_| SatelError::WorkerDropped)?
    }

    /// Returns a thread-safe shared handle to the in-memory cache.
    pub fn state_handle(&self) -> SatelStateHandle {
        self.state.clone()
    }

    /// Subscribes to the live event broadcast stream.
    pub fn subscribe(&self) -> broadcast::Receiver<SatelEvent> {
        self.event_tx.subscribe()
    }

    /// Subscribes to the live event broadcast stream (alias for `subscribe`).
    pub fn subscribe_events(&self) -> broadcast::Receiver<SatelEvent> {
        self.event_tx.subscribe()
    }

    /// Sends a command frame to the panel and awaits the response frame.
    pub async fn exchange(
        &self,
        data: Vec<u8>,
        write_timeout: Option<Duration>,
        read_timeout: Option<Duration>,
    ) -> Result<Vec<u8>, SatelError> {
        let (response_tx, response_rx) = oneshot::channel();

        let msg = InternalMessage::ExchangeStandard {
            data,
            write_timeout: write_timeout
                .unwrap_or(Duration::from_millis(self.config.write_timeout_ms)),
            read_timeout: read_timeout
                .unwrap_or(Duration::from_millis(self.config.read_timeout_ms)),
            created_at: Instant::now(),
            max_queue_time: Duration::from_millis(self.config.buffer_timeout_ms),
            response_tx,
        };

        self.tx
            .send(msg)
            .await
            .map_err(|_| SatelError::WorkerDropped)?;

        response_rx.await.map_err(|_| SatelError::WorkerDropped)?
    }

    /// Executes a priority data exchange (e.g. during handshake).
    pub async fn exchange_priority(
        &self,
        data: Vec<u8>,
        write_timeout: Option<Duration>,
        read_timeout: Option<Duration>,
    ) -> Result<Vec<u8>, SatelError> {
        let (response_tx, response_rx) = oneshot::channel();

        let msg = InternalMessage::ExchangePriority {
            data,
            write_timeout: write_timeout
                .unwrap_or(Duration::from_millis(self.config.write_timeout_ms)),
            read_timeout: read_timeout
                .unwrap_or(Duration::from_millis(self.config.read_timeout_ms)),
            response_tx,
        };

        self.tx
            .send(msg)
            .await
            .map_err(|_| SatelError::WorkerDropped)?;

        response_rx.await.map_err(|_| SatelError::WorkerDropped)?
    }

    /// Queries the Integra panel model and firmware version (0x7E).
    pub async fn get_integra_version(&self) -> Result<IntegraVersion, SatelError> {
        tracing::info!("Querying panel version (0x7E)...");

        let cmd = vec![SatelCommand::IntegraVersion.to_byte()];
        let response = self.exchange(cmd, None, None).await?;

        let version = process_integra_version(&response)?;
        self.update_integra_version_internal(version.clone())?;

        Ok(version)
    }

    /// Queries the ETHM / INT-RS communication module version (0x7C).
    pub async fn get_ethm_version(&self) -> Result<EthmVersion, SatelError> {
        tracing::info!("Querying ETHM/INT-RS module version (0x7C)...");

        let cmd = vec![SatelCommand::ModuleVersion.to_byte()];
        let response = self.exchange(cmd, None, None).await?;

        let version = process_ethm_version(&response)?;
        self.update_ethm_version_internal(version.clone())?;

        Ok(version)
    }

    /// Returns the cached panel version information from memory.
    pub fn get_cached_version(&self) -> Result<Option<IntegraVersion>, SatelError> {
        let state = self.state.read().map_err(|_| SatelError::StatePoisoned)?;
        Ok(state.integra_version.clone())
    }

    /// Queries the UTF-8 name of a zone (0xEE type 1).
    pub async fn get_zone_name(&self, zone_id: u16) -> Result<ZoneName, SatelError> {
        tracing::info!("Querying zone name #{}", zone_id);

        let device_type: u8 = 1;
        let device_id: u8 = if zone_id == 256 { 0 } else { zone_id as u8 };

        let cmd = vec![
            SatelCommand::ReadDeviceName.to_byte(),
            device_type,
            device_id,
        ];
        let response = self.exchange(cmd, None, None).await?;

        if !response.is_empty() && response[0] == SatelCommand::ResultCode.to_byte() {
            // Panel returned 0xEF (ResultCode) indicating the requested zone is not configured or unassigned
            let empty_name = ZoneName {
                name: String::new(),
                read_at: chrono::Local::now(),
            };
            {
                let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
                if let Some(zone) = state.zones.get_mut((zone_id.wrapping_sub(1) % 256) as usize) {
                    zone.zone_name = String::new();
                    zone.zone_name_read_at = empty_name.read_at;
                }
            }
            return Ok(empty_name);
        }

        let (id, s_name) = process_zone_name(&response)?;

        {
            let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
            if let Some(zone) = state.zones.get_mut((id.wrapping_sub(1) % 256) as usize) {
                zone.zone_name = s_name.name.clone();
                zone.zone_name_read_at = s_name.read_at;
            }
        }

        let _ = self.event_tx.send(SatelEvent::ZoneNameReceived {
            id,
            name: s_name.name.clone(),
        });

        tracing::info!("Retrieved zone #{} name: {}", id, s_name.name);
        Ok(s_name)
    }

    /// Returns the cached zone name from memory.
    pub fn get_cached_zone_name(&self, zone_id: u16) -> Result<Option<ZoneName>, SatelError> {
        let state = self.state.read().map_err(|_| SatelError::StatePoisoned)?;
        Ok(state
            .zones
            .get((zone_id.wrapping_sub(1) % 256) as usize)
            .map(|z| z.to_zone_name()))
    }

    /// Queries the UTF-8 name of an output (0xEE type 4).
    pub async fn get_output_name(&self, output_id: u16) -> Result<OutputName, SatelError> {
        tracing::info!("Querying output name #{}", output_id);

        let device_type: u8 = 4;
        let device_id: u8 = if output_id == 256 { 0 } else { output_id as u8 };

        let cmd = vec![
            SatelCommand::ReadDeviceName.to_byte(),
            device_type,
            device_id,
        ];
        let response = self.exchange(cmd, None, None).await?;

        if !response.is_empty() && response[0] == SatelCommand::ResultCode.to_byte() {
            let empty_name = OutputName {
                name: String::new(),
                read_at: chrono::Local::now(),
            };
            {
                let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
                if let Some(output) = state.outputs.get_mut((output_id.wrapping_sub(1) % 256) as usize) {
                    output.name = String::new();
                    output.name_read_at = empty_name.read_at;
                }
            }
            return Ok(empty_name);
        }

        let (id, s_name) = process_output_name(&response)?;

        {
            let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
            if let Some(output) = state.outputs.get_mut((id.wrapping_sub(1) % 256) as usize) {
                output.name = s_name.name.clone();
                output.name_read_at = s_name.read_at;
            }
        }

        let _ = self.event_tx.send(SatelEvent::OutputNameReceived {
            id,
            name: s_name.name.clone(),
        });

        tracing::info!("Retrieved output #{} name: {}", id, s_name.name);
        Ok(s_name)
    }

    /// Returns the cached output name from memory.
    pub fn get_cached_output_name(&self, output_id: u16) -> Result<Option<OutputName>, SatelError> {
        let state = self.state.read().map_err(|_| SatelError::StatePoisoned)?;
        Ok(state
            .outputs
            .get((output_id.wrapping_sub(1) % 256) as usize)
            .map(|o| o.to_output_name()))
    }

    /// Queries the UTF-8 name of a partition (0xEE type 0).
    pub async fn get_partition_name(&self, partition_id: u16) -> Result<PartitionName, SatelError> {
        tracing::info!("Querying partition name #{}", partition_id);

        let device_type: u8 = 0;
        let device_id: u8 = partition_id as u8;

        let cmd = vec![
            SatelCommand::ReadDeviceName.to_byte(),
            device_type,
            device_id,
        ];
        let response = self.exchange(cmd, None, None).await?;

        if !response.is_empty() && response[0] == SatelCommand::ResultCode.to_byte() {
            let empty_name = PartitionName {
                name: String::new(),
                read_at: chrono::Local::now(),
            };
            {
                let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
                if let Some(partition) = state.partitions.get_mut((partition_id.wrapping_sub(1) % 32) as usize) {
                    partition.name = String::new();
                    partition.name_read_at = empty_name.read_at;
                }
            }
            return Ok(empty_name);
        }

        let (id, partition_name) = process_partition_name(&response)?;

        {
            let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
            if let Some(partition) = state.partitions.get_mut((id.wrapping_sub(1) % 32) as usize) {
                partition.name = partition_name.name.clone();
                partition.name_read_at = partition_name.read_at;
            }
        }

        let _ = self.event_tx.send(SatelEvent::PartitionNameReceived {
            id,
            name: partition_name.name.clone(),
        });

        tracing::info!("Retrieved partition #{} name: {}", id, partition_name.name);
        Ok(partition_name)
    }

    /// Returns the cached partition name from memory.
    pub fn get_cached_partition_name(
        &self,
        partition_id: u16,
    ) -> Result<Option<PartitionName>, SatelError> {
        let state = self.state.read().map_err(|_| SatelError::StatePoisoned)?;
        Ok(state
            .partitions
            .get((partition_id.wrapping_sub(1) % 32) as usize)
            .map(|p| p.to_partition_name()))
    }

    /// Queries the temperature of a zone (0x7D).
    /// If `temp_blocking_enabled` is active, faulty probes are verified and automatically blocked.
    pub async fn get_zone_temperature(&self, zone_id: u16) -> Result<ZoneTemperature, SatelError> {
        if self.config.temp_blocking_enabled {
            self.get_zone_temperature_with_blocking(zone_id).await
        } else {
            self.get_zone_temperature_raw(zone_id).await
        }
    }

    /// Queries the zone temperature directly over the network without blocking checks.
    pub async fn get_zone_temperature_raw(&self, zone_id: u16) -> Result<ZoneTemperature, SatelError> {
        tracing::info!("Querying zone #{} temperature (0x7D)", zone_id);

        let cmd = vec![
            SatelCommand::ReadZoneTemperature.to_byte(),
            if zone_id == 256 { 0 } else { zone_id as u8 },
        ];

        let response_result = self
            .exchange(
                cmd,
                None,
                Some(Duration::from_millis(self.config.temp_read_timeout_ms)),
            )
            .await;

        match response_result {
            Ok(response) => match process_zone_temperature(&response) {
                Ok((id, temp)) => {
                    let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
                    let zone = state
                        .zones
                        .get_mut((id.wrapping_sub(1) % 256) as usize)
                        .ok_or(SatelError::InvalidFrame)?;

                    let old_temp = zone.temperature_value;
                    zone.temperature_value = temp;
                    zone.temperature_read_at = Local::now();

                    if zone.temperature_status == TemperatureSensorStatus::NoRead {
                        zone.temperature_status = TemperatureSensorStatus::Ok;
                    }

                    if zone.temperature_timeout_errors_current > 0 {
                        zone.temperature_timeout_errors_current -= 1;
                    }
                    if zone.temperature_sensor_errors_current > 0 {
                        zone.temperature_sensor_errors_current -= 1;
                    }

                    if self.config.emit_unchanged_temperatures || (old_temp - temp).abs() > 0.01 {
                        let _ = self.event_tx.send(SatelEvent::ZoneTemperatureChanged {
                            id: zone.id,
                            temperature: temp,
                        });
                    }

                    tracing::info!("Retrieved zone #{} temperature: {}°C", id, temp);
                    Ok(zone.to_zone_temperature())
                }
                Err(e) => {
                    self.update_temp_error(zone_id, &e)?;
                    Err(e)
                }
            },
            Err(e) => {
                self.update_temp_error(zone_id, &e)?;
                match e {
                    SatelError::Timeout => Err(SatelError::TemperatureNotSupportedOrTimeOut),
                    _ => Err(e),
                }
            }
        }
    }

    /// Returns the cached zone temperature reading from memory.
    pub fn get_cached_zone_temperature(
        &self,
        zone_id: u16,
    ) -> Result<Option<ZoneTemperature>, SatelError> {
        let state = self.state.read().map_err(|_| SatelError::StatePoisoned)?;
        Ok(state
            .zones
            .get((zone_id.wrapping_sub(1) % 256) as usize)
            .map(|z| z.to_zone_temperature()))
    }

    /// Queries the zone temperature with automatic error blocking for faulty probes.
    pub async fn get_zone_temperature_with_blocking(
        &self,
        zone_id: u16,
    ) -> Result<ZoneTemperature, SatelError> {
        let zone_info = {
            let state = self.state.read().map_err(|_| SatelError::StatePoisoned)?;
            state
                .zones
                .get((zone_id.wrapping_sub(1) % 256) as usize)
                .cloned()
        };

        if let Some(info) = zone_info {
            let is_blocked = info.temperature_status == TemperatureSensorStatus::BlockSensorMissing
                || info.temperature_status == TemperatureSensorStatus::BlockCommunicationError
                || info.temperature_timeout_errors_current >= self.config.temp_max_timeout_errors
                || info.temperature_sensor_errors_current >= self.config.temp_max_sensor_errors;

            if is_blocked {
                let status = if info.temperature_status == TemperatureSensorStatus::BlockSensorMissing
                    || info.temperature_timeout_errors_current >= self.config.temp_max_timeout_errors
                {
                    TemperatureSensorStatus::BlockSensorMissing
                } else {
                    TemperatureSensorStatus::BlockCommunicationError
                };

                {
                    let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
                    if let Some(zone) = state.zones.get_mut((zone_id.wrapping_sub(1) % 256) as usize) {
                        zone.temperature_status = status;
                    }
                }

                if self.config.emit_unchanged_temperatures {
                    let _ = self.event_tx.send(SatelEvent::ZoneTemperatureError {
                        id: zone_id,
                        status,
                    });
                }

                return Err(SatelError::TempTooManyErrors);
            }
        }

        self.get_zone_temperature_raw(zone_id).await
    }

    /// Returns the aggregated cached diagnostic status of a zone.
    pub fn get_cached_zone_status(&self, zone_id: u16) -> Result<Option<ZoneStatus>, SatelError> {
        let state = self.state.read().map_err(|_| SatelError::StatePoisoned)?;

        Ok(state
            .zones
            .get((zone_id.wrapping_sub(1) % 256) as usize)
            .map(|z| ZoneStatus {
                id: z.id,
                name: z.zone_name.clone(),
                temperature: z.temperature_value,
                violation_state: z.violation_state,
                violation_at: z.violation_read_at,
                tamper_state: z.tamper_state,
                tamper_at: z.tamper_read_at,
                alarm_state: z.alarm_state,
                alarm_at: z.alarm_read_at,
                tamper_alarm_state: z.tamper_alarm_state,
                tamper_alarm_at: z.tamper_alarm_read_at,
                alarm_memory_state: z.alarm_memory_state,
                alarm_memory_at: z.alarm_memory_read_at,
                tamper_alarm_memory_state: z.tamper_alarm_memory_state,
                tamper_alarm_memory_at: z.tamper_alarm_memory_read_at,
                bypass_state: z.bypass_state,
                bypass_at: z.bypass_read_at,
                no_violation_trouble_state: z.no_violation_trouble_state,
                no_violation_trouble_at: z.no_violation_trouble_read_at,
                long_violation_trouble_state: z.long_violation_trouble_state,
                long_violation_trouble_at: z.long_violation_trouble_read_at,
            }))
    }

    /// Queries the tamper states of all zones (0x01) and updates the cache.
    pub async fn get_zones_tamper(&self) -> Result<(), SatelError> {
        tracing::info!("Querying all zones tamper state (0x01)...");
        let cmd = vec![SatelCommand::ZonesTamper.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_tamper(&response, &self.config.io_tamper_invert)?;
        self.update_zones_tamper_internal(result)?;
        tracing::info!("Updated zones tamper states in cache");
        Ok(())
    }

    /// Queries the alarm states of all zones (0x02) and updates the cache.
    pub async fn get_zones_alarm(&self) -> Result<(), SatelError> {
        tracing::info!("Querying all zones alarm state (0x02)...");
        let cmd = vec![SatelCommand::ZonesAlarm.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_alarm(&response, &self.config.io_alarm_invert)?;
        self.update_zones_alarm_internal(result)?;
        tracing::info!("Updated zones alarm states in cache");
        Ok(())
    }

    /// Queries the violation states of all zones (0x00) and updates the cache.
    pub async fn get_zones_violation(&self) -> Result<(), SatelError> {
        tracing::info!("Querying all zones violation state (0x00)...");
        let cmd = vec![SatelCommand::ZonesViolation.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_violation(&response, &self.config.io_violation_invert)?;
        self.update_zones_violation_internal(result)?;
        tracing::info!("Updated zones violation states in cache");
        Ok(())
    }

    /// Queries the tamper alarm states of all zones (0x03) and updates the cache.
    pub async fn get_zones_tamper_alarm(&self) -> Result<(), SatelError> {
        tracing::info!("Querying all zones tamper alarm state (0x03)...");
        let cmd = vec![SatelCommand::ZonesTamperAlarm.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_tamper_alarm(&response, &self.config.io_tamper_alarm_invert)?;
        self.update_zones_tamper_alarm_internal(result)?;
        tracing::info!("Updated zones tamper alarm states in cache");
        Ok(())
    }

    /// Queries the alarm memory states of all zones (0x04) and updates the cache.
    pub async fn get_zones_alarm_memory(&self) -> Result<(), SatelError> {
        tracing::info!("Querying all zones alarm memory state (0x04)...");
        let cmd = vec![SatelCommand::ZonesAlarmMemory.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_alarm_memory(&response, &self.config.io_alarm_memory_invert)?;
        self.update_zones_alarm_memory_internal(result)?;
        tracing::info!("Updated zones alarm memory states in cache");
        Ok(())
    }

    /// Queries the tamper alarm memory states of all zones (0x05) and updates the cache.
    pub async fn get_zones_tamper_alarm_memory(&self) -> Result<(), SatelError> {
        tracing::info!("Querying all zones tamper alarm memory state (0x05)...");
        let cmd = vec![SatelCommand::ZonesTamperAlarmMemory.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_tamper_alarm_memory(&response, &self.config.io_tamper_alarm_memory_invert)?;
        self.update_zones_tamper_alarm_memory_internal(result)?;
        tracing::info!("Updated zones tamper alarm memory states in cache");
        Ok(())
    }

    /// Queries the bypass states of all zones (0x06) and updates the cache.
    pub async fn get_zones_bypass(&self) -> Result<(), SatelError> {
        tracing::info!("Querying all zones bypass state (0x06)...");
        let cmd = vec![SatelCommand::ZonesBypass.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_bypass(&response, &self.config.io_bypass_invert)?;
        self.update_zones_bypass_internal(result)?;
        tracing::info!("Updated zones bypass states in cache");
        Ok(())
    }

    /// Queries the 'no violation trouble' states of all zones (0x07) and updates the cache.
    pub async fn get_zones_no_violation_trouble(&self) -> Result<(), SatelError> {
        tracing::info!("Querying all zones 'no violation trouble' state (0x07)...");
        let cmd = vec![SatelCommand::ZonesNoViolationTrouble.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_no_violation_trouble(&response, &self.config.io_no_violation_trouble_invert)?;
        self.update_zones_no_violation_trouble_internal(result)?;
        tracing::info!("Updated zones 'no violation trouble' states in cache");
        Ok(())
    }

    /// Queries the 'long violation trouble' states of all zones (0x08) and updates the cache.
    pub async fn get_zones_long_violation_trouble(&self) -> Result<(), SatelError> {
        tracing::info!("Querying all zones 'long violation trouble' state (0x08)...");
        let cmd = vec![SatelCommand::ZonesLongViolationTrouble.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_long_violation_trouble(&response, &self.config.io_long_violation_trouble_invert)?;
        self.update_zones_long_violation_trouble_internal(result)?;
        tracing::info!("Updated zones 'long violation trouble' states in cache");
        Ok(())
    }

    /// Queries the suppressed armed partition states (0x09) and updates the cache.
    pub async fn get_partitions_armed_suppressed(&self) -> Result<(), SatelError> {
        tracing::info!("Querying suppressed partition arm states (0x09)...");
        let cmd = vec![SatelCommand::ArmedPartitionsSuppressed.to_byte()];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_partitions_armed_suppressed(&response)?;
        self.update_partitions_armed_internal(result)?;
        tracing::info!("Updated suppressed partition arm states in cache");
        Ok(())
    }

    /// Queries the real armed partition states (0x0A) and updates the cache.
    pub async fn get_partitions_armed_really(&self) -> Result<(), SatelError> {
        tracing::info!("Querying real partition arm states (0x0A)...");
        let cmd = vec![SatelCommand::ArmedPartitionsReally.to_byte()];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_partitions_armed_really(&response)?;
        self.update_partitions_armed_really_internal(result)?;
        tracing::info!("Updated real partition arm states in cache");
        Ok(())
    }

    /// Queries the partition alarm states (0x13) and updates the cache.
    pub async fn get_partitions_alarm(&self) -> Result<(), SatelError> {
        tracing::info!("Querying partition alarm states (0x13)...");
        let cmd = vec![SatelCommand::PartitionsAlarm.to_byte()];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_partitions_alarm(&response)?;
        self.update_partitions_alarm_internal(result)?;
        tracing::info!("Updated partition alarm states in cache");
        Ok(())
    }

    /// Queries the partition alarm memory states (0x15) and updates the cache.
    pub async fn get_partitions_alarm_memory(&self) -> Result<(), SatelError> {
        tracing::info!("Querying partition alarm memory states (0x15)...");
        let cmd = vec![SatelCommand::PartitionsAlarmMemory.to_byte()];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_partitions_alarm_memory(&response)?;
        self.update_partitions_alarm_memory_internal(result)?;
        tracing::info!("Updated partition alarm memory states in cache");
        Ok(())
    }

    /// Queries partition entry/exit countdown timers (0x0E, 0x0F, 0x10) and updates the cache.
    pub async fn get_partitions_times(&self) -> Result<(), SatelError> {
        tracing::info!("Querying partition entry/exit timer countdowns...");

        let cmd = vec![SatelCommand::PartitionsEntryTime.to_byte()];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_partitions_entry_time(&response)?;
        self.update_partitions_entry_time_internal(result)?;

        let cmd = vec![SatelCommand::PartitionsExitTimeMore10s.to_byte()];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_partitions_exit_time_gt_10s(&response)?;
        self.update_partitions_exit_time_gt_10s_internal(result)?;

        let cmd = vec![SatelCommand::PartitionsExitTimeLess10s.to_byte()];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_partitions_exit_time_lt_10s(&response)?;
        self.update_partitions_exit_time_lt_10s_internal(result)?;

        tracing::info!("Updated partition countdown timers in cache");
        Ok(())
    }

    /// Queries all output states (0x17) and updates the cache.
    pub async fn get_outputs_state(&self) -> Result<(), SatelError> {
        tracing::info!("Querying all output states (0x17)...");
        let cmd = vec![SatelCommand::OutputsState.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_outputs_state(&response)?;
        self.update_outputs_state_internal(result)?;
        tracing::info!("Updated output states in cache");
        Ok(())
    }

    /// Queries general system status bits and RTC time (0x1A) and updates the cache.
    pub async fn get_system_status(&self) -> Result<SystemStatus, SatelError> {
        tracing::info!("Querying system status & RTC time (0x1A)...");
        let cmd = vec![SatelCommand::RtcAndBasicStatusBits.to_byte()];
        let response = self.exchange(cmd, None, None).await?;
        let status = process_rtc_and_status(&response)?;
        self.update_system_status_internal(status)?;
        tracing::info!("Updated system status in cache");
        Ok(status)
    }

    /// Queries the current RTC clock time from the panel.
    pub async fn get_satel_time(&self) -> Result<chrono::DateTime<chrono::Local>, SatelError> {
        let status = self.get_system_status().await?;
        Ok(status.rtc)
    }

    /// Queries a specific trouble part (0x1B..0x1F, 0x2C..0x2D, 0x30 or memory) and returns strongly-typed data.
    pub async fn get_troubles(&self, cmd: SatelCommand) -> Result<TroublesData, SatelError> {
        tracing::info!("Querying system hardware troubles: {:02X?}", cmd);
        let response = self.exchange(vec![cmd.to_byte()], None, None).await?;
        let parsed = process_troubles_frame(&response)?;
        let states = process_troubles(&response)?;
        self.update_troubles_internal(cmd, states)?;
        Ok(parsed)
    }

    /// Queries Troubles Part 1 (0x1B: technical zones, panel & expander power, buses, ETHM).
    pub async fn get_troubles_part1(&self) -> Result<TroublesPart1Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesPart1.to_byte()], None, None).await?;
        let parsed = process_troubles_part1(&response)?;
        let states = process_troubles(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesPart1, states)?;
        Ok(parsed)
    }

    /// Queries Troubles Memory Part 1 (0x20).
    pub async fn get_troubles_memory_part1(&self) -> Result<TroublesPart1Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesMemoryPart1.to_byte()], None, None).await?;
        let parsed = process_troubles_part1(&response)?;
        let states = process_troubles(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesMemoryPart1, states)?;
        Ok(parsed)
    }

    /// Queries Troubles Part 2 (0x1C: card readers, ACU synchro, power supplies, KNX).
    pub async fn get_troubles_part2(&self) -> Result<TroublesPart2Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesPart2.to_byte()], None, None).await?;
        let parsed = process_troubles_part2(&response)?;
        let states = process_troubles(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesPart2, states)?;
        Ok(parsed)
    }

    /// Queries Troubles Memory Part 2 (0x21).
    pub async fn get_troubles_memory_part2(&self) -> Result<TroublesPart2Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesMemoryPart2.to_byte()], None, None).await?;
        let parsed = process_troubles_part2(&response)?;
        let states = process_troubles(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesMemoryPart2, states)?;
        Ok(parsed)
    }

    /// Queries Troubles Part 3 (0x1D: ACU jamming, ABAX wireless sensor batteries & comm 1..120).
    pub async fn get_troubles_part3(&self) -> Result<TroublesPart3Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesPart3.to_byte()], None, None).await?;
        let parsed = process_troubles_part3(&response)?;
        let states = process_troubles(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesPart3, states)?;
        Ok(parsed)
    }

    /// Queries Troubles Memory Part 3 (0x22).
    pub async fn get_troubles_memory_part3(&self) -> Result<TroublesPart3Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesMemoryPart3.to_byte()], None, None).await?;
        let parsed = process_troubles_part3(&response)?;
        let states = process_troubles(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesMemoryPart3, states)?;
        Ok(parsed)
    }

    /// Queries Troubles Part 4 (0x1E: communication & tamper of expanders 1..64 and keypads 1..8).
    pub async fn get_troubles_part4(&self) -> Result<TroublesPart4Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesPart4.to_byte()], None, None).await?;
        let parsed = process_troubles_part4(&response)?;
        let states = process_troubles(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesPart4, states)?;
        Ok(parsed)
    }

    /// Queries Troubles Memory Part 4 (0x23).
    pub async fn get_troubles_memory_part4(&self) -> Result<TroublesPart4Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesMemoryPart4.to_byte()], None, None).await?;
        let parsed = process_troubles_part4(&response)?;
        let states = process_troubles(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesMemoryPart4, states)?;
        Ok(parsed)
    }

    /// Queries Troubles Part 5 (0x1F: user 1..240 and master key fobs low battery).
    pub async fn get_troubles_part5(&self) -> Result<TroublesPart5Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesPart5.to_byte()], None, None).await?;
        let parsed = process_troubles_part5(&response)?;
        let states = process_troubles(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesPart5, states)?;
        Ok(parsed)
    }

    /// Queries Troubles Memory Part 5 (0x24).
    pub async fn get_troubles_memory_part5(&self) -> Result<TroublesPart5Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesMemoryPart5.to_byte()], None, None).await?;
        let parsed = process_troubles_part5(&response)?;
        let states = process_troubles(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesMemoryPart5, states)?;
        Ok(parsed)
    }

    /// Queries Troubles Part 6 (0x2C: wireless devices 121..240 - Integra 256).
    pub async fn get_troubles_part6(&self) -> Result<TroublesPart6Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesPart6.to_byte()], None, None).await?;
        let parsed = process_troubles_part6(&response)?;
        let states = process_troubles(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesPart6, states)?;
        Ok(parsed)
    }

    /// Queries Troubles Memory Part 6 (0x2E).
    pub async fn get_troubles_memory_part6(&self) -> Result<TroublesPart6Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesMemoryPart6.to_byte()], None, None).await?;
        let parsed = process_troubles_part6(&response)?;
        let states = process_troubles(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesMemoryPart6, states)?;
        Ok(parsed)
    }

    /// Queries Troubles Part 7 (0x2D: technical zones 129..256 - Integra 256).
    pub async fn get_troubles_part7(&self) -> Result<TroublesPart7Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesPart7.to_byte()], None, None).await?;
        let parsed = process_troubles_part7(&response)?;
        let states = process_troubles(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesPart7, states)?;
        Ok(parsed)
    }

    /// Queries Troubles Memory Part 7 (0x2F).
    pub async fn get_troubles_memory_part7(&self) -> Result<TroublesPart7Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesMemoryPart7.to_byte()], None, None).await?;
        let parsed = process_troubles_part7(&response)?;
        let states = process_troubles(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesMemoryPart7, states)?;
        Ok(parsed)
    }

    /// Queries Troubles Part 8 (0x30: INT-GSM modules addresses 0..7).
    pub async fn get_troubles_part8(&self) -> Result<TroublesPart8Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesPart8.to_byte()], None, None).await?;
        let parsed = process_troubles_part8(&response)?;
        let states = process_troubles(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesPart8, states)?;
        Ok(parsed)
    }

    /// Queries Troubles Memory Part 8 (0x31).
    pub async fn get_troubles_memory_part8(&self) -> Result<TroublesPart8Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesMemoryPart8.to_byte()], None, None).await?;
        let parsed = process_troubles_part8(&response)?;
        let states = process_troubles(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesMemoryPart8, states)?;
        Ok(parsed)
    }

    // --- Control methods ---

    /// Arms the specified partition with the chosen mode and optional forced arming.
    pub async fn arm(
        &self,
        partition_id: u16,
        mode: u8,
        force: bool,
        code: Option<&str>,
    ) -> Result<(), SatelError> {
        if !(1..=32).contains(&partition_id) {
            return Err(SatelError::NoAccess);
        }

        let resolved_code = self.resolve_code(code)?;
        tracing::info!("Arming partition #{} with mode {} (force: {})...", partition_id, mode, force);

        let cmd_byte = if force {
            match mode {
                1 => SatelCommand::ForceArmMode1.to_byte(),
                2 => SatelCommand::ForceArmMode2.to_byte(),
                3 => SatelCommand::ForceArmMode3.to_byte(),
                _ => SatelCommand::ForceArmMode0.to_byte(),
            }
        } else {
            match mode {
                1 => SatelCommand::ArmMode1.to_byte(),
                2 => SatelCommand::ArmMode2.to_byte(),
                3 => SatelCommand::ArmMode3.to_byte(),
                _ => SatelCommand::ArmMode0.to_byte(),
            }
        };

        let mut data = vec![cmd_byte];
        data.extend_from_slice(&Self::format_user_code(&resolved_code));

        let mut mask = [0u8; 4];
        let idx = (partition_id - 1) as usize;
        mask[idx / 8] |= 1 << (idx % 8);
        data.extend_from_slice(&mask);

        let response = self.exchange(data, None, None).await?;
        Self::handle_result_code(&response)
    }

    /// Arms the partition in Full Arm mode (Mode 0).
    pub async fn arm_full(&self, partition_id: u16, code: Option<&str>) -> Result<(), SatelError> {
        self.arm(partition_id, 0, false, code).await
    }

    /// Arms the partition in STAY mode (Mode 1).
    pub async fn arm_stay(&self, partition_id: u16, code: Option<&str>) -> Result<(), SatelError> {
        self.arm(partition_id, 1, false, code).await
    }

    /// Arms the partition in STAY mode with 0s delay (Mode 2).
    pub async fn arm_stay_delay0(&self, partition_id: u16, code: Option<&str>) -> Result<(), SatelError> {
        self.arm(partition_id, 2, false, code).await
    }

    /// Arms the partition in STAY mode without exit delay (Mode 3).
    pub async fn arm_stay_no_exit(&self, partition_id: u16, code: Option<&str>) -> Result<(), SatelError> {
        self.arm(partition_id, 3, false, code).await
    }

    /// Force-arms the partition in Full Arm mode (Mode 0).
    pub async fn force_arm_full(&self, partition_id: u16, code: Option<&str>) -> Result<(), SatelError> {
        self.arm(partition_id, 0, true, code).await
    }

    /// Force-arms the partition in STAY mode (Mode 1).
    pub async fn force_arm_stay(&self, partition_id: u16, code: Option<&str>) -> Result<(), SatelError> {
        self.arm(partition_id, 1, true, code).await
    }

    /// Disarms the specified partition.
    pub async fn disarm(&self, partition_id: u16, code: Option<&str>) -> Result<(), SatelError> {
        if !(1..=32).contains(&partition_id) {
            return Err(SatelError::NoAccess);
        }

        let resolved_code = self.resolve_code(code)?;
        tracing::info!("Disarming partition #{}...", partition_id);

        let mut data = vec![SatelCommand::Disarm.to_byte()];
        data.extend_from_slice(&Self::format_user_code(&resolved_code));

        let mut mask = [0u8; 4];
        let idx = (partition_id - 1) as usize;
        mask[idx / 8] |= 1 << (idx % 8);
        data.extend_from_slice(&mask);

        let response = self.exchange(data, None, None).await?;
        Self::handle_result_code(&response)
    }

    /// Clears alarms in the specified partition.
    pub async fn clear_alarm(&self, partition_id: u16, code: Option<&str>) -> Result<(), SatelError> {
        if !(1..=32).contains(&partition_id) {
            return Err(SatelError::NoAccess);
        }

        let resolved_code = self.resolve_code(code)?;
        tracing::info!("Clearing alarms in partition #{}...", partition_id);

        let mut data = vec![SatelCommand::ClearAlarm.to_byte()];
        data.extend_from_slice(&Self::format_user_code(&resolved_code));

        let mut mask = [0u8; 4];
        let idx = (partition_id - 1) as usize;
        mask[idx / 8] |= 1 << (idx % 8);
        data.extend_from_slice(&mask);

        let response = self.exchange(data, None, None).await?;
        Self::handle_result_code(&response)
    }

    /// Sets the real-time clock (RTC) in the panel.
    pub async fn set_satel_time(
        &self,
        datetime: chrono::DateTime<chrono::Local>,
        code: Option<&str>,
    ) -> Result<(), SatelError> {
        let resolved_code = self.resolve_code(code)?;
        tracing::info!("Setting panel RTC clock to {}...", datetime);

        let mut data = vec![SatelCommand::SetRtcClock.to_byte()];
        data.extend_from_slice(&Self::format_user_code(&resolved_code));

        let time_str = datetime.format("%Y%m%d%H%M%S").to_string();
        data.extend_from_slice(time_str.as_bytes());

        let response = self.exchange(data, None, None).await?;
        Self::handle_result_code(&response)
    }

    /// Switches the specified output ON.
    pub async fn set_output_on(&self, output_id: u16, code: Option<&str>) -> Result<(), SatelError> {
        self.set_output(output_id, true, code).await
    }

    /// Switches the specified output OFF.
    pub async fn set_output_off(&self, output_id: u16, code: Option<&str>) -> Result<(), SatelError> {
        self.set_output(output_id, false, code).await
    }

    /// Toggles the state of the specified output.
    pub async fn set_output_toggle(
        &self,
        output_id: u16,
        code: Option<&str>,
    ) -> Result<(), SatelError> {
        if !(1..=256).contains(&output_id) {
            return Err(SatelError::NoAccess);
        }

        let resolved_code = self.resolve_code(code)?;
        tracing::info!("Toggling output #{} state...", output_id);

        let mut data = vec![SatelCommand::OutputsSwitch.to_byte()];
        data.extend_from_slice(&Self::format_user_code(&resolved_code));

        let mut mask = [0u8; 32];
        let idx = (output_id - 1) as usize;
        mask[idx / 8] |= 1 << (idx % 8);
        data.extend_from_slice(&mask);

        let response = self.exchange(data, None, None).await?;
        Self::handle_result_code(&response)
    }

    // --- Private helpers ---

    /// Switches an output ON or OFF.
    async fn set_output(&self, output_id: u16, state: bool, code: Option<&str>) -> Result<(), SatelError> {
        if !(1..=256).contains(&output_id) {
            return Err(SatelError::NoAccess);
        }

        let resolved_code = self.resolve_code(code)?;
        tracing::info!("Setting output #{} state to {}...", output_id, state);

        let cmd_byte = if state {
            SatelCommand::OutputsOn.to_byte()
        } else {
            SatelCommand::OutputsOff.to_byte()
        };

        let mut data = vec![cmd_byte];
        data.extend_from_slice(&Self::format_user_code(&resolved_code));

        let mut mask = [0u8; 32];
        let idx = (output_id - 1) as usize;
        mask[idx / 8] |= 1 << (idx % 8);
        data.extend_from_slice(&mask);

        let response = self.exchange(data, None, None).await?;
        Self::handle_result_code(&response)
    }

    /// Formats a user access code to 8 bytes (BCD packed with 0xFF padding).
    fn format_user_code(code: &str) -> [u8; 8] {
        let mut bcd_code = [0xFFu8; 8];
        let mut current_byte = 0;
        let mut half_byte = false;

        for c in code.chars() {
            if let Some(digit) = c.to_digit(10) {
                if !half_byte {
                    bcd_code[current_byte] = (digit as u8) << 4 | 0x0F;
                    half_byte = true;
                } else {
                    bcd_code[current_byte] = (bcd_code[current_byte] & 0xF0) | (digit as u8);
                    current_byte += 1;
                    half_byte = false;
                    if current_byte >= 8 {
                        break;
                    }
                }
            }
        }
        bcd_code
    }

    /// Evaluates the 0xEF result frame returned by the panel.
    fn handle_result_code(response: &[u8]) -> Result<(), SatelError> {
        tracing::info!("Received response frame: {:02X?}", response);

        if response.is_empty() {
            tracing::error!("Empty response frame from panel");
            return Err(SatelError::InvalidFrame);
        }

        if response[0] != SatelCommand::ResultCode.to_byte() {
            tracing::error!("Expected result frame (0xEF), received: {:02X?}", response[0]);
            return Err(SatelError::InvalidFrame);
        }

        let code = response.get(1).cloned().unwrap_or(0xFF);
        match code {
            0x00 => {
                tracing::debug!("Panel response: OK (0x00)");
                Ok(())
            }
            0x01 => {
                tracing::warn!("Panel response: Invalid user access code (0x01)");
                Err(SatelError::InvalidUserCode)
            }
            0x02 => {
                tracing::warn!("Panel response: No access rights (0x02)");
                Err(SatelError::NoAccess)
            }
            0x11 | 0x12 => {
                tracing::warn!("Panel response: Cannot arm partition (0x{:02X})", code);
                Err(SatelError::CanNotArm)
            }
            0xFF => {
                tracing::debug!("Panel response: Command accepted / operation in progress (0xFF)");
                Ok(())
            }
            _ => {
                tracing::error!("Panel response: Unknown error code (0x{:02X})", code);
                Err(SatelError::IntegraResultError(code))
            }
        }
    }

    /// Resolves the user access code (explicit or from Config).
    fn resolve_code(&self, code: Option<&str>) -> Result<String, SatelError> {
        if let Some(c) = code {
            return Ok(c.to_string());
        }
        if let Some(ref c) = self.config.user_code {
            return Ok(c.clone());
        }
        Err(SatelError::InvalidUserCode)
    }
}
```

### src/client_internal.rs

```rust
//! Internal state update methods for in-memory cache synchronization.
//! All methods are `pub(crate)` and used by `SatelIntegra` and `SatelAutoRequester`.
use crate::client::SatelIntegra;
use crate::command::SatelCommand;
use crate::error::SatelError;
use crate::event::SatelEvent;
use crate::parsers::map_trouble_part_bit;
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
                    if zone.temperature_timeout_errors_current >= self.config.temp_max_timeout_errors {
                        zone.temperature_status = crate::state::TemperatureSensorStatus::BlockSensorMissing;
                    } else {
                        zone.temperature_status = crate::state::TemperatureSensorStatus::SensorMissing;
                    }
                    // Real network attempt was a timeout
                    crate::state::TemperatureSensorStatus::SensorMissing
                }
                SatelError::TemperatureSensorError => {
                    zone.temperature_sensor_errors_total += 1;
                    zone.temperature_sensor_errors_current += 1;
                    if zone.temperature_sensor_errors_current >= self.config.temp_max_sensor_errors {
                        zone.temperature_status = crate::state::TemperatureSensorStatus::BlockCommunicationError;
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
                if changed || self.config.emit_unchanged_zones {
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
                if changed || self.config.emit_unchanged_zones {
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
                if changed || self.config.emit_unchanged_zones {
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
                if changed || self.config.emit_unchanged_zones {
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
                if changed || self.config.emit_unchanged_zones {
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
                if changed || self.config.emit_unchanged_zones {
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
                if changed || self.config.emit_unchanged_zones {
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
                if changed || self.config.emit_unchanged_zones {
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
                if changed || self.config.emit_unchanged_zones {
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
                if changed || self.config.emit_unchanged_partitions {
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
                if changed || self.config.emit_unchanged_partitions {
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
                if changed || self.config.emit_unchanged_partitions {
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
                if changed || self.config.emit_unchanged_partitions {
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
                if changed || self.config.emit_unchanged_partitions {
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
                if changed || self.config.emit_unchanged_partitions {
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
                if changed || self.config.emit_unchanged_partitions {
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
                if changed || self.config.emit_unchanged_outputs {
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
        if changed || self.config.emit_unchanged_system_status {
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

        let part_index = match byte_cmd {
            0x1B | 0x20 => 0,
            0x1C | 0x21 => 1,
            0x1D | 0x22 => 2,
            0x1E | 0x23 => 3,
            0x1F | 0x24 => 4,
            0x2C | 0x2E => 5,
            0x2D | 0x2F => 6,
            0x30 | 0x31 => 7,
            _ => return Ok(()),
        };

        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        let target = if is_memory {
            &mut state.troubles_memory[part_index]
        } else {
            &mut state.troubles[part_index]
        };

        if target.len() < states.len() {
            target.resize(states.len(), false);
        }

        for (bit_idx, &new_val) in states.iter().enumerate() {
            let changed = target[bit_idx] != new_val;
            if changed || self.config.emit_unchanged_troubles {
                target[bit_idx] = new_val;
                let trouble_type = map_trouble_part_bit(part_index as u8, bit_idx as u16);
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
```

### src/codec.rs

```rust
use bytes::{Buf, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

/// Calculates the 16-bit CRC checksum for the Satel Integra protocol.
/// Initialization: 0x147A. For each byte: rotate_left(1), XOR 0xFFFF, + crc_high + byte.
fn calculate_crc(data: &[u8]) -> u16 {
    let mut crc: u16 = 0x147A;
    for &byte in data {
        crc = crc.rotate_left(1);
        crc ^= 0xFFFF;
        crc = crc.wrapping_add(crc >> 8);
        crc = crc.wrapping_add(byte as u16);
    }
    crc
}

/// Satel Integra protocol framing codec (Function 2).
///
/// Frame format: `0xFE 0xFE [cmd] [data...] [crc_high] [crc_low] 0xFE 0x0D`
///
/// Any `0xFE` byte within the data payload is replaced with the `0xFE 0xF0` escape sequence (byte stuffing).
#[derive(Default)]
pub struct SatelCodec;

impl Decoder for SatelCodec {
    type Item = Vec<u8>;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 2 {
            return Ok(None);
        }
        if let Some(pos) = src.windows(2).position(|w| w == [0xFE, 0xFE]) {
            if pos > 0 {
                src.advance(pos);
            }
        } else {
            let to_advance = if src.last() == Some(&0xFE) {
                src.len() - 1
            } else {
                src.len()
            };
            src.advance(to_advance);
            return Ok(None);
        }

        if let Some(pos) = src.windows(2).position(|window| window == [0xFE, 0x0D]) {
            let frame_raw = src.split_to(pos + 2).to_vec();
            let mut data_with_crc = Vec::new();
            let mut i = 2;
            while i < frame_raw.len() - 2 {
                if frame_raw[i] == 0xFE && frame_raw.get(i + 1) == Some(&0xF0) {
                    data_with_crc.push(0xFE);
                    i += 2;
                } else {
                    data_with_crc.push(frame_raw[i]);
                    i += 1;
                }
            }
            if data_with_crc.len() < 2 {
                return self.decode(src);
            }
            let low = data_with_crc.pop().unwrap();
            let high = data_with_crc.pop().unwrap();
            let received_crc = u16::from_be_bytes([high, low]);
            if received_crc == calculate_crc(&data_with_crc) {
                Ok(Some(data_with_crc))
            } else {
                self.decode(src)
            }
        } else {
            Ok(None)
        }
    }
}

impl Encoder<Vec<u8>> for SatelCodec {
    type Error = io::Error;
    fn encode(&mut self, item: Vec<u8>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend_from_slice(&[0xFE, 0xFE]);
        for &byte in &item {
            if byte == 0xFE {
                dst.extend_from_slice(&[0xFE, 0xF0]);
            } else {
                dst.extend_from_slice(&[byte]);
            }
        }
        let crc = calculate_crc(&item);
        for &byte in &crc.to_be_bytes() {
            if byte == 0xFE {
                dst.extend_from_slice(&[0xFE, 0xF0]);
            } else {
                dst.extend_from_slice(&[byte]);
            }
        }
        dst.extend_from_slice(&[0xFE, 0x0D]);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc_known_value() {
        // CRC verification on single byte command 0x7E (IntegraVersion)
        let data = vec![0x7E];
        let crc = calculate_crc(&data);
        assert_ne!(crc, 0);
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        use bytes::BytesMut;
        let mut codec = SatelCodec;
        let original = vec![0x7E, 0x01, 0x02];
        let mut buf = BytesMut::new();
        codec.encode(original.clone(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap();
        assert_eq!(decoded, Some(original));
    }

    #[test]
    fn test_byte_stuffing_encode() {
        use bytes::BytesMut;
        let mut codec = SatelCodec;
        // Data containing 0xFE must be escaped to 0xFE 0xF0
        let data = vec![0xFE];
        let mut buf = BytesMut::new();
        codec.encode(data, &mut buf).unwrap();
        // Verify 0xFE 0xF0 byte-stuffing sequence appears after header 0xFE 0xFE
        let buf_vec: Vec<u8> = buf.to_vec();
        assert!(buf_vec.windows(2).any(|w| w == [0xFE, 0xF0]));
    }
}
```

### src/command.rs

```rust
/// Enum representing Satel Integra protocol integration command codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SatelCommand {
    ZonesViolation = 0x00,
    ZonesTamper = 0x01,
    ZonesAlarm = 0x02,
    ZonesTamperAlarm = 0x03,
    ZonesAlarmMemory = 0x04,
    ZonesTamperAlarmMemory = 0x05,
    ZonesBypass = 0x06,
    ZonesNoViolationTrouble = 0x07,
    ZonesLongViolationTrouble = 0x08,
    ArmedPartitionsSuppressed = 0x09,
    ArmedPartitionsReally = 0x0A,
    PartitionsArmedMode2 = 0x0B,
    PartitionsArmedMode3 = 0x0C,
    PartitionsWith1stCodeEntered = 0x0D,
    PartitionsEntryTime = 0x0E,
    PartitionsExitTimeMore10s = 0x0F,
    PartitionsExitTimeLess10s = 0x10,
    PartitionsTemporaryBlocked = 0x11,
    PartitionsBlockedForGuardRound = 0x12,
    PartitionsAlarm = 0x13,
    PartitionsFireAlarm = 0x14,
    PartitionsAlarmMemory = 0x15,
    PartitionsFireAlarmMemory = 0x16,
    OutputsState = 0x17,
    DoorsOpened = 0x18,
    DoorsOpenedLong = 0x19,
    RtcAndBasicStatusBits = 0x1A,
    TroublesPart1 = 0x1B,
    TroublesPart2 = 0x1C,
    TroublesPart3 = 0x1D,
    TroublesPart4 = 0x1E,
    TroublesPart5 = 0x1F,
    TroublesMemoryPart1 = 0x20,
    TroublesMemoryPart2 = 0x21,
    TroublesMemoryPart3 = 0x22,
    TroublesMemoryPart4 = 0x23,
    TroublesMemoryPart5 = 0x24,
    PartitionsWithViolatedZones = 0x25,
    ZonesIsolate = 0x26,
    PartitionsWithVerifiedAlarms = 0x27,
    ZonesMasked = 0x28,
    ZonesMaskedMemory = 0x29,
    PartitionsArmedInMode1 = 0x2A,
    PartitionsWithWarningAlarms = 0x2B,
    TroublesPart6 = 0x2C,
    TroublesPart7 = 0x2D,
    TroublesMemoryPart6 = 0x2E,
    TroublesMemoryPart7 = 0x2F,
    TroublesPart8 = 0x30,
    TroublesMemoryPart8 = 0x31,
    ReadOutputPower = 0x7B,
    ModuleVersion = 0x7C,
    ReadZoneTemperature = 0x7D,
    IntegraVersion = 0x7E,
    ListOfNewData = 0x7F,
    ArmMode0 = 0x80,
    ArmMode1 = 0x81,
    ArmMode2 = 0x82,
    ArmMode3 = 0x83,
    Disarm = 0x84,
    ClearAlarm = 0x85,
    ZonesBypassCmd = 0x86,
    ZonesUnbypass = 0x87,
    OutputsOn = 0x88,
    OutputsOff = 0x89,
    OpenDoor = 0x8A,
    ClearTroubleMemory = 0x8B,
    ReadEvent = 0x8C,
    Enter1stCode = 0x8D,
    SetRtcClock = 0x8E,
    GetEventText = 0x8F,
    ZonesIsolateCmd = 0x90,
    OutputsSwitch = 0x91,
    ForceArmMode0 = 0xA0,
    ForceArmMode1 = 0xA1,
    ForceArmMode2 = 0xA2,
    ForceArmMode3 = 0xA3,
    ReadSelfInfo = 0xE0,
    ReadUser = 0xE1,
    ReadUsersList = 0xE2,
    ReadUserLocks = 0xE3,
    WriteUserLocks = 0xE4,
    RemoveUser = 0xE5,
    CreateUser = 0xE6,
    ChangeUser = 0xE7,
    UserDallasCardKeyFobMgmt = 0xE8,
    ChangeUserCode = 0xE9,
    ChangeUserTelCode = 0xEA,
    ReadDeviceName = 0xEE,
    ResultCode = 0xEF,
}

impl SatelCommand {
    pub fn to_byte(self) -> u8 {
        self as u8
    }

    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0x00 => Some(Self::ZonesViolation),
            0x01 => Some(Self::ZonesTamper),
            0x02 => Some(Self::ZonesAlarm),
            0x03 => Some(Self::ZonesTamperAlarm),
            0x04 => Some(Self::ZonesAlarmMemory),
            0x05 => Some(Self::ZonesTamperAlarmMemory),
            0x06 => Some(Self::ZonesBypass),
            0x07 => Some(Self::ZonesNoViolationTrouble),
            0x08 => Some(Self::ZonesLongViolationTrouble),
            0x09 => Some(Self::ArmedPartitionsSuppressed),
            0x0A => Some(Self::ArmedPartitionsReally),
            0x0B => Some(Self::PartitionsArmedMode2),
            0x0C => Some(Self::PartitionsArmedMode3),
            0x0D => Some(Self::PartitionsWith1stCodeEntered),
            0x0E => Some(Self::PartitionsEntryTime),
            0x0F => Some(Self::PartitionsExitTimeMore10s),
            0x10 => Some(Self::PartitionsExitTimeLess10s),
            0x11 => Some(Self::PartitionsTemporaryBlocked),
            0x12 => Some(Self::PartitionsBlockedForGuardRound),
            0x13 => Some(Self::PartitionsAlarm),
            0x14 => Some(Self::PartitionsFireAlarm),
            0x15 => Some(Self::PartitionsAlarmMemory),
            0x16 => Some(Self::PartitionsFireAlarmMemory),
            0x17 => Some(Self::OutputsState),
            0x18 => Some(Self::DoorsOpened),
            0x19 => Some(Self::DoorsOpenedLong),
            0x1A => Some(Self::RtcAndBasicStatusBits),
            0x1B => Some(Self::TroublesPart1),
            0x1C => Some(Self::TroublesPart2),
            0x1D => Some(Self::TroublesPart3),
            0x1E => Some(Self::TroublesPart4),
            0x1F => Some(Self::TroublesPart5),
            0x20 => Some(Self::TroublesMemoryPart1),
            0x21 => Some(Self::TroublesMemoryPart2),
            0x22 => Some(Self::TroublesMemoryPart3),
            0x23 => Some(Self::TroublesMemoryPart4),
            0x24 => Some(Self::TroublesMemoryPart5),
            0x25 => Some(Self::PartitionsWithViolatedZones),
            0x26 => Some(Self::ZonesIsolate),
            0x27 => Some(Self::PartitionsWithVerifiedAlarms),
            0x28 => Some(Self::ZonesMasked),
            0x29 => Some(Self::ZonesMaskedMemory),
            0x2A => Some(Self::PartitionsArmedInMode1),
            0x2B => Some(Self::PartitionsWithWarningAlarms),
            0x2C => Some(Self::TroublesPart6),
            0x2D => Some(Self::TroublesPart7),
            0x2E => Some(Self::TroublesMemoryPart6),
            0x2F => Some(Self::TroublesMemoryPart7),
            0x30 => Some(Self::TroublesPart8),
            0x31 => Some(Self::TroublesMemoryPart8),
            0x7B => Some(Self::ReadOutputPower),
            0x7C => Some(Self::ModuleVersion),
            0x7D => Some(Self::ReadZoneTemperature),
            0x7E => Some(Self::IntegraVersion),
            0x7F => Some(Self::ListOfNewData),
            0x80 => Some(Self::ArmMode0),
            0x81 => Some(Self::ArmMode1),
            0x82 => Some(Self::ArmMode2),
            0x83 => Some(Self::ArmMode3),
            0x84 => Some(Self::Disarm),
            0x85 => Some(Self::ClearAlarm),
            0x86 => Some(Self::ZonesBypassCmd),
            0x87 => Some(Self::ZonesUnbypass),
            0x88 => Some(Self::OutputsOn),
            0x89 => Some(Self::OutputsOff),
            0x8A => Some(Self::OpenDoor),
            0x8B => Some(Self::ClearTroubleMemory),
            0x8C => Some(Self::ReadEvent),
            0x8D => Some(Self::Enter1stCode),
            0x8E => Some(Self::SetRtcClock),
            0x8F => Some(Self::GetEventText),
            0x90 => Some(Self::ZonesIsolateCmd),
            0x91 => Some(Self::OutputsSwitch),
            0xA0 => Some(Self::ForceArmMode0),
            0xA1 => Some(Self::ForceArmMode1),
            0xA2 => Some(Self::ForceArmMode2),
            0xA3 => Some(Self::ForceArmMode3),
            0xE0 => Some(Self::ReadSelfInfo),
            0xE1 => Some(Self::ReadUser),
            0xE2 => Some(Self::ReadUsersList),
            0xE3 => Some(Self::ReadUserLocks),
            0xE4 => Some(Self::WriteUserLocks),
            0xE5 => Some(Self::RemoveUser),
            0xE6 => Some(Self::CreateUser),
            0xE7 => Some(Self::ChangeUser),
            0xE8 => Some(Self::UserDallasCardKeyFobMgmt),
            0xE9 => Some(Self::ChangeUserCode),
            0xEA => Some(Self::ChangeUserTelCode),
            0xEE => Some(Self::ReadDeviceName),
            0xEF => Some(Self::ResultCode),
            _ => None,
        }
    }
}

/// Represents readable result status codes returned by the Integra panel (0xEF).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SatelResult {
    /// Operation completed successfully (0x00).
    Ok,
    /// Invalid user code (0x01).
    InvalidUserCode,
    /// No access rights (0x02).
    NoAccess,
    /// Cannot arm selected partition(s) (0x11, 0x12).
    CanNotArm,
    /// Command accepted for processing (0xFF).
    CommandAccepted,
    /// Other, unknown error or result code.
    Other(u8),
}

impl SatelResult {
    pub fn from_byte(b: u8) -> Self {
        match b {
            0x00 => Self::Ok,
            0x01 => Self::InvalidUserCode,
            0x02 => Self::NoAccess,
            0x11 | 0x12 => Self::CanNotArm,
            0xFF => Self::CommandAccepted,
            _ => Self::Other(b),
        }
    }

    pub fn to_description(&self) -> &str {
        match self {
            Self::Ok => "OK",
            Self::InvalidUserCode => "Invalid user access code",
            Self::NoAccess => "No access rights",
            Self::CanNotArm => "Cannot arm partition (violated zones or fault)",
            Self::CommandAccepted => "Command accepted for processing",
            Self::Other(_) => "Unknown error or status code",
        }
    }
}
```

### src/config.rs

```rust
use crate::error::SatelError;
use serde::Deserialize;

/// Main client configuration structure.
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// Connection transport configuration (TCP/IP or UART/RS-232).
    pub connection: ConnectionConfig,

    /// Whether to enable AES-192 encrypted communication with ETHM-1 Plus.
    /// Requires `integration_key`. Only supported for TCP connections.
    /// Default: false.
    #[serde(default = "default_encryption")]
    pub encryption: bool,

    /// Integration encryption key (up to 12 ASCII characters).
    /// Configured in DLOADX (Structure -> Modules -> ETHM-1 -> Integration key).
    /// Required when `encryption = true`. Stored in memory in plaintext.
    #[serde(default)]
    pub integration_key: Option<String>,

    /// Network / stream read timeout in milliseconds.
    #[serde(default = "default_read_timeout_ms")]
    pub read_timeout_ms: u64,

    /// Network / stream write timeout in milliseconds.
    #[serde(default = "default_write_timeout_ms")]
    pub write_timeout_ms: u64,

    /// Zone temperature sensor query timeout in milliseconds.
    #[serde(default = "default_temp_read_timeout_ms")]
    pub temp_read_timeout_ms: u64,

    /// Maximum message lifetime in the buffer queue before expiration (ms).
    #[serde(default = "default_buffer_timeout_ms")]
    pub buffer_timeout_ms: u64,

    /// Optional user access code required for control commands.
    pub user_code: Option<String>,

    /// Whether to automatically reconnect when the connection drops.
    #[serde(default = "default_auto_reconnect")]
    pub auto_reconnect: bool,

    /// Whether smart blocking for faulty temperature sensors is enabled.
    #[serde(default = "default_temp_blocking_enabled")]
    pub temp_blocking_enabled: bool,

    /// Maximum consecutive timeout / missing errors before blocking a temperature sensor.
    #[serde(default = "default_temp_max_timeout_errors")]
    pub temp_max_timeout_errors: u32,

    /// Maximum consecutive sensor errors (0xFFFF) before blocking a temperature sensor.
    #[serde(default = "default_temp_max_sensor_errors")]
    pub temp_max_sensor_errors: u32,

    /// List of zone IDs (1..256) whose tamper state should be logically inverted.
    #[serde(default)]
    pub io_tamper_invert: Vec<u16>,

    /// List of zone IDs (1..256) whose alarm state should be logically inverted.
    #[serde(default)]
    pub io_alarm_invert: Vec<u16>,

    /// List of zone IDs (1..256) whose violation state should be logically inverted.
    #[serde(default)]
    pub io_violation_invert: Vec<u16>,

    /// List of zone IDs (1..256) whose tamper alarm state should be logically inverted.
    #[serde(default)]
    pub io_tamper_alarm_invert: Vec<u16>,

    /// List of zone IDs (1..256) whose alarm memory state should be logically inverted.
    #[serde(default)]
    pub io_alarm_memory_invert: Vec<u16>,

    /// List of zone IDs (1..256) whose tamper alarm memory state should be logically inverted.
    #[serde(default)]
    pub io_tamper_alarm_memory_invert: Vec<u16>,

    /// List of zone IDs (1..256) whose bypass state should be logically inverted.
    #[serde(default)]
    pub io_bypass_invert: Vec<u16>,

    /// List of zone IDs (1..256) whose 'no violation trouble' state should be logically inverted.
    #[serde(default)]
    pub io_no_violation_trouble_invert: Vec<u16>,

    /// List of zone IDs (1..256) whose 'long violation trouble' state should be logically inverted.
    #[serde(default)]
    pub io_long_violation_trouble_invert: Vec<u16>,

    /// Whether to auto-read zone violations (0x00) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_violation: bool,

    /// Whether to auto-read zone tampers (0x01) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_tamper: bool,

    /// Whether to auto-read zone alarms (0x02) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_alarm: bool,

    /// Whether to auto-read zone tamper alarms (0x03) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_tamper_alarm: bool,

    /// Whether to auto-read zone alarm memory (0x04) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_alarm_memory: bool,

    /// Whether to auto-read zone tamper alarm memory (0x05) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_tamper_alarm_memory: bool,

    /// Whether to auto-read zone bypasses (0x06) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_bypass: bool,

    /// Whether to auto-read zone 'no violation trouble' (0x07) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_no_violation_trouble: bool,

    /// Whether to auto-read zone 'long violation trouble' (0x08) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_zones_long_violation_trouble: bool,

    /// Whether to auto-read partition suppressed arm state (0x09) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_partitions_armed_suppressed: bool,

    /// Whether to auto-read partition real arm state (0x0A) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_partitions_armed_really: bool,

    /// Whether to auto-read partition alarms (0x13) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_partitions_alarm: bool,

    /// Whether to auto-read partition alarm memory (0x15) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_partitions_alarm_memory: bool,

    /// Whether to auto-read partition entry countdown time (0x0E) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_partitions_entry_time: bool,

    /// Whether to auto-read partition exit countdown time (0x0F, 0x10) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_partitions_exit_time: bool,

    /// Whether to auto-read output states (0x17) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_outputs_state: bool,

    /// Whether to auto-read system hardware troubles (0x1B-0x30) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_system_troubles: bool,

    /// Whether to auto-read system troubles memory (0x20-0x31) via push notifications.
    #[serde(default = "default_auto_read")]
    pub auto_read_troubles_memory: bool,

    /// Whether automated background cyclic polling for temperature sensors is enabled.
    /// Default: false.
    #[serde(default = "default_polling_temperatures")]
    pub polling_temperatures: bool,

    /// List of zone IDs (1..256) configured as temperature probes to poll cyclically.
    /// Requires `polling_temperatures: true`.
    #[serde(default)]
    pub polling_temperatures_zones: Vec<u16>,

    /// Interval (in minutes) between consecutive temperature polling cycles.
    /// Minimum: 1 minute.
    #[serde(default = "default_polling_temperatures_interval_minutes")]
    pub polling_temperatures_interval_minutes: u64,

    /// Whether to emit temperature events (`ZoneTemperature`) on every read cycle,
    /// even if the measured value has not changed. Default: false.
    #[serde(default = "default_emit_unchanged")]
    pub emit_unchanged_temperatures: bool,

    /// Whether to emit zone events (`ZoneViolation`, `ZoneTamper`, etc.) on every read cycle,
    /// even if the zone state has not changed. Default: false.
    #[serde(default = "default_emit_unchanged")]
    pub emit_unchanged_zones: bool,

    /// Whether to emit output events (`OutputChanged`) on every read cycle,
    /// even if the output state has not changed. Default: false.
    #[serde(default = "default_emit_unchanged")]
    pub emit_unchanged_outputs: bool,

    /// Whether to emit partition events (`PartitionArmed`, `PartitionAlarm`, etc.) on every read cycle,
    /// even if the partition state has not changed. Default: false.
    #[serde(default = "default_emit_unchanged")]
    pub emit_unchanged_partitions: bool,

    /// Whether to emit trouble events (`TroubleChanged`) on every read cycle,
    /// even if the hardware trouble state has not changed. Default: false.
    #[serde(default = "default_emit_unchanged")]
    pub emit_unchanged_troubles: bool,

    /// Whether to emit system status events (`SystemStatusChanged`) on every 0x1A read,
    /// even if status bits have not changed. Default: false.
    #[serde(default = "default_emit_unchanged")]
    pub emit_unchanged_system_status: bool,
}

impl Config {
    /// Validates the configuration consistency.
    ///
    /// Checks if encryption settings are valid:
    /// - If encryption is enabled, `integration_key` must be specified.
    /// - `integration_key` must be 1-12 ASCII characters.
    /// - Encryption is only supported for TCP connections, not UART.
    pub fn validate(&self) -> Result<(), SatelError> {
        if self.encryption {
            let key = self.integration_key.as_deref().ok_or_else(|| {
                SatelError::InvalidIntegrationKey(
                    "encryption is enabled but integration_key is not set".into(),
                )
            })?;
            if key.is_empty() || key.len() > 12 {
                return Err(SatelError::InvalidIntegrationKey(format!(
                    "integration_key must be 1-12 characters long, got {}",
                    key.len()
                )));
            }
            if !key.is_ascii() {
                return Err(SatelError::InvalidIntegrationKey(
                    "integration_key must contain only ASCII characters".into(),
                ));
            }
            if matches!(self.connection, ConnectionConfig::Uart { .. }) {
                return Err(SatelError::InvalidIntegrationKey(
                    "encryption is only supported for TCP connections (ETHM-1 Plus), not UART (INT-RS)".into(),
                ));
            }
        }
        Ok(())
    }

    /// Returns true if any auto-read (0x7F push notification) category is enabled.
    pub fn is_auto_read_enabled(&self) -> bool {
        self.auto_read_zones_violation
            || self.auto_read_zones_tamper
            || self.auto_read_zones_alarm
            || self.auto_read_zones_tamper_alarm
            || self.auto_read_zones_alarm_memory
            || self.auto_read_zones_tamper_alarm_memory
            || self.auto_read_zones_bypass
            || self.auto_read_zones_no_violation_trouble
            || self.auto_read_zones_long_violation_trouble
            || self.auto_read_partitions_armed_suppressed
            || self.auto_read_partitions_armed_really
            || self.auto_read_partitions_alarm
            || self.auto_read_partitions_alarm_memory
            || self.auto_read_partitions_entry_time
            || self.auto_read_partitions_exit_time
            || self.auto_read_outputs_state
            || self.auto_read_system_troubles
            || self.auto_read_troubles_memory
    }

    /// Returns true if background temperature polling is configured and enabled.
    pub fn is_polling_enabled(&self) -> bool {
        self.polling_temperatures && !self.polling_temperatures_zones.is_empty()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            connection: ConnectionConfig::default(),
            encryption: default_encryption(),
            integration_key: None,
            read_timeout_ms: default_read_timeout_ms(),
            write_timeout_ms: default_write_timeout_ms(),
            temp_read_timeout_ms: default_temp_read_timeout_ms(),
            buffer_timeout_ms: default_buffer_timeout_ms(),
            user_code: None,
            auto_reconnect: default_auto_reconnect(),
            temp_blocking_enabled: default_temp_blocking_enabled(),
            temp_max_timeout_errors: default_temp_max_timeout_errors(),
            temp_max_sensor_errors: default_temp_max_sensor_errors(),
            io_tamper_invert: Vec::new(),
            io_alarm_invert: Vec::new(),
            io_violation_invert: Vec::new(),
            io_tamper_alarm_invert: Vec::new(),
            io_alarm_memory_invert: Vec::new(),
            io_tamper_alarm_memory_invert: Vec::new(),
            io_bypass_invert: Vec::new(),
            io_no_violation_trouble_invert: Vec::new(),
            io_long_violation_trouble_invert: Vec::new(),
            auto_read_zones_violation: default_auto_read(),
            auto_read_zones_tamper: default_auto_read(),
            auto_read_zones_alarm: default_auto_read(),
            auto_read_zones_tamper_alarm: default_auto_read(),
            auto_read_zones_alarm_memory: default_auto_read(),
            auto_read_zones_tamper_alarm_memory: default_auto_read(),
            auto_read_zones_bypass: default_auto_read(),
            auto_read_zones_no_violation_trouble: default_auto_read(),
            auto_read_zones_long_violation_trouble: default_auto_read(),
            auto_read_partitions_armed_suppressed: default_auto_read(),
            auto_read_partitions_armed_really: default_auto_read(),
            auto_read_partitions_alarm: default_auto_read(),
            auto_read_partitions_alarm_memory: default_auto_read(),
            auto_read_partitions_entry_time: default_auto_read(),
            auto_read_partitions_exit_time: default_auto_read(),
            auto_read_outputs_state: default_auto_read(),
            auto_read_system_troubles: default_auto_read(),
            auto_read_troubles_memory: default_auto_read(),
            polling_temperatures: default_polling_temperatures(),
            polling_temperatures_zones: Vec::new(),
            polling_temperatures_interval_minutes: default_polling_temperatures_interval_minutes(),
            emit_unchanged_temperatures: default_emit_unchanged(),
            emit_unchanged_zones: default_emit_unchanged(),
            emit_unchanged_outputs: default_emit_unchanged(),
            emit_unchanged_partitions: default_emit_unchanged(),
            emit_unchanged_troubles: default_emit_unchanged(),
            emit_unchanged_system_status: default_emit_unchanged(),
        }
    }
}

fn default_encryption() -> bool { false }
fn default_emit_unchanged() -> bool { false }
fn default_polling_temperatures() -> bool { false }
fn default_polling_temperatures_interval_minutes() -> u64 { 1 }
fn default_auto_read() -> bool { false }
fn default_auto_reconnect() -> bool { true }
fn default_temp_blocking_enabled() -> bool { true }
fn default_temp_max_timeout_errors() -> u32 { 4 }
fn default_temp_max_sensor_errors() -> u32 { 10 }
fn default_baud_rate() -> u32 { 19200 }
fn default_read_timeout_ms() -> u64 { 2000 }
fn default_write_timeout_ms() -> u64 { 500 }
fn default_temp_read_timeout_ms() -> u64 { 2000 }
fn default_buffer_timeout_ms() -> u64 { 10000 }

/// Transport connection parameters.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type")]
pub enum ConnectionConfig {
    #[serde(rename = "tcp")]
    Tcp { host: String, port: u16 },
    #[serde(rename = "uart")]
    Uart {
        path: String,
        #[serde(default = "default_baud_rate")]
        baud_rate: u32,
    },
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self::Tcp {
            host: "localhost".to_string(),
            port: 7094,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default_validation() {
        let config = Config::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_encryption_without_key() {
        let mut config = Config::default();
        config.encryption = true;
        config.integration_key = None;
        assert!(matches!(
            config.validate(),
            Err(SatelError::InvalidIntegrationKey(_))
        ));
    }

    #[test]
    fn test_config_encryption_empty_key() {
        let mut config = Config::default();
        config.encryption = true;
        config.integration_key = Some("".to_string());
        assert!(matches!(
            config.validate(),
            Err(SatelError::InvalidIntegrationKey(_))
        ));
    }

    #[test]
    fn test_config_encryption_key_too_long() {
        let mut config = Config::default();
        config.encryption = true;
        config.integration_key = Some("1234567890123".to_string()); // 13 chars
        assert!(matches!(
            config.validate(),
            Err(SatelError::InvalidIntegrationKey(_))
        ));
    }

    #[test]
    fn test_config_encryption_key_non_ascii() {
        let mut config = Config::default();
        config.encryption = true;
        config.integration_key = Some("KluczZażółć".to_string());
        assert!(matches!(
            config.validate(),
            Err(SatelError::InvalidIntegrationKey(_))
        ));
    }

    #[test]
    fn test_config_encryption_uart_unsupported() {
        let mut config = Config::default();
        config.connection = ConnectionConfig::Uart {
            path: "COM1".to_string(),
            baud_rate: 19200,
        };
        config.encryption = true;
        config.integration_key = Some("MyKey123".to_string());
        assert!(matches!(
            config.validate(),
            Err(SatelError::InvalidIntegrationKey(_))
        ));
    }

    #[test]
    fn test_config_encryption_valid() {
        let mut config = Config::default();
        config.encryption = true;
        config.integration_key = Some("MyKey123".to_string());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_encryption_disabled_ignores_invalid_key() {
        let mut config = Config::default();
        config.encryption = false;
        config.integration_key = Some("This key is too long but encryption is off".to_string());
        assert!(config.validate().is_ok());
    }
}
```

### src/encryption.rs

```rust
use crate::error::SatelError;
use aes::cipher::{BlockDecrypt, BlockEncrypt, KeyInit, generic_array::GenericArray};
use aes::Aes192;
use bytes::{Buf, BytesMut};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[allow(dead_code)]
pub const AES_BLOCK_SIZE: usize = 16;
pub const AES_KEY_SIZE: usize = 24; // AES-192

/// Converts a Satel integration key (up to 12 ASCII characters) into a 24-byte AES-192 key.
///
/// Algorithm:
/// 1. The key characters are padded with ASCII space (`0x20`) up to 12 bytes.
/// 2. The 12-byte block is duplicated: `key[0..12] == key[12..24]`.
pub fn derive_aes_key(integration_key: &str) -> [u8; AES_KEY_SIZE] {
    let key_bytes = integration_key.as_bytes();
    let mut key = [0x20u8; AES_KEY_SIZE];

    for i in 0..12 {
        let b = if i < key_bytes.len() {
            key_bytes[i]
        } else {
            0x20
        };
        key[i] = b;
        key[i + 12] = b;
    }

    key
}

/// Helper performing in-place Satel ETHM-1 AES-192 encryption and decryption.
#[derive(Clone)]
pub struct SatelAesCipher {
    cipher: Aes192,
}

impl SatelAesCipher {
    pub fn new(key: [u8; AES_KEY_SIZE]) -> Self {
        Self {
            cipher: Aes192::new(GenericArray::from_slice(&key)),
        }
    }

    /// Encrypts given buffer of bytes in place according to Satel ETHM-1 CBC/stream cipher specification.
    pub fn encrypt(&self, buffer: &mut [u8]) {
        let mut cv = [0u8; 16];
        self.cipher.encrypt_block(GenericArray::from_mut_slice(&mut cv));

        let mut index = 0;
        let mut count = buffer.len();

        while count > 0 {
            if count > 15 {
                count -= 16;
                let mut p = [0u8; 16];
                p.copy_from_slice(&buffer[index..index + 16]);
                for i in 0..16 {
                    p[i] ^= cv[i];
                }
                self.cipher.encrypt_block(GenericArray::from_mut_slice(&mut p));
                cv.copy_from_slice(&p);
                buffer[index..index + 16].copy_from_slice(&p);
                index += 16;
            } else {
                let mut p = [0u8; 16];
                p[..count].copy_from_slice(&buffer[index..index + count]);
                self.cipher.encrypt_block(GenericArray::from_mut_slice(&mut cv));
                for i in 0..count {
                    p[i] ^= cv[i];
                }
                buffer[index..index + count].copy_from_slice(&p[..count]);
                count = 0;
            }
        }
    }

    /// Decrypts given buffer of bytes in place according to Satel ETHM-1 CBC/stream cipher specification.
    pub fn decrypt(&self, buffer: &mut [u8]) {
        let mut cv = [0u8; 16];
        self.cipher.encrypt_block(GenericArray::from_mut_slice(&mut cv));

        let mut index = 0;
        let mut count = buffer.len();

        while count > 0 {
            if count > 15 {
                count -= 16;
                let mut temp = [0u8; 16];
                temp.copy_from_slice(&buffer[index..index + 16]);
                let mut c = [0u8; 16];
                c.copy_from_slice(&buffer[index..index + 16]);
                self.cipher.decrypt_block(GenericArray::from_mut_slice(&mut c));
                for i in 0..16 {
                    c[i] ^= cv[i];
                    cv[i] = temp[i];
                }
                buffer[index..index + 16].copy_from_slice(&c);
                index += 16;
            } else {
                let mut c = [0u8; 16];
                c[..count].copy_from_slice(&buffer[index..index + count]);
                self.cipher.encrypt_block(GenericArray::from_mut_slice(&mut cv));
                for i in 0..count {
                    c[i] ^= cv[i];
                }
                buffer[index..index + count].copy_from_slice(&c[..count]);
                count = 0;
            }
        }
    }
}

/// Helper function to encrypt a single message buffer into an encrypted wire frame.
#[allow(dead_code)]
pub fn encrypt_data(key: &[u8; AES_KEY_SIZE], plaintext: &[u8]) -> Vec<u8> {
    if plaintext.is_empty() {
        return Vec::new();
    }
    let cipher = SatelAesCipher::new(*key);
    let bytes_count = std::cmp::max(16, 6 + plaintext.len());
    let mut data = vec![0u8; bytes_count];
    data[6..6 + plaintext.len()].copy_from_slice(plaintext);
    cipher.encrypt(&mut data);

    let mut out = Vec::with_capacity(1 + bytes_count);
    out.push(bytes_count as u8);
    out.extend_from_slice(&data);
    out
}

/// Helper function to decrypt a single message buffer from an encrypted wire frame.
#[allow(dead_code)]
pub fn decrypt_data(key: &[u8; AES_KEY_SIZE], ciphertext: &[u8]) -> Result<Vec<u8>, SatelError> {
    if ciphertext.is_empty() {
        return Ok(Vec::new());
    }
    if ciphertext.len() < 17 {
        return Err(SatelError::InvalidEncryptedDataLength(ciphertext.len()));
    }
    let cipher = SatelAesCipher::new(*key);
    let len_prefix = ciphertext[0] as usize;
    if ciphertext.len() < 1 + len_prefix || len_prefix < 16 {
        return Err(SatelError::InvalidEncryptedDataLength(ciphertext.len()));
    }
    let mut payload = ciphertext[1..1 + len_prefix].to_vec();
    cipher.decrypt(&mut payload);
    Ok(payload[6..].to_vec())
}

/// Transparent asynchronous stream wrapper implementing Satel ETHM-1 encrypted session protocol.
///
/// Wire packet structure:
/// `[1 byte length prefix] [AES-192 encrypted payload]`
///
/// Encrypted payload layout before encryption:
/// - bytes 0..2: 16-bit random value
/// - bytes 2..4: 16-bit rolling counter (big-endian)
/// - byte 4: `id_s` (random session sender ID for this message)
/// - byte 5: `id_r` (last received `id_s` from panel)
/// - bytes 6..: Plaintext Satel frame (e.g. `0xFE 0xFE ... 0xFE 0x0D`)
pub struct EncryptedStream<S> {
    inner: S,
    cipher: SatelAesCipher,
    id_s: u8,
    id_r: u8,
    rolling_counter: u16,
    read_buffer: BytesMut,
    raw_read_buffer: BytesMut,
    expected_resp_len: Option<usize>,
    write_buffer: BytesMut,
}

impl<S> EncryptedStream<S> {
    /// Creates a new `EncryptedStream` wrapping the inner stream with the given AES-192 key.
    pub fn new(inner: S, key: [u8; AES_KEY_SIZE]) -> Self {
        Self {
            inner,
            cipher: SatelAesCipher::new(key),
            id_s: 0,
            id_r: 0,
            rolling_counter: 0,
            read_buffer: BytesMut::with_capacity(512),
            raw_read_buffer: BytesMut::with_capacity(512),
            expected_resp_len: None,
            write_buffer: BytesMut::with_capacity(512),
        }
    }

    /// Consumes the wrapper and returns the inner stream.
    #[allow(dead_code)]
    pub fn into_inner(self) -> S {
        self.inner
    }

    /// Returns a reference to the inner stream.
    #[allow(dead_code)]
    pub fn get_ref(&self) -> &S {
        &self.inner
    }

    /// Returns a mutable reference to the inner stream.
    #[allow(dead_code)]
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    fn pseudo_random_u16(&self) -> u16 {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos();
        (nanos & 0xFFFF) as u16
    }

    fn pseudo_random_u8(&self) -> u8 {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos();
        ((nanos >> 16) & 0xFF) as u8
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for EncryptedStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        loop {
            // 1. If we have decrypted plaintext data available, yield it to the caller
            if !this.read_buffer.is_empty() {
                let to_copy = std::cmp::min(this.read_buffer.len(), buf.remaining());
                buf.put_slice(&this.read_buffer[..to_copy]);
                this.read_buffer.advance(to_copy);
                return Poll::Ready(Ok(()));
            }

            // 2. Read raw bytes from the underlying stream
            let mut tmp_buf = [0u8; 512];
            let mut read_buf = ReadBuf::new(&mut tmp_buf);

            match Pin::new(&mut this.inner).poll_read(cx, &mut read_buf) {
                Poll::Ready(Ok(())) => {
                    let n = read_buf.filled().len();
                    if n == 0 {
                        // EOF reached
                        return Poll::Ready(Ok(()));
                    }
                    this.raw_read_buffer.extend_from_slice(read_buf.filled());

                    // 3. Process incoming packets
                    loop {
                        if this.expected_resp_len.is_none() {
                            if this.raw_read_buffer.is_empty() {
                                break;
                            }
                            let len_prefix = this.raw_read_buffer[0] as usize;
                            this.raw_read_buffer.advance(1);
                            this.expected_resp_len = Some(len_prefix);
                        }

                        if let Some(target_len) = this.expected_resp_len {
                            if this.raw_read_buffer.len() < target_len {
                                // Need more bytes for this encrypted packet
                                break;
                            }

                            let mut packet_data = this.raw_read_buffer.split_to(target_len).to_vec();
                            this.expected_resp_len = None;

                            // Decrypt payload
                            this.cipher.decrypt(&mut packet_data);

                            if packet_data.len() >= 6 {
                                this.id_r = packet_data[4];
                                // Plaintext is from offset 6 onward
                                this.read_buffer.extend_from_slice(&packet_data[6..]);
                            }
                        }
                    }

                    // If plaintext data became available, yield it
                    if !this.read_buffer.is_empty() {
                        let to_copy = std::cmp::min(this.read_buffer.len(), buf.remaining());
                        buf.put_slice(&this.read_buffer[..to_copy]);
                        this.read_buffer.advance(to_copy);
                        return Poll::Ready(Ok(()));
                    }
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for EncryptedStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();

        // 1. Flush any previously buffered output
        while !this.write_buffer.is_empty() {
            match Pin::new(&mut this.inner).poll_write(cx, &this.write_buffer) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write encrypted data to stream",
                    )));
                }
                Poll::Ready(Ok(n)) => {
                    this.write_buffer.advance(n);
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }

        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        // 2. Wrap incoming plaintext message into Satel encrypted frame
        let bytes_count = std::cmp::max(16, 6 + buf.len());
        let mut data = vec![0u8; bytes_count];

        let random_val = this.pseudo_random_u16();
        data[0] = (random_val >> 8) as u8;
        data[1] = (random_val & 0xFF) as u8;
        data[2] = (this.rolling_counter >> 8) as u8;
        data[3] = (this.rolling_counter & 0xFF) as u8;
        this.id_s = this.pseudo_random_u8();
        data[4] = this.id_s;
        data[5] = this.id_r;
        this.rolling_counter = this.rolling_counter.wrapping_add(1);

        data[6..6 + buf.len()].copy_from_slice(buf);

        // Encrypt in-place
        this.cipher.encrypt(&mut data);

        // Prepare wire output: [length prefix] + [encrypted payload]
        this.write_buffer.clear();
        this.write_buffer.reserve(1 + bytes_count);
        this.write_buffer.extend_from_slice(&[bytes_count as u8]);
        this.write_buffer.extend_from_slice(&data);

        // 3. Write to the inner stream
        while !this.write_buffer.is_empty() {
            match Pin::new(&mut this.inner).poll_write(cx, &this.write_buffer) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write encrypted data to stream",
                    )));
                }
                Poll::Ready(Ok(n)) => {
                    this.write_buffer.advance(n);
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => break,
            }
        }

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        while !this.write_buffer.is_empty() {
            match Pin::new(&mut this.inner).poll_write(cx, &this.write_buffer) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to flush encrypted buffer to stream",
                    )));
                }
                Poll::Ready(Ok(n)) => {
                    this.write_buffer.advance(n);
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }

        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        while !this.write_buffer.is_empty() {
            match Pin::new(&mut this.inner).poll_write(cx, &this.write_buffer) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to flush encrypted buffer before shutdown",
                    )));
                }
                Poll::Ready(Ok(n)) => {
                    this.write_buffer.advance(n);
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }

        Pin::new(&mut this.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn test_derive_key_exact_12_chars() {
        let key = derive_aes_key("123456789012");
        let expected_half = b"123456789012";
        assert_eq!(&key[0..12], expected_half);
        assert_eq!(&key[12..24], expected_half);
    }

    #[test]
    fn test_derive_key_short() {
        let key = derive_aes_key("ABC");
        let mut expected_half = [0x20u8; 12];
        expected_half[0..3].copy_from_slice(b"ABC");
        assert_eq!(&key[0..12], &expected_half);
        assert_eq!(&key[12..24], &expected_half);
    }

    #[test]
    fn test_derive_key_empty() {
        let key = derive_aes_key("");
        assert_eq!(key, [0x20u8; 24]);
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip_data() {
        let key = derive_aes_key("TestKey123");
        let original = vec![0xFE, 0xFE, 0x7E, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0xFE, 0x0D];

        let encrypted = encrypt_data(&key, &original);
        assert!(encrypted.len() >= 17);
        assert_ne!(&encrypted[1..], &original);

        let decrypted = decrypt_data(&key, &encrypted).unwrap();
        assert_eq!(&decrypted[..original.len()], &original[..]);
    }

    #[tokio::test]
    async fn test_encrypted_stream_roundtrip() {
        let (client_raw, server_raw) = tokio::io::duplex(1024);
        let key = derive_aes_key("SecretKey123");

        let mut client = EncryptedStream::new(client_raw, key);
        let mut server = EncryptedStream::new(server_raw, key);

        let message = b"Hello Satel Integra encrypted world!";
        client.write_all(message).await.unwrap();
        client.flush().await.unwrap();

        let mut received = vec![0u8; message.len()];
        server.read_exact(&mut received).await.unwrap();
        assert_eq!(&received, message);
    }

    #[tokio::test]
    async fn test_encrypted_stream_chunked_packets() {
        let (client_raw, mut server_raw) = tokio::io::duplex(1024);
        let key = derive_aes_key("SecretKey123");

        let mut client = EncryptedStream::new(client_raw, key);

        let message = b"Chunked test 123";
        client.write_all(message).await.unwrap();
        client.flush().await.unwrap();

        let (mut feed_writer, feed_reader) = tokio::io::duplex(1024);
        let mut decrypting_stream = EncryptedStream::new(feed_reader, key);

        tokio::spawn(async move {
            let mut byte = [0u8; 1];
            while let Ok(_) = server_raw.read_exact(&mut byte).await {
                if feed_writer.write_all(&byte).await.is_err() {
                    break;
                }
            }
        });

        let mut received = vec![0u8; message.len()];
        decrypting_stream.read_exact(&mut received).await.unwrap();
        assert_eq!(&received, message);
    }
}
```

### src/error.rs

```rust
use std::io;
use thiserror::Error;

/// Errors that can occur during communication with the Satel Integra alarm panel.
#[derive(Error, Debug)]
pub enum SatelError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("Serial port error: {0}")]
    Serial(#[from] tokio_serial::Error),
    #[error("Operation timed out")]
    Timeout,
    #[error("Connection is not active")]
    NotConnected,
    #[error("Stream closed by remote host")]
    StreamClosed,
    #[error("Connection lost, reconnect may be attempted")]
    ConnectionLost,
    #[error("Invalid frame checksum (CRC)")]
    InvalidCrc,
    #[error("Invalid frame format")]
    InvalidFrame,
    #[error("Message expired in buffer queue")]
    MessageExpired,
    #[error("Worker thread dropped")]
    WorkerDropped,
    #[error("Worker / Connection already active")]
    AlreadyConnected,
    #[error("Temperature sensor error (0xFFFF)")]
    TemperatureSensorError,
    #[error("Temperature sensor missing or query timed out")]
    TemperatureNotSupportedOrTimeOut,
    #[error("Too many temperature errors - sensor blocked to protect queue")]
    TempTooManyErrors,
    #[error("Internal state lock poisoned")]
    StatePoisoned,
    #[error("Invalid user access code")]
    InvalidUserCode,
    #[error("No access / Invalid ID")]
    NoAccess,
    #[error("Cannot arm (forced arming required)")]
    CanNotArm,
    #[error("Unknown panel result error (0xEF): {0}")]
    IntegraResultError(u8),
    #[error("Invalid integration key: {0}")]
    InvalidIntegrationKey(String),
    #[error("Encryption error: {0}")]
    EncryptionError(String),
    #[error("Encrypted data length is not a multiple of AES block size (16 bytes), got {0} bytes")]
    InvalidEncryptedDataLength(usize),
}
```

### src/event.rs

```rust
use crate::command::SatelResult;
use crate::state::{
    AutoReadReport, ConnectionState, EthmVersion, IntegraVersion, SystemStatus,
    TemperatureSensorStatus, TroubleType,
};

/// Events broadcasted in real time to subscribers.
/// Can be received via `SatelIntegra::subscribe_events()`.
#[derive(Debug, Clone)]
pub enum SatelEvent {
    /// Connection state transition.
    ConnectionChanged(ConnectionState),
    /// Zone violation state changed (0x00).
    ZoneViolation { id: u16, state: bool },
    /// Zone tamper state changed (0x01).
    ZoneTamper { id: u16, state: bool },
    /// Zone alarm state changed (0x02).
    ZoneAlarm { id: u16, state: bool },
    /// Zone tamper alarm state changed (0x03).
    ZoneTamperAlarm { id: u16, state: bool },
    /// Zone alarm memory state changed (0x04).
    ZoneAlarmMemory { id: u16, state: bool },
    /// Zone tamper alarm memory state changed (0x05).
    ZoneTamperAlarmMemory { id: u16, state: bool },
    /// Zone bypass state changed (0x06).
    ZoneBypass { id: u16, state: bool },
    /// Zone 'no violation trouble' state changed (0x07).
    ZoneNoViolationTrouble { id: u16, state: bool },
    /// Zone 'long violation trouble' state changed (0x08).
    ZoneLongViolationTrouble { id: u16, state: bool },
    /// Partition suppressed arm state changed (0x09).
    PartitionArmed { id: u16, state: bool },
    /// Partition real arm state changed (0x0A).
    PartitionArmedReally { id: u16, state: bool },
    /// Partition alarm state changed (0x13).
    PartitionAlarm { id: u16, state: bool },
    /// Partition alarm memory state changed (0x15).
    PartitionAlarmMemory { id: u16, state: bool },
    /// Partition entry countdown time state changed (0x0E).
    PartitionEntryTime { id: u16, state: bool },
    /// Partition exit countdown time (>10s) state changed (0x0F).
    PartitionExitTimeGt10s { id: u16, state: bool },
    /// Partition exit countdown time (<10s) state changed (0x10).
    PartitionExitTimeLt10s { id: u16, state: bool },
    /// Output state changed (0x17).
    OutputChanged { id: u16, state: bool },
    /// Zone temperature sensor reading updated (0x7D).
    ZoneTemperatureChanged { id: u16, temperature: f32 },
    /// Zone temperature sensor fault error (timeout, missing, sensor error, blocked).
    ZoneTemperatureError { id: u16, status: TemperatureSensorStatus },
    /// Zone UTF-8 name received (0xEE type 1).
    ZoneNameReceived { id: u16, name: String },
    /// Output UTF-8 name received (0xEE type 4).
    OutputNameReceived { id: u16, name: String },
    /// Partition UTF-8 name received (0xEE type 0).
    PartitionNameReceived { id: u16, name: String },
    /// Integra panel model and firmware version received (0x7E).
    IntegraVersionReceived(IntegraVersion),
    /// ETHM/UART communication module version received (0x7C).
    EthmVersionReceived(EthmVersion),
    /// Panel command result code received (0xEF).
    PanelMessage(SatelResult),
    /// Auto-read push notification categories configured (0x7F).
    AutoReadConfigured(AutoReadReport),
    /// System hardware trouble state changed.
    Trouble(TroubleType, bool),
    /// System trouble memory state changed.
    TroubleMemory(TroubleType, bool),
    /// System status bits updated (0x1A).
    SystemStatusChanged(SystemStatus),
}
```

### src/lib.rs

```rust
//! Asynchronous Rust client for Satel Integra alarm control panels
//! communicating via ETHM-1 Plus (TCP/IP) or UART (RS-232).
//!
//! # Quick Example
//!
//! ```no_run
//! use satel_integra::{SatelIntegra, Config, ConnectionConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = Config {
//!         connection: ConnectionConfig::Tcp {
//!             host: "192.168.1.100".to_string(),
//!             port: 7094,
//!         },
//!         ..Default::default()
//!     };
//!
//!     let satel = SatelIntegra::new(config);
//!     satel.connect().await?;
//!
//!     let mut events = satel.subscribe_events();
//!     while let Ok(event) = events.recv().await {
//!         println!("{:?}", event);
//!     }
//!
//!     Ok(())
//! }
//! ```

// --- Internal modules (private) ---
pub(crate) mod auto_requester;
pub(crate) mod client_internal;
pub(crate) mod encryption;
pub(crate) mod polling_worker;
pub(crate) mod worker;

// --- Public modules ---
pub mod client;
pub mod codec;
pub mod command;
pub mod config;
pub mod error;
pub mod event;
pub mod parsers;
pub mod state;

// --- Re-exports of primary public types ---
pub use client::SatelIntegra;
pub use codec::SatelCodec;
pub use command::{SatelCommand, SatelResult};
pub use config::{Config, ConnectionConfig};
pub use error::SatelError;
pub use event::SatelEvent;
pub use state::{
    AutoReadItemState, AutoReadItemStatus, AutoReadReport, ConnectionState, ConnectionStatus,
    ConnectionTelemetry, ConnectionType, EthmCapabilities, EthmPtsaTroubles, EthmVersion,
    GsmModuleTroubles, IntegraVersion, MainBoardTroubles, Output, OutputName, Partition,
    PartitionName, SatelState, SatelStateHandle, SystemStatus, TemperatureSensorStatus, TroubleType,
    TroublesData, TroublesPart1Data, TroublesPart2Data, TroublesPart3Data, TroublesPart4Data,
    TroublesPart5Data, TroublesPart6Data, TroublesPart7Data, TroublesPart8Data, Zone, ZoneName,
    ZoneStatus, ZoneTemperature,
};

use std::sync::Once;

static LOG_INIT: Once = Once::new();

/// Initializes the tracing subscriber with a custom formatted output tailored for Satel Integra.
/// Invoked automatically upon creating a `SatelIntegra` instance.
pub fn init_logging() {
    LOG_INIT.call_once(|| {
        use tracing_subscriber::fmt::format::Writer;
        use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
        use tracing::Event;
        use core::fmt;

        struct CustomFormatter;

        impl<S, N> FormatEvent<S, N> for CustomFormatter
        where
            S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
            N: for<'a> FormatFields<'a> + 'static,
        {
            fn format_event(
                &self,
                _ctx: &FmtContext<'_, S, N>,
                mut writer: Writer<'_>,
                event: &Event<'_>,
            ) -> fmt::Result {
                let now = chrono::Local::now();
                let meta = event.metadata();

                let file = meta.file().unwrap_or("unknown");
                let file_short = std::path::Path::new(file)
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or(file);

                let target = meta.module_path().unwrap_or(meta.target());

                writeln!(
                    writer,
                    "{} | {:5} | {} | {} |",
                    now.format("%H:%M:%S:%3f %d.%m.%Y"),
                    meta.level().to_string(),
                    file_short,
                    target,
                )?;

                write!(writer, "--> ")?;
                _ctx.format_fields(writer.by_ref(), event)?;
                writeln!(writer)
            }
        }

        use tracing::Level;
        let _ = tracing_subscriber::fmt()
            .event_format(CustomFormatter)
            .with_max_level(Level::WARN)
            .try_init();
    });
}
```

### src/parsers/mod.rs

```rust
pub mod names;
pub mod outputs;
pub mod partitions;
pub mod system;
pub mod zones;

pub use names::{process_output_name, process_partition_name, process_zone_name};
pub use outputs::process_outputs_state;
pub use partitions::{
    process_partitions_alarm, process_partitions_alarm_memory, process_partitions_armed_really,
    process_partitions_armed_suppressed, process_partitions_entry_time,
    process_partitions_exit_time_gt_10s, process_partitions_exit_time_lt_10s,
};
pub use system::{
    map_trouble_bit, map_trouble_part_bit, process_auto_read_response, process_ethm_version,
    process_integra_version, process_rtc_and_status, process_troubles, process_troubles_frame,
    process_troubles_part1, process_troubles_part2, process_troubles_part3, process_troubles_part4,
    process_troubles_part5, process_troubles_part6, process_troubles_part7, process_troubles_part8,
};
pub use zones::{
    process_zone_temperature, process_zones_alarm, process_zones_alarm_memory,
    process_zones_bypass, process_zones_long_violation_trouble, process_zones_no_violation_trouble,
    process_zones_tamper, process_zones_tamper_alarm, process_zones_tamper_alarm_memory,
    process_zones_violation,
};
```

### src/parsers/names.rs

```rust
use crate::error::SatelError;
use crate::state::SatelName;
use chrono::Local;

/// Parses the complete response frame for command 0xEE (Read device name).
/// Returns a tuple `(Device ID, SatelName)`.
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

/// Parses the name response for partitions (device type 0).
pub fn process_partition_name(data: &[u8]) -> Result<(u16, SatelName), SatelError> {
    process_device_name(data, 0)
}

/// Parses the name response for zones (device type 1).
pub fn process_zone_name(data: &[u8]) -> Result<(u16, SatelName), SatelError> {
    process_device_name(data, 1)
}

/// Parses the name response for outputs (device type 4).
pub fn process_output_name(data: &[u8]) -> Result<(u16, SatelName), SatelError> {
    process_device_name(data, 4)
}
```

### src/parsers/outputs.rs

```rust
use crate::error::SatelError;
use crate::state::OutputsStateData;
use chrono::Local;

/// Parses the complete response frame for command 0x17 (Outputs state).
pub fn process_outputs_state(frame: &[u8]) -> Result<OutputsStateData, SatelError> {
    if frame.is_empty() || frame[0] != 0x17 {
        tracing::error!(
            "Invalid command byte in output state frame: {:02X?}",
            frame.first()
        );
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Output state frame too short: {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    let mut states = Vec::with_capacity(data.len() * 8);
    for &byte in data {
        for bit in 0..8 {
            states.push((byte & (1 << bit)) != 0);
        }
    }

    Ok(OutputsStateData {
        states,
        read_at: Local::now(),
    })
}
```

### src/parsers/partitions.rs

```rust
use crate::error::SatelError;
use crate::state::{PartitionsArmedData, PartitionsData};
use chrono::Local;

/// Parses the complete response frame for command 0x09 (Armed partitions suppressed).
pub fn process_partitions_armed_suppressed(frame: &[u8]) -> Result<PartitionsArmedData, SatelError> {
    if frame.is_empty() || frame[0] != 0x09 {
        tracing::error!("Invalid command byte in suppressed partition arm frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 4 {
        tracing::error!("Partition arm data too short: {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(PartitionsArmedData {
        states: parse_partition_bits(data),
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x0A (Armed partitions really).
pub fn process_partitions_armed_really(frame: &[u8]) -> Result<PartitionsData, SatelError> {
    if frame.is_empty() || frame[0] != 0x0A {
        tracing::error!("Invalid command byte in real partition arm frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 4 {
        return Err(SatelError::InvalidFrame);
    }

    Ok(PartitionsData {
        states: parse_partition_bits(data),
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x0E (Partitions entry time).
pub fn process_partitions_entry_time(frame: &[u8]) -> Result<PartitionsData, SatelError> {
    if frame.is_empty() || frame[0] != 0x0E {
        tracing::error!("Invalid command byte in partition entry time frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 4 {
        return Err(SatelError::InvalidFrame);
    }

    Ok(PartitionsData {
        states: parse_partition_bits(data),
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x0F (Partitions exit time > 10s).
pub fn process_partitions_exit_time_gt_10s(frame: &[u8]) -> Result<PartitionsData, SatelError> {
    if frame.is_empty() || frame[0] != 0x0F {
        tracing::error!("Invalid command byte in partition exit time >10s frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 4 {
        return Err(SatelError::InvalidFrame);
    }

    Ok(PartitionsData {
        states: parse_partition_bits(data),
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x10 (Partitions exit time < 10s).
pub fn process_partitions_exit_time_lt_10s(frame: &[u8]) -> Result<PartitionsData, SatelError> {
    if frame.is_empty() || frame[0] != 0x10 {
        tracing::error!("Invalid command byte in partition exit time <10s frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 4 {
        return Err(SatelError::InvalidFrame);
    }

    Ok(PartitionsData {
        states: parse_partition_bits(data),
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x13 (Partitions alarm).
pub fn process_partitions_alarm(frame: &[u8]) -> Result<PartitionsData, SatelError> {
    if frame.is_empty() || frame[0] != 0x13 {
        tracing::error!("Invalid command byte in partition alarm frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 4 {
        return Err(SatelError::InvalidFrame);
    }

    Ok(PartitionsData {
        states: parse_partition_bits(data),
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x15 (Partitions alarm memory).
pub fn process_partitions_alarm_memory(frame: &[u8]) -> Result<PartitionsData, SatelError> {
    if frame.is_empty() || frame[0] != 0x15 {
        tracing::error!("Invalid command byte in partition alarm memory frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 4 {
        return Err(SatelError::InvalidFrame);
    }

    Ok(PartitionsData {
        states: parse_partition_bits(data),
        read_at: Local::now(),
    })
}

/// Common helper: parses 4 mask bytes into `Vec<bool>` (32 partitions).
fn parse_partition_bits(data: &[u8]) -> Vec<bool> {
    let mut states = Vec::with_capacity(32);
    for &byte in data.iter().take(4) {
        for bit in 0..8 {
            states.push((byte & (1 << bit)) != 0);
        }
    }
    states
}
```

### src/parsers/system.rs

```rust
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
```

### src/parsers/zones.rs

```rust
use crate::error::SatelError;
use crate::state::{
    ZonesAlarmData, ZonesAlarmMemoryData, ZonesBypassData, ZonesLongViolationTroubleData,
    ZonesNoViolationTroubleData, ZonesTamperAlarmData, ZonesTamperAlarmMemoryData,
    ZonesTamperData, ZonesViolationData,
};
use chrono::Local;

/// Parses the complete response frame for command 0x7D (Read zone temperature).
/// Returns a tuple `(Zone ID, Temperature in °C)`.
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
        return Err(SatelError::TemperatureSensorError);
    }

    // 0x0000 = -55.0°C, 0.5°C step
    let temperature = (temp_raw as f32 * 0.5) - 55.0;

    Ok((zone_id, temperature))
}

/// Parses the complete response frame for command 0x01 (Zones tamper).
/// Applies inversion for zones listed in `invert_list`.
pub fn process_zones_tamper(frame: &[u8], invert_list: &[u16]) -> Result<ZonesTamperData, SatelError> {
    if frame.is_empty() || frame[0] != 0x01 {
        tracing::error!("Invalid command byte in zone tamper frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Zone tamper data frame too short: {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesTamperData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x02 (Zones alarm).
/// Applies inversion for zones listed in `invert_list`.
pub fn process_zones_alarm(frame: &[u8], invert_list: &[u16]) -> Result<ZonesAlarmData, SatelError> {
    if frame.is_empty() || frame[0] != 0x02 {
        tracing::error!("Invalid command byte in zone alarm frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Zone alarm data frame too short: {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesAlarmData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x00 (Zones violation).
/// Applies inversion for zones listed in `invert_list`.
pub fn process_zones_violation(frame: &[u8], invert_list: &[u16]) -> Result<ZonesViolationData, SatelError> {
    if frame.is_empty() || frame[0] != 0x00 {
        tracing::error!("Invalid command byte in zone violation frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Zone violation data frame too short: {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesViolationData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x03 (Zones tamper alarm).
/// Applies inversion for zones listed in `invert_list`.
pub fn process_zones_tamper_alarm(frame: &[u8], invert_list: &[u16]) -> Result<ZonesTamperAlarmData, SatelError> {
    if frame.is_empty() || frame[0] != 0x03 {
        tracing::error!("Invalid command byte in zone tamper alarm frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Zone tamper alarm data frame too short: {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesTamperAlarmData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x04 (Zones alarm memory).
/// Applies inversion for zones listed in `invert_list`.
pub fn process_zones_alarm_memory(frame: &[u8], invert_list: &[u16]) -> Result<ZonesAlarmMemoryData, SatelError> {
    if frame.is_empty() || frame[0] != 0x04 {
        tracing::error!("Invalid command byte in zone alarm memory frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Zone alarm memory data frame too short: {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesAlarmMemoryData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x05 (Zones tamper alarm memory).
/// Applies inversion for zones listed in `invert_list`.
pub fn process_zones_tamper_alarm_memory(frame: &[u8], invert_list: &[u16]) -> Result<ZonesTamperAlarmMemoryData, SatelError> {
    if frame.is_empty() || frame[0] != 0x05 {
        tracing::error!("Invalid command byte in zone tamper alarm memory frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Zone tamper alarm memory data frame too short: {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesTamperAlarmMemoryData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x06 (Zones bypass).
/// Applies inversion for zones listed in `invert_list`.
pub fn process_zones_bypass(frame: &[u8], invert_list: &[u16]) -> Result<ZonesBypassData, SatelError> {
    if frame.is_empty() || frame[0] != 0x06 {
        tracing::error!("Invalid command byte in zone bypass frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Zone bypass data frame too short: {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesBypassData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x07 (Zones 'no violation' trouble).
/// Applies inversion for zones listed in `invert_list`.
pub fn process_zones_no_violation_trouble(frame: &[u8], invert_list: &[u16]) -> Result<ZonesNoViolationTroubleData, SatelError> {
    if frame.is_empty() || frame[0] != 0x07 {
        tracing::error!("Invalid command byte in zone 'no violation' trouble frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Zone 'no violation' trouble data frame too short: {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesNoViolationTroubleData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Parses the complete response frame for command 0x08 (Zones 'long violation' trouble).
/// Applies inversion for zones listed in `invert_list`.
pub fn process_zones_long_violation_trouble(frame: &[u8], invert_list: &[u16]) -> Result<ZonesLongViolationTroubleData, SatelError> {
    if frame.is_empty() || frame[0] != 0x08 {
        tracing::error!("Invalid command byte in zone 'long violation' trouble frame: {:02X?}", frame.first());
        return Err(SatelError::InvalidFrame);
    }

    let data = &frame[1..];
    if data.len() < 16 {
        tracing::error!("Zone 'long violation' trouble data frame too short: {} bytes", data.len());
        return Err(SatelError::InvalidFrame);
    }

    Ok(ZonesLongViolationTroubleData {
        states: parse_bit_states(data, invert_list),
        read_at: Local::now(),
    })
}

/// Common helper: parses bitwise mask bytes into `Vec<bool>` with optional zone state inversion.
fn parse_bit_states(data: &[u8], invert_list: &[u16]) -> Vec<bool> {
    let mut states = Vec::with_capacity(data.len() * 8);
    for (byte_idx, &byte) in data.iter().enumerate() {
        for bit in 0..8 {
            let zone_id = (byte_idx * 8 + bit + 1) as u16;
            let raw_state = (byte & (1 << bit)) != 0;
            let final_state = if invert_list.contains(&zone_id) {
                !raw_state
            } else {
                raw_state
            };
            states.push(final_state);
        }
    }
    states
}
```

### src/polling_worker.rs

```rust
use crate::client::SatelIntegra;
use crate::state::ConnectionState;
use futures::future::BoxFuture;
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Trait defining a scheduled periodic polling task.
pub(crate) trait PollingTask: Send + Sync {
    /// Task identifier name for logging.
    fn name(&self) -> &str;

    /// Execution recurrence interval.
    #[allow(dead_code)]
    fn interval(&self) -> Duration;

    /// Checks if the task is due for execution.
    fn is_due(&self) -> bool;

    /// Executes the task asynchronously.
    fn execute<'a>(&'a mut self, integra: &'a SatelIntegra) -> BoxFuture<'a, ()>;
}

/// Periodic background task querying configured zone temperature sensors.
pub(crate) struct TemperaturePollingTask {
    pub zones: Vec<u16>,
    pub interval: Duration,
    pub last_run: Option<Instant>,
}

impl TemperaturePollingTask {
    pub fn new(zones: Vec<u16>, interval: Duration) -> Self {
        Self {
            zones,
            interval,
            last_run: None,
        }
    }
}

impl PollingTask for TemperaturePollingTask {
    fn name(&self) -> &str {
        "TemperaturePollingTask"
    }

    fn interval(&self) -> Duration {
        self.interval
    }

    fn is_due(&self) -> bool {
        match self.last_run {
            None => true,
            Some(last) => last.elapsed() >= self.interval,
        }
    }

    fn execute<'a>(&'a mut self, integra: &'a SatelIntegra) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.last_run = Some(Instant::now());
            tracing::info!(
                "Polling worker: starting temperature polling cycle for {} zones",
                self.zones.len()
            );

            for &zone_id in &self.zones {
                // Check if connection is active
                let is_connected = {
                    if let Ok(state) = integra.state_handle().read() {
                        state.telemetry.status.state == ConnectionState::Connected
                    } else {
                        false
                    }
                };

                if !is_connected {
                    tracing::warn!(
                        "Polling worker: connection inactive, aborting current temperature polling cycle"
                    );
                    break;
                }

                // Query zone temperature using the public client API (handles smart blocking, cache & events)
                match integra.get_zone_temperature(zone_id).await {
                    Ok(temp) => {
                        tracing::debug!(
                            "Polling worker: retrieved zone #{:03} temperature: {}°C",
                            zone_id,
                            temp.temperature
                        );
                    }
                    Err(e) => {
                        tracing::debug!(
                            "Polling worker: error querying zone #{:03} temperature: {:?}",
                            zone_id,
                            e
                        );
                    }
                }

                // Brief pause between sensor queries (100ms) to avoid queue saturation
                sleep(Duration::from_millis(100)).await;
            }

            tracing::info!("Polling worker: completed temperature polling cycle");
        })
    }
}

/// Generic scheduler worker managing background polling tasks.
pub(crate) struct SatelPollingWorker {
    pub integra: SatelIntegra,
    pub tasks: Vec<Box<dyn PollingTask>>,
}

impl SatelPollingWorker {
    pub fn new(integra: SatelIntegra) -> Self {
        Self {
            integra,
            tasks: Vec::new(),
        }
    }

    #[allow(dead_code)]
    pub fn register(&mut self, task: Box<dyn PollingTask>) {
        self.tasks.push(task);
    }

    pub fn register_task(&mut self, task: Box<dyn PollingTask>) {
        self.tasks.push(task);
    }

    pub async fn run(mut self) {
        tracing::info!(
            "SatelPollingWorker started with {} registered tasks",
            self.tasks.len()
        );

        if self.tasks.is_empty() {
            tracing::info!("SatelPollingWorker: no tasks registered, stopping worker");
            return;
        }

        // Initial delay of 5 seconds post-connection to let handshake and auto-push stabilize
        let initial_delay = Duration::from_secs(5);

        tracing::info!(
            "SatelPollingWorker: waiting 5s post-connection before first polling cycle..."
        );
        sleep(initial_delay).await;

        loop {
            // Check if connection is active
            let is_connected = {
                if let Ok(state) = self.integra.state_handle().read() {
                    state.telemetry.status.state == ConnectionState::Connected
                } else {
                    false
                }
            };

            if is_connected {
                for task in &mut self.tasks {
                    if task.is_due() {
                        tracing::debug!("SatelPollingWorker: running task '{}'", task.name());
                        task.execute(&self.integra).await;
                    }
                }
            } else {
                tracing::debug!("SatelPollingWorker: waiting for connection...");
            }

            // Evaluate task schedule every 1 second
            sleep(Duration::from_secs(1)).await;
        }
    }
}
```

### src/state.rs

```rust
use chrono::{DateTime, Local};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Instant, SystemTime};

// --- Connection ---

/// Connection state machine variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConnectionState {
    Connected,
    #[default]
    Disconnected,
    ConnectionLost,
    Connecting,
    Handshake,
}

/// Connection status containing current state, retry count, and activity timestamps.
#[derive(Debug, Clone, Copy)]
pub struct ConnectionStatus {
    pub state: ConnectionState,
    pub failed_attempts: u32,
    pub last_event_at: Instant,
}

impl Default for ConnectionStatus {
    fn default() -> Self {
        Self {
            state: ConnectionState::Disconnected,
            failed_attempts: 0,
            last_event_at: Instant::now(),
        }
    }
}

/// Active transport connection type.
#[derive(Debug, Clone)]
pub enum ConnectionType {
    Tcp(String, u16),
    Uart(String),
}

/// Data transmission telemetry and connection health counters.
#[derive(Debug)]
pub struct ConnectionTelemetry {
    pub status: ConnectionStatus,
    pub last_connected_at: Option<SystemTime>,
    pub last_send_at: Instant,
    pub bytes_sent: AtomicUsize,
    pub bytes_received: AtomicUsize,
    pub reconnect_count: AtomicUsize,
}

impl Default for ConnectionTelemetry {
    fn default() -> Self {
        Self {
            status: ConnectionStatus::default(),
            last_connected_at: None,
            last_send_at: Instant::now(),
            bytes_sent: AtomicUsize::new(0),
            bytes_received: AtomicUsize::new(0),
            reconnect_count: AtomicUsize::new(0),
        }
    }
}

impl ConnectionTelemetry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&self) {
        self.bytes_sent.store(0, Ordering::Relaxed);
        self.bytes_received.store(0, Ordering::Relaxed);
        self.reconnect_count.store(0, Ordering::Relaxed);
    }
}

// --- Version info ---

/// Integra alarm panel version and model information.
#[derive(Debug, Clone)]
pub struct IntegraVersion {
    pub model: String,
    pub firmware_version: String,
    pub language: String,
    pub stored_in_flash: bool,
    pub io_count: u16,
    pub read_at: DateTime<Local>,
}

/// Capabilities and features supported by the ETHM module (from 0x7C frame).
#[derive(Debug, Clone, Copy)]
pub struct EthmCapabilities {
    /// Bit 0: Support for expanded 32-byte frames (256 zones/outputs).
    pub support_32_byte_frames: bool,
    /// Bit 1: Support for 8 trouble groups and 14-byte 0x7F mask.
    pub support_8_troubles_groups: bool,
    /// Bit 2: Support for extended arming commands.
    pub support_extended_arming_commands: bool,
    pub reserved_bit3: bool,
    pub reserved_bit4: bool,
    pub reserved_bit5: bool,
    pub reserved_bit6: bool,
    pub reserved_bit7: bool,
}

/// ETHM / UART communication module version information (from 0x7C frame).
#[derive(Debug, Clone)]
pub struct EthmVersion {
    pub version_raw: String,
    pub capabilities: EthmCapabilities,
    pub read_at: DateTime<Local>,
}

// --- Names ---

/// Name structure for zones, outputs, or partitions with retrieval timestamp.
#[derive(Debug, Clone)]
pub struct SatelName {
    pub name: String,
    pub read_at: DateTime<Local>,
}

/// Alias for zone name.
pub type ZoneName = SatelName;
/// Alias for output name.
pub type OutputName = SatelName;
/// Alias for partition name.
pub type PartitionName = SatelName;

// --- Temperature ---

/// Status and health state of a zone temperature probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TemperatureSensorStatus {
    #[default]
    NoRead,                 // Initial state before first query
    Ok,                     // Healthy reading received
    SensorMissing,          // Missing sensor or query timeout (during retries)
    CommunicationError,     // Communication failure or 0xFFFF (during retries)
    BlockSensorMissing,     // Blocked in RAM after exceeding timeout threshold
    BlockCommunicationError,// Blocked in RAM after exceeding sensor error threshold
}

/// Zone temperature reading with retrieval timestamp.
#[derive(Debug, Clone)]
pub struct ZoneTemperature {
    pub zone_id: u16,
    pub temperature: f32,
    pub read_at: DateTime<Local>,
}

// --- Parser intermediate raw data structures ---

/// Zone tamper data parsed from the panel.
#[derive(Debug, Clone)]
pub struct ZonesTamperData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Zone alarm data parsed from the panel.
#[derive(Debug, Clone)]
pub struct ZonesAlarmData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Zone violation data parsed from the panel.
#[derive(Debug, Clone)]
pub struct ZonesViolationData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Zone tamper alarm data parsed from the panel.
#[derive(Debug, Clone)]
pub struct ZonesTamperAlarmData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Zone alarm memory data parsed from the panel.
#[derive(Debug, Clone)]
pub struct ZonesAlarmMemoryData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Zone tamper alarm memory data parsed from the panel.
#[derive(Debug, Clone)]
pub struct ZonesTamperAlarmMemoryData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Zone bypass data parsed from the panel.
#[derive(Debug, Clone)]
pub struct ZonesBypassData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Zone 'no violation trouble' data parsed from the panel.
#[derive(Debug, Clone)]
pub struct ZonesNoViolationTroubleData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Zone 'long violation trouble' data parsed from the panel.
#[derive(Debug, Clone)]
pub struct ZonesLongViolationTroubleData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Partition states data parsed from the panel.
#[derive(Debug, Clone)]
pub struct PartitionsData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Alias for backwards compatibility.
pub type PartitionsArmedData = PartitionsData;

/// Output states data parsed from the panel.
#[derive(Debug, Clone)]
pub struct OutputsStateData {
    pub states: Vec<bool>,
    pub read_at: DateTime<Local>,
}

// --- Aggregated public models ---

/// Aggregated diagnostic status for a single zone.
#[derive(Debug, Clone)]
pub struct ZoneStatus {
    pub id: u16,
    pub name: String,
    pub temperature: f32,
    pub violation_state: bool,
    pub violation_at: DateTime<Local>,
    pub tamper_state: bool,
    pub tamper_at: DateTime<Local>,
    pub alarm_state: bool,
    pub alarm_at: DateTime<Local>,
    pub tamper_alarm_state: bool,
    pub tamper_alarm_at: DateTime<Local>,
    pub alarm_memory_state: bool,
    pub alarm_memory_at: DateTime<Local>,
    pub tamper_alarm_memory_state: bool,
    pub tamper_alarm_memory_at: DateTime<Local>,
    pub bypass_state: bool,
    pub bypass_at: DateTime<Local>,
    pub no_violation_trouble_state: bool,
    pub no_violation_trouble_at: DateTime<Local>,
    pub long_violation_trouble_state: bool,
    pub long_violation_trouble_at: DateTime<Local>,
}

// --- Zone, Partition, Output unified entities ---

/// Unified Zone representation containing all cached states and telemetry.
#[derive(Debug, Clone)]
pub struct Zone {
    pub id: u16,
    pub zone_name: String,
    pub zone_name_read_at: DateTime<Local>,
    pub temperature_value: f32,
    pub temperature_read_at: DateTime<Local>,
    pub temperature_status: TemperatureSensorStatus,
    pub temperature_timeout_errors_total: u32,
    pub temperature_sensor_errors_total: u32,
    pub temperature_timeout_errors_current: u32,
    pub temperature_sensor_errors_current: u32,
    pub tamper_state: bool,
    pub tamper_read_at: DateTime<Local>,
    pub alarm_state: bool,
    pub alarm_read_at: DateTime<Local>,
    pub violation_state: bool,
    pub violation_read_at: DateTime<Local>,
    pub tamper_alarm_state: bool,
    pub tamper_alarm_read_at: DateTime<Local>,
    pub alarm_memory_state: bool,
    pub alarm_memory_read_at: DateTime<Local>,
    pub tamper_alarm_memory_state: bool,
    pub tamper_alarm_memory_read_at: DateTime<Local>,
    pub bypass_state: bool,
    pub bypass_read_at: DateTime<Local>,
    pub no_violation_trouble_state: bool,
    pub no_violation_trouble_read_at: DateTime<Local>,
    pub long_violation_trouble_state: bool,
    pub long_violation_trouble_read_at: DateTime<Local>,
}

impl Zone {
    pub fn new(id: u16) -> Self {
        let now = Local::now();
        Self {
            id,
            zone_name: String::new(),
            zone_name_read_at: now,
            temperature_value: 0.0,
            temperature_read_at: now,
            temperature_status: TemperatureSensorStatus::NoRead,
            temperature_timeout_errors_total: 0,
            temperature_sensor_errors_total: 0,
            temperature_timeout_errors_current: 0,
            temperature_sensor_errors_current: 0,
            tamper_state: false,
            tamper_read_at: now,
            alarm_state: false,
            alarm_read_at: now,
            violation_state: false,
            violation_read_at: now,
            tamper_alarm_state: false,
            tamper_alarm_read_at: now,
            alarm_memory_state: false,
            alarm_memory_read_at: now,
            tamper_alarm_memory_state: false,
            tamper_alarm_memory_read_at: now,
            bypass_state: false,
            bypass_read_at: now,
            no_violation_trouble_state: false,
            no_violation_trouble_read_at: now,
            long_violation_trouble_state: false,
            long_violation_trouble_read_at: now,
        }
    }

    pub fn to_zone_name(&self) -> ZoneName {
        ZoneName {
            name: self.zone_name.clone(),
            read_at: self.zone_name_read_at,
        }
    }

    pub fn to_zone_temperature(&self) -> ZoneTemperature {
        ZoneTemperature {
            zone_id: self.id,
            temperature: self.temperature_value,
            read_at: self.temperature_read_at,
        }
    }
}

/// Ujednolicona struktura Strefy (Partition).
#[derive(Debug, Clone)]
pub struct Partition {
    pub id: u16,
    pub name: String,
    pub name_read_at: DateTime<Local>,
    pub armed_suppressed: bool,
    pub armed_suppressed_at: DateTime<Local>,
    pub armed_really: bool,
    pub armed_really_at: DateTime<Local>,
    pub alarm: bool,
    pub alarm_at: DateTime<Local>,
    pub alarm_memory: bool,
    pub alarm_memory_at: DateTime<Local>,
    pub entry_time: bool,
    pub entry_time_at: DateTime<Local>,
    pub exit_time_gt_10s: bool,
    pub exit_time_gt_10s_at: DateTime<Local>,
    pub exit_time_lt_10s: bool,
    pub exit_time_lt_10s_at: DateTime<Local>,
}

impl Partition {
    pub fn new(id: u16) -> Self {
        let now = Local::now();
        Self {
            id,
            name: String::new(),
            name_read_at: now,
            armed_suppressed: false,
            armed_suppressed_at: now,
            armed_really: false,
            armed_really_at: now,
            alarm: false,
            alarm_at: now,
            alarm_memory: false,
            alarm_memory_at: now,
            entry_time: false,
            entry_time_at: now,
            exit_time_gt_10s: false,
            exit_time_gt_10s_at: now,
            exit_time_lt_10s: false,
            exit_time_lt_10s_at: now,
        }
    }

    pub fn to_partition_name(&self) -> PartitionName {
        PartitionName {
            name: self.name.clone(),
            read_at: self.name_read_at,
        }
    }
}

/// Unified Output structure.
#[derive(Debug, Clone)]
pub struct Output {
    pub id: u16,
    pub name: String,
    pub name_read_at: DateTime<Local>,
    pub state: bool,
    pub state_read_at: DateTime<Local>,
}

impl Output {
    pub fn new(id: u16) -> Self {
        let now = Local::now();
        Self {
            id,
            name: String::new(),
            name_read_at: now,
            state: false,
            state_read_at: now,
        }
    }

    pub fn to_output_name(&self) -> OutputName {
        OutputName {
            name: self.name.clone(),
            read_at: self.name_read_at,
        }
    }
}

// --- 1:1 Trouble Data Structures (Parts 1..8) ---

/// Main control panel board troubles (from Part 1 frame).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MainBoardTroubles {
    pub out1_trouble: bool,
    pub out2_trouble: bool,
    pub out3_trouble: bool,
    pub out4_trouble: bool,
    pub kpd_power_trouble: bool,
    pub ex1_ex2_power_trouble: bool,
    pub battery_trouble: bool,
    pub ac_trouble: bool,
    pub dt1_trouble: bool,
    pub dt2_trouble: bool,
    pub dtm_trouble: bool,
    pub rtc_trouble: bool,
    pub no_dtr_signal: bool,
    pub no_battery_present: bool,
    pub external_modem_init_trouble: bool,
    pub external_modem_cmd_trouble: bool,
    pub tel_line_no_voltage: bool,
    pub tel_line_bad_signal: bool,
    pub tel_line_no_signal: bool,
    pub monitoring_station_1_trouble: bool,
    pub monitoring_station_2_trouble: bool,
    pub eeprom_rtc_trouble: bool,
    pub ram_trouble: bool,
    pub main_panel_restart: bool,
}

/// ETHM-1 / INT-GSM / PTSA communication module troubles (from Part 1 frame).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EthmPtsaTroubles {
    pub ethm_ping_trouble: bool,
    pub server_id_error: bool,
    pub no_server_connection: bool,
    pub no_ethm_mon_station_1: bool,
    pub no_ethm_mon_station_2: bool,
    pub no_gprs_mon_station_1: bool,
    pub no_gprs_mon_station_2: bool,
    pub time_server_trouble: bool,
    pub gsm_init_error: bool,
    pub ip_mon_station_1_trouble: bool,
    pub ip_mon_station_2_trouble: bool,
}

/// Parsed trouble frame for Part 1 (0x1B / 0x20 - 47 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TroublesPart1Data {
    pub is_memory: bool,
    pub technical_zones: Vec<bool>,
    pub expanders_ac: Vec<bool>,
    pub expanders_battery: Vec<bool>,
    pub expanders_no_battery: Vec<bool>,
    pub main_board: MainBoardTroubles,
    pub ethm_ptsa: EthmPtsaTroubles,
    pub read_at: DateTime<Local>,
}

/// Parsed trouble frame for Part 2 (0x1C / 0x21 - 26 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TroublesPart2Data {
    pub is_memory: bool,
    pub card_readers_head_a_or_synchro: Vec<bool>,
    pub card_readers_head_b_or_charging: Vec<bool>,
    pub expanders_supply_overload: Vec<bool>,
    pub acu_jammed_or_short_circuit: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Parsed trouble frame for Part 3 (0x1D / 0x22 - 60 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TroublesPart3Data {
    pub is_memory: bool,
    pub acu_jam_levels: Vec<u8>,
    pub wireless_devices_low_battery: Vec<bool>,
    pub wireless_devices_no_comm: Vec<bool>,
    pub wireless_outputs_no_comm: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Parsed trouble frame for Part 4 (0x1E / 0x23 - 30 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TroublesPart4Data {
    pub is_memory: bool,
    pub expanders_no_comm: Vec<bool>,
    pub expanders_substituted: Vec<bool>,
    pub keypads_no_comm: Vec<bool>,
    pub keypads_substituted: Vec<bool>,
    pub ethm_no_lan_or_intrs_no_dsr: Vec<bool>,
    pub expanders_tamper: Vec<bool>,
    pub keypads_tamper: Vec<bool>,
    pub keypad_init_errors: Vec<bool>,
    pub auxiliary_stm_troubles: u8,
    pub read_at: DateTime<Local>,
}

/// Parsed trouble frame for Part 5 (0x1F / 0x24 - 31 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TroublesPart5Data {
    pub is_memory: bool,
    pub masters_key_fobs_low_battery: Vec<bool>,
    pub users_key_fobs_low_battery: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Parsed trouble frame for Part 6 (0x2C / 0x2E - 45 bytes - Integra 256).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TroublesPart6Data {
    pub is_memory: bool,
    pub wireless_devices_low_battery: Vec<bool>,
    pub wireless_devices_no_comm: Vec<bool>,
    pub wireless_outputs_no_comm: Vec<bool>,
    pub read_at: DateTime<Local>,
}

/// Parsed trouble frame for Part 7 (0x2D / 0x2F - 47 bytes - Integra 256).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TroublesPart7Data {
    pub is_memory: bool,
    pub technical_zones: Vec<bool>,
    pub technical_zones_memory: Vec<bool>,
    pub acu_jam_levels: Vec<u8>,
    pub read_at: DateTime<Local>,
}

/// Detailed troubles for an individual INT-GSM module (from Part 8 frame).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GsmModuleTroubles {
    pub module_address: u8,
    pub no_ethm_mon_station_1: bool,
    pub no_ethm_mon_station_2: bool,
    pub no_gprs_sim1_mon_station_1: bool,
    pub no_gprs_sim1_mon_station_2: bool,
    pub no_gprs_sim2_mon_station_1: bool,
    pub no_gprs_sim2_mon_station_2: bool,
    pub no_sms_sim1_mon_station_1: bool,
    pub no_sms_sim1_mon_station_2: bool,
    pub no_sms_sim2_mon_station_1: bool,
    pub no_sms_sim2_mon_station_2: bool,
    pub wrong_sim1_pin: bool,
    pub wrong_sim2_pin: bool,
    pub sim1_logging_error: bool,
    pub sim2_logging_error: bool,
    pub sim1_credit_low: bool,
    pub sim2_credit_low: bool,
    pub sim1_sms_error: bool,
    pub sim2_sms_error: bool,
    pub gsm_jamming: bool,
    pub settings_crc_error: bool,
    pub missing_module: bool,
    pub changed_module: bool,
    pub satel_server_conn_error: bool,
    pub mail_server_conn_error: bool,
    pub ntp_server_conn_error: bool,
    pub sim1_cme_error: u16,
    pub sim2_cme_error: u16,
}

/// Parsed trouble frame for Part 8 (0x30 / 0x31 - 64 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TroublesPart8Data {
    pub is_memory: bool,
    pub gsm_modules: Vec<GsmModuleTroubles>,
    pub read_at: DateTime<Local>,
}

/// Universal enum representing any decoded trouble command frame (Parts 1..8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TroublesData {
    Part1(TroublesPart1Data),
    Part2(TroublesPart2Data),
    Part3(TroublesPart3Data),
    Part4(TroublesPart4Data),
    Part5(TroublesPart5Data),
    Part6(TroublesPart6Data),
    Part7(TroublesPart7Data),
    Part8(TroublesPart8Data),
}

/// System trouble variants for Satel Integra panels (matching 100% of protocol spec).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TroubleType {
    // --- Main panel board ---
    MainBoardAcLoss,
    MainBoardBatteryLow,
    MainBoardBatteryMissing,
    MainBoardOutOverload(u8),
    MainBoardKpdPowerOverload,
    MainBoardExPowerOverload,
    MainBoardDataBusDt1,
    MainBoardDataBusDt2,
    MainBoardDataBusDtm,
    RtcLoss,
    NoDtrSignal,
    ExternalModemInitTrouble,
    ExternalModemCmdTrouble,
    TelephoneLineNoVoltage,
    TelephoneLineBadSignal,
    TelephoneLineNoSignal,
    MonitoringStation1Trouble,
    MonitoringStation2Trouble,
    EepromRtcTrouble,
    RamMemoryError,
    MainPanelRestartMemory,

    // --- Technical zones ---
    TechnicalZoneTrouble(u16),

    // --- Expanders (1..64) ---
    ExpanderAcLoss(u8),
    ExpanderBatteryLow(u8),
    ExpanderBatteryMissing(u8),
    ExpanderSupplyOverload(u8),
    ExpanderNoComm(u8),
    ExpanderSubstituted(u8),
    ExpanderTamper(u8),
    ExpanderCardReaderHeadA(u8),
    ExpanderCardReaderHeadB(u8),
    ExpanderAcuJammedOrShortCircuit(u8),

    // --- Keypads (1..8) ---
    KeypadNoComm(u8),
    KeypadSubstituted(u8),
    KeypadTamper(u8),
    KeypadInitError(u8),

    // --- Communication modules (ETHM-1 / INT-GSM / PTSA) ---
    EthmNoLanCable(u8),
    EthmPingTrouble,
    EthmServerIdError,
    EthmSatelServerConnectionError,
    EthmMonitoringStation1Error,
    EthmMonitoringStation2Error,
    GprsMonitoringStation1Error,
    GprsMonitoringStation2Error,
    IpMonitoringStation1Trouble,
    IpMonitoringStation2Trouble,
    TimeServerTrouble,
    GsmInitError,
    IntGsmSignalLoss,
    GsmJamming(u8),
    GsmSimPinError { module: u8, sim: u8 },
    GsmSimLoggingError { module: u8, sim: u8 },
    GsmSimCreditLow { module: u8, sim: u8 },
    GsmSimSmsError { module: u8, sim: u8 },
    GsmSettingsCrcError(u8),
    GsmModuleMissing(u8),
    GsmModuleChanged(u8),
    GsmServerConnError(u8),
    GsmMailServerConnError(u8),
    GsmNtpServerConnError(u8),
    GsmCmeError { module: u8, sim: u8, code: u16 },
    GsmTrouble { module_address: u8, desc: &'static str },

    // --- Wireless devices (ABAX / ABAX 2) ---
    WirelessDeviceLowBattery { zone_id: u16 },
    WirelessDeviceNoComm { zone_id: u16 },
    WirelessOutputNoComm { output_id: u16 },
    AcuModuleJamLevel(u8),

    // --- Key fobs ---
    MasterKeyFobLowBattery(u8),
    UserKeyFobLowBattery { user_id: u16 },

    // --- Other ---
    AuxiliaryStmTroubles,
    GenericTrouble { part: u8, bit: u16 },
}

impl TroubleType {
    pub fn to_description(&self) -> String {
        match self {
            Self::MainBoardAcLoss => "Main Board: AC Power Loss (230V)".to_string(),
            Self::MainBoardBatteryLow => "Main Board: Battery Low Voltage".to_string(),
            Self::MainBoardBatteryMissing => "Main Board: Battery Missing / Disconnected".to_string(),
            Self::MainBoardOutOverload(out) => format!("Main Board: Supply Output #{} Overload", out),
            Self::MainBoardKpdPowerOverload => "Main Board: Keypad Power Supply (+KPD) Overload".to_string(),
            Self::MainBoardExPowerOverload => "Main Board: Expander Power Supply (+EX1/+EX2) Overload".to_string(),
            Self::MainBoardDataBusDt1 => "Main Board: Data Bus DT1 Communication Error".to_string(),
            Self::MainBoardDataBusDt2 => "Main Board: Data Bus DT2 Communication Error".to_string(),
            Self::MainBoardDataBusDtm => "Main Board: Data Bus DTM Communication Error".to_string(),
            Self::RtcLoss => "Main Board: Real-Time Clock (RTC) Loss / Not Set".to_string(),
            Self::NoDtrSignal => "Main Board: No DTR Signal on RS-232 Port".to_string(),
            Self::ExternalModemInitTrouble => "Main Board: External Modem Initialization Error".to_string(),
            Self::ExternalModemCmdTrouble => "Main Board: External Modem Command Error".to_string(),
            Self::TelephoneLineNoVoltage => "Telephone Line: No Voltage".to_string(),
            Self::TelephoneLineBadSignal => "Telephone Line: Bad Signal".to_string(),
            Self::TelephoneLineNoSignal => "Telephone Line: No Dial Tone".to_string(),
            Self::MonitoringStation1Trouble => "Monitoring: Station 1 Transmission Fault".to_string(),
            Self::MonitoringStation2Trouble => "Monitoring: Station 2 Transmission Fault".to_string(),
            Self::EepromRtcTrouble => "Main Board: EEPROM / RTC Access Trouble".to_string(),
            Self::RamMemoryError => "Main Board: RAM Memory Error".to_string(),
            Self::MainPanelRestartMemory => "Main Board: Panel Restart Latched in Memory".to_string(),

            Self::TechnicalZoneTrouble(zone) => format!("Technical Zone #{:03}: Trouble Detected", zone),

            Self::ExpanderAcLoss(exp) => format!("Expander #{:02}: AC Power Loss", exp),
            Self::ExpanderBatteryLow(exp) => format!("Expander #{:02}: Battery Low Voltage", exp),
            Self::ExpanderBatteryMissing(exp) => format!("Expander #{:02}: Battery Missing", exp),
            Self::ExpanderSupplyOverload(exp) => format!("Expander #{:02}: Power Supply Overload", exp),
            Self::ExpanderNoComm(exp) => format!("Expander #{:02}: No Communication", exp),
            Self::ExpanderSubstituted(exp) => format!("Expander #{:02}: Substituted / Unknown Hardware", exp),
            Self::ExpanderTamper(exp) => format!("Expander #{:02}: Tamper / Sabotage", exp),
            Self::ExpanderCardReaderHeadA(exp) => format!("Expander #{:02}: Card Reader Head A / Synchro Trouble", exp),
            Self::ExpanderCardReaderHeadB(exp) => format!("Expander #{:02}: Card Reader Head B / Charging Trouble", exp),
            Self::ExpanderAcuJammedOrShortCircuit(exp) => format!("Expander #{:02}: Jammed / Addressable Loop Short Circuit", exp),

            Self::KeypadNoComm(kpd) => format!("Keypad #{:02}: No Communication", kpd),
            Self::KeypadSubstituted(kpd) => format!("Keypad #{:02}: Substituted Keypad", kpd),
            Self::KeypadTamper(kpd) => format!("Keypad #{:02}: Tamper / Sabotage", kpd),
            Self::KeypadInitError(kpd) => format!("Keypad #{:02}: Initialization Error", kpd),

            Self::EthmNoLanCable(mod_id) => format!("ETHM-1 #{:02}: Ethernet LAN Cable Unplugged", mod_id),
            Self::EthmPingTrouble => "ETHM-1: Ping Network Test Failed".to_string(),
            Self::EthmServerIdError => "ETHM-1: SATEL Server MAC/ID Verification Error".to_string(),
            Self::EthmSatelServerConnectionError => "ETHM-1: No Connection to SATEL Server".to_string(),
            Self::EthmMonitoringStation1Error => "ETHM-1: Monitoring Station 1 Connection Error".to_string(),
            Self::EthmMonitoringStation2Error => "ETHM-1: Monitoring Station 2 Connection Error".to_string(),
            Self::GprsMonitoringStation1Error => "INT-GSM: GPRS Monitoring Station 1 Error".to_string(),
            Self::GprsMonitoringStation2Error => "INT-GSM: GPRS Monitoring Station 2 Error".to_string(),
            Self::IpMonitoringStation1Trouble => "IP Monitoring: Station 1 Communication Trouble".to_string(),
            Self::IpMonitoringStation2Trouble => "IP Monitoring: Station 2 Communication Trouble".to_string(),
            Self::TimeServerTrouble => "Network: NTP Time Synchronization Server Error".to_string(),
            Self::GsmInitError => "INT-GSM: GSM Module Initialization Error".to_string(),
            Self::IntGsmSignalLoss => "INT-GSM: Cellular Signal Lost".to_string(),
            Self::GsmJamming(addr) => format!("INT-GSM (Addr {}): Cellular Jamming Detected", addr),
            Self::GsmSimPinError { module, sim } => format!("INT-GSM (Addr {}): SIM{} Wrong PIN", module, sim),
            Self::GsmSimLoggingError { module, sim } => format!("INT-GSM (Addr {}): SIM{} Network Registration Error", module, sim),
            Self::GsmSimCreditLow { module, sim } => format!("INT-GSM (Addr {}): SIM{} Account Credit Low", module, sim),
            Self::GsmSimSmsError { module, sim } => format!("INT-GSM (Addr {}): SIM{} SMS Sending Error", module, sim),
            Self::GsmSettingsCrcError(addr) => format!("INT-GSM (Addr {}): Settings CRC Checksum Error", addr),
            Self::GsmModuleMissing(addr) => format!("INT-GSM (Addr {}): Module Missing", addr),
            Self::GsmModuleChanged(addr) => format!("INT-GSM (Addr {}): Module Changed", addr),
            Self::GsmServerConnError(addr) => format!("INT-GSM (Addr {}): Server Connection Error", addr),
            Self::GsmMailServerConnError(addr) => format!("INT-GSM (Addr {}): Mail Server Error", addr),
            Self::GsmNtpServerConnError(addr) => format!("INT-GSM (Addr {}): NTP Server Error", addr),
            Self::GsmCmeError { module, sim, code } => format!("INT-GSM (Addr {}): SIM{} Modem CME Error #{:04X}", module, sim, code),
            Self::GsmTrouble { module_address, desc } => format!("INT-GSM (Addr {}): {}", module_address, desc),

            Self::WirelessDeviceLowBattery { zone_id } => format!("Wireless Sensor (Zone #{:03}): Low Battery", zone_id),
            Self::WirelessDeviceNoComm { zone_id } => format!("Wireless Sensor (Zone #{:03}): No Radio Communication", zone_id),
            Self::WirelessOutputNoComm { output_id } => format!("Wireless Output #{:03}: No Radio Communication", output_id),
            Self::AcuModuleJamLevel(acu) => format!("ACU-100/220 Module #{:02}: Radio Jamming Detected", acu),

            Self::MasterKeyFobLowBattery(master) => format!("Master User #{:02} Key Fob: Low Battery", master),
            Self::UserKeyFobLowBattery { user_id } => format!("User #{:03} Key Fob: Low Battery", user_id),

            Self::AuxiliaryStmTroubles => "Auxiliary Microprocessor (STM) Trouble".to_string(),
            Self::GenericTrouble { part, bit } => format!("Diagnostic Trouble (Part {}, Bit #{:03})", part + 1, bit),
        }
    }
}

/// General system status flags (from 0x1A frame).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SystemStatus {
    pub service_mode: bool,
    pub troubles_present: bool,
    pub troubles_memory: bool,
    pub rtc: DateTime<Local>,
}

// --- Auto-read (Push notifications) ---

/// State of an individual auto-read category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoReadItemState {
    /// Active and operational.
    Active,
    /// Not requested in configuration.
    NotRequested,
    /// Unsupported by hardware (requires 14-byte support, panel provides 12).
    UnsupportedByHardware,
    /// Rejected by panel (error code 0xEF).
    RejectedByPanel(u8),
}

impl AutoReadItemState {
    pub fn to_description(&self) -> String {
        match self {
            Self::Active => "Active".to_string(),
            Self::NotRequested => "Not requested".to_string(),
            Self::UnsupportedByHardware => "Unsupported by ETHM hardware (requires 14B mask)".to_string(),
            Self::RejectedByPanel(code) => {
                let desc = match code {
                    0x01 => "Error: Unknown user code",
                    0x02 => "Error: No access rights",
                    0x03 => "Error: User does not exist",
                    0x04 => "Error: User already exists",
                    0x05 => "Error: Wrong user code",
                    0x08 => "Error: Other panel error",
                    _ => "Error: Rejected (0xEF)",
                };
                format!("{} ({:02X})", desc, code)
            }
        }
    }
}

/// Status of a specific auto-read category.
#[derive(Debug, Clone)]
pub struct AutoReadItemStatus {
    pub name: String,
    pub state: AutoReadItemState,
}

/// Report summarizing auto-read configuration results.
#[derive(Debug, Clone)]
pub struct AutoReadReport {
    pub items: Vec<AutoReadItemStatus>,
    pub success_count: usize,
    pub total_requested: usize,
}

// --- Shared state cache ---

/// Thread-safe in-memory cache of the Integra panel state.
#[derive(Debug)]
pub struct SatelState {
    pub connection_type: Option<ConnectionType>,
    pub telemetry: ConnectionTelemetry,
    pub integra_version: Option<IntegraVersion>,
    pub ethm_version: Option<EthmVersion>,
    pub zones: Vec<Zone>,
    pub outputs: Vec<Output>,
    pub partitions: Vec<Partition>,
    pub system_status: Option<SystemStatus>,
    pub troubles: [Vec<bool>; 8],
    pub troubles_memory: [Vec<bool>; 8],
}

impl Default for SatelState {
    fn default() -> Self {
        Self::new()
    }
}

impl SatelState {
    pub fn new() -> Self {
        let mut zones = Vec::with_capacity(256);
        let mut outputs = Vec::with_capacity(256);
        for i in 1..=256 {
            zones.push(Zone::new(i as u16));
            outputs.push(Output::new(i as u16));
        }

        let mut partitions = Vec::with_capacity(32);
        for i in 1..=32 {
            partitions.push(Partition::new(i as u16));
        }

        Self {
            connection_type: None,
            telemetry: ConnectionTelemetry::new(),
            integra_version: None,
            ethm_version: None,
            zones,
            outputs,
            partitions,
            system_status: None,
            troubles: Default::default(),
            troubles_memory: Default::default(),
        }
    }
}

/// Thread-safe shared handle to `SatelState`.
pub type SatelStateHandle = Arc<RwLock<SatelState>>;
```

### src/worker.rs

```rust
use crate::codec::SatelCodec;
use crate::command::SatelCommand;
use crate::config::{Config, ConnectionConfig};
use crate::error::SatelError;
use crate::parsers::{process_auto_read_response, process_ethm_version, process_integra_version};
use crate::state::{ConnectionState, ConnectionType, SatelStateHandle};
use futures::{SinkExt, StreamExt};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, timeout};
use tokio_serial::SerialPortBuilderExt;
use tokio_util::codec::Framed;

/// Helper trait combining `AsyncRead` and `AsyncWrite`.
pub(crate) trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncReadWrite for T {}

/// Framing type wrapping the I/O stream with `SatelCodec`.
type FramedStream = Framed<Box<dyn AsyncReadWrite>, SatelCodec>;

/// Messages forwarded to the `SatelAutoRequester` state worker.
pub(crate) enum StateWorkerMessage {
    /// Frame received from the panel (e.g. unsolicited Push notification).
    Frame(Vec<u8>),
    /// Connection state transition.
    StatusChanged(ConnectionState),
    /// Integra panel version received.
    IntegraVersion(crate::state::IntegraVersion),
    /// ETHM module version received.
    EthmVersion(crate::state::EthmVersion),
    /// Auto-read configuration result report.
    AutoReadReport(crate::state::AutoReadReport),
}

/// Internal actor messages passed between the client and worker actor.
pub(crate) enum InternalMessage {
    /// Standard command exchange (valid only in Connected state).
    ExchangeStandard {
        data: Vec<u8>,
        write_timeout: Duration,
        read_timeout: Duration,
        created_at: Instant,
        max_queue_time: Duration,
        response_tx: oneshot::Sender<Result<Vec<u8>, SatelError>>,
    },
    /// Priority command exchange (allowed during Connecting, Handshake, Connected).
    ExchangePriority {
        data: Vec<u8>,
        write_timeout: Duration,
        read_timeout: Duration,
        response_tx: oneshot::Sender<Result<Vec<u8>, SatelError>>,
    },
    Connect {
        response_tx: oneshot::Sender<Result<(), SatelError>>,
    },
    Disconnect {
        response_tx: oneshot::Sender<Result<(), SatelError>>,
    },
}

/// Background actor worker managing the physical socket/serial connection.
pub(crate) struct SatelCommunicationWorker {
    pub config: Config,
    pub state: SatelStateHandle,
    pub rx: mpsc::Receiver<InternalMessage>,
    pub stream: Option<FramedStream>,
    pub state_worker_tx: Option<mpsc::Sender<StateWorkerMessage>>,
}

impl SatelCommunicationWorker {
    /// Main worker loop with initial connect result signaling.
    pub async fn run(mut self, on_connect: oneshot::Sender<Result<(), SatelError>>) {
        let result = self.satel_connection_worker_connect().await;
        if let Err(e) = &result {
            tracing::error!("Initial connection failed: {:?}", e);
        }
        let _ = on_connect.send(result);
        self.run_loop().await;
    }

    /// Primary actor event loop.
    pub async fn run_loop(&mut self) {
        let mut ping_interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            let should_reconnect = self.config.auto_reconnect;

            tokio::select! {
                maybe_msg = self.rx.recv() => {
                    if let Some(msg) = maybe_msg {
                        self.handle_message_internal(msg).await;
                    } else {
                        break;
                    }
                }

                res = Self::receive_push_internal(&mut self.stream, &self.state, &self.state_worker_tx), if self.stream.is_some() && self.get_current_state() == ConnectionState::Connected => {
                    if let Err(e) = res {
                        tracing::error!("Error receiving Push / Stream frame: {:?}", e);
                        self.satel_connection_worker_connection_lost().await;
                    }
                }

                _ = ping_interval.tick() => {
                    let (current_state, last_send, last_event) = {
                        let s = self.state.read().unwrap();
                        (s.telemetry.status.state, s.telemetry.last_send_at, s.telemetry.status.last_event_at)
                    };

                    // 1. Watchdog for Connecting / Handshake
                    if (current_state == ConnectionState::Connecting || current_state == ConnectionState::Handshake)
                        && last_event.elapsed() > Duration::from_secs(15) {
                        tracing::warn!("Watchdog: Connection/handshake timeout exceeded (15s)");
                        self.satel_connection_worker_connection_lost().await;
                    }

                    // 2. Ping keep-alive
                    if current_state == ConnectionState::Connected && last_send.elapsed() >= Duration::from_secs(2) {
                        let cmd = vec![SatelCommand::IntegraVersion.to_byte()];
                        let _ = self.satel_connection_worker_exchange(cmd, 0x7E, Duration::from_millis(500), Duration::from_millis(500)).await;
                    }

                    // 3. Automatic reconnect with exponential backoff
                    if should_reconnect
                        && current_state == ConnectionState::ConnectionLost
                        && last_event.elapsed() >= self.calculate_backoff()
                    {
                        tracing::info!("Auto-reconnect: Attempting reconnection...");
                        if let Ok(s) = self.state.read() {
                            s.telemetry.reconnect_count.fetch_add(1, Ordering::Relaxed);
                        }
                        let _ = self.satel_connection_worker_connect().await;
                    }
                }
            }
        }
        tracing::info!("SatelCommunicationWorker terminated");
    }

    async fn handle_message_internal(&mut self, msg: InternalMessage) {
        match msg {
            InternalMessage::ExchangeStandard {
                data,
                write_timeout,
                read_timeout,
                created_at,
                max_queue_time,
                response_tx,
            } => {
                let current_state = self.get_current_state();
                if current_state != ConnectionState::Connected {
                    let _ = response_tx.send(Err(SatelError::NotConnected));
                    return;
                }
                if created_at.elapsed() > max_queue_time {
                    let _ = response_tx.send(Err(SatelError::MessageExpired));
                    return;
                }
                let cmd_byte = data.first().cloned().unwrap_or(0);
                let result = self
                    .satel_connection_worker_exchange(data, cmd_byte, write_timeout, read_timeout)
                    .await;
                let _ = response_tx.send(result);
            }
            InternalMessage::ExchangePriority {
                data,
                write_timeout,
                read_timeout,
                response_tx,
            } => {
                let current_state = self.get_current_state();
                if current_state == ConnectionState::Disconnected || current_state == ConnectionState::ConnectionLost {
                    let _ = response_tx.send(Err(SatelError::NotConnected));
                    return;
                }
                let cmd_byte = data.first().cloned().unwrap_or(0);
                let result = self
                    .satel_connection_worker_exchange(data, cmd_byte, write_timeout, read_timeout)
                    .await;
                let _ = response_tx.send(result);
            }
            InternalMessage::Connect { response_tx } => {
                let state = self.get_current_state();
                if state == ConnectionState::Connected
                    || state == ConnectionState::Connecting
                    || state == ConnectionState::Handshake
                {
                    let _ = response_tx.send(Err(SatelError::AlreadyConnected));
                } else {
                    let result = self.satel_connection_worker_connect().await;
                    let _ = response_tx.send(result);
                }
            }
            InternalMessage::Disconnect { response_tx } => {
                let state = self.get_current_state();
                if state == ConnectionState::Disconnected {
                    let _ = response_tx.send(Err(SatelError::NotConnected));
                } else {
                    self.satel_connection_worker_disconnect().await;
                    let _ = response_tx.send(Ok(()));
                }
            }
        }
    }

    async fn satel_connection_worker_exchange(
        &mut self,
        data: Vec<u8>,
        expected_cmd: u8,
        write_timeout: Duration,
        read_timeout: Duration,
    ) -> Result<Vec<u8>, SatelError> {
        let stream = self.stream.as_mut().ok_or(SatelError::NotConnected)?;

        // Send
        match timeout(write_timeout, stream.send(data.clone())).await {
            Ok(Ok(_)) => {
                let mut s = self.state.write().unwrap();
                s.telemetry.bytes_sent.fetch_add(data.len(), Ordering::Relaxed);
                s.telemetry.last_send_at = Instant::now();
            }
            Ok(Err(e)) => {
                self.satel_connection_worker_connection_lost().await;
                return Err(SatelError::Io(e));
            }
            Err(_) => return Err(SatelError::Timeout),
        }

        // Receive with filtering Push notifications
        let start = Instant::now();
        while start.elapsed() < read_timeout {
            let remaining = read_timeout.saturating_sub(start.elapsed());
            let stream = self.stream.as_mut().unwrap();
            match timeout(remaining, stream.next()).await {
                Ok(Some(Ok(frame))) => {
                    {
                        let s = self.state.read().unwrap();
                        s.telemetry.bytes_received.fetch_add(frame.len(), Ordering::Relaxed);
                    }
                    if frame.is_empty() {
                        continue;
                    }

                    let is_result_code = frame[0] == 0xEF;
                    let is_accepted = is_result_code && frame.get(1) == Some(&0xFF);

                    if (frame[0] != expected_cmd || is_result_code) && !is_accepted {
                        Self::notify_state_worker(
                            &self.state_worker_tx,
                            StateWorkerMessage::Frame(frame.clone()),
                        )
                        .await;
                    }

                    if frame[0] == expected_cmd || frame[0] == 0xEF {
                        return Ok(frame);
                    }
                }
                Ok(Some(Err(e))) => {
                    self.satel_connection_worker_connection_lost().await;
                    return Err(SatelError::Io(e));
                }
                Ok(None) => {
                    self.satel_connection_worker_connection_lost().await;
                    return Err(SatelError::StreamClosed);
                }
                Err(_) => break,
            }
        }
        Err(SatelError::Timeout)
    }

    // --- Helpery Stanu ---

    fn get_current_state(&self) -> ConnectionState {
        self.state.read().unwrap().telemetry.status.state
    }

    async fn set_state_connecting(&mut self) {
        {
            let mut s = self.state.write().unwrap();
            s.telemetry.status.state = ConnectionState::Connecting;
            s.telemetry.status.last_event_at = Instant::now();
        }
        Self::notify_state_worker(&self.state_worker_tx, StateWorkerMessage::StatusChanged(ConnectionState::Connecting)).await;
    }

    async fn set_state_handshake(&mut self) {
        {
            let mut s = self.state.write().unwrap();
            s.telemetry.status.state = ConnectionState::Handshake;
            s.telemetry.status.last_event_at = Instant::now();
        }
        Self::notify_state_worker(&self.state_worker_tx, StateWorkerMessage::StatusChanged(ConnectionState::Handshake)).await;
    }

    async fn set_state_connected(&mut self) {
        {
            let mut s = self.state.write().unwrap();
            s.telemetry.status.state = ConnectionState::Connected;
            s.telemetry.status.last_event_at = Instant::now();
            s.telemetry.status.failed_attempts = 0;
            s.telemetry.last_connected_at = Some(SystemTime::now());
            s.connection_type = Some(match &self.config.connection {
                ConnectionConfig::Tcp { host, port } => ConnectionType::Tcp(host.clone(), *port),
                ConnectionConfig::Uart { path, .. } => ConnectionType::Uart(path.clone()),
            });
        }
        Self::notify_state_worker(&self.state_worker_tx, StateWorkerMessage::StatusChanged(ConnectionState::Connected)).await;
    }

    async fn satel_connection_worker_connection_lost(&mut self) {
        self.stream = None;
        {
            let mut s = self.state.write().unwrap();
            s.telemetry.status.state = ConnectionState::ConnectionLost;
            s.telemetry.status.last_event_at = Instant::now();
            s.telemetry.status.failed_attempts += 1;
        }
        Self::notify_state_worker(&self.state_worker_tx, StateWorkerMessage::StatusChanged(ConnectionState::ConnectionLost)).await;
    }

    async fn satel_connection_worker_disconnect(&mut self) {
        self.stream = None;
        {
            let mut s = self.state.write().unwrap();
            s.telemetry.status.state = ConnectionState::Disconnected;
            s.telemetry.status.last_event_at = Instant::now();
            s.telemetry.status.failed_attempts = 0;
            s.telemetry.reset();
        }
        Self::notify_state_worker(&self.state_worker_tx, StateWorkerMessage::StatusChanged(ConnectionState::Disconnected)).await;
    }

    async fn satel_connection_worker_connect(&mut self) -> Result<(), SatelError> {
        self.set_state_connecting().await;

        let conn_timeout = Duration::from_millis(self.config.read_timeout_ms);

        // STEP 1: Physical transport connection
        let stream = match self.satel_connection_worker_connect_physical(conn_timeout).await {
            Ok(s) => s,
            Err(e) => {
                self.satel_connection_worker_connection_lost().await;
                return Err(e);
            }
        };

        self.stream = Some(Framed::new(stream, SatelCodec));
        self.set_state_handshake().await;

        sleep(Duration::from_millis(200)).await;

        // STEP 2: Protocol Handshake
        self.satel_connection_worker_connect_handshake(conn_timeout).await?;

        // STEP 3: Configure Auto-read push notifications
        if self.config.is_auto_read_enabled() {
            self.satel_connection_worker_connect_auto_read(conn_timeout).await?;
        }

        self.set_state_connected().await;
        tracing::info!("Connection and Handshake successfully established");
        Ok(())
    }

    async fn satel_connection_worker_connect_physical(
        &mut self,
        conn_timeout: Duration,
    ) -> Result<Box<dyn AsyncReadWrite>, SatelError> {
        tracing::info!("Attempting physical connection...");

        let connection_config = self.config.connection.clone();
        let encryption = self.config.encryption;
        let integration_key = self.config.integration_key.clone();

        let stream_result = timeout(conn_timeout, async move {
            match connection_config {
                ConnectionConfig::Tcp { host, port } => {
                    let stream = TcpStream::connect((host.as_str(), port))
                        .await
                        .map_err(SatelError::from)?;
                    let boxed: Box<dyn AsyncReadWrite> = if encryption {
                        let key_str = integration_key.as_deref().ok_or_else(|| {
                            SatelError::InvalidIntegrationKey(
                                "encryption is enabled but integration_key is not set".into(),
                            )
                        })?;
                        let aes_key = crate::encryption::derive_aes_key(key_str);
                        tracing::info!("TCP connection established with AES-192 encryption");
                        Box::new(crate::encryption::EncryptedStream::new(stream, aes_key))
                    } else {
                        tracing::info!("TCP connection established (plaintext)");
                        Box::new(stream)
                    };
                    Ok::<Box<dyn AsyncReadWrite>, SatelError>(boxed)
                }
                ConnectionConfig::Uart { path, baud_rate } => {
                    let stream = tokio_serial::new(path, baud_rate)
                        .open_native_async()
                        .map_err(SatelError::from)?;
                    let boxed: Box<dyn AsyncReadWrite> = Box::new(stream);
                    Ok::<Box<dyn AsyncReadWrite>, SatelError>(boxed)
                }
            }
        })
        .await;

        match stream_result {
            Ok(Ok(s)) => Ok(s),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(SatelError::Timeout),
        }
    }

    async fn satel_connection_worker_connect_handshake(
        &mut self,
        conn_timeout: Duration,
    ) -> Result<(), SatelError> {
        // 2a. Query ETHM/INT-RS module version
        let cmd_ethm = vec![SatelCommand::ModuleVersion.to_byte()];
        match self
            .satel_connection_worker_exchange(cmd_ethm, 0x7C, conn_timeout, conn_timeout)
            .await
        {
            Ok(response) => {
                let version = process_ethm_version(&response)?;
                Self::notify_state_worker(&self.state_worker_tx, StateWorkerMessage::EthmVersion(version)).await;
            }
            Err(e) => {
                tracing::error!("Handshake: critical error querying module version: {:?}", e);
                self.satel_connection_worker_connection_lost().await;
                return Err(e);
            }
        }

        // 2b. Query Integra panel version
        let cmd_integra = vec![SatelCommand::IntegraVersion.to_byte()];
        match self
            .satel_connection_worker_exchange(cmd_integra, 0x7E, conn_timeout, conn_timeout)
            .await
        {
            Ok(response) => {
                let version = process_integra_version(&response)?;
                Self::notify_state_worker(&self.state_worker_tx, StateWorkerMessage::IntegraVersion(version)).await;
            }
            Err(e) => {
                tracing::error!("Handshake: critical error querying panel version: {:?}", e);
                self.satel_connection_worker_connection_lost().await;
                return Err(e);
            }
        }

        sleep(Duration::from_millis(100)).await;
        Ok(())
    }

    async fn satel_connection_worker_connect_auto_read(
        &mut self,
        conn_timeout: Duration,
    ) -> Result<(), SatelError> {
        let support_14_byte_mask = {
            let s = self.state.read().unwrap();
            s.ethm_version
                .as_ref()
                .map(|v| v.capabilities.support_8_troubles_groups)
                .unwrap_or(false)
        };

        let mask = Self::satel_connection_worker_connect_build_push_mask(&self.config, support_14_byte_mask);
        let mut auto_push_data = vec![SatelCommand::ListOfNewData.to_byte()];
        auto_push_data.extend_from_slice(&mask);

        match self
            .satel_connection_worker_exchange(auto_push_data, 0x7F, conn_timeout, conn_timeout)
            .await
        {
            Ok(response) => {
                tracing::info!("Handshake: push notification configuration successful");
                let report = process_auto_read_response(&self.config, support_14_byte_mask, &response);
                Self::notify_state_worker(&self.state_worker_tx, StateWorkerMessage::AutoReadReport(report)).await;
                Ok(())
            }
            Err(e) => {
                tracing::warn!("Handshake: error during push notification configuration: {:?}", e);
                self.satel_connection_worker_connection_lost().await;
                Err(e)
            }
        }
    }

    fn satel_connection_worker_connect_build_push_mask(config: &Config, support_14_byte_mask: bool) -> Vec<u8> {
        let mask_len = if support_14_byte_mask { 14 } else { 12 };
        let mut mask = vec![0u8; mask_len];

        if config.auto_read_zones_violation { mask[0] |= 1 << 0; }
        if config.auto_read_zones_tamper { mask[0] |= 1 << 1; }
        if config.auto_read_zones_alarm { mask[0] |= 1 << 2; }
        if config.auto_read_zones_tamper_alarm { mask[0] |= 1 << 3; }
        if config.auto_read_zones_alarm_memory { mask[0] |= 1 << 4; }
        if config.auto_read_zones_tamper_alarm_memory { mask[0] |= 1 << 5; }
        if config.auto_read_zones_bypass { mask[0] |= 1 << 6; }
        if config.auto_read_zones_no_violation_trouble { mask[0] |= 1 << 7; }
        if config.auto_read_zones_long_violation_trouble { mask[1] |= 1 << 0; }
        if config.auto_read_partitions_armed_suppressed { mask[1] |= 1 << 1; }
        if config.auto_read_partitions_armed_really { mask[1] |= 1 << 2; }
        if config.auto_read_partitions_alarm { mask[2] |= 1 << 3; }
        if config.auto_read_partitions_alarm_memory { mask[2] |= 1 << 5; }
        if config.auto_read_partitions_entry_time { mask[1] |= 1 << 6; }
        if config.auto_read_partitions_exit_time {
            mask[1] |= 1 << 7;
            mask[2] |= 1 << 0;
        }
        if config.auto_read_outputs_state { mask[2] |= 1 << 7; }
        if config.auto_read_system_troubles {
            mask[3] |= 1 << 2;
            mask[3] |= 1 << 3;
            mask[3] |= 1 << 4;
            mask[3] |= 1 << 5;
            mask[3] |= 1 << 6;
            mask[3] |= 1 << 7;
            mask[5] |= 1 << 4;
            mask[5] |= 1 << 5;
            mask[6] |= 1 << 0;
        }
        if config.auto_read_troubles_memory {
            mask[4] |= 1 << 0;
            mask[4] |= 1 << 1;
            mask[4] |= 1 << 2;
            mask[4] |= 1 << 3;
            mask[4] |= 1 << 4;
            mask[5] |= 1 << 6;
            mask[5] |= 1 << 7;
            mask[6] |= 1 << 1;
        }

        mask
    }

    fn calculate_backoff(&self) -> Duration {
        let attempts = self.state.read().unwrap().telemetry.status.failed_attempts;
        if attempts == 0 {
            return Duration::from_millis(500);
        }
        let ms = 500 * (2u64.pow(attempts.saturating_sub(1).min(7)));
        Duration::from_millis(ms.min(60000))
    }

    pub async fn notify_state_worker(
        state_worker_tx: &Option<mpsc::Sender<StateWorkerMessage>>,
        msg: StateWorkerMessage,
    ) {
        if let Some(tx) = state_worker_tx {
            let _ = tx.send(msg).await;
        }
    }

    async fn receive_push_internal(
        stream_opt: &mut Option<FramedStream>,
        state: &SatelStateHandle,
        state_worker_tx: &Option<mpsc::Sender<StateWorkerMessage>>,
    ) -> Result<(), SatelError> {
        let Some(ref mut stream) = stream_opt else {
            return Ok(());
        };
        match timeout(Duration::from_millis(100), stream.next()).await {
            Ok(Some(Ok(frame))) => {
                {
                    let s = state.read().map_err(|_| SatelError::StatePoisoned)?;
                    s.telemetry.bytes_received.fetch_add(frame.len(), Ordering::Relaxed);
                }

                let is_accepted = frame[0] == 0xEF && frame.get(1) == Some(&0xFF);
                if !is_accepted {
                    Self::notify_state_worker(state_worker_tx, StateWorkerMessage::Frame(frame)).await;
                }
                Ok(())
            }
            Ok(Some(Err(e))) => Err(SatelError::Io(e)),
            Ok(None) => Err(SatelError::StreamClosed),
            Err(_) => Ok(()),
        }
    }
}
```

---

## Interactive Examples (examples/)

### examples/1_03_encrypted_connection.rs

```rust
﻿//! Example 1_03: Connect to Satel Integra panel using encrypted communication (AES-192 ECB).
//!
//! ============================================================================
//! 1. ENCRYPTED INTEGRATION PROTOCOL OVERVIEW:
//! ============================================================================
//! The ETHM-1 Plus module supports transparent AES-192 ECB encrypted communication
//! over TCP/IP using an Integration Key configured in DLOADX:
//!
//! DLOADX Configuration:
//!   1. Open DLOADX -> Structure -> Hardware -> Modules -> ETHM-1 Plus.
//!   2. Enable "Integration (open protocol)".
//!   3. Enable "Encrypted integration".
//!   4. Set "Integration key" (up to 12 alphanumeric ASCII characters).
//!   5. Set TCP port (default: 7094).
//!
//! Client Configuration in Rust:
//!   ```rust
//!   let config = Config {
//!       connection: ConnectionConfig::Tcp {
//!           host: "192.168.1.100".to_string(),
//!           port: 7094,
//!       },
//!       encryption: true,
//!       integration_key: Some("MyKey123".to_string()),
//!       ..Config::default()
//!   };
//!   ```
//!
//! ============================================================================
//! 2. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with environment variables:
//!   cargo run --example 1_03_encrypted_connection
//!
//! Environment variables (optional):
//!   SATEL_HOST            - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT            - TCP port (default: 7094)
//!   SATEL_INTEGRATION_KEY - Integration key configured in DLOADX (default: "MyKey123")
//!   SATEL_CODE            - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelIntegra};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let integration_key = env::var("SATEL_INTEGRATION_KEY").unwrap_or_else(|_| "Jmtp".to_string());
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Encrypted Communication (AES-192)");
    println!("==================================================");
    println!("Host:            {}:{}", host, port);
    println!("Encryption:      ENABLED (AES-192 ECB)");
    println!("Integration key: {}", integration_key);
    println!("User PIN:        {}", user_code.as_deref().unwrap_or("None"));
    println!("--------------------------------------------------");

    // 1. Configure encrypted client
    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        encryption: true,
        integration_key: Some(integration_key),
        user_code,
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 2. Connect to the alarm panel over encrypted transport
    println!("Connecting over encrypted TCP socket...");
    satel.connect().await?;
    println!("Encrypted connection established successfully!\n");

    // 3. Query Integra panel version over encrypted connection
    println!("--- Querying Alarm Panel Version ---");
    let panel_version = satel.get_integra_version().await?;
    println!("Panel Model:     {}", panel_version.model);
    println!("Firmware:        {}", panel_version.firmware_version);
    println!("Language:        {}", panel_version.language);
    println!("Max I/O zones:   {}", panel_version.io_count);

    // 4. Query ETHM module version over encrypted connection
    println!("\n--- Querying ETHM-1 Module Version ---");
    let ethm_version = satel.get_ethm_version().await?;
    println!("ETHM Firmware:   {}", ethm_version.version_raw);
    println!("Capabilities:    {:?}", ethm_version.capabilities);

    println!("\n==================================================");
    println!(" Encrypted communication test completed successfully!");
    println!("==================================================");

    Ok(())
}
```

### examples/2_01_get_version.rs

```rust
﻿//! Example 2_01: Connect to Satel Integra panel and query device & module versions.
//!
//! ============================================================================
//! 1. 2-STEP VERSION WORKFLOW (NETWORK FETCH VS CACHE READ):
//! ============================================================================
//! The client operates with a 2-step data model:
//!
//! STEP 1: Network Queries (Fetches version frames from panel & updates internal cache):
//!   Command | Async Method                        | Description
//!   --------+-------------------------------------+---------------------------------------------------
//!   0x7E    | `satel.get_integra_version().await` | Query Integra alarm panel version, model, I/O capacity
//!   0x7C    | `satel.get_ethm_version().await`    | Query ETHM-1 / INT-RS communication module version & caps
//!
//! STEP 2: Instant Cache Inspection (Synchronous, zero network overhead):
//!   - `satel.get_cached_version()`:
//!       Returns `Option<IntegraVersion>` from local memory cache without network latency.
//!   - `satel.state_handle()`:
//!       Access `state.integra_version` and `state.ethm_version` directly via `RwLock`.
//!
//! ============================================================================
//! 2. HARDWARE & CAPABILITIES NOTES:
//! ============================================================================
//! - `IntegraVersion` fields:
//!     * `model`: Derived panel model name (e.g. "INTEGRA 128", "INTEGRA 256 Plus").
//!     * `firmware_version`: Firmware release string (e.g. "1.22 2023-05-10").
//!     * `language`: Configured panel language.
//!     * `io_count`: Maximum supported inputs/outputs (24, 32, 64, 128, 256).
//!     * `stored_in_flash`: Whether the firmware runs from FLASH memory.
//! - `EthmCapabilities` features:
//!     * `support_32_byte_frames`: Module supports 256 I/O bitmasks (32-byte frames).
//!     * `support_8_troubles_groups`: Module supports expanded 8 trouble groups (0x7F).
//!     * `support_extended_arming_commands`: Module supports mode-specific partition arming.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 2_01_get_version
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel / ETHM-1 Plus module (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelIntegra};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Read connection parameters from environment variables (or fall back to defaults)
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Query Panel & Module Versions");
    println!("==================================================");
    println!("Connection parameters:");
    println!("  Host:      {}", host);
    println!("  Port:      {}", port);
    println!("  User code: {:?}", user_code);
    println!("--------------------------------------------------");

    // 2. Configure client
    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 3. Connect to the alarm panel
    println!("Connecting to the control panel...");
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 4. STEP 1: Query Integra alarm panel version over the network (0x7E)
    println!("--- Step 1: Querying Versions Over Network ---");
    println!("Querying Integra panel version (0x7E)...");
    match satel.get_integra_version().await {
        Ok(ver) => {
            println!("  Panel model:      {}", ver.model);
            println!("  Firmware version: {}", ver.firmware_version);
            println!("  Language:         {}", ver.language);
            println!("  I/O capacity:     {}", ver.io_count);
            println!("  Stored in FLASH:  {}", if ver.stored_in_flash { "Yes" } else { "No" });
            println!("  Read timestamp:   {}", ver.read_at.format("%Y-%m-%d %H:%M:%S"));
        }
        Err(e) => {
            eprintln!("  Error reading panel version: {}", e);
        }
    }
    println!();

    // Query ETHM-1 Plus / INT-RS module version & capabilities (0x7C)
    println!("Querying ETHM-1 / INT-RS communication module version & capabilities (0x7C)...");
    match satel.get_ethm_version().await {
        Ok(ethm) => {
            println!("  Module version:             {}", ethm.version_raw);
            println!("  Capabilities:");
            println!("    - 32-byte frames (256 IO): {}", if ethm.capabilities.support_32_byte_frames { "Supported" } else { "No" });
            println!("    - 8 trouble groups:       {}", if ethm.capabilities.support_8_troubles_groups { "Supported" } else { "No" });
            println!("    - Extended arming commands: {}", if ethm.capabilities.support_extended_arming_commands { "Supported" } else { "No" });
            println!("  Read timestamp:             {}", ethm.read_at.format("%Y-%m-%d %H:%M:%S"));
        }
        Err(e) => {
            eprintln!("  Error reading module version: {}", e);
        }
    }
    println!();

    // 5. STEP 2: Instant Cache Read (Zero Network I/O)
    println!("--- Step 2: Instant Cache Verification (Zero Network I/O) ---");
    if let Ok(Some(cached_ver)) = satel.get_cached_version() {
        println!(
            "  Cached Panel: {} (Firmware: {}, Read at: {})",
            cached_ver.model,
            cached_ver.firmware_version,
            cached_ver.read_at.format("%H:%M:%S")
        );
    }
    println!();

    // 6. Disconnect from panel
    println!("Disconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}
```

### examples/2_02_get_names.rs

```rust
﻿//! Example 2_02: Dynamically query ALL names of partitions, zones, and outputs based on panel capacity.
//!
//! ============================================================================
//! 1. ADAPTIVE NAME QUERY WORKFLOW:
//! ============================================================================
//! The Integra protocol stores user-assigned UTF-8 names (16 characters max each)
//! for physical/logical devices inside the panel (Command 0xEE).
//!
//! Name queries are executed asynchronously on demand:
//!   - `satel.get_zone_name(id).await`       -> Zone name (e.g. "Salon Ruch")
//!   - `satel.get_partition_name(id).await`  -> Partition name (e.g. "Dom Parter")
//!   - `satel.get_output_name(id).await`     -> Output name (e.g. "Syrena Zewn.")
//!
//! This example queries panel capacity (0x7E) first, then iterates over active IDs:
//!   - Partitions: 1..=partitions_count (4 for Integra 24, 8 for 32/64, 16 for 128, 32 for 256)
//!   - Zones (Inputs): 1..=io_count (24, 32, 64, 128, or 256)
//!   - Outputs: 1..=io_count (24, 32, 64, 128, or 256)
//!
//! ============================================================================
//! 2. CACHE ACCESS:
//! ============================================================================
//! After fetching names over the network, names are stored in local memory cache:
//!   - `satel.get_cached_zone_name(id)`
//!   - `satel.get_cached_partition_name(id)`
//!   - `satel.get_cached_output_name(id)`
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 2_02_get_names
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelIntegra};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Connection parameters
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Query All System Object Names");
    println!("==================================================");

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 2. Connect
    println!("Connecting to the panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 3. Query panel version to determine capacity
    let version = satel.get_integra_version().await?;
    let io_count = version.io_count;
    let max_partitions = match io_count {
        24 => 4,
        32 | 64 => 8,
        128 => 16,
        _ => 32,
    };

    println!(
        "Integra panel model: {} ({} zones, {} outputs, {} partitions)\n",
        version.model, io_count, io_count, max_partitions
    );

    // 4. STEP 1: Fetch Partition Names (1..=max_partitions)
    println!("--- Fetching Partition Names from Panel (1..={}) ---", max_partitions);
    for partition_id in 1..=max_partitions {
        match satel.get_partition_name(partition_id).await {
            Ok(partition) => {
                if partition.name.trim().is_empty() {
                    println!("  Partition #{:02}: [Empty / Unassigned]", partition_id);
                } else {
                    println!("  Partition #{:02}: \"{}\"", partition_id, partition.name);
                }
            }
            Err(e) => eprintln!("  Partition #{:02}: Error ({})", partition_id, e),
        }
    }
    println!();

    // 5. STEP 1: Fetch ALL Zone Names (1..=io_count)
    println!("--- Fetching Zone Names from Panel (1..={}) ---", io_count);
    for zone_id in 1..=io_count {
        match satel.get_zone_name(zone_id).await {
            Ok(zone) => {
                if zone.name.trim().is_empty() {
                    println!("  Zone #{:03}: [Empty / Unassigned]", zone_id);
                } else {
                    println!("  Zone #{:03}: \"{}\"", zone_id, zone.name);
                }
            }
            Err(e) => eprintln!("  Zone #{:03}: Error ({})", zone_id, e),
        }
    }
    println!();

    // 6. STEP 1: Fetch ALL Output Names (1..=io_count)
    println!("--- Fetching Output Names from Panel (1..={}) ---", io_count);
    for output_id in 1..=io_count {
        match satel.get_output_name(output_id).await {
            Ok(output) => {
                if output.name.trim().is_empty() {
                    println!("  Output #{:03}: [Empty / Unassigned]", output_id);
                } else {
                    println!("  Output #{:03}: \"{}\"", output_id, output.name);
                }
            }
            Err(e) => eprintln!("  Output #{:03}: Error ({})", output_id, e),
        }
    }
    println!();

    // 7. STEP 2: Inspect Local Cache (Synchronous read)
    println!("--- Step 2: Instant Cache Verification (Zero Network I/O) ---");
    println!("Quick verification of first 4 items in local memory cache:");
    for id in 1..=4.min(max_partitions) {
        if let Ok(Some(cached)) = satel.get_cached_partition_name(id) {
            println!("  Cached Partition #{:02}: \"{}\"", id, cached.name);
        }
    }
    for id in 1..=4.min(io_count) {
        if let Ok(Some(cached)) = satel.get_cached_zone_name(id) {
            println!("  Cached Zone      #{:03}: \"{}\"", id, cached.name);
        }
    }
    for id in 1..=4.min(io_count) {
        if let Ok(Some(cached)) = satel.get_cached_output_name(id) {
            println!("  Cached Output    #{:03}: \"{}\"", id, cached.name);
        }
    }
    println!();

    // 8. Disconnect cleanly
    println!("Disconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}
```

### examples/2_03_get_zones_status.rs

```rust
﻿//! Example 2_03: Query real-time status of all zones with Software State Inversion demo.
//!
//! ============================================================================
//! 1. 2-STEP ZONE STATUS WORKFLOW (NETWORK FETCH VS CACHE READ):
//! ============================================================================
//! The client operates with a 2-step data model:
//!
//! STEP 1: Network Queries (Fetches latest data from panel & updates internal cache):
//!   Command | Async Method                              | Description
//!   --------+-------------------------------------------+---------------------------------------------------
//!   0x00    | `satel.get_zones_violation().await`       | Active violation (e.g. PIR movement, contact opened)
//!   0x01    | `satel.get_zones_tamper().await`          | Line/case tamper state
//!   0x02    | `satel.get_zones_alarm().await`           | Active alarm triggered by zone
//!   0x03    | `satel.get_zones_tamper_alarm().await`    | Active tamper alarm triggered by zone
//!   0x04    | `satel.get_zones_alarm_memory().await`    | Alarm memory indicator (latched until cleared)
//!   0x05    | `satel.get_zones_tamper_alarm_memory().await` | Tamper alarm memory indicator
//!   0x06    | `satel.get_zones_bypass().await`          | Bypassed / disabled zone state
//!   0x07    | `satel.get_zones_no_violation_trouble().await` | Trouble: No violation detected within expected time
//!   0x08    | `satel.get_zones_long_violation_trouble().await` | Trouble: Zone continuously violated for too long
//!
//! STEP 2: Instant Cache Inspection (Synchronous, zero network overhead):
//!   - `satel.get_cached_zone_status(zone_id)`:
//!       Returns aggregated `ZoneStatus` struct for a single zone (name, temperature,
//!       all 9 state booleans, and individual timestamps).
//!   - `satel.get_cached_zone_temperature(zone_id)`:
//!       Returns `Option<ZoneTemperature>` from local cache.
//!   - `satel.get_cached_zone_name(zone_id)`:
//!       Returns `Option<ZoneName>` from local cache.
//!   - `satel.state_handle()`:
//!       Returns an `RwLock` read handle to entire internal `SatelState` for bulk read.
//!
//! ============================================================================
//! 2. LOGICAL STATE INVERSION (INVERT LIST):
//! ============================================================================
//! The library provides optional software-level logical inversion for any of the 9 zone states.
//! This directly toggles the reported boolean value:
//!   - Violated / Active (true)  <--> Inverted to Normal / OK (false)
//!   - Normal / OK (false)       <--> Inverted to Violated / Active (true)
//!
//! IMPORTANT DISTINCTION:
//! This is purely logical state inversion in the client software. It is completely
//! independent and distinct from hardware zone wiring configurations (NO, NC, EOL, 2EOL)
//! which are configured on the panel level using DLOADX.
//!
//! AVAILABLE INVERSION FIELDS IN `Config`:
//!   - `io_violation_invert`:            Vec<u16> (0x00 Violation <-> Normal)
//!   - `io_tamper_invert`:               Vec<u16> (0x01 Tamper <-> Normal)
//!   - `io_alarm_invert`:                Vec<u16> (0x02 Alarm <-> Normal)
//!   - `io_tamper_alarm_invert`:         Vec<u16> (0x03 Tamper Alarm <-> Normal)
//!   - `io_alarm_memory_invert`:         Vec<u16> (0x04 Alarm Memory <-> Normal)
//!   - `io_tamper_alarm_memory_invert`:  Vec<u16> (0x05 Tamper Alarm Memory <-> Normal)
//!   - `io_bypass_invert`:               Vec<u16> (0x06 Bypass <-> Normal)
//!   - `io_no_violation_trouble_invert`: Vec<u16> (0x07 "No violation" trouble <-> Normal)
//!   - `io_long_violation_trouble_invert`: Vec<u16> (0x08 "Long violation" trouble <-> Normal)
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 2_03_get_zones_status
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelIntegra};
use std::env;

/// Example list of zone IDs to apply software inversion to (e.g. zones 1 and 2)
const DEMO_INVERT_ZONES: &[u16] = &[1, 2];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Connection parameters
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Query Zones Real-Time Status");
    println!("==================================================");
    println!("Software inversion demo enabled for zones: {:?}", DEMO_INVERT_ZONES);
    println!("(Logical states for these zones will be inverted: Active <-> Normal)\n");

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,
        // Demonstrating logical state inversion for zones 1 and 2:
        io_violation_invert: DEMO_INVERT_ZONES.to_vec(),
        io_tamper_invert: DEMO_INVERT_ZONES.to_vec(),
        io_alarm_invert: DEMO_INVERT_ZONES.to_vec(),
        io_tamper_alarm_invert: DEMO_INVERT_ZONES.to_vec(),
        io_alarm_memory_invert: DEMO_INVERT_ZONES.to_vec(),
        io_tamper_alarm_memory_invert: DEMO_INVERT_ZONES.to_vec(),
        io_bypass_invert: DEMO_INVERT_ZONES.to_vec(),
        io_no_violation_trouble_invert: DEMO_INVERT_ZONES.to_vec(),
        io_long_violation_trouble_invert: DEMO_INVERT_ZONES.to_vec(),
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 2. Connect
    println!("Connecting to the panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 3. Query panel version to determine the exact number of supported zones
    let version = satel.get_integra_version().await?;
    let io_count = version.io_count;
    println!("Integra panel model: {}, supported zones: {}\n", version.model, io_count);

    // 4. Refresh all zone statuses from panel
    println!("Fetching zone statuses from control panel (applying inversion lists)...");
    satel.get_zones_violation().await?;
    satel.get_zones_tamper().await?;
    satel.get_zones_alarm().await?;
    satel.get_zones_tamper_alarm().await?;
    satel.get_zones_alarm_memory().await?;
    satel.get_zones_tamper_alarm_memory().await?;
    satel.get_zones_bypass().await?;
    satel.get_zones_no_violation_trouble().await?;
    satel.get_zones_long_violation_trouble().await?;
    println!("Zone statuses refreshed successfully.\n");

    // 5. Display ALL zones status table
    println!("--- Zones Status Table (1..={}) ---", io_count);
    println!("{:-<110}", "");
    println!(
        "{: <8} | {: <11} | {: <8} | {: <7} | {: <10} | {: <8} | {: <16}",
        "Zone", "Violation", "Tamper", "Alarm", "Alarm Mem", "Bypass", "Trouble (No/Long)"
    );
    println!("{:-<110}", "");

    let mut violation_count = 0;
    let mut tamper_count = 0;
    let mut alarm_count = 0;
    let mut bypass_count = 0;

    for zone_id in 1..=io_count {
        if let Ok(Some(status)) = satel.get_cached_zone_status(zone_id) {
            if status.violation_state {
                violation_count += 1;
            }
            if status.tamper_state || status.tamper_alarm_state {
                tamper_count += 1;
            }
            if status.alarm_state || status.tamper_alarm_state {
                alarm_count += 1;
            }
            if status.bypass_state {
                bypass_count += 1;
            }

            let trouble_text =
                match (status.no_violation_trouble_state, status.long_violation_trouble_state) {
                    (true, true) => "NoViol+LongViol",
                    (true, false) => "No Violation",
                    (false, true) => "Long Violation",
                    (false, false) => "ok",
                };

            let is_inverted = DEMO_INVERT_ZONES.contains(&zone_id);
            let zone_label = if is_inverted {
                format!("#{:03}*", status.id)
            } else {
                format!("#{:03} ", status.id)
            };

            println!(
                "{: <8} | {: <11} | {: <8} | {: <7} | {: <10} | {: <8} | {: <16}",
                zone_label,
                if status.violation_state { "VIOLATED" } else { "ok" },
                if status.tamper_state { "TAMPER" } else { "ok" },
                if status.alarm_state { "ALARM" } else { "ok" },
                if status.alarm_memory_state { "MEMORY" } else { "ok" },
                if status.bypass_state { "BYPASSED" } else { "ok" },
                trouble_text
            );
        }
    }
    println!("{:-<110}", "");
    println!("* Marked zone has active software inversion applied in Config.");
    println!(
        "Summary: Violations: {} | Tampers: {} | Alarms: {} | Bypassed: {}\n",
        violation_count, tamper_count, alarm_count, bypass_count
    );

    // 6. Disconnect cleanly
    println!("Disconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}
```

### examples/2_04_get_outputs_status.rs

```rust
﻿//! Example 2_04: Query real-time status of all outputs (ON / OFF) from Satel Integra.
//!
//! ============================================================================
//! 1. 2-STEP OUTPUTS STATUS WORKFLOW (NETWORK FETCH VS CACHE READ):
//! ============================================================================
//! The client operates with a 2-step data model:
//!
//! STEP 1: Network Query (Fetches all outputs state from panel & updates internal cache):
//!   Command | Async Method                        | Description
//!   --------+-------------------------------------+---------------------------------------------------
//!   0x17    | `satel.get_outputs_state().await`   | Query real-time state of all outputs (ON / OFF)
//!
//! STEP 2: Instant Cache Inspection (Synchronous, zero network overhead):
//!   - `satel.get_cached_output_name(output_id)`:
//!       Returns `Option<OutputName>` from local cache.
//!   - `satel.state_handle()`:
//!       Returns an `RwLock` read handle to entire internal `SatelState`.
//!       Access `state.outputs[0..io_count]` for individual `OutputStatus` items containing:
//!         * `id`: Output number (1..=256)
//!         * `state`: Boolean status (`true` = ON/Active, `false` = OFF/Inactive)
//!         * `state_read_at`: Timestamp of the last successful state update
//!         * `output_name`: Optional cached output name
//!
//! ============================================================================
//! 2. HARDWARE & LOGICAL OUTPUT NOTES:
//! ============================================================================
//! - In Integra panels, outputs can represent sirens, strobe lights, locks, heating
//!   valves, lighting relays, or internal logic gates used by the automation system.
//! - The number of available outputs depends on the panel model (e.g. Integra 24: 20 outputs,
//!   Integra 64: 64 outputs, Integra 128: 128 outputs, Integra 256 Plus: 256 outputs).
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 2_04_get_outputs_status
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelIntegra};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Connection parameters
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Query Outputs Real-Time Status");
    println!("==================================================");

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 2. Connect
    println!("Connecting to the panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 3. Query panel version to determine IO capacity
    let version = satel.get_integra_version().await?;
    let io_count = version.io_count;
    println!("Integra panel model: {}, supported outputs: {}\n", version.model, io_count);

    // 4. Fetch real-time state of all outputs
    println!("Fetching real-time output states from panel...");
    satel.get_outputs_state().await?;
    println!("Output states refreshed successfully.\n");

    // 5. Display ALL outputs status table
    println!("--- Outputs Status Table (1..={}) ---", io_count);
    println!("{:-<45}", "");
    println!("{: <5} | {: <14} | {: <10}", "ID", "State", "Updated At");
    println!("{:-<45}", "");

    let mut active_count = 0;
    let mut inactive_count = 0;

    // Scoped block ensures the RwLock read guard is dropped immediately after reading
    {
        let handle = satel.state_handle();
        let state = handle.read().unwrap();

        for output in &state.outputs[0..io_count as usize] {
            if output.state {
                active_count += 1;
            } else {
                inactive_count += 1;
            }

            let state_display = if output.state {
                "ON  [ACTIVE]"
            } else {
                "OFF [idle]"
            };

            println!(
                "#{:03}  | {: <14} | {: <10}",
                output.id,
                state_display,
                output.state_read_at.format("%H:%M:%S")
            );
        }
    }

    println!("{:-<45}", "");
    println!(
        "Summary: Active (ON): {} | Inactive (OFF): {}\n",
        active_count, inactive_count
    );

    // 6. Disconnect cleanly
    println!("Disconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}
```

### examples/2_05_get_partitions_status.rs

```rust
﻿//! Example 2_05: Query real-time status of all partitions (arming, alarms, entry/exit times).
//!
//! ============================================================================
//! 1. 2-STEP PARTITIONS STATUS WORKFLOW (NETWORK FETCH VS CACHE READ):
//! ============================================================================
//! The client operates with a 2-step data model:
//!
//! STEP 1: Network Queries (Fetches latest partition data from panel & updates internal cache):
//!   Command | Async Method                               | Description
//!   --------+--------------------------------------------+---------------------------------------------------
//!   0x0A    | `satel.get_partitions_armed_really().await` | Actually armed partitions (full protection active)
//!   0x09    | `satel.get_partitions_armed_suppressed().await` | Armed suppressed partitions (e.g. stay/partial arming)
//!   0x13    | `satel.get_partitions_alarm().await`        | Partitions currently in active alarm state
//!   0x15    | `satel.get_partitions_alarm_memory().await` | Partitions with latched alarm memory
//!   0x0E/F  | `satel.get_partitions_times().await`       | Combined entry delay and exit delay timers (>10s / <10s)
//!
//! STEP 2: Instant Cache Inspection (Synchronous, zero network overhead):
//!   - `satel.get_cached_partition_name(partition_id)`:
//!       Returns `Option<PartitionName>` from local cache.
//!   - `satel.state_handle()`:
//!       Returns an `RwLock` read handle to entire internal `SatelState`.
//!       Access `state.partitions[0..max_partitions]` for `PartitionStatus` items containing:
//!         * `id`: Partition number (1..=32)
//!         * `armed_really`: Boolean (full arming status)
//!         * `armed_suppressed`: Boolean (suppressed/stay arming)
//!         * `alarm`: Boolean (active alarm)
//!         * `alarm_memory`: Boolean (stored alarm flag)
//!         * `entry_time`: Boolean (entry delay timer countdown in progress)
//!         * `exit_time_gt_10s`: Boolean (exit delay > 10 seconds remaining)
//!         * `exit_time_lt_10s`: Boolean (exit delay < 10 seconds remaining)
//!         * `partition_name`: Optional cached partition name
//!         * Individual `read_at` timestamps for each state component
//!
//! ============================================================================
//! 2. INTEGRA PARTITION ARCHITECTURE & CAPACITIES:
//! ============================================================================
//! - Partition capacity is dynamically derived from the panel model:
//!     * Integra 24:       up to 4 partitions
//!     * Integra 32 / 64:  up to 8 partitions
//!     * Integra 128:      up to 16 partitions
//!     * Integra 256 Plus: up to 32 partitions
//! - `Armed Really` indicates physical security is active.
//! - `Armed Suppressed` indicates arming modes where certain interior zones or alarms are bypassed.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 2_05_get_partitions_status
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelIntegra};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Connection parameters
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Query Partitions Real-Time Status");
    println!("==================================================");

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 2. Connect
    println!("Connecting to the panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 3. Query panel version to determine partition capacity
    let version = satel.get_integra_version().await?;
    let max_partitions = match version.io_count {
        24 => 4,
        32 | 64 => 8,
        128 => 16,
        _ => 32,
    };
    println!("Integra panel model: {}, supported partitions: {}\n", version.model, max_partitions);

    // 4. Fetch real-time partition statuses from panel
    println!("Fetching partition statuses from control panel...");
    satel.get_partitions_armed_really().await?;
    satel.get_partitions_armed_suppressed().await?;
    satel.get_partitions_alarm().await?;
    satel.get_partitions_alarm_memory().await?;
    satel.get_partitions_times().await?;
    println!("Partition statuses refreshed successfully.\n");

    // 5. Display ALL partitions status table
    println!("--- Partitions Status Table (1..={}) ---", max_partitions);
    println!("{:-<85}", "");
    println!(
        "{: <5} | {: <14} | {: <14} | {: <7} | {: <10} | {: <10} | {: <10}",
        "ID", "Armed (Really)", "Armed (Suppr)", "Alarm", "Alarm Mem", "Entry Time", "Exit Time"
    );
    println!("{:-<85}", "");

    let mut armed_count = 0;
    let mut alarm_count = 0;
    let mut timing_count = 0;

    // Scoped block ensures the RwLock read guard is dropped immediately after reading
    {
        let handle = satel.state_handle();
        let state = handle.read().unwrap();

        for partition in &state.partitions[0..max_partitions as usize] {
            if partition.armed_really || partition.armed_suppressed {
                armed_count += 1;
            }
            if partition.alarm {
                alarm_count += 1;
            }
            if partition.entry_time || partition.exit_time_gt_10s || partition.exit_time_lt_10s {
                timing_count += 1;
            }

            let exit_time_str = if partition.exit_time_lt_10s {
                "< 10s"
            } else if partition.exit_time_gt_10s {
                "> 10s"
            } else {
                "ok"
            };

            println!(
                "#{:02}   | {: <14} | {: <14} | {: <7} | {: <10} | {: <10} | {: <10}",
                partition.id,
                if partition.armed_really { "ARMED" } else { "disarmed" },
                if partition.armed_suppressed { "SUPPRESSED" } else { "no" },
                if partition.alarm { "ALARM" } else { "ok" },
                if partition.alarm_memory { "MEMORY" } else { "ok" },
                if partition.entry_time { "COUNTING" } else { "ok" },
                exit_time_str
            );
        }
    }

    println!("{:-<85}", "");
    println!(
        "Summary: Armed Partitions: {} | In Alarm: {} | In Entry/Exit Delay: {}\n",
        armed_count, alarm_count, timing_count
    );

    // 6. Disconnect cleanly
    println!("Disconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}
```

### examples/2_06_get_temperatures.rs

```rust
﻿//! Example 2_06: Query temperature from zones with connected wireless/wired sensors.
//!
//! ============================================================================
//! 1. 2-STEP TEMPERATURE WORKFLOW (NETWORK FETCH VS CACHE READ):
//! ============================================================================
//! The client operates with a 2-step data model:
//!
//! STEP 1: Network Query (Fetches temperature from panel via 0x7D & updates internal cache):
//!   Command | Async Method                            | Description
//!   --------+-----------------------------------------+---------------------------------------------------
//!   0x7D    | `satel.get_zone_temperature(zone_id)`   | Query zone temperature (raw 16-bit word, 0.5°C step)
//!
//! STEP 2: Instant Cache Inspection (Synchronous, zero network overhead):
//!   - `satel.get_cached_zone_temperature(zone_id)`:
//!       Returns `Option<ZoneTemperature>` (temperature in °C, read_at timestamp).
//!   - `satel.get_cached_zone_status(zone_id)`:
//!       Returns aggregated `ZoneStatus`, where `.temperature` contains `Option<f32>`.
//!   - `satel.state_handle()`:
//!       Access `state.zones[index]` directly for `temperature_value`, `temperature_read_at`,
//!       and `temperature_status` (Ok / Timeout / CommunicationError / NotSupported).
//!
//! ============================================================================
//! 2. HARDWARE & PROTOCOL SPECIFICS:
//! ============================================================================
//! - Supported wireless sensors: Satel ABAX 2 (e.g. ATD-100, APD-200, AOCD-260, APMD-250).
//! - Measurement range: -55.0°C to +72.5°C with 0.5°C step resolution.
//! - Error 0xFFFF: The panel returns 0xFFFF when the sensor probe is damaged or disconnected.
//! - Timeout: If no sensor is paired with the given zone ID, the query times out after `temp_read_timeout_ms`.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 2_06_get_temperatures
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelError, SatelIntegra};
use std::env;

/// List of zone IDs with actual temperature sensors installed (e.g. ATD-100, APD-200)
const TEMPERATURE_ZONES: &[u16] = &[21, 23, 26, 28, 30];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Connection parameters
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Query Temperature Sensors");
    println!("==================================================");
    println!("Target temperature zones: {:?}\n", TEMPERATURE_ZONES);

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 2. Connect
    println!("Connecting to the panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 3. STEP 1: Query temperature from each configured zone over the network
    println!("--- Step 1: Querying Temperature Over Network (0x7D) ---");
    println!("{:-<55}", "");
    println!("{: <5} | {: <14} | {: <10} | {: <12}", "ID", "Temperature", "Read Time", "Query Status");
    println!("{:-<55}", "");

    let mut sensors_found = 0;

    for &zone_id in TEMPERATURE_ZONES {
        match satel.get_zone_temperature(zone_id).await {
            Ok(temp) => {
                sensors_found += 1;
                println!(
                    "#{:03}  | {: >6.1} °C      | {: <10} | OK",
                    zone_id,
                    temp.temperature,
                    temp.read_at.format("%H:%M:%S")
                );
            }
            Err(SatelError::TemperatureNotSupportedOrTimeOut) | Err(SatelError::Timeout) => {
                println!("#{:03}  | [No sensor]    | -          | Timeout", zone_id);
            }
            Err(SatelError::TemperatureSensorError) => {
                println!("#{:03}  | [Probe Error]  | -          | Error 0xFFFF", zone_id);
            }
            Err(e) => {
                println!("#{:03}  | [Error]        | -          | {}", zone_id, e);
            }
        }
    }

    println!("{:-<55}", "");
    println!(
        "Summary: Successfully read {} / {} temperature sensors.\n",
        sensors_found,
        TEMPERATURE_ZONES.len()
    );

    // 4. STEP 2: Instant cache inspection (synchronous local read)
    println!("--- Step 2: Instant Cache Verification (Zero Network I/O) ---");
    for &zone_id in TEMPERATURE_ZONES {
        if let Ok(Some(cached_temp)) = satel.get_cached_zone_temperature(zone_id) {
            println!(
                "  Cache #{:03}: {:.1} °C (Timestamp: {})",
                zone_id,
                cached_temp.temperature,
                cached_temp.read_at.format("%H:%M:%S")
            );
        }
    }
    println!();

    // 5. Disconnect cleanly
    println!("Disconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}
```

### examples/2_07_get_temperatures_smart_blocking.rs

```rust
﻿//! Example 2_07: Demonstration of Smart Temperature Error Blocking & Self-Healing.
//!
//! ============================================================================
//! 1. SMART ERROR BLOCKING & QUEUE PROTECTION OVERVIEW:
//! ============================================================================
//! Querying temperature from a non-existent, broken, or unconfigured wireless sensor
//! normally results in a 2000ms protocol timeout.
//!
//! If an automation loop queries multiple missing sensors, timeout delays can quickly
//! block the entire communication queue for 10-30 seconds, starving other vital
//! alarm events and relay commands.
//!
//! The `satel_integra` library includes built-in protective logic:
//!   - Method: `satel.get_zone_temperature(zone_id).await` (automatically applies blocking when `temp_blocking_enabled: true`)
//!   - If consecutive timeouts reach `temp_max_timeout_errors` (e.g. 3):
//!       * The sensor is marked as `TemperatureSensorStatus::NotSupported`.
//!       * Subsequent queries return `Err(SatelError::TempTooManyErrors)` INSTANTLY (0 ms),
//!         preventing queue starvation!
//!   - If consecutive 0xFFFF errors reach `temp_max_sensor_errors` (e.g. 10):
//!       * The sensor is marked as `TemperatureSensorStatus::CommunicationError`.
//!       * Subsequent queries are blocked instantly (0 ms).
//!
//! ============================================================================
//! 2. AUTOMATIC SELF-HEALING MECHANISM:
//! ============================================================================
//! Temporary radio interference or low battery events should not permanently lock out
//! a real sensor.
//!
//! Whenever a previously failing sensor successfully returns a temperature reading,
//! its internal error counter is automatically decremented (`counter -= 1`).
//! Once errors drop back below the threshold, the sensor fully self-heals without
//! requiring client restart!
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 2_07_get_temperatures_smart_blocking
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelError, SatelIntegra};
use std::env;
use std::time::Instant;
use tokio::time::{sleep, Duration};

/// IMPORTANT: Specify zone IDs where NO temperature sensor is connected (e.g. regular PIR zones)
const ZONES_WITHOUT_TEMP_SENSORS: &[u16] = &[1, 2];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Temperature Blocking & Self-Healing Demo");
    println!("==================================================");
    println!(
        "Testing missing sensors on zones: {:?}",
        ZONES_WITHOUT_TEMP_SENSORS
    );
    println!("Expected behavior: First 3 queries will timeout (~2000ms), subsequent queries will be blocked instantly (0ms).\n");

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,
        temp_read_timeout_ms: 2000,  // 2s timeout for sensor response
        temp_blocking_enabled: true, // Enable smart error blocking
        temp_max_timeout_errors: 3,  // Block after 3 consecutive timeouts (missing sensor)
        temp_max_sensor_errors: 10,  // Block after 10 sensor 0xFFFF communication errors
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    println!("Connecting to the panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    // Run 11 test cycles (sufficient to trigger both timeout and 0xFFFF sensor error limits)
    for cycle in 1..=11 {
        println!("--- Test Cycle {} / 11 ---", cycle);

        for &zone_id in ZONES_WITHOUT_TEMP_SENSORS {
            let start = Instant::now();

            match satel.get_zone_temperature(zone_id).await {
                Ok(temp) => {
                    let elapsed = start.elapsed();
                    println!(
                        "  Zone #{:03}: Got {:.1}°C (Time: {:?}) -> Error counter decremented (Self-Healing)",
                        zone_id, temp.temperature, elapsed
                    );
                }
                Err(SatelError::TemperatureNotSupportedOrTimeOut) | Err(SatelError::Timeout) => {
                    let elapsed = start.elapsed();
                    println!(
                        "  Zone #{:03}: TIMEOUT (waited: {:?}) -> Counted as failed attempt",
                        zone_id, elapsed
                    );
                }
                Err(SatelError::TempTooManyErrors) => {
                    let elapsed = start.elapsed();
                    println!(
                        "  Zone #{:03}: BLOCKED! Response time: {:?} (Zero wait time - queue protected!)",
                        zone_id, elapsed
                    );
                }
                Err(e) => {
                    let elapsed = start.elapsed();
                    println!("  Zone #{:03}: Error: {} (Time: {:?})", zone_id, e, elapsed);
                }
            }
        }

        println!();
        sleep(Duration::from_millis(500)).await;
    }

    println!("Demo completed successfully. Disconnecting...");
    satel.disconnect().await?;
    println!("Done.");

    Ok(())
}
```

### examples/2_08_get_troubles.rs

```rust
﻿//! Example 2_08: Query real-time system troubles, diagnostic faults, and trouble memory.
//!
//! ============================================================================
//! 1. 2-STEP TROUBLES WORKFLOW (NETWORK FETCH VS CACHE READ):
//! ============================================================================
//! The client operates with a 2-step data model:
//!
//! STEP 1: Network Queries (Fetches system status & trouble parts from panel):
//!   Command | Async Method                              | Description
//!   --------+-------------------------------------------+---------------------------------------------------
//!   0x1A    | `satel.get_system_status().await`         | Query RTC clock, service mode & general trouble flags
//!   0x1B-30 | `satel.get_system_troubles(cmd).await`    | Query active trouble groups 1..8 (0x1B, 0x1C, 0x1D, ...)
//!   0x20-31 | `satel.get_system_troubles(cmd).await`    | Query trouble memory groups 1..8 (0x20, 0x21, 0x22, ...)
//!
//! STEP 2: Instant Cache Inspection (Synchronous, zero network overhead):
//!   - `satel.state_handle()`:
//!       Returns an `RwLock` read handle to entire internal `SatelState`.
//!       Access:
//!         * `state.system_status`: `Option<SystemStatus>` (troubles_present, troubles_memory, service_mode)
//!         * `state.troubles`: `[Vec<bool>; 8]` (currently active troubles mapped via `map_trouble_part_bit`)
//!         * `state.troubles_memory`: `[Vec<bool>; 8]` (stored trouble memory flags)
//!
//! ============================================================================
//! 2. INTEGRA TROUBLE ARCHITECTURE & GROUPS:
//! ============================================================================
//! The Integra panel monitors diagnostic bits split across 8 parts:
//!   - Part 1 (0x1B): Main board AC loss, low battery, missing battery, output overloads, RTC loss, expanders 1-8.
//!   - Part 2 (0x1C): Expanders 9-32 AC loss and battery low.
//!   - Part 3 (0x1D): Expanders 1-32 missing battery.
//!   - Part 4 (0x1E): Expanders 1-32 output overloads & data bus errors.
//!   - Part 5 (0x1F): ETHM/GSM monitoring errors, server connection, GSM signal, zones 1-8 technical troubles.
//!   - Parts 6-8 (0x2C, 0x2D, 0x30): Zones 9-128 technical troubles.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 2_08_get_troubles
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelCommand, SatelIntegra};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Connection parameters
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - System Troubles & Diagnostics");
    println!("==================================================");

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 2. Connect
    println!("Connecting to the panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 3. STEP 1: Query general system status (0x1A)
    println!("--- Step 1: Querying System Status & Troubles Over Network ---");
    let status = satel.get_system_status().await?;
    println!("System Status (0x1A):");
    println!("  Panel RTC Time:     {}", status.rtc.format("%Y-%m-%d %H:%M:%S"));
    println!("  Service Mode:       {}", if status.service_mode { "ACTIVE" } else { "Normal operation" });
    println!("  Troubles Present:   {}", if status.troubles_present { "YES (Active Faults!)" } else { "No (OK)" });
    println!("  Troubles in Memory: {}", if status.troubles_memory { "YES (Stored in Memory)" } else { "No" });
    println!();

    // 3.1 Direct 1:1 strongly-typed query for Part 1 (Main board, AC/DC, Expander power, ETHM)
    println!("--- Direct 1:1 Typed Query: Troubles Part 1 (0x1B) ---");
    let p1 = satel.get_troubles_part1().await?;
    println!("Main Board Power & System Status:");
    println!("  - AC Power Trouble:      {}", p1.main_board.ac_trouble);
    println!("  - Battery Trouble:       {}", p1.main_board.battery_trouble);
    println!("  - Battery Missing:       {}", p1.main_board.no_battery_present);
    println!("  - OUT1..4 Overload:      {}", p1.main_board.out1_trouble || p1.main_board.out2_trouble || p1.main_board.out3_trouble || p1.main_board.out4_trouble);
    println!("  - DT1/DT2 Data Bus:      {}", p1.main_board.dt1_trouble || p1.main_board.dt2_trouble);
    println!("  - Telephone Line:        {}", p1.main_board.tel_line_no_signal || p1.main_board.tel_line_no_voltage);
    println!("  - ETHM Ping / Server:    {}", p1.ethm_ptsa.ethm_ping_trouble || p1.ethm_ptsa.no_server_connection);
    println!();

    // Query all active trouble parts 1..8
    let active_trouble_cmds = [
        SatelCommand::TroublesPart1,
        SatelCommand::TroublesPart2,
        SatelCommand::TroublesPart3,
        SatelCommand::TroublesPart4,
        SatelCommand::TroublesPart5,
        SatelCommand::TroublesPart6,
        SatelCommand::TroublesPart7,
        SatelCommand::TroublesPart8,
    ];

    println!("Fetching all trouble parts (Parts 1..8)...");
    for cmd in active_trouble_cmds {
        let _ = satel.get_troubles(cmd).await?;
    }

    // Query all trouble memory parts 1..8
    let memory_trouble_cmds = [
        SatelCommand::TroublesMemoryPart1,
        SatelCommand::TroublesMemoryPart2,
        SatelCommand::TroublesMemoryPart3,
        SatelCommand::TroublesMemoryPart4,
        SatelCommand::TroublesMemoryPart5,
        SatelCommand::TroublesMemoryPart6,
        SatelCommand::TroublesMemoryPart7,
        SatelCommand::TroublesMemoryPart8,
    ];

    println!("Fetching all trouble memory parts (Parts 1..8)...");
    for cmd in memory_trouble_cmds {
        let _ = satel.get_troubles(cmd).await?;
    }
    println!("All trouble parts updated in internal cache.\n");

    // 4. STEP 2: Inspect Local Cache (Zero Network I/O)
    println!("--- Step 2: Diagnostic Inspection from Local Cache ---");
    {
        let handle = satel.state_handle();
        let state = handle.read().unwrap();

        let mut active_trouble_count = 0;
        let mut memory_trouble_count = 0;

        println!("{:-<85}", "");
        println!(
            "{: <12} | {: <10} | {: <10} | {: <50}",
            "Part / Bit", "Active", "In Memory", "Description / Diagnostic Target"
        );
        println!("{:-<85}", "");

        for part_idx in 0..8 {
            let active_part = &state.troubles[part_idx];
            let memory_part = &state.troubles_memory[part_idx];
            let max_len = active_part.len().max(memory_part.len());

            for bit_idx in 0..max_len {
                let is_active = active_part.get(bit_idx).copied().unwrap_or(false);
                let in_memory = memory_part.get(bit_idx).copied().unwrap_or(false);

                if is_active {
                    active_trouble_count += 1;
                }
                if in_memory {
                    memory_trouble_count += 1;
                }

                // Display row only if trouble is currently active or latched in memory
                if is_active || in_memory {
                    let trouble_type = satel_integra::parsers::map_trouble_part_bit(
                        part_idx as u8,
                        bit_idx as u16,
                    );
                    let description = trouble_type.to_description();

                    println!(
                        "P{}:Bit #{:03} | {: <10} | {: <10} | {: <50}",
                        part_idx + 1,
                        bit_idx,
                        if is_active { "ACTIVE" } else { "-" },
                        if in_memory { "MEMORY" } else { "-" },
                        description
                    );
                }
            }
        }

        println!("{:-<85}", "");
        if active_trouble_count == 0 && memory_trouble_count == 0 {
            println!("Diagnostic Result: System is healthy. Zero active troubles or memory records.\n");
        } else {
            println!(
                "Diagnostic Summary: Active Faults: {} | Memory Records: {}\n",
                active_trouble_count, memory_trouble_count
            );
        }
    }

    // 5. Disconnect cleanly
    println!("Disconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}
```

### examples/2_09_get_time.rs

```rust
﻿//! Example 2_09: Query real-time clock (RTC) and system time from Satel Integra panel.
//!
//! ============================================================================
//! 1. 2-STEP RTC TIME WORKFLOW (NETWORK FETCH VS CACHE READ):
//! ============================================================================
//! The client operates with a 2-step data model:
//!
//! STEP 1: Network Queries (Fetches RTC clock & basic status from panel via 0x1A):
//!   Command | Async Method                        | Description
//!   --------+-------------------------------------+---------------------------------------------------
//!   0x1A    | `satel.get_satel_time().await`      | Query current DateTime<Local> directly from panel RTC
//!   0x1A    | `satel.get_system_status().await`   | Query full SystemStatus (RTC, Service Mode, Troubles)
//!
//! STEP 2: Instant Cache Inspection (Synchronous, zero network overhead):
//!   - `satel.state_handle()`:
//!       Returns an `RwLock` read handle to entire internal `SatelState`.
//!       Access `state.system_status` for:
//!         * `rtc`: Last recorded DateTime<Local> from panel
//!         * `service_mode`: Boolean (service technician mode active)
//!         * `troubles_present`: Boolean (active hardware faults detected)
//!         * `troubles_memory`: Boolean (stored trouble memory flags)
//!
//! Time Drift Inspection:
//!   - Compares panel RTC against local PC system clock (`chrono::Local::now()`)
//!     and displays precise drift delta in seconds.
//!
//! ============================================================================
//! 2. HARDWARE & PROTOCOL NOTES:
//! ============================================================================
//! - Satel Integra panels maintain an internal battery-backed RTC clock.
//! - Protocol frame 0x1A returns BCD-encoded date and time (Century, Year, Month,
//!   Day, Hour, Minute, Second) along with system status bit flags.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 2_09_get_time
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use chrono::Local;
use satel_integra::{Config, ConnectionConfig, SatelIntegra};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Connection parameters
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Query System RTC Clock (0x1A)");
    println!("==================================================");

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 2. Connect
    println!("Connecting to the panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 3. STEP 1: Query panel RTC time over the network (0x1A)
    println!("--- Step 1: Querying RTC Time Over Network ---");
    let panel_time = satel.get_satel_time().await?;
    let pc_time = Local::now();
    let drift_seconds = (panel_time.timestamp() - pc_time.timestamp()).abs();

    println!("  Panel RTC Time:   {}", panel_time.format("%Y-%m-%d %H:%M:%S"));
    println!("  Local PC Time:    {}", pc_time.format("%Y-%m-%d %H:%M:%S"));
    println!("  Clock Difference: {} seconds\n", drift_seconds);

    // Also fetch full status
    let status = satel.get_system_status().await?;
    println!("  System Status Flags:");
    println!("    - Service Mode:       {}", if status.service_mode { "ACTIVE" } else { "Normal operation" });
    println!("    - Troubles Present:   {}", if status.troubles_present { "YES (Active Faults)" } else { "No (OK)" });
    println!("    - Troubles in Memory: {}", if status.troubles_memory { "YES" } else { "No" });
    println!();

    // 4. STEP 2: Instant Cache Inspection (Zero Network I/O)
    println!("--- Step 2: Instant Cache Verification (Zero Network I/O) ---");
    {
        let handle = satel.state_handle();
        let state = handle.read().unwrap();

        if let Some(cached_status) = &state.system_status {
            println!(
                "  Cached Panel RTC Time: {}",
                cached_status.rtc.format("%Y-%m-%d %H:%M:%S")
            );
        } else {
            println!("  No cached status found in memory.");
        }
    }
    println!();

    // 5. Disconnect cleanly
    println!("Disconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}
```

### examples/3_01_control_outputs.rs

```rust
﻿//! Example 3_01: Controlling outputs (ON, OFF, TOGGLE) using global and per-call user codes.
//!
//! ============================================================================
//! 1. OUTPUT CONTROL OVERVIEW & API METHODS:
//! ============================================================================
//! The Satel Integra panel allows authorized output control via three asynchronous methods:
//!
//!   Command | Async Method                                     | Description
//!   --------+--------------------------------------------------+---------------------------------------------------
//!   0x88    | `satel.set_output_on(output_id, pin).await`     | Turn specified output ON (Active)
//!   0x89    | `satel.set_output_off(output_id, pin).await`    | Turn specified output OFF (Inactive)
//!   0x91    | `satel.set_output_toggle(output_id, pin).await` | Toggle specified output state (Switch)
//!
//! PIN Authentication Modes:
//!   - Global PIN (`pin = None`):
//!       Uses the system code pre-configured at startup in `Config.user_code`.
//!       Ideal for automated background daemons and smart home integrations.
//!   - Dynamic / Per-call PIN (`pin = Some("123456")`):
//!       Allows passing a dynamic user PIN per operation.
//!       Ideal for web portals and multi-user mobile apps where each end-user
//!       authorizes actions with their own personal PIN.
//!
//! ============================================================================
//! 2. PROTOCOL CONFIRMATION & SECURITY BEHAVIOR:
//! ============================================================================
//! Receiving a successful response (`Ok(())` / "Command received") indicates that
//! the command packet was successfully received and queued by the ETHM-1 module.
//!
//! Important Satel protocol note: The panel does NOT return error frames when:
//!   - The user PIN is invalid ("Wrong PIN"),
//!   - The user lacks authority to switch outputs ("No Access"),
//!   - The target output is not configured for user switching in DLOADX.
//! In all unauthorized cases, the Integra panel silently drops the relay execution
//! and logs an unauthorized access attempt to its internal event log.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 3_01_control_outputs
//!
//! Environment variables (optional):
//!   SATEL_HOST           - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT           - TCP port (default: 7094)
//!   SATEL_CODE           - Global user access code (default: "1234")
//!   SATEL_DEDICATED_CODE - Dedicated/dynamic user access code (default: "123456")

use satel_integra::{Config, ConnectionConfig, SatelIntegra};
use std::env;
use std::time::Duration;
use tokio::time::sleep;

/// ID of the output to test control on (e.g. relay, light, test output)
const TEST_OUTPUT_ID: u16 = 10;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Connection parameters
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));
    let dedicated_code = env::var("SATEL_DEDICATED_CODE").unwrap_or_else(|_| "123456".to_string());

    println!("==================================================");
    println!(" SATEL INTEGRA - Control Outputs (ON/OFF/TOGGLE)");
    println!("==================================================");
    println!("Target test output ID: #{:03}", TEST_OUTPUT_ID);
    println!("Global PIN (Config):   \"{}\"", user_code.as_deref().unwrap_or("[None]"));
    println!("Dedicated PIN:         \"{}\"\n", dedicated_code);

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code: user_code.clone(), // Global PIN configured here
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 2. Connect
    println!("Connecting to the panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    // ==========================================================
    // ROUND 1: Using globally configured user_code (passing None)
    // ==========================================================
    println!(
        "--- ROUND 1: Control with Global PIN (Config: \"{}\") ---",
        user_code.as_deref().unwrap_or("[None]")
    );

    println!("  1. Turning Output #{:03} ON...", TEST_OUTPUT_ID);
    satel.set_output_on(TEST_OUTPUT_ID, None).await?;
    println!("     -> Command received (ON)");
    sleep(Duration::from_secs(2)).await;

    println!("  2. Turning Output #{:03} OFF...", TEST_OUTPUT_ID);
    satel.set_output_off(TEST_OUTPUT_ID, None).await?;
    println!("     -> Command received (OFF)");
    sleep(Duration::from_secs(2)).await;

    println!("  3. Toggling Output #{:03} state (Switch)...", TEST_OUTPUT_ID);
    satel.set_output_toggle(TEST_OUTPUT_ID, None).await?;
    println!("     -> Command received (Toggled)");
    sleep(Duration::from_secs(2)).await;

    println!("  4. Toggling Output #{:03} back...", TEST_OUTPUT_ID);
    satel.set_output_toggle(TEST_OUTPUT_ID, None).await?;
    println!("     -> Command received (Toggled back)\n");
    sleep(Duration::from_secs(2)).await;

    // ==========================================================
    // ROUND 2: Using dedicated per-call PIN (passing Some(PIN))
    // Useful for UI/interactive applications where each action
    // is authorized on-the-fly with the user's personal PIN.
    // ==========================================================
    println!(
        "--- ROUND 2: Control with Dedicated PIN (Some(\"{}\")) ---",
        dedicated_code
    );

    println!("  1. Turning Output #{:03} ON...", TEST_OUTPUT_ID);
    satel.set_output_on(TEST_OUTPUT_ID, Some(&dedicated_code)).await?;
    println!("     -> Command received (ON)");
    sleep(Duration::from_secs(2)).await;

    println!("  2. Turning Output #{:03} OFF...", TEST_OUTPUT_ID);
    satel.set_output_off(TEST_OUTPUT_ID, Some(&dedicated_code)).await?;
    println!("     -> Command received (OFF)");
    sleep(Duration::from_secs(2)).await;

    println!("  3. Toggling Output #{:03} state (Switch)...", TEST_OUTPUT_ID);
    satel.set_output_toggle(TEST_OUTPUT_ID, Some(&dedicated_code)).await?;
    println!("     -> Command received (Toggled)");
    sleep(Duration::from_secs(2)).await;

    println!("  4. Toggling Output #{:03} back...", TEST_OUTPUT_ID);
    satel.set_output_toggle(TEST_OUTPUT_ID, Some(&dedicated_code)).await?;
    println!("     -> Command received (Toggled back)\n");

    // 3. Disconnect cleanly
    println!("Disconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}
```

### examples/3_02_arm_disarm_partitions.rs

```rust
﻿//! Example 3_02: Arming, disarming, and clearing alarms in partitions using global and per-call user codes.
//!
//! ============================================================================
//! 1. PARTITION CONTROL OVERVIEW & API METHODS:
//! ============================================================================
//! The Satel Integra panel provides flexible arming modes and alarm management:
//!
//!   Command | Async Method                                     | Mode | Description
//!   --------+--------------------------------------------------+------+---------------------------------------------
//!   0x80    | `satel.arm_full(partition_id, pin).await`        | 0    | Full arming (all zones armed)
//!   0x81    | `satel.arm_stay(partition_id, pin).await`        | 1    | Stay arming (interior zones bypassed)
//!   0x82    | `satel.arm_stay_delay0(partition_id, pin).await` | 2    | Stay arming without entry delay
//!   0x83    | `satel.arm_stay_no_exit(partition_id, pin).await`| 3    | Stay arming without exit delay
//!   0xA0    | `satel.force_arm_full(partition_id, pin).await`  | 0    | Force full arming (violating zones bypassed)
//!   0xA1    | `satel.force_arm_stay(partition_id, pin).await`  | 1    | Force stay arming
//!   0x84    | `satel.disarm(partition_id, pin).await`          | -    | Disarm partition
//!   0x85    | `satel.clear_alarm(partition_id, pin).await`     | -    | Clear active alarm / alarm memory
//!
//! PIN Authentication Modes:
//!   - Global PIN (`pin = None`):
//!       Uses pre-configured `Config.user_code` from startup (daemons, smart home).
//!   - Dynamic / Per-call PIN (`pin = Some("123456")`):
//!       Passes personal user code on-the-fly (mobile apps, web portals).
//!
//! ============================================================================
//! 2. PROTOCOL CONFIRMATION & SECURITY BEHAVIOR:
//! ============================================================================
//! Receiving a successful response (`Ok(())` / "Command received") indicates that
//! the command packet was successfully received and queued by the ETHM-1 module.
//!
//! Important Satel protocol note: Just like with output control, the panel does NOT return
//! error frames when:
//!   - The user PIN is invalid ("Wrong PIN"),
//!   - The user lacks authority to arm/disarm this partition ("No Access"),
//!   - The partition has violated zones preventing arming (unless force arming is used).
//! In all unauthorized cases, the Integra panel silently drops the physical execution
//! and logs an unauthorized access attempt to its internal event log.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 3_02_arm_disarm_partitions
//!
//! Environment variables (optional):
//!   SATEL_HOST           - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT           - TCP port (default: 7094)
//!   SATEL_CODE           - Global user access code (default: "1234")
//!   SATEL_DEDICATED_CODE - Dedicated/dynamic user access code (default: "123456")

use satel_integra::{Config, ConnectionConfig, SatelIntegra};
use std::env;
use std::time::Duration;
use tokio::time::sleep;

/// ID of the partition to test arming/disarming on (Set to Partition #05)
const TEST_PARTITION_ID: u16 = 5;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Connection parameters
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));
    let dedicated_code = env::var("SATEL_DEDICATED_CODE").unwrap_or_else(|_| "123456".to_string());

    println!("==================================================");
    println!(" SATEL INTEGRA - Arm / Disarm Partitions");
    println!("==================================================");
    println!("Target partition ID: #{:02}", TEST_PARTITION_ID);
    println!("Global PIN (Config): \"{}\"", user_code.as_deref().unwrap_or("[None]"));
    println!("Dedicated PIN:       \"{}\"\n", dedicated_code);

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code: user_code.clone(),
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 2. Connect
    println!("Connecting to the panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    // Check partition name
    if let Ok(partition) = satel.get_partition_name(TEST_PARTITION_ID).await {
        println!("Selected Partition #{:02}: \"{}\"\n", TEST_PARTITION_ID, partition.name);
    }

    // ==========================================================
    // ROUND 1: Arm & Disarm with Global PIN (passing None)
    // ==========================================================
    println!(
        "--- ROUND 1: Control with Global PIN (Config: \"{}\") ---",
        user_code.as_deref().unwrap_or("[None]")
    );

    println!("  1. Arming Partition #{:02} in Mode 0 (Full Arm)...", TEST_PARTITION_ID);
    satel.arm_full(TEST_PARTITION_ID, None).await?;
    println!("     -> Command received (Full Arm)");
    sleep(Duration::from_secs(3)).await;

    println!("  2. Disarming Partition #{:02}...", TEST_PARTITION_ID);
    satel.disarm(TEST_PARTITION_ID, None).await?;
    println!("     -> Command received (Disarm)\n");
    sleep(Duration::from_secs(2)).await;

    // ==========================================================
    // ROUND 2: Stay Arm, Disarm & Clear Alarm with Dedicated PIN
    // ==========================================================
    println!(
        "--- ROUND 2: Control with Dedicated PIN (Some(\"{}\")) ---",
        dedicated_code
    );

    println!("  1. Arming Partition #{:02} in Mode 1 (Stay / Home Arm)...", TEST_PARTITION_ID);
    satel.arm_stay(TEST_PARTITION_ID, Some(&dedicated_code)).await?;
    println!("     -> Command received (Stay Arm)");
    sleep(Duration::from_secs(3)).await;

    println!("  2. Disarming Partition #{:02}...", TEST_PARTITION_ID);
    satel.disarm(TEST_PARTITION_ID, Some(&dedicated_code)).await?;
    println!("     -> Command received (Disarm)");
    sleep(Duration::from_secs(2)).await;

    println!("  3. Clearing active alarms / alarm memory in Partition #{:02}...", TEST_PARTITION_ID);
    satel.clear_alarm(TEST_PARTITION_ID, Some(&dedicated_code)).await?;
    println!("     -> Command received (Clear Alarm)\n");

    // 3. Disconnect cleanly
    println!("Disconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}
```

### examples/3_03_control_time.rs

```rust
﻿//! Example 3_03: Set and synchronize real-time clock (RTC) in Satel Integra panel.
//!
//! ============================================================================
//! 1. RTC CONTROL OVERVIEW & API METHODS:
//! ============================================================================
//! The Satel Integra panel allows reading and updating its internal RTC clock:
//!
//!   Command | Async Method                                     | Description
//!   --------+--------------------------------------------------+---------------------------------------------
//!   0x1A    | `satel.get_satel_time().await`                   | Query current DateTime<Local> from panel
//!   0x8E    | `satel.set_satel_time(datetime, pin).await`      | Set panel RTC clock (YYYYMMDDhhmmss)
//!
//! PIN Authentication Modes:
//!   - Global PIN (`pin = None`):
//!       Uses the system code pre-configured at startup in `Config.user_code`.
//!       Ideal for automated background NTP synchronization daemons.
//!   - Dynamic / Per-call PIN (`pin = Some("123456")`):
//!       Allows passing a dynamic user PIN per operation.
//!       Ideal for UI applications and administrative management tools.
//!
//! ============================================================================
//! 2. PROTOCOL CONFIRMATION & SECURITY BEHAVIOR:
//! ============================================================================
//! Receiving a successful response (`Ok(())` / "Command received") indicates that
//! the command packet was successfully received and queued by the ETHM-1 module.
//!
//! Important Satel protocol note: The panel does NOT return error frames when:
//!   - The user PIN is invalid ("Wrong PIN"),
//!   - The user lacks authority to change system time ("No Access").
//! In unauthorized cases, the Integra panel silently drops the clock update
//! and logs an unauthorized access attempt to its internal event log.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 3_03_control_time
//!
//! Environment variables (optional):
//!   SATEL_HOST           - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT           - TCP port (default: 7094)
//!   SATEL_CODE           - Global user access code (default: "1234")
//!   SATEL_DEDICATED_CODE - Dedicated/dynamic user access code (default: "123456")

use chrono::Local;
use satel_integra::{Config, ConnectionConfig, SatelIntegra};
use std::env;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Connection parameters
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));
    let dedicated_code = env::var("SATEL_DEDICATED_CODE").unwrap_or_else(|_| "123456".to_string());

    println!("==================================================");
    println!(" SATEL INTEGRA - Synchronize RTC Clock (0x8E)");
    println!("==================================================");
    println!("Global PIN (Config): \"{}\"", user_code.as_deref().unwrap_or("[None]"));
    println!("Dedicated PIN:       \"{}\"\n", dedicated_code);

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code: user_code.clone(),
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 2. Connect
    println!("Connecting to the panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 3. Initial Clock Inspection
    println!("--- Initial Clock Inspection ---");
    let initial_panel_time = satel.get_satel_time().await?;
    let local_pc_time = Local::now();
    let initial_drift = (initial_panel_time.timestamp() - local_pc_time.timestamp()).abs();

    println!("  Panel RTC Time:   {}", initial_panel_time.format("%Y-%m-%d %H:%M:%S"));
    println!("  Local PC Time:    {}", local_pc_time.format("%Y-%m-%d %H:%M:%S"));
    println!("  Clock Difference: {} seconds\n", initial_drift);

    // ==========================================================
    // ROUND 1: Sync RTC using Global PIN (passing None)
    // ==========================================================
    println!(
        "--- ROUND 1: Synchronize with Global PIN (Config: \"{}\") ---",
        user_code.as_deref().unwrap_or("[None]")
    );

    let target_time = Local::now();
    println!("  1. Setting panel clock to current PC time ({})", target_time.format("%H:%M:%S"));
    satel.set_satel_time(target_time, None).await?;
    println!("     -> Command received (Set RTC Time)");
    sleep(Duration::from_secs(2)).await;

    // Verify
    let updated_panel_time = satel.get_satel_time().await?;
    println!(
        "  2. Verification: Panel RTC is now: {}\n",
        updated_panel_time.format("%Y-%m-%d %H:%M:%S")
    );

    // ==========================================================
    // ROUND 2: Sync RTC using Dedicated per-call PIN
    // ==========================================================
    println!(
        "--- ROUND 2: Synchronize with Dedicated PIN (Some(\"{}\")) ---",
        dedicated_code
    );

    let target_time = Local::now();
    println!("  1. Setting panel clock with dedicated PIN ({})", target_time.format("%H:%M:%S"));
    satel.set_satel_time(target_time, Some(&dedicated_code)).await?;
    println!("     -> Command received (Set RTC Time)");
    sleep(Duration::from_secs(2)).await;

    // Verify
    let updated_panel_time = satel.get_satel_time().await?;
    println!(
        "  2. Verification: Panel RTC is now: {}\n",
        updated_panel_time.format("%Y-%m-%d %H:%M:%S")
    );

    // 4. Disconnect cleanly
    println!("Disconnecting...");
    satel.disconnect().await?;
    println!("Disconnected cleanly. Done.");

    Ok(())
}
```

### examples/4_01_monitor_system_info.rs

```rust
﻿//! Example 4_01: Asynchronous event stream monitoring for System Info, Names, and RTC clock.
//!
//! ============================================================================
//! 1. EVENT STREAMING OVERVIEW & FILTERED DOMAIN (SYSTEM INFO):
//! ============================================================================
//! The `satel_integra` library uses a multi-producer multi-consumer broadcast channel
//! (`tokio::sync::broadcast`) for real-time reactive event delivery.
//!
//! This example filters and processes only System Information events:
//!   - `SatelEvent::ConnectionChanged(state)`:
//!       Connection lifecycle transitions (Connecting, Handshake, Connected, ConnectionLost).
//!   - `SatelEvent::IntegraVersionReceived(version)`:
//!       Alarm panel model, firmware version, and I/O capacity.
//!   - `SatelEvent::EthmVersionReceived(ethm_version)`:
//!       ETHM-1 / INT-RS module capabilities and hardware flags.
//!   - `SatelEvent::PartitionNameReceived { id, name }`:
//!       Decoded 16-character partition names.
//!   - `SatelEvent::ZoneNameReceived { id, name }`:
//!       Decoded 16-character zone (input) names.
//!   - `SatelEvent::OutputNameReceived { id, name }`:
//!       Decoded 16-character output names.
//!   - `SatelEvent::SystemStatusChanged(status)`:
//!       RTC clock updates, service mode state, and trouble presence flags.
//!
//! All other event variants (violations, armed states, temperatures, troubles) are
//! ignored (`_ => {}`) by this specialized listener.
//!
//! ============================================================================
//! 2. AUTOMATIC HANDSHAKE VS EXPLICIT QUERIES:
//! ============================================================================
//! - Automatic Handshake Events:
//!     During `satel.connect().await`, the client library automatically performs an internal
//!     handshake by querying Integra version (0x7E) and ETHM-1 module capabilities (0x7C)
//!     to determine panel capacity (I/O count) and 32-byte frame support.
//!     Subscribers attached *before* calling `connect()` receive these handshake events
//!     immediately upon connection establishment.
//! - Explicit / Manual Queries:
//!     Subsequent explicit method calls (e.g. `satel.get_integra_version().await`,
//!     `satel.get_zone_name(id).await`) query the panel and automatically re-broadcast
//!     fresh event copies across the channel.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 4_01_monitor_system_info
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelEvent, SatelIntegra};
use std::env;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Connection parameters
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Monitor System Info Events (3_01)");
    println!("==================================================");

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 2. Subscribe to event broadcast BEFORE connecting
    // This allows capturing automatic connection handshake events (0x7E, 0x7C)
    let mut rx = satel.subscribe();

    // 3. Spawn specialized background listener task
    let listener_handle = tokio::spawn(async move {
        println!("[Listener Task] Started listening for System Info events...\n");

        while let Ok(event) = rx.recv().await {
            match event {
                SatelEvent::ConnectionChanged(state) => {
                    println!("[EVENT: CONNECTION] State changed -> {:?}", state);
                }
                SatelEvent::IntegraVersionReceived(ver) => {
                    println!(
                        "[EVENT: VERSION] Panel: {} | Firmware: {} | Language: {} | I/O: {}",
                        ver.model, ver.firmware_version, ver.language, ver.io_count
                    );
                }
                SatelEvent::EthmVersionReceived(ethm) => {
                    println!(
                        "[EVENT: ETHM] Module: {} (32B frames: {})",
                        ethm.version_raw, ethm.capabilities.support_32_byte_frames
                    );
                }
                SatelEvent::PartitionNameReceived { id, name } => {
                    println!("[EVENT: NAME] Partition #{:02}: \"{}\"", id, name);
                }
                SatelEvent::ZoneNameReceived { id, name } => {
                    println!("[EVENT: NAME] Zone      #{:03}: \"{}\"", id, name);
                }
                SatelEvent::OutputNameReceived { id, name } => {
                    println!("[EVENT: NAME] Output    #{:03}: \"{}\"", id, name);
                }
                SatelEvent::SystemStatusChanged(status) => {
                    println!(
                        "[EVENT: RTC/STATUS] Time: {} | Service: {} | Troubles: {}",
                        status.rtc.format("%Y-%m-%d %H:%M:%S"),
                        status.service_mode,
                        status.troubles_present
                    );
                }
                // All other security/temperature/trouble events are ignored in this listener
                _ => {}
            }
        }
        println!("[Listener Task] Event channel closed.");
    });

    // 4. Connect to the panel
    // Note: The library performs an automatic handshake querying 0x7E & 0x7C,
    // which triggers the first emission of version events in the listener above.
    println!("Connecting to the panel (automatic handshake queries 0x7E & 0x7C)...");
    satel.connect().await?;
    println!("Connected successfully!\n");

    // Allow handshake events to be processed and printed
    sleep(Duration::from_millis(500)).await;

    // 5. Trigger explicit network queries (re-emitting events and fetching names/RTC)
    println!("--- Triggering explicit queries (fetching names & RTC status) ---");
    let _ = satel.get_system_status().await;

    // Fetch first 3 partition names and first 3 zone names
    for id in 1..=3 {
        let _ = satel.get_partition_name(id).await;
        let _ = satel.get_zone_name(id).await;
    }

    // Keep listening for 10 seconds
    println!("\nMonitoring events for 10 seconds...");
    sleep(Duration::from_secs(10)).await;

    // 6. Disconnect cleanly
    println!("\nDisconnecting...");
    satel.disconnect().await?;
    sleep(Duration::from_millis(500)).await;

    // Abort background listener
    listener_handle.abort();
    println!("Disconnected cleanly. Done.");

    Ok(())
}
```

### examples/4_02_monitor_security_states.rs

```rust
﻿//! Example 4_02: Real-time event monitoring for security states (Zones, Outputs, Partitions).
//!
//! ============================================================================
//! 1. EVENT STREAMING OVERVIEW & FILTERED DOMAIN (SECURITY STATES):
//! ============================================================================
//! This specialized listener focuses entirely on physical and logical security states:
//!   - Zone (Input) Events:
//!       * `ZoneViolation { id, state }`          (PIR movement, magnetic contact open/close)
//!       * `ZoneTamper { id, state }`             (Line or casing sabotage)
//!       * `ZoneAlarm { id, state }`              (Active alarm triggered by zone)
//!       * `ZoneTamperAlarm { id, state }`       (Tamper alarm triggered)
//!       * `ZoneAlarmMemory { id, state }`        (Stored alarm flag in memory)
//!       * `ZoneBypass { id, state }`             (Zone disabled / bypassed)
//!   - Output (Relay) Events:
//!       * `OutputChanged { id, state }`          (Siren, light, relay ON / OFF)
//!   - Partition Events:
//!       * `PartitionArmed { id, state }`         (Partition armed state)
//!       * `PartitionArmedReally { id, state }`   (Full physical arming active)
//!       * `PartitionAlarm { id, state }`         (Active alarm in partition)
//!       * `PartitionAlarmMemory { id, state }`   (Alarm latched in partition memory)
//!       * `PartitionEntryTime { id, state }`     (Entry delay countdown active)
//!       * `PartitionExitTimeGt10s { id, state }` (Exit delay countdown > 10s)
//!       * `PartitionExitTimeLt10s { id, state }` (Exit delay countdown < 10s)
//!
//! All system information, temperatures, and diagnostic troubles are ignored (`_ => {}`).
//!
//! ============================================================================
//! 2. AUTOMATIC CHANGE DETECTION & DEDUPLICATION:
//! ============================================================================
//! The library features built-in change detection (`old_state != new_state`):
//!   - Initial State: Only inputs/outputs/partitions that are currently active (e.g. violated PIR)
//!     trigger an initial event upon first read (`false -> true`). Normal / idle zones (false)
//!     remain completely silent without generating unnecessary traffic.
//!   - Subsequent Cycles: If nothing changes between queries, zero duplicate events are emitted.
//!     Events fire strictly when physical transitions occur (e.g. PIR movement, contact restored).
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 4_02_monitor_security_states
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelEvent, SatelIntegra};
use std::env;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Connection parameters
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Monitor Security States (3_02)");
    println!("==================================================");

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 2. Subscribe to event broadcast before connecting
    let mut rx = satel.subscribe();

    // 3. Spawn specialized security event listener
    let listener_handle = tokio::spawn(async move {
        println!("[Security Listener] Task active. Monitoring state changes...\n");

        while let Ok(event) = rx.recv().await {
            match event {
                // --- Zone (Input) Events ---
                SatelEvent::ZoneViolation { id, state } => {
                    let status = if state { "VIOLATED (Movement / Open)" } else { "NORMAL (Restored)" };
                    println!("[ZONE VIOLATION] Zone #{:03} -> {}", id, status);
                }
                SatelEvent::ZoneTamper { id, state } => {
                    let status = if state { "TAMPERED (Sabotage!)" } else { "NORMAL (OK)" };
                    println!("[ZONE TAMPER]    Zone #{:03} -> {}", id, status);
                }
                SatelEvent::ZoneAlarm { id, state } => {
                    let status = if state { "ALARM ACTIVE!" } else { "ALARM RESTORED" };
                    println!("[ZONE ALARM]     Zone #{:03} -> {}", id, status);
                }
                SatelEvent::ZoneBypass { id, state } => {
                    let status = if state { "BYPASSED (Disabled)" } else { "UNBYPASSED (Active)" };
                    println!("[ZONE BYPASS]    Zone #{:03} -> {}", id, status);
                }

                // --- Output (Relay) Events ---
                SatelEvent::OutputChanged { id, state } => {
                    let status = if state { "ON (Active)" } else { "OFF (Inactive)" };
                    println!("[OUTPUT CHANGE]  Output #{:03} -> {}", id, status);
                }

                // --- Partition Events ---
                SatelEvent::PartitionArmed { id, state } => {
                    let status = if state { "ARMED" } else { "DISARMED" };
                    println!("[PARTITION ARM]  Partition #{:02} -> {}", id, status);
                }
                SatelEvent::PartitionArmedReally { id, state } => {
                    let status = if state { "ARMED REALLY (Full Protection)" } else { "DISARMED" };
                    println!("[PARTITION REAL] Partition #{:02} -> {}", id, status);
                }
                SatelEvent::PartitionAlarm { id, state } => {
                    let status = if state { "ALARM ACTIVE!" } else { "Alarm Cleared" };
                    println!("[PARTITION ALARM]Partition #{:02} -> {}", id, status);
                }
                SatelEvent::PartitionEntryTime { id, state } => {
                    let status = if state { "COUNTDOWN ACTIVE" } else { "Expired/Stopped" };
                    println!("[ENTRY DELAY]    Partition #{:02} -> Entry delay {}", id, status);
                }
                SatelEvent::PartitionExitTimeGt10s { id, state } => {
                    if state {
                        println!("[EXIT DELAY]     Partition #{:02} -> Exit delay > 10s remaining", id);
                    }
                }
                SatelEvent::PartitionExitTimeLt10s { id, state } => {
                    if state {
                        println!("[EXIT DELAY]     Partition #{:02} -> Exit delay < 10s remaining (Last seconds!)", id);
                    }
                }

                // All other events are ignored by this security monitor
                _ => {}
            }
        }
        println!("[Security Listener] Event channel closed.");
    });

    // 4. Connect to the panel
    println!("Connecting to the panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 5. Query cycles (demonstrating built-in change detection & zero duplication)
    println!("--- Querying Security States Across 3 Cycles (2-second intervals) ---");
    println!("Events will appear below only for active states or live transitions.\n");

    for cycle in 1..=3 {
        println!(">>> Query Cycle {}/3 (Fetching current panel states)...", cycle);
        let _ = satel.get_zones_violation().await;
        let _ = satel.get_zones_tamper().await;
        let _ = satel.get_outputs_state().await;
        let _ = satel.get_partitions_armed_really().await;
        let _ = satel.get_partitions_alarm().await;

        sleep(Duration::from_secs(2)).await;
    }

    println!("\nAll 3 query cycles completed. Monitoring for another 5 seconds...");
    sleep(Duration::from_secs(5)).await;

    // 6. Disconnect cleanly
    println!("\nDisconnecting...");
    satel.disconnect().await?;
    sleep(Duration::from_millis(500)).await;

    // Abort background listener
    listener_handle.abort();
    println!("Disconnected cleanly. Done.");

    Ok(())
}
```

### examples/4_03_monitor_temperatures.rs

```rust
﻿//! Example 4_03: Real-time event monitoring for wireless temperature sensors (ABAX 2).
//!
//! ============================================================================
//! 1. EVENT STREAMING OVERVIEW & FILTERED DOMAIN (TEMPERATURES):
//! ============================================================================
//! This specialized listener filters and processes only temperature measurement events:
//!   - `SatelEvent::ZoneTemperatureChanged { id, temperature }`:
//!       Emitted whenever a new temperature reading (0x7D) is successfully received
//!       from a wireless or wired sensor (e.g. ATD-100, APD-200, AOCD-260).
//!       Temperature is provided as an `f32` value in degrees Celsius (°C) with 0.5°C resolution.
//!
//! All other security, system, and trouble events are ignored (`_ => {}`) by this listener.
//!
//! ============================================================================
//! 2. TEMPERATURE DISPATCH & POLLING ARCHITECTURE:
//! ============================================================================
//! - Unlike digital states (0x00..0x17) which support 0x7F push bitmasks, temperature
//!   sensors (0x7D) are queried per zone ID.
//! - Whenever `satel.get_zone_temperature(zone_id).await` is called (or executed by
//!   an automation task), the library parses the 16-bit sensor word, updates local cache,
//!   and broadcasts `ZoneTemperatureChanged` to all active subscribers.
//! - Smart error blocking (`temp_blocking_enabled: true`) protects the communication queue
//!   from being blocked by missing/unpaired sensor IDs.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 4_03_monitor_temperatures
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelEvent, SatelIntegra};
use std::env;
use std::time::Duration;
use tokio::time::sleep;

/// List of zone IDs with actual temperature sensors installed (e.g. ATD-100, APD-200)
const TEMPERATURE_ZONES: &[u16] = &[21, 23, 26, 28, 30];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Connection parameters
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Monitor Temperatures (3_03)");
    println!("==================================================");
    println!("Target Temperature Zones: {:?}\n", TEMPERATURE_ZONES);

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,
        temp_blocking_enabled: true, // Protect communication queue from broken sensors
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 2. Subscribe to event broadcast before connecting
    let mut rx = satel.subscribe();

    // 3. Spawn specialized temperature event listener
    let listener_handle = tokio::spawn(async move {
        println!("[Temperature Listener] Task active. Monitoring temperature updates...\n");

        while let Ok(event) = rx.recv().await {
            match event {
                SatelEvent::ZoneTemperatureChanged { id, temperature } => {
                    println!(
                        "[TEMPERATURE EVENT] Zone #{:03} -> {:>5.1}°C",
                        id, temperature
                    );
                }
                SatelEvent::ZoneTemperatureError { id, status } => {
                    println!(
                        "[TEMPERATURE ERROR] Zone #{:03} -> Status: {:?}",
                        id, status
                    );
                }
                // All other security/system events are ignored
                _ => {}
            }
        }
        println!("[Temperature Listener] Event channel closed.");
    });

    // 4. Connect to the panel
    println!("Connecting to the panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 5. Query temperature sensors across 2 measurement cycles
    println!("--- Triggering Temperature Queries Across 2 Cycles ---");
    println!("Successful reads will emit events to the background listener.");
    println!("Failed reads (missing sensors / timeouts) will print error diagnostics below:\n");

    for cycle in 1..=2 {
        println!(">>> Measurement Cycle {}/2...", cycle);
        for &zone_id in TEMPERATURE_ZONES {
            match satel.get_zone_temperature(zone_id).await {
                Ok(_) => {
                    // Successful read -> event will be captured and printed by [Temperature Listener]
                }
                Err(e) => {
                    // Failed read -> sensor is missing, unconfigured, or queue-blocked
                    eprintln!("  [Query Failed] Zone #{:03} -> Error: {} (No temperature event emitted)", zone_id, e);
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
        sleep(Duration::from_secs(3)).await;
    }

    // Keep listening for 5 more seconds
    println!("\nMeasurement cycles finished. Monitoring for another 5 seconds...");
    sleep(Duration::from_secs(5)).await;

    // 6. Disconnect cleanly
    println!("\nDisconnecting...");
    satel.disconnect().await?;
    sleep(Duration::from_millis(500)).await;

    // Abort background listener
    listener_handle.abort();
    println!("Disconnected cleanly. Done.");

    Ok(())
}
```

### examples/4_04_monitor_troubles.rs

```rust
﻿//! Example 4_04: Real-time event monitoring for system troubles and trouble memory.
//!
//! ============================================================================
//! 1. EVENT STREAMING OVERVIEW & FILTERED DOMAIN (SYSTEM TROUBLES):
//! ============================================================================
//! This specialized listener filters and processes only system diagnostic events:
//!   - `SatelEvent::Trouble(trouble_type, state)`:
//!       Emitted when an active hardware/communication trouble appears (`state = true`)
//!       or is resolved/restored (`state = false`).
//!   - `SatelEvent::TroubleMemory(trouble_type, state)`:
//!       Emitted when a trouble condition is recorded into non-volatile panel memory (`true`)
//!       or cleared from trouble memory (`false`).
//!   - `SatelEvent::SystemStatusChanged(status)`:
//!       Emitted when general trouble indicator flags change (`troubles_present`, `troubles_memory`).
//!
//! All zone violations, output states, armed partitions, and temperatures are
//! ignored (`_ => {}`) by this specialized diagnostic listener.
//!
//! ============================================================================
//! 2. TROUBLE DIAGNOSTICS & CHANGE DETECTION:
//! ============================================================================
//! - Initial Cycle: If the panel currently has active troubles (e.g. AC loss, low battery,
//!   GSM error), initial `Trouble(..., true)` events are emitted immediately upon first read.
//! - Subsequent Cycles: Zero duplicate events are emitted if the diagnostic state remains
//!   unchanged. Events fire strictly when a trouble occurs or gets restored.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 4_04_monitor_troubles
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelCommand, SatelEvent, SatelIntegra};
use std::env;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Connection parameters
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Monitor System Troubles (3_04)");
    println!("==================================================");

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 2. Subscribe to event broadcast before connecting
    let mut rx = satel.subscribe();

    // 3. Spawn specialized trouble diagnostic listener
    let listener_handle = tokio::spawn(async move {
        println!("[Troubles Listener] Task active. Monitoring diagnostic faults...\n");

        while let Ok(event) = rx.recv().await {
            match event {
                SatelEvent::Trouble(kind, state) => {
                    let status = if state { "FAULT ACTIVE (Present!)" } else { "FAULT RESTORED (OK)" };
                    println!("[ACTIVE TROUBLE]  {:?} -> {}", kind, status);
                }
                SatelEvent::TroubleMemory(kind, state) => {
                    let status = if state { "LATCHED IN MEMORY" } else { "MEMORY CLEARED" };
                    println!("[TROUBLE MEMORY]  {:?} -> {}", kind, status);
                }
                SatelEvent::SystemStatusChanged(status) => {
                    println!(
                        "[SYSTEM STATUS]   Troubles Present: {} | In Memory: {} | Service Mode: {}",
                        status.troubles_present,
                        status.troubles_memory,
                        status.service_mode
                    );
                }
                // All other security/temperature/names events are ignored
                _ => {}
            }
        }
        println!("[Troubles Listener] Event channel closed.");
    });

    // 4. Connect to the panel
    println!("Connecting to the panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 5. Query system status and trouble parts across 2 diagnostic cycles
    let active_trouble_cmds = [
        SatelCommand::TroublesPart1,
        SatelCommand::TroublesPart2,
        SatelCommand::TroublesPart3,
        SatelCommand::TroublesPart4,
        SatelCommand::TroublesPart5,
        SatelCommand::TroublesPart6,
        SatelCommand::TroublesPart7,
        SatelCommand::TroublesPart8,
    ];

    let memory_trouble_cmds = [
        SatelCommand::TroublesMemoryPart1,
        SatelCommand::TroublesMemoryPart2,
        SatelCommand::TroublesMemoryPart3,
        SatelCommand::TroublesMemoryPart4,
        SatelCommand::TroublesMemoryPart5,
        SatelCommand::TroublesMemoryPart6,
        SatelCommand::TroublesMemoryPart7,
        SatelCommand::TroublesMemoryPart8,
    ];

    println!("--- Querying System Troubles Across 2 Diagnostic Cycles ---");
    for cycle in 1..=2 {
        println!(">>> Diagnostic Query Cycle {}/2...", cycle);
        let _ = satel.get_system_status().await;

        for cmd in &active_trouble_cmds {
            let _ = satel.get_troubles(*cmd).await;
            sleep(Duration::from_millis(50)).await;
        }

        for cmd in &memory_trouble_cmds {
            let _ = satel.get_troubles(*cmd).await;
            sleep(Duration::from_millis(50)).await;
        }

        sleep(Duration::from_secs(3)).await;
    }

    // Keep listening for 5 more seconds
    println!("\nDiagnostic cycles completed. Monitoring for another 5 seconds...");
    sleep(Duration::from_secs(5)).await;

    // 6. Disconnect cleanly
    println!("\nDisconnecting...");
    satel.disconnect().await?;
    sleep(Duration::from_millis(500)).await;

    // Abort background listener
    listener_handle.abort();
    println!("Disconnected cleanly. Done.");

    Ok(())
}
```

### examples/4_05_monitor_all_events.rs

```rust
﻿//! Example 4_05: Comprehensive real-time event monitor handling all SatelEvent variants.
//!
//! ============================================================================
//! 1. EVENT STREAMING OVERVIEW & EXHAUSTIVE MATCHING:
//! ============================================================================
//! This universal event monitor demonstrates complete, exhaustive pattern matching
//! on every variant of `SatelEvent` emitted by the `satel_integra` broadcast channel.
//!
//! Event Categories Covered:
//!   1. Connection & System Metadata:
//!        - `ConnectionChanged`
//!        - `IntegraVersionReceived`, `EthmVersionReceived`
//!        - `ZoneNameReceived`, `OutputNameReceived`, `PartitionNameReceived`
//!        - `SystemStatusChanged` (RTC, Service mode, Trouble flags)
//!   2. Physical & Logical Security States:
//!        - Zones: `ZoneViolation`, `ZoneTamper`, `ZoneAlarm`, `ZoneTamperAlarm`,
//!          `ZoneAlarmMemory`, `ZoneTamperAlarmMemory`, `ZoneBypass`,
//!          `ZoneNoViolationTrouble`, `ZoneLongViolationTrouble`
//!        - Outputs: `OutputChanged`
//!        - Partitions: `PartitionArmed`, `PartitionArmedReally`, `PartitionAlarm`,
//!          `PartitionAlarmMemory`, `PartitionEntryTime`, `PartitionExitTimeGt10s`,
//!          `PartitionExitTimeLt10s`
//!   3. Environmental & Analog:
//!        - `ZoneTemperatureChanged`
//!   4. System Diagnostics:
//!        - `Trouble`, `TroubleMemory`
//!   5. Protocol Notifications:
//!        - `AutoReadConfigured`, `PanelMessage`
//!
//! ============================================================================
//! 2. ARCHITECTURAL PATTERN:
//! ============================================================================
//! Spawns a dedicated background listener task with timestamped log formatting
//! (`%H:%M:%S%.3f`). Smart temperature error blocking (`temp_blocking_enabled: true`)
//! is enabled to protect the communication queue.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 4_05_monitor_all_events
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use chrono::Local;
use satel_integra::{Config, ConnectionConfig, SatelCommand, SatelEvent, SatelIntegra};
use std::env;
use std::time::Duration;
use tokio::time::sleep;

/// Sample temperature sensor zone to query
const TEST_TEMP_ZONE: u16 = 21;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Connection parameters
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Universal Event Monitor (3_05)");
    println!("==================================================");

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,
        temp_blocking_enabled: true, // Enable smart temperature queue protection
        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 2. Subscribe to event broadcast before connecting
    let mut rx = satel.subscribe();

    // 3. Spawn comprehensive background listener task
    let listener_handle = tokio::spawn(async move {
        println!("[Universal Listener] Active. Receiving all system events...\n");

        while let Ok(event) = rx.recv().await {
            let ts = Local::now().format("%H:%M:%S%.3f");

            match event {
                // --- Connection & System Metadata ---
                SatelEvent::ConnectionChanged(state) => {
                    println!("[{}] [CONNECTION]        State -> {:?}", ts, state);
                }
                SatelEvent::IntegraVersionReceived(ver) => {
                    println!("[{}] [INTEGRA VERSION]   Model: {} | Firmware: {} | I/O: {}", ts, ver.model, ver.firmware_version, ver.io_count);
                }
                SatelEvent::EthmVersionReceived(ethm) => {
                    println!("[{}] [ETHM VERSION]      Module: {} (32B frames: {})", ts, ethm.version_raw, ethm.capabilities.support_32_byte_frames);
                }
                SatelEvent::PartitionNameReceived { id, name } => {
                    println!("[{}] [NAME]              Partition #{:02}: \"{}\"", ts, id, name);
                }
                SatelEvent::ZoneNameReceived { id, name } => {
                    println!("[{}] [NAME]              Zone      #{:03}: \"{}\"", ts, id, name);
                }
                SatelEvent::OutputNameReceived { id, name } => {
                    println!("[{}] [NAME]              Output    #{:03}: \"{}\"", ts, id, name);
                }
                SatelEvent::SystemStatusChanged(status) => {
                    println!(
                        "[{}] [SYSTEM STATUS]     RTC: {} | Service: {} | Troubles: {}",
                        ts, status.rtc.format("%Y-%m-%d %H:%M:%S"), status.service_mode, status.troubles_present
                    );
                }

                // --- Zone (Input) Events ---
                SatelEvent::ZoneViolation { id, state } => {
                    println!("[{}] [ZONE VIOLATION]    Zone #{:03} -> {}", ts, id, if state { "VIOLATED" } else { "NORMAL" });
                }
                SatelEvent::ZoneTamper { id, state } => {
                    println!("[{}] [ZONE TAMPER]       Zone #{:03} -> {}", ts, id, if state { "TAMPERED" } else { "OK" });
                }
                SatelEvent::ZoneAlarm { id, state } => {
                    println!("[{}] [ZONE ALARM]        Zone #{:03} -> {}", ts, id, if state { "ALARM ACTIVE!" } else { "RESTORED" });
                }
                SatelEvent::ZoneTamperAlarm { id, state } => {
                    println!("[{}] [ZONE TAMPER ALARM] Zone #{:03} -> {}", ts, id, if state { "TAMPER ALARM!" } else { "RESTORED" });
                }
                SatelEvent::ZoneAlarmMemory { id, state } => {
                    println!("[{}] [ZONE ALARM MEMORY] Zone #{:03} -> {}", ts, id, if state { "LATCHED" } else { "CLEARED" });
                }
                SatelEvent::ZoneTamperAlarmMemory { id, state } => {
                    println!("[{}] [ZONE TMP MEMORY]   Zone #{:03} -> {}", ts, id, if state { "LATCHED" } else { "CLEARED" });
                }
                SatelEvent::ZoneBypass { id, state } => {
                    println!("[{}] [ZONE BYPASS]       Zone #{:03} -> {}", ts, id, if state { "BYPASSED" } else { "ACTIVE" });
                }
                SatelEvent::ZoneNoViolationTrouble { id, state } => {
                    println!("[{}] [ZONE NO-VIOLATION] Zone #{:03} -> {}", ts, id, if state { "TROUBLE" } else { "OK" });
                }
                SatelEvent::ZoneLongViolationTrouble { id, state } => {
                    println!("[{}] [ZONE LONG-VIOLAT.] Zone #{:03} -> {}", ts, id, if state { "TROUBLE" } else { "OK" });
                }

                // --- Output (Relay) Events ---
                SatelEvent::OutputChanged { id, state } => {
                    println!("[{}] [OUTPUT CHANGE]     Output #{:03} -> {}", ts, id, if state { "ON" } else { "OFF" });
                }

                // --- Partition Events ---
                SatelEvent::PartitionArmed { id, state } => {
                    println!("[{}] [PARTITION ARMED]   Partition #{:02} -> {}", ts, id, if state { "ARMED" } else { "DISARMED" });
                }
                SatelEvent::PartitionArmedReally { id, state } => {
                    println!("[{}] [PARTITION REALLY]  Partition #{:02} -> {}", ts, id, if state { "ARMED REALLY" } else { "DISARMED" });
                }
                SatelEvent::PartitionAlarm { id, state } => {
                    println!("[{}] [PARTITION ALARM]   Partition #{:02} -> {}", ts, id, if state { "ALARM ACTIVE!" } else { "CLEARED" });
                }
                SatelEvent::PartitionAlarmMemory { id, state } => {
                    println!("[{}] [PARTITION MEMORY]  Partition #{:02} -> {}", ts, id, if state { "ALARM LATCHED" } else { "CLEARED" });
                }
                SatelEvent::PartitionEntryTime { id, state } => {
                    println!("[{}] [ENTRY DELAY]       Partition #{:02} -> {}", ts, id, if state { "COUNTDOWN ACTIVE" } else { "STOPPED" });
                }
                SatelEvent::PartitionExitTimeGt10s { id, state } => {
                    if state {
                        println!("[{}] [EXIT DELAY >10S]   Partition #{:02} -> Exit countdown active", ts, id);
                    }
                }
                SatelEvent::PartitionExitTimeLt10s { id, state } => {
                    if state {
                        println!("[{}] [EXIT DELAY <10S]   Partition #{:02} -> Final exit countdown seconds!", ts, id);
                    }
                }

                // --- Temperature Events ---
                SatelEvent::ZoneTemperatureChanged { id, temperature } => {
                    println!("[{}] [TEMPERATURE]       Zone #{:03} -> {:>5.1}°C", ts, id, temperature);
                }
                SatelEvent::ZoneTemperatureError { id, status } => {
                    println!("[{}] [TEMP FAULT]        Zone #{:03} -> Status: {:?}", ts, id, status);
                }

                // --- Diagnostic Troubles ---
                SatelEvent::Trouble(kind, state) => {
                    println!("[{}] [ACTIVE TROUBLE]    {:?} -> {}", ts, kind, if state { "PRESENT" } else { "RESTORED" });
                }
                SatelEvent::TroubleMemory(kind, state) => {
                    println!("[{}] [TROUBLE MEMORY]    {:?} -> {}", ts, kind, if state { "LATCHED" } else { "CLEARED" });
                }

                // --- Protocol Messages ---
                SatelEvent::AutoReadConfigured(report) => {
                    println!("[{}] [AUTOREAD CONFIG]   Active Items: {}/{}", ts, report.success_count, report.total_requested);
                }
                SatelEvent::PanelMessage(result) => {
                    println!("[{}] [PANEL MESSAGE]     Result code: {:?}", ts, result);
                }
            }
        }
        println!("[Universal Listener] Event channel closed.");
    });

    // 4. Connect to the panel
    println!("Connecting to the panel at {}:{}...", host, port);
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 5. Query states to generate a rich event stream
    println!("--- Triggering sample queries across all domains ---");
    let _ = satel.get_system_status().await;
    let _ = satel.get_partition_name(1).await;
    let _ = satel.get_zone_name(1).await;
    let _ = satel.get_output_name(1).await;
    let _ = satel.get_zones_violation().await;
    let _ = satel.get_outputs_state().await;
    let _ = satel.get_partitions_armed_really().await;
    let _ = satel.get_troubles(SatelCommand::TroublesPart1).await;

    // Sample temperature query
    match satel.get_zone_temperature(TEST_TEMP_ZONE).await {
        Ok(_) => {}
        Err(e) => {
            eprintln!("  [Temp Query Note] Zone #{:03}: {}", TEST_TEMP_ZONE, e);
        }
    }

    // Keep monitoring for 15 seconds
    println!("\nMonitoring event stream for 15 seconds (walk in front of a PIR or trigger an action)...");
    sleep(Duration::from_secs(15)).await;

    // 6. Disconnect cleanly
    println!("\nDisconnecting...");
    satel.disconnect().await?;
    sleep(Duration::from_millis(500)).await;

    // Abort background listener
    listener_handle.abort();
    println!("Disconnected cleanly. Done.");

    Ok(())
}
```

### examples/5_01_auto_read_push.rs

```rust
﻿//! Example 5_01: Automated real-time state streaming via ETHM-1 Auto-Push (Command 0x7F).
//!
//! ============================================================================
//! 1. AUTO-PUSH / AUTO-READ OVERVIEW & HARDWARE REGISTRATION (0x7F):
//! ============================================================================
//! In default configuration (`Config::default()`), all auto-read flags are disabled (`false`).
//! The client operates purely in manual request-response mode.
//!
//! When one or more `auto_read_*` flags are enabled in `Config`:
//!   - During `satel.connect().await`, the library automatically sends a one-time
//!     registration frame (**Command `0x7F`**) with a bitmask of requested data groups.
//!   - The ETHM-1 / INT-RS module saves this subscription in its active session and
//!     returns an `AutoReadReport`.
//!   - From this point forward, the ETHM-1 module **autonomously pushes raw state frames**
//!     over TCP whenever a physical state changes in the panel (e.g. PIR triggered,
//!     relay activated, partition armed).
//!
//! Supported Auto-Push Groups:
//!   - `auto_read_zones_violation`:         Zones movement / open state (0x00)
//!   - `auto_read_zones_tamper`:            Zones line / casing sabotage (0x01)
//!   - `auto_read_zones_alarm`:             Zones active alarms (0x02)
//!   - `auto_read_partitions_armed_really`: Partitions full arming state (0x0A)
//!   - `auto_read_partitions_alarm`:        Partitions alarm status (0x13)
//!   - `auto_read_outputs_state`:           Outputs / relays ON/OFF states (0x17)
//!   - `auto_read_system_troubles`:         System troubles parts 1..8 (0x1B-0x30)
//!
//! ============================================================================
//! 2. 100% PASSIVE OPERATION (ZERO POLLING OVERHEAD):
//! ============================================================================
//! In this example, `main()` does NOT perform a single `satel.get_*()` query!
//! The client sits completely idle. All events received in the listener arrive
//! exclusively via asynchronous push notifications streamed by the ETHM-1 module.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 5_01_auto_read_push
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use chrono::Local;
use satel_integra::{Config, ConnectionConfig, SatelEvent, SatelIntegra};
use std::env;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Connection parameters
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Automated Push Streaming (4_01)");
    println!("==================================================");

    // 2. Configure Auto-Push flags
    // Enabling these flags instructs the ETHM-1 module to autonomously stream changes
    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,

        // --- Enable Auto-Push Subscriptions (All 18 Supported Groups) ---
        auto_read_zones_violation: true,              // Zone violations (0x00)
        auto_read_zones_tamper: true,                 // Zone tampers (0x01)
        auto_read_zones_alarm: true,                  // Zone alarms (0x02)
        auto_read_zones_tamper_alarm: true,           // Zone tamper alarms (0x03)
        auto_read_zones_alarm_memory: true,           // Zone alarm memory (0x04)
        auto_read_zones_tamper_alarm_memory: true,    // Zone tamper alarm memory (0x05)
        auto_read_zones_bypass: true,                 // Zone bypasses (0x06)
        auto_read_zones_no_violation_trouble: true,   // Zone 'no violation' trouble (0x07)
        auto_read_zones_long_violation_trouble: true, // Zone 'long violation' trouble (0x08)
        auto_read_partitions_armed_suppressed: true,  // Partitions armed suppressed (0x09)
        auto_read_partitions_armed_really: true,      // Partitions armed really (0x0A)
        auto_read_partitions_alarm: true,             // Partitions alarm (0x13)
        auto_read_partitions_alarm_memory: true,      // Partitions alarm memory (0x15)
        auto_read_partitions_entry_time: true,        // Partitions entry time (0x0E)
        auto_read_partitions_exit_time: true,         // Partitions exit time (0x0F, 0x10)
        auto_read_outputs_state: true,                // Outputs state (0x17)
        auto_read_system_troubles: true,              // System troubles (0x1B-0x30)
        auto_read_troubles_memory: true,              // Troubles memory (0x20-0x31)

        ..Config::default()
    };

    let satel = SatelIntegra::new(config);

    // 3. Subscribe to event broadcast BEFORE connecting
    let mut rx = satel.subscribe();

    // 4. Spawn background listener task to receive spontaneous push events
    let listener_handle = tokio::spawn(async move {
        println!("[Push Listener] Task active. Waiting for unsolicited panel frames...\n");

        while let Ok(event) = rx.recv().await {
            let ts = Local::now().format("%H:%M:%S%.3f");

            match event {
                // Auto-Push Registration Report from Handshake
                SatelEvent::AutoReadConfigured(report) => {
                    println!("------------------------------------------------------------------");
                    println!("[{}] [AUTO-PUSH REGISTRATION REPORT]", ts);
                    println!("Active items accepted by ETHM: {}/{}", report.success_count, report.total_requested);
                    for item in &report.items {
                        println!("  * {:<35} -> {:?}", item.name, item.state);
                    }
                    println!("------------------------------------------------------------------\n");
                }

                // Spontaneous State Push Notifications from ETHM-1
                SatelEvent::ZoneViolation { id, state } => {
                    println!("[{}] [PUSH: ZONE VIOLATION] Zone #{:03} -> {}", ts, id, if state { "VIOLATED (Movement)" } else { "NORMAL" });
                }
                SatelEvent::ZoneTamper { id, state } => {
                    println!("[{}] [PUSH: ZONE TAMPER]    Zone #{:03} -> {}", ts, id, if state { "TAMPERED!" } else { "NORMAL" });
                }
                SatelEvent::OutputChanged { id, state } => {
                    println!("[{}] [PUSH: OUTPUT CHANGED] Output #{:03} -> {}", ts, id, if state { "ON" } else { "OFF" });
                }
                SatelEvent::PartitionArmedReally { id, state } => {
                    println!("[{}] [PUSH: PARTITION ARM]  Partition #{:02} -> {}", ts, id, if state { "ARMED" } else { "DISARMED" });
                }
                SatelEvent::PartitionAlarm { id, state } => {
                    println!("[{}] [PUSH: PARTITION ALARM]Partition #{:02} -> {}", ts, id, if state { "ALARM ACTIVE!" } else { "CLEARED" });
                }
                SatelEvent::Trouble(kind, state) => {
                    println!("[{}] [PUSH: SYSTEM TROUBLE] {:?} -> {}", ts, kind, if state { "FAULT ACTIVE" } else { "RESTORED" });
                }
                SatelEvent::ConnectionChanged(state) => {
                    println!("[{}] [CONNECTION STATE]     State -> {:?}", ts, state);
                }

                // Ignore other metadata events in this demo
                _ => {}
            }
        }
        println!("[Push Listener] Event channel closed.");
    });

    // 5. Connect to the panel (executes handshake & registers 0x7F Push mask with ETHM-1)
    println!("Connecting to the panel (registering 0x7F push mask during handshake)...");
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 6. 100% Passive Listening Mode (NO manual queries!)
    println!("==================================================================");
    println!(" PASSIVE LISTENING ACTIVE (25 seconds)");
    println!(" Notice: Zero manual get_*() queries are executed by this script!");
    println!(" Walk in front of a PIR, open a door, or switch an output in DLOADX");
    println!(" to observe real-time spontaneous push frames streamed by ETHM-1.");
    println!("==================================================================\n");

    sleep(Duration::from_secs(25)).await;

    // 7. Disconnect cleanly
    println!("\nDisconnecting...");
    satel.disconnect().await?;
    sleep(Duration::from_millis(500)).await;

    // Abort background listener
    listener_handle.abort();
    println!("Disconnected cleanly. Done.");

    Ok(())
}
```

### examples/5_02_auto_poll_temperatures.rs

```rust
﻿//! Example 5_02: Automated cyclic background polling for wireless/wired temperatures.
//!
//! ============================================================================
//! 1. TEMPERATURE POLLING OVERVIEW & ARCHITECTURE:
//! ============================================================================
//! In the Satel Integra protocol, wireless ABAX temperature sensors (Command `0x7D`)
//! are NOT part of the hardware 0x7F Auto-Push subscription mask. Therefore,
//! temperature values cannot be streamed unsolicited by the ETHM-1 module.
//!
//! To provide fully autonomous, hands-off temperature monitoring without requiring
//! custom polling loops in user applications, the library includes a built-in
//! universal background worker (`SatelPollingWorker`):
//!
//!   - `polling_temperatures_zones`: List of zone IDs (1..256) with temperature sensors.
//!   - `polling_temperatures_interval_minutes`: Polling interval in minutes (min: 1).
//!
//! When configured:
//!   1. A background task runs alongside the network worker and state worker.
//!   2. Every N minutes, it queries each configured zone sequentially (with a 100ms
//!      queue safety pause between sensors).
//!   3. The poller automatically uses smart error blocking (`temp_blocking_enabled`),
//!      protecting the queue from missing or broken sensors.
//!   4. When a temperature changes by > 0.01°C, `SatelEvent::ZoneTemperatureChanged`
//!      is broadcast to all subscribers.
//!
//! ============================================================================
//! 2. 100% PASSIVE CONSUMER PATTERN:
//! ============================================================================
//! In this example, `main()` does NOT call `satel.get_zone_temperature()` directly!
//! The application simply subscribes to the event stream, connects, and receives
//! periodic temperature updates automatically.
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 5_02_auto_poll_temperatures
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use chrono::Local;
use satel_integra::{Config, ConnectionConfig, SatelEvent, SatelIntegra};
use std::env;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Read connection parameters
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Background Temperature Poller (4_02)");
    println!("==================================================");

    // 2. Configure background temperature polling
    // Define which zones have temperature sensors and set the polling cycle
    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        user_code,

        // --- Background Temperature Polling Configuration ---
        // Explicitly enable background temperature polling
        polling_temperatures: true,

        // List zone IDs where temperature probes are installed (e.g. ABAX wireless sensors)
        polling_temperatures_zones: vec![21, 22, 23, 24],

        // Polling interval in full minutes (minimum enforced: 1 minute)
        polling_temperatures_interval_minutes: 1,

        // Emit temperature events on every polling cycle, even if unchanged
        emit_unchanged_temperatures: true,

        // --- Smart Queue Protection & Blocking Parameters ---
        // Protect communication queue by blocking faulty/missing sensors
        temp_blocking_enabled: true,
        // Block sensor after 2 consecutive timeouts (default is 4)
        temp_max_timeout_errors: 3,
        // Block sensor after 2 consecutive 0xFFFF communication faults (default is 10)
        temp_max_sensor_errors: 3,

        // All auto_read_* flags are disabled (false) by default in Config::default()
        ..Config::default()
    };

    println!("Configuration:");
    println!("  - Target:                {}:{}", host, port);
    println!("  - Polling Temp Active:   {}", config.polling_temperatures);
    println!("  - Polled Temp Zones:     {:?}", config.polling_temperatures_zones);
    println!("  - Polling Interval:      {} min", config.polling_temperatures_interval_minutes);
    println!("  - Smart Blocking:        {}", config.temp_blocking_enabled);
    println!("  - Max Timeout Errors:    {}", config.temp_max_timeout_errors);
    println!("  - Max Sensor Errors:     {}", config.temp_max_sensor_errors);
    println!("  - Emit Unchanged Temps:  {}\n", config.emit_unchanged_temperatures);

    let satel = SatelIntegra::new(config);

    // 3. Subscribe to the event channel BEFORE connecting
    let mut rx = satel.subscribe();

    // 4. Spawn listener task to receive background temperature updates
    let listener_handle = tokio::spawn(async move {
        println!("[Event Listener] Active. Awaiting background temperature events...\n");

        while let Ok(event) = rx.recv().await {
            let ts = Local::now().format("%H:%M:%S%.3f");

            match event {
                // Background Temperature Updates
                SatelEvent::ZoneTemperatureChanged { id, temperature } => {
                    println!(
                        "[{}] [TEMPERATURE UPDATE] Zone #{:03} -> {:.1}°C",
                        ts, id, temperature
                    );
                }

                // Temperature Sensor Faults / Timeouts
                SatelEvent::ZoneTemperatureError { id, status } => {
                    println!(
                        "[{}] [TEMPERATURE ERROR]  Zone #{:03} -> Fault Status: {:?}",
                        ts, id, status
                    );
                }

                // Connection State Changes
                SatelEvent::ConnectionChanged(state) => {
                    println!("[{}] [CONNECTION] State -> {:?}", ts, state);
                }

                _ => {}
            }
        }
        println!("[Event Listener] Channel closed.");
    });

    // 5. Connect to the panel (spawns network worker, push receiver, and temperature poller)
    println!("Connecting to the panel...");
    satel.connect().await?;
    println!("Connected successfully!\n");

    // 6. Passive listening mode (watch background cycles arrive)
    println!("==================================================================");
    println!(" PASSIVE MONITORING ACTIVE (250 seconds)");
    println!(" The background poller will query configured temperature zones");
    println!(" every 1 minute. Temperature events will appear above.");
    println!(" Notice: Zero manual get_zone_temperature() calls in main()!");
    println!("==================================================================\n");

    sleep(Duration::from_secs(250)).await;

    // 7. Check cached values before exit
    println!("\n--- Cached Temperature Values ---");
    for zone_id in [21, 22, 23, 24] {
        if let Ok(Some(temp)) = satel.get_cached_zone_temperature(zone_id) {
            println!(
                "  Zone #{:03}: {:.1}°C (read at: {})",
                zone_id,
                temp.temperature,
                temp.read_at.format("%H:%M:%S")
            );
        } else {
            println!("  Zone #{:03}: No cached data", zone_id);
        }
    }

    // 8. Clean disconnect
    println!("\nDisconnecting...");
    satel.disconnect().await?;
    sleep(Duration::from_millis(500)).await;

    listener_handle.abort();
    println!("Disconnected cleanly. Done.");

    Ok(())
}
```

### examples/5_03_auto_poll_temperatures_encrypted.rs

```rust
//! Example 5_03: Automated cyclic background polling for temperatures over AES-192 encrypted connection.
//!
//! ============================================================================
//! 1. OVERVIEW:
//! ============================================================================
//! This example combines automated background temperature polling (`SatelPollingWorker`)
//! with transparent AES-192 encrypted TCP communication (`EncryptedStream`).
//!
//! When configured:
//!   1. Encrypted TCP connection is established using the integration key.
//!   2. A background worker queries configured ABAX / wired temperature zones every 1 minute.
//!   3. The application runs for 10 minutes (600 seconds) to test long-term encrypted connection stability.
//!
//! ============================================================================
//! 2. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 5_03_auto_poll_temperatures_encrypted
//!
//! Environment variables (optional):
//!   SATEL_HOST            - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT            - TCP port (default: 7094)
//!   SATEL_INTEGRATION_KEY - Integration key configured in DLOADX (default: "Jmtp")
//!   SATEL_CODE            - User access code (default: "1234")

use chrono::Local;
use satel_integra::{Config, ConnectionConfig, SatelEvent, SatelIntegra};
use std::env;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Read connection parameters
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let integration_key = env::var("SATEL_INTEGRATION_KEY").unwrap_or_else(|_| "Jmtp".to_string());
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Encrypted Temperature Poller (5_03)");
    println!("==================================================");

    // 2. Configure encrypted connection & background temperature polling
    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        encryption: true,
        integration_key: Some(integration_key.clone()),
        user_code,

        // --- Background Temperature Polling Configuration ---
        polling_temperatures: true,
        polling_temperatures_zones: vec![21, 22, 23, 24],
        polling_temperatures_interval_minutes: 1,
        emit_unchanged_temperatures: true,

        // --- Smart Queue Protection & Blocking Parameters ---
        temp_blocking_enabled: true,
        temp_max_timeout_errors: 3,
        temp_max_sensor_errors: 3,

        ..Config::default()
    };

    println!("Configuration:");
    println!("  - Target:                {}:{}", host, port);
    println!("  - Encryption:            ENABLED (AES-192 ECB session)");
    println!("  - Integration Key:       {}", integration_key);
    println!("  - Polling Temp Active:   {}", config.polling_temperatures);
    println!("  - Polled Temp Zones:     {:?}", config.polling_temperatures_zones);
    println!("  - Polling Interval:      {} min", config.polling_temperatures_interval_minutes);
    println!("  - Smart Blocking:        {}", config.temp_blocking_enabled);
    println!("  - Runtime Duration:      10 minutes (600s)\n");

    let satel = SatelIntegra::new(config);

    // 3. Subscribe to the event channel BEFORE connecting
    let mut rx = satel.subscribe();

    // 4. Spawn listener task to receive background temperature updates
    let listener_handle = tokio::spawn(async move {
        println!("[Event Listener] Active. Awaiting background temperature events...\n");

        while let Ok(event) = rx.recv().await {
            let ts = Local::now().format("%H:%M:%S%.3f");

            match event {
                SatelEvent::ZoneTemperatureChanged { id, temperature } => {
                    println!(
                        "[{}] [TEMPERATURE UPDATE] Zone #{:03} -> {:.1}°C",
                        ts, id, temperature
                    );
                }
                SatelEvent::ZoneTemperatureError { id, status } => {
                    println!(
                        "[{}] [TEMPERATURE ERROR]  Zone #{:03} -> Fault Status: {:?}",
                        ts, id, status
                    );
                }
                SatelEvent::ConnectionChanged(state) => {
                    println!("[{}] [CONNECTION] State -> {:?}", ts, state);
                }
                _ => {}
            }
        }
        println!("[Event Listener] Channel closed.");
    });

    // 5. Connect over encrypted TCP socket
    println!("Connecting over encrypted TCP socket...");
    satel.connect().await?;
    println!("Encrypted connection established successfully!\n");

    // 6. Passive monitoring loop for 10 minutes (with progress logging every minute)
    println!("==================================================================");
    println!(" PASSIVE ENCRYPTED MONITORING ACTIVE (10 minutes)");
    println!(" Temperature updates will appear automatically every 1 minute.");
    println!("==================================================================\n");

    for minute in 1..=10 {
        sleep(Duration::from_secs(60)).await;
        println!(
            "[{}] --- Status: {} of 10 minutes elapsed (connection active) ---",
            Local::now().format("%H:%M:%S"),
            minute
        );
    }

    // 7. Check cached values before exit
    println!("\n--- Cached Temperature Values at End of Run ---");
    for zone_id in [21, 22, 23, 24] {
        if let Ok(Some(temp)) = satel.get_cached_zone_temperature(zone_id) {
            println!(
                "  Zone #{:03}: {:.1}°C (read at: {})",
                zone_id,
                temp.temperature,
                temp.read_at.format("%H:%M:%S")
            );
        } else {
            println!("  Zone #{:03}: No cached data", zone_id);
        }
    }

    // 8. Clean disconnect
    println!("\nDisconnecting encrypted session...");
    satel.disconnect().await?;
    sleep(Duration::from_millis(500)).await;

    listener_handle.abort();
    println!("Disconnected cleanly. 10-minute stability test completed successfully!");

    Ok(())
}
```

### examples/6_01_config_all.rs

```rust
﻿//! Example 6_01: Complete reference guide showcasing all configuration fields in `Config`.
//!
//! ============================================================================
//! 1. CONFIGURATION SYSTEM OVERVIEW & ARCHITECTURE:
//! ============================================================================
//! The `satel_integra::Config` struct controls all operational aspects of the library,
//! categorized into 7 core architectural areas:
//!
//!   1. Connection Transport (TCP vs UART / RS-232)
//!   2. Network Timeouts & Queue TTL management
//!   3. User PIN Authentication
//!   4. Automatic Reconnection & Resilience
//!   5. Smart Temperature Error Blocking & Queue Protection
//!   6. Software Zone State Inversions (Violated <-> Normal, Tamper <-> OK)
//!   7. Hardware Auto-Push Subscriptions (ETHM-1 / INT-RS Command 0x7F)
//!   8. Background Periodic Polling (Wireless Temperature & Telemetry)
//!   9. Event Emission & Deduplication Filtering (emit_unchanged_*)
//!
//! ============================================================================
//! 2. CONFIGURATION REFERENCE TABLE & DEFAULTS:
//! ============================================================================
//! Field Name                          | Type             | Default      | Description
//! ------------------------------------+------------------+--------------+---------------------------------------------------
//! connection                          | ConnectionConfig | Tcp (7094)   | Network TCP or Serial UART transport
//! read_timeout_ms                     | u64              | 2000 ms      | Max time to wait for a standard response frame
//! write_timeout_ms                    | u64              | 500 ms       | Max time to flush a command to the socket/serial
//! temp_read_timeout_ms                | u64              | 2000 ms      | Dedicated timeout for wireless ABAX temperature
//! buffer_timeout_ms                   | u64              | 10000 ms     | Command TTL: expired commands are dropped
//! user_code                           | Option<String>   | None         | Global user PIN for control commands
//! auto_reconnect                      | bool             | true         | Automatically recover lost TCP/serial sessions
//! temp_blocking_enabled               | bool             | true         | Protect queue by blocking missing/broken sensors
//! temp_max_timeout_errors             | u32              | 4 errors     | Timeout threshold before blocking (0 ms return)
//! temp_max_sensor_errors              | u32              | 10 errors    | 0xFFFF error threshold before blocking (0 ms return)
//! io_violation_invert                 | Vec<u16>         | [] (Empty)   | List of zone IDs to logically invert violation
//! io_tamper_invert                    | Vec<u16>         | [] (Empty)   | List of zone IDs to logically invert tamper
//! io_alarm_invert                     | Vec<u16>         | [] (Empty)   | List of zone IDs to logically invert alarm
//! io_tamper_alarm_invert              | Vec<u16>         | [] (Empty)   | List of zone IDs to invert tamper alarm
//! io_alarm_memory_invert              | Vec<u16>         | [] (Empty)   | List of zone IDs to invert alarm memory
//! io_tamper_alarm_memory_invert       | Vec<u16>         | [] (Empty)   | List of zone IDs to invert tamper alarm memory
//! io_bypass_invert                    | Vec<u16>         | [] (Empty)   | List of zone IDs to invert bypass status
//! io_no_violation_trouble_invert      | Vec<u16>         | [] (Empty)   | List of zone IDs to invert no-violation trouble
//! io_long_violation_trouble_invert    | Vec<u16>         | [] (Empty)   | List of zone IDs to invert long-violation trouble
//! auto_read_zones_violation           | bool             | false        | Auto-Push: Zone movement / open states (0x00)
//! auto_read_zones_tamper              | bool             | false        | Auto-Push: Zone line/casing sabotage (0x01)
//! auto_read_zones_alarm               | bool             | false        | Auto-Push: Zone active alarms (0x02)
//! auto_read_zones_tamper_alarm        | bool             | false        | Auto-Push: Zone tamper alarms (0x03)
//! auto_read_zones_alarm_memory        | bool             | false        | Auto-Push: Zone alarm memory (0x04)
//! auto_read_zones_tamper_alarm_memory | bool             | false        | Auto-Push: Zone tamper alarm memory (0x05)
//! auto_read_zones_bypass              | bool             | false        | Auto-Push: Zone bypass status (0x06)
//! auto_read_zones_no_violation_trouble| bool             | false        | Auto-Push: Zone 'no violation' trouble (0x07)
//! auto_read_zones_long_violation_tr...| bool             | false        | Auto-Push: Zone 'long violation' trouble (0x08)
//! auto_read_partitions_armed_suppr... | bool             | false        | Auto-Push: Partitions armed suppressed (0x09)
//! auto_read_partitions_armed_really  | bool             | false        | Auto-Push: Partitions armed really (0x0A)
//! auto_read_partitions_alarm         | bool             | false        | Auto-Push: Partitions active alarms (0x13)
//! auto_read_partitions_alarm_memory  | bool             | false        | Auto-Push: Partitions alarm memory (0x15)
//! auto_read_partitions_entry_time    | bool             | false        | Auto-Push: Partitions entry delay time (0x0E)
//! auto_read_partitions_exit_time     | bool             | false        | Auto-Push: Partitions exit delay time (0x0F, 0x10)
//! auto_read_outputs_state            | bool             | false        | Auto-Push: Outputs / relays ON/OFF (0x17)
//! auto_read_system_troubles          | bool             | false        | Auto-Push: Active troubles parts 1..8 (0x1B-0x30)
//! auto_read_troubles_memory          | bool             | false        | Auto-Push: Trouble memory parts 1..8 (0x20-0x31)
//! polling_temperatures               | bool             | false        | Enable background cyclic polling for temperatures
//! polling_temperatures_zones         | Vec<u16>         | [] (Empty)   | Background cyclic polling: Zone IDs with temp sensors
//! polling_temperatures_interval_m... | u64              | 1 minute     | Background cyclic polling: Interval in minutes (min: 1)
//! emit_unchanged_temperatures        | bool             | false        | Emit temp event on every read even if unchanged
//! emit_unchanged_zones               | bool             | false        | Emit zone events on every read even if unchanged
//! emit_unchanged_outputs             | bool             | false        | Emit output events on every read even if unchanged
//! emit_unchanged_partitions          | bool             | false        | Emit partition events on every read even if unchanged
//! emit_unchanged_troubles            | bool             | false        | Emit trouble events on every read even if unchanged
//! emit_unchanged_system_status       | bool             | false        | Emit system status on every read even if unchanged
//!
//! ============================================================================
//! 3. EXECUTION INSTRUCTIONS:
//! ============================================================================
//! Run with default settings:
//!   cargo run --example 6_01_config_all
//!
//! Environment variables (optional):
//!   SATEL_HOST - IP address of the panel (default: "192.168.1.100")
//!   SATEL_PORT - TCP port (default: 7094)
//!   SATEL_CODE - User access code (default: "1234")

use satel_integra::{Config, ConnectionConfig, SatelEvent, SatelIntegra};
use std::env;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Read environment overrides
    let host = env::var("SATEL_HOST").unwrap_or_else(|_| "192.168.1.100".to_string());
    let port = env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = env::var("SATEL_CODE").ok().or_else(|| Some("1234".to_string()));

    println!("==================================================");
    println!(" SATEL INTEGRA - Complete Configuration Guide (5_01)");
    println!("==================================================");

    // ========================================================================
    // COMPLETE CONFIGURATION STRUCT (ALL FIELDS EXPLICITLY POPULATED)
    // ========================================================================
    let config = Config {
        // --------------------------------------------------------------------
        // 1. Connection Transport & Security
        // --------------------------------------------------------------------
        // TCP Connection (ETHM-1 / ETHM-1 Plus modules)
        connection: ConnectionConfig::Tcp {
            host: host.clone(),
            port,
        },
        // Alternatively, for Serial RS-232 / INT-RS hardware modules:
        // connection: ConnectionConfig::Uart {
        //     path: "COM3".to_string(), // or "/dev/ttyUSB0" on Linux
        //     baud_rate: 19200,         // Standard Satel INT-RS baud rate
        // },

        // Enable AES-192 ECB encrypted communication with ETHM-1 Plus. Default: false.
        // Requires setting `integration_key` (up to 12 ASCII characters).
        encryption: false,

        // Integration encryption key configured in DLOADX (Structure -> Modules -> ETHM-1).
        // Required when `encryption = true`. Stored in memory in plaintext.
        integration_key: None,

        // --------------------------------------------------------------------
        // 2. Network Timeouts & Queue Expiry
        // --------------------------------------------------------------------
        // Maximum time (in ms) to wait for a standard response frame from the panel.
        // Default: 2000 ms. Increase for slow GSM/cellular connections.
        read_timeout_ms: 2000,

        // Maximum time (in ms) to flush a request frame into the socket buffer.
        // Default: 500 ms.
        write_timeout_ms: 500,

        // Dedicated timeout (in ms) for wireless ABAX temperature sensor queries.
        // Default: 2000 ms.
        temp_read_timeout_ms: 2000,

        // Message TTL (in ms) in the buffer queue. If an outgoing command spends
        // more than this duration waiting in queue due to connection delays,
        // it expires and returns `Err(SatelError::MessageExpired)` rather than
        // performing obsolete actions later. Default: 10000 ms.
        buffer_timeout_ms: 10000,

        // --------------------------------------------------------------------
        // 3. User PIN Authentication
        // --------------------------------------------------------------------
        // Global 4-8 digit user access code for output control, arming, and clock sync.
        // When set, passing `None` to control methods automatically resolves to this PIN.
        user_code: user_code.clone(),

        // --------------------------------------------------------------------
        // 4. Resilience & Reconnection
        // --------------------------------------------------------------------
        // When true, the client automatically attempts to reconnect upon unexpected
        // socket termination or network drops with exponential backoff. Default: true.
        auto_reconnect: true,

        // --------------------------------------------------------------------
        // 5. Smart Temperature Error Blocking
        // --------------------------------------------------------------------
        // Protects the communication queue from being blocked by non-existent,
        // broken, or battery-depleted wireless temperature sensors. Default: true.
        temp_blocking_enabled: true,

        // Consecutive timeout threshold before a missing sensor is blocked.
        // Subsequent queries return `Err(SatelError::TempTooManyErrors)` instantly (0 ms).
        // Default: 4.
        temp_max_timeout_errors: 4,

        // Consecutive 0xFFFF error threshold before a broken sensor is blocked.
        // Default: 10.
        temp_max_sensor_errors: 10,

        // --------------------------------------------------------------------
        // 6. Software Zone State Inversions (Violated <-> Normal, Tamper <-> OK)
        // --------------------------------------------------------------------
        // Zone IDs (1..256) where physical violation state should be inverted (`!state`).
        io_violation_invert: vec![1, 2],

        // Zone IDs (1..256) where tamper state should be inverted.
        io_tamper_invert: Vec::new(),

        // Zone IDs (1..256) where alarm state should be inverted.
        io_alarm_invert: Vec::new(),

        // Zone IDs (1..256) where tamper alarm state should be inverted.
        io_tamper_alarm_invert: Vec::new(),

        // Zone IDs (1..256) where alarm memory state should be inverted.
        io_alarm_memory_invert: Vec::new(),

        // Zone IDs (1..256) where tamper alarm memory state should be inverted.
        io_tamper_alarm_memory_invert: Vec::new(),

        // Zone IDs (1..256) where bypass state should be inverted.
        io_bypass_invert: Vec::new(),

        // Zone IDs (1..256) where 'no violation' trouble state should be inverted.
        io_no_violation_trouble_invert: Vec::new(),

        // Zone IDs (1..256) where 'long violation' trouble state should be inverted.
        io_long_violation_trouble_invert: Vec::new(),

        // --------------------------------------------------------------------
        // 7. Hardware Auto-Push Subscriptions (ETHM-1 Command 0x7F)
        // --------------------------------------------------------------------
        // Automatically stream zone movement / open state changes (0x00).
        auto_read_zones_violation: true,

        // Automatically stream zone line/casing sabotage changes (0x01).
        auto_read_zones_tamper: true,

        // Automatically stream zone active alarms (0x02).
        auto_read_zones_alarm: true,

        // Automatically stream zone tamper alarms (0x03).
        auto_read_zones_tamper_alarm: true,

        // Automatically stream zone alarm memory changes (0x04).
        auto_read_zones_alarm_memory: true,

        // Automatically stream zone tamper alarm memory changes (0x05).
        auto_read_zones_tamper_alarm_memory: true,

        // Automatically stream zone bypass / unbypass state changes (0x06).
        auto_read_zones_bypass: true,

        // Automatically stream zone 'no violation' trouble changes (0x07).
        auto_read_zones_no_violation_trouble: true,

        // Automatically stream zone 'long violation' trouble changes (0x08).
        auto_read_zones_long_violation_trouble: true,

        // Automatically stream partition suppressed arming state (0x09).
        auto_read_partitions_armed_suppressed: true,

        // Automatically stream partition real physical arming state (0x0A).
        auto_read_partitions_armed_really: true,

        // Automatically stream partition active alarms (0x13).
        auto_read_partitions_alarm: true,

        // Automatically stream partition alarm memory (0x15).
        auto_read_partitions_alarm_memory: true,

        // Automatically stream partition entry delay countdown (0x0E).
        auto_read_partitions_entry_time: true,

        // Automatically stream partition exit delay countdown (0x0F, 0x10).
        auto_read_partitions_exit_time: true,

        // Automatically stream output relay ON / OFF transitions (0x17).
        auto_read_outputs_state: true,

        // Automatically stream active system troubles across parts 1..8 (0x1B-0x30).
        auto_read_system_troubles: true,

        // Automatically stream trouble memory changes across parts 1..8 (0x20-0x31).
        auto_read_troubles_memory: true,

        // --------------------------------------------------------------------
        // 8. Background Periodic Polling (Wireless Temperature & Telemetry)
        // --------------------------------------------------------------------
        // Explicitly enable background periodic temperature polling. Default: false.
        polling_temperatures: true,

        // List zone IDs (1..256) with temperature sensors to poll cyclically in background.
        // Default: [] (empty — manual polling only).
        polling_temperatures_zones: vec![21, 22, 23, 24],

        // Interval in minutes between temperature polling cycles (min: 1). Default: 1.
        polling_temperatures_interval_minutes: 1,

        // --------------------------------------------------------------------
        // 9. Event Emission & Deduplication Filtering (emit_unchanged_*)
        // --------------------------------------------------------------------
        // Emit temperature events on every query even if value is unchanged. Default: false.
        emit_unchanged_temperatures: true,

        // Emit zone events on every read even if state is unchanged. Default: false.
        emit_unchanged_zones: false,

        // Emit output events on every read even if state is unchanged. Default: false.
        emit_unchanged_outputs: false,

        // Emit partition events on every read even if state is unchanged. Default: false.
        emit_unchanged_partitions: false,

        // Emit trouble events on every read even if state is unchanged. Default: false.
        emit_unchanged_troubles: false,

        // Emit system status events on every read even if state is unchanged. Default: false.
        emit_unchanged_system_status: false,
    };

    println!("Config initialized successfully.");
    println!("  - Target Transport:    {}:{}", host, port);
    println!("  - Read Timeout:        {} ms", config.read_timeout_ms);
    println!("  - Write Timeout:       {} ms", config.write_timeout_ms);
    println!("  - Buffer Queue TTL:    {} ms", config.buffer_timeout_ms);
    println!("  - Auto Reconnect:      {}", config.auto_reconnect);
    println!("  - Temp Smart Blocking: {}", config.temp_blocking_enabled);
    println!("  - Violated Inversions: {:?}", config.io_violation_invert);
    println!("  - Auto-Push Enabled:   {}", config.is_auto_read_enabled());
    println!("  - Polling Temp Active: {}", config.polling_temperatures);
    println!("  - Polling Temp Zones:  {:?}", config.polling_temperatures_zones);
    println!("  - Polling Interval:    {} min", config.polling_temperatures_interval_minutes);
    println!("  - Emit Unchanged Temp: {}", config.emit_unchanged_temperatures);
    println!("  - Emit Unchanged Zones:{}", config.emit_unchanged_zones);
    println!("  - Emit Unchanged Out:  {}\n", config.emit_unchanged_outputs);

    let satel = SatelIntegra::new(config);

    // Subscribe to event stream before connect
    let mut rx = satel.subscribe();

    let listener_handle = tokio::spawn(async move {
        while let Ok(event) = rx.recv().await {
            if let SatelEvent::AutoReadConfigured(report) = event {
                println!("--- Auto-Push Hardware Registration Report ---");
                println!("Accepted by ETHM: {}/{} items active", report.success_count, report.total_requested);
                for item in &report.items {
                    println!("  * {:<35} -> {:?}", item.name, item.state);
                }
                println!("----------------------------------------------\n");
            }
        }
    });

    // Connect
    println!("Connecting to the panel...");
    satel.connect().await?;
    println!("Connected successfully!\n");

    // Wait a moment for handshake report
    sleep(Duration::from_secs(2)).await;

    // Display Connection Telemetry
    {
        let handle = satel.state_handle();
        let state = handle.read().unwrap();
        println!("Connection Telemetry Summary:");
        println!("  - Current State:    {:?}", state.telemetry.status.state);
        println!("  - Bytes Sent:       {}", state.telemetry.bytes_sent.load(std::sync::atomic::Ordering::Relaxed));
        println!("  - Bytes Received:   {}", state.telemetry.bytes_received.load(std::sync::atomic::Ordering::Relaxed));
        println!("  - Failed Attempts:  {}", state.telemetry.status.failed_attempts);
    }

    // Disconnect cleanly
    println!("\nDisconnecting...");
    satel.disconnect().await?;
    sleep(Duration::from_millis(500)).await;

    listener_handle.abort();
    println!("Disconnected cleanly. Done.");

    Ok(())
}
```
