use crate::auto_requester::SatelAutoRequester;
use crate::command::SatelCommand;
use crate::config::Config;
use crate::error::SatelError;
use crate::event::{SatelEvent, SyncCategory};
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
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, oneshot};

/// Primary client handle for communicating with the Satel Integra alarm control panel.
/// Cheaply cloneable (`Arc`-backed) — all clones share the same underlying connection.
#[derive(Clone)]
pub struct SatelIntegra {
    pub(crate) tx: mpsc::Sender<InternalMessage>,
    pub(crate) state: SatelStateHandle,
    pub(crate) config: Arc<RwLock<Config>>,
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
        let config_arc = Arc::new(RwLock::new(config));

        let worker = SatelCommunicationWorker {
            config: config_arc.clone(),
            state: state.clone(),
            rx,
            stream: None,
            state_worker_tx: None,
        };

        Self {
            tx,
            state,
            config: config_arc,
            worker: Arc::new(Mutex::new(Some(worker))),
            event_tx,
        }
    }

    /// Connects to the panel and spawns background tasks (actor worker, auto-requester, poller).
    pub async fn connect(&self) -> Result<(), SatelError> {
        self.config.read().unwrap().validate()?;

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

            let mut poller = SatelPollingWorker::new(self.clone());
            poller.register_task(Box::new(TemperaturePollingTask::new()));
            tokio::spawn(async move {
                poller.run().await;
            });

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

    /// Dynamically updates the configuration without dropping the active connection.
    /// WARNING: This function guarantees that the physical connection parameters 
    /// (IP, port, RS, encryption, key) will NEVER change, 
    /// even if the `new_config` object contains different values.
    /// The new connection parameters will be silently overwritten by the old ones from the current state.
    pub fn hot_reload_config(&self, mut new_config: Config) -> Result<(), SatelError> {
        new_config.validate()?;
        {
            let mut guard = self.config.write().unwrap();
            // Preserve connection parameters
            new_config.connection = guard.connection.clone();
            new_config.encryption = guard.encryption;
            new_config.integration_key = guard.integration_key.clone();
            
            *guard = new_config;
        }
        let _ = self.event_tx.send(SatelEvent::ConfigUpdated);
        Ok(())
    }

    /// Full configuration reload.
    /// Always disconnects the current connection (unless already disconnected) and 
    /// establishes it again, forcing the client to apply the full, 
    /// new hardware configuration from `new_config`.
    pub async fn reload_config(&self, new_config: Config) -> Result<(), SatelError> {
        new_config.validate()?;
        {
            let mut guard = self.config.write().unwrap();
            *guard = new_config;
        }
        let _ = self.event_tx.send(SatelEvent::ConfigUpdated);
        
        // Reconnect if currently connected or trying to connect
        let state = self.state_handle().read().unwrap().telemetry.status.state;
        if state != crate::state::ConnectionState::Disconnected {
            let _ = self.disconnect().await;
            self.connect().await?;
        }
        
        Ok(())
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
        
        let (cfg_write, cfg_read, cfg_buf) = {
            let c = self.config.read().unwrap();
            (c.write_timeout_ms, c.read_timeout_ms, c.buffer_timeout_ms)
        };

        let msg = InternalMessage::ExchangeStandard {
            data,
            write_timeout: write_timeout
                .unwrap_or(Duration::from_millis(cfg_write)),
            read_timeout: read_timeout
                .unwrap_or(Duration::from_millis(cfg_read)),
            created_at: Instant::now(),
            max_queue_time: Duration::from_millis(cfg_buf),
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

        let (cfg_write, cfg_read) = {
            let c = self.config.read().unwrap();
            (c.write_timeout_ms, c.read_timeout_ms)
        };

        let msg = InternalMessage::ExchangePriority {
            data,
            write_timeout: write_timeout
                .unwrap_or(Duration::from_millis(cfg_write)),
            read_timeout: read_timeout
                .unwrap_or(Duration::from_millis(cfg_read)),
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

    /// Queries the UTF-8 names of all zones configured in the panel.
    /// Emits `SatelEvent::SyncStarted`, `SatelEvent::SyncProgress` per item, and `SatelEvent::SyncFinished`.
    /// Returns `Err(SatelError::PanelVersionUnknown)` if Integra version has not been retrieved yet.
    /// Returns `Ok(true)` after querying all zones (1..=io_count).
    pub async fn get_all_zone_names(&self) -> Result<bool, SatelError> {
        let version = self.get_cached_version()?.ok_or(SatelError::PanelVersionUnknown)?;
        if version.io_count == 0 {
            return Err(SatelError::PanelVersionUnknown);
        }

        let total = version.io_count;
        let _ = self.event_tx.send(SatelEvent::SyncStarted {
            category: SyncCategory::Zones,
            total,
        });

        tracing::info!("Querying all zone names (1..={})...", total);
        let mut success_count = 0;
        let mut last_error: Option<SatelError> = None;

        for zone_id in 1..=total {
            match self.get_zone_name(zone_id).await {
                Ok(res) => {
                    success_count += 1;
                    let _ = self.event_tx.send(SatelEvent::SyncProgress {
                        category: SyncCategory::Zones,
                        current: zone_id,
                        total,
                        name: res.name,
                    });
                }
                Err(e) => {
                    tracing::warn!("Failed to query zone #{} name: {:?}", zone_id, e);
                    last_error = Some(e);
                }
            }
        }

        let _ = self.event_tx.send(SatelEvent::SyncFinished {
            category: SyncCategory::Zones,
            total,
            success_count,
            error: last_error.as_ref().map(|e| e.to_string()),
        });

        if success_count == 0 {
            if let Some(err) = last_error {
                return Err(err);
            }
        }

        Ok(true)
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

    /// Queries the UTF-8 names of all outputs configured in the panel.
    /// Emits `SatelEvent::SyncStarted`, `SatelEvent::SyncProgress` per item, and `SatelEvent::SyncFinished`.
    /// Returns `Err(SatelError::PanelVersionUnknown)` if Integra version has not been retrieved yet.
    /// Returns `Ok(true)` after querying all outputs (1..=io_count).
    pub async fn get_all_output_names(&self) -> Result<bool, SatelError> {
        let version = self.get_cached_version()?.ok_or(SatelError::PanelVersionUnknown)?;
        if version.io_count == 0 {
            return Err(SatelError::PanelVersionUnknown);
        }

        let total = version.io_count;
        let _ = self.event_tx.send(SatelEvent::SyncStarted {
            category: SyncCategory::Outputs,
            total,
        });

        tracing::info!("Querying all output names (1..={})...", total);
        let mut success_count = 0;
        let mut last_error: Option<SatelError> = None;

        for output_id in 1..=total {
            match self.get_output_name(output_id).await {
                Ok(res) => {
                    success_count += 1;
                    let _ = self.event_tx.send(SatelEvent::SyncProgress {
                        category: SyncCategory::Outputs,
                        current: output_id,
                        total,
                        name: res.name,
                    });
                }
                Err(e) => {
                    tracing::warn!("Failed to query output #{} name: {:?}", output_id, e);
                    last_error = Some(e);
                }
            }
        }

        let _ = self.event_tx.send(SatelEvent::SyncFinished {
            category: SyncCategory::Outputs,
            total,
            success_count,
            error: last_error.as_ref().map(|e| e.to_string()),
        });

        if success_count == 0 {
            if let Some(err) = last_error {
                return Err(err);
            }
        }

        Ok(true)
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

    /// Queries the UTF-8 names of all partitions configured in the panel.
    /// Emits `SatelEvent::SyncStarted`, `SatelEvent::SyncProgress` per item, and `SatelEvent::SyncFinished`.
    /// Returns `Err(SatelError::PanelVersionUnknown)` if Integra version has not been retrieved yet.
    /// Returns `Ok(true)` after querying all partitions (1..=32).
    pub async fn get_all_partition_names(&self) -> Result<bool, SatelError> {
        let version = self.get_cached_version()?.ok_or(SatelError::PanelVersionUnknown)?;
        if version.io_count == 0 {
            return Err(SatelError::PanelVersionUnknown);
        }

        let total = 32u16;
        let _ = self.event_tx.send(SatelEvent::SyncStarted {
            category: SyncCategory::Partitions,
            total,
        });

        tracing::info!("Querying all partition names (1..=32)...");
        let mut success_count = 0;
        let mut last_error: Option<SatelError> = None;

        for partition_id in 1..=total {
            match self.get_partition_name(partition_id).await {
                Ok(res) => {
                    success_count += 1;
                    let _ = self.event_tx.send(SatelEvent::SyncProgress {
                        category: SyncCategory::Partitions,
                        current: partition_id,
                        total,
                        name: res.name,
                    });
                }
                Err(e) => {
                    tracing::warn!("Failed to query partition #{} name: {:?}", partition_id, e);
                    last_error = Some(e);
                }
            }
        }

        let _ = self.event_tx.send(SatelEvent::SyncFinished {
            category: SyncCategory::Partitions,
            total,
            success_count,
            error: last_error.as_ref().map(|e| e.to_string()),
        });

        if success_count == 0 {
            if let Some(err) = last_error {
                return Err(err);
            }
        }

        Ok(true)
    }

    /// Queries the temperature of a zone (0x7D).
    /// If `temp_blocking_enabled` is active, faulty probes are verified and automatically blocked.
    pub async fn get_zone_temperature(&self, zone_id: u16) -> Result<ZoneTemperature, SatelError> {
        if self.config.read().unwrap().temp_blocking_enabled {
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

        let temp_read_timeout = self.config.read().unwrap().temp_read_timeout_ms;
        let response_result = self
            .exchange(
                cmd,
                None,
                Some(Duration::from_millis(temp_read_timeout)),
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

                    if self.config.read().unwrap().emit_unchanged_temperatures || (old_temp - temp).abs() > 0.01 {
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
                || info.temperature_timeout_errors_current >= self.config.read().unwrap().temp_max_timeout_errors
                || info.temperature_sensor_errors_current >= self.config.read().unwrap().temp_max_sensor_errors;

            if is_blocked {
                let status = if info.temperature_status == TemperatureSensorStatus::BlockSensorMissing
                    || info.temperature_timeout_errors_current >= self.config.read().unwrap().temp_max_timeout_errors
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

                if self.config.read().unwrap().emit_unchanged_temperatures {
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
        let result = process_zones_tamper(&response, &self.config.read().unwrap().io_tamper_invert)?;
        self.update_zones_tamper_internal(result)?;
        tracing::info!("Updated zones tamper states in cache");
        Ok(())
    }

    /// Queries the alarm states of all zones (0x02) and updates the cache.
    pub async fn get_zones_alarm(&self) -> Result<(), SatelError> {
        tracing::info!("Querying all zones alarm state (0x02)...");
        let cmd = vec![SatelCommand::ZonesAlarm.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_alarm(&response, &self.config.read().unwrap().io_alarm_invert)?;
        self.update_zones_alarm_internal(result)?;
        tracing::info!("Updated zones alarm states in cache");
        Ok(())
    }

    /// Queries the violation states of all zones (0x00) and updates the cache.
    pub async fn get_zones_violation(&self) -> Result<(), SatelError> {
        tracing::info!("Querying all zones violation state (0x00)...");
        let cmd = vec![SatelCommand::ZonesViolation.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_violation(&response, &self.config.read().unwrap().io_violation_invert)?;
        self.update_zones_violation_internal(result)?;
        tracing::info!("Updated zones violation states in cache");
        Ok(())
    }

    /// Queries the tamper alarm states of all zones (0x03) and updates the cache.
    pub async fn get_zones_tamper_alarm(&self) -> Result<(), SatelError> {
        tracing::info!("Querying all zones tamper alarm state (0x03)...");
        let cmd = vec![SatelCommand::ZonesTamperAlarm.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_tamper_alarm(&response, &self.config.read().unwrap().io_tamper_alarm_invert)?;
        self.update_zones_tamper_alarm_internal(result)?;
        tracing::info!("Updated zones tamper alarm states in cache");
        Ok(())
    }

    /// Queries the alarm memory states of all zones (0x04) and updates the cache.
    pub async fn get_zones_alarm_memory(&self) -> Result<(), SatelError> {
        tracing::info!("Querying all zones alarm memory state (0x04)...");
        let cmd = vec![SatelCommand::ZonesAlarmMemory.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_alarm_memory(&response, &self.config.read().unwrap().io_alarm_memory_invert)?;
        self.update_zones_alarm_memory_internal(result)?;
        tracing::info!("Updated zones alarm memory states in cache");
        Ok(())
    }

    /// Queries the tamper alarm memory states of all zones (0x05) and updates the cache.
    pub async fn get_zones_tamper_alarm_memory(&self) -> Result<(), SatelError> {
        tracing::info!("Querying all zones tamper alarm memory state (0x05)...");
        let cmd = vec![SatelCommand::ZonesTamperAlarmMemory.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_tamper_alarm_memory(&response, &self.config.read().unwrap().io_tamper_alarm_memory_invert)?;
        self.update_zones_tamper_alarm_memory_internal(result)?;
        tracing::info!("Updated zones tamper alarm memory states in cache");
        Ok(())
    }

    /// Queries the bypass states of all zones (0x06) and updates the cache.
    pub async fn get_zones_bypass(&self) -> Result<(), SatelError> {
        tracing::info!("Querying all zones bypass state (0x06)...");
        let cmd = vec![SatelCommand::ZonesBypass.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_bypass(&response, &self.config.read().unwrap().io_bypass_invert)?;
        self.update_zones_bypass_internal(result)?;
        tracing::info!("Updated zones bypass states in cache");
        Ok(())
    }

    /// Queries the 'no violation trouble' states of all zones (0x07) and updates the cache.
    pub async fn get_zones_no_violation_trouble(&self) -> Result<(), SatelError> {
        tracing::info!("Querying all zones 'no violation trouble' state (0x07)...");
        let cmd = vec![SatelCommand::ZonesNoViolationTrouble.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_no_violation_trouble(&response, &self.config.read().unwrap().io_no_violation_trouble_invert)?;
        self.update_zones_no_violation_trouble_internal(result)?;
        tracing::info!("Updated zones 'no violation trouble' states in cache");
        Ok(())
    }

    /// Queries the 'long violation trouble' states of all zones (0x08) and updates the cache.
    pub async fn get_zones_long_violation_trouble(&self) -> Result<(), SatelError> {
        tracing::info!("Querying all zones 'long violation trouble' state (0x08)...");
        let cmd = vec![SatelCommand::ZonesLongViolationTrouble.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_long_violation_trouble(&response, &self.config.read().unwrap().io_long_violation_trouble_invert)?;
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
        if let Some(ref c) = self.config.read().unwrap().user_code {
            return Ok(c.clone());
        }
        Err(SatelError::InvalidUserCode)
    }
}
