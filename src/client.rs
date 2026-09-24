use crate::auto_requester::SatelAutoRequester;
use crate::command::SatelCommand;
use crate::config::Config;
use crate::error::SatelError;
use crate::event::{SatelEvent, SyncCategory};
use crate::output_catalog::{OutputControl, OutputFunction};
use crate::parsers::{
    process_ethm_version, process_integra_version,
    process_output_response, process_outputs_state,
    process_partition_response,
    process_partitions_alarm, process_partitions_alarm_memory,
    process_partitions_armed_really, process_partitions_armed_suppressed,
    process_partitions_entry_time, process_partitions_exit_time_gt_10s,
    process_partitions_exit_time_lt_10s, process_rtc_and_status,
    process_troubles_frame, process_troubles_memory_part2,
    process_troubles_memory_part3, process_troubles_memory_part5,
    process_troubles_memory_part7, process_troubles_part1,
    process_troubles_part2, process_troubles_part3, process_troubles_part4,
    process_troubles_part5, process_troubles_part6, process_troubles_part7,
    process_troubles_part8, process_zone_response,
    process_zone_temperature,
    process_zones_alarm, process_zones_alarm_memory, process_zones_bypass,
    process_zones_long_violation_trouble, process_zones_no_violation_trouble,
    process_zones_tamper, process_zones_tamper_alarm,
    process_zones_tamper_alarm_memory, process_zones_violation,
};
use crate::partition_catalog::PartitionType;
use crate::polling_worker::{SatelPollingWorker, TemperaturePollingTask};
use crate::state::{
    AutoReadReport, ConnectionStatistics, EthmVersion, IntegraVersion, OutputName, OutputParams,
    PartitionName, PartitionParams, SatelState, SatelStateHandle, SystemStatus,
    TemperatureSensorStatus, TroublesData, TroublesPart1Data, TroublesPart2Data,
    TroublesPart3Data, TroublesPart4Data, TroublesPart5Data, TroublesPart6Data, TroublesPart7Data,
    TroublesPart8Data, TroublesMemoryPart2Data, TroublesMemoryPart3Data, TroublesMemoryPart5Data,
    TroublesMemoryPart7Data, ZoneName, ZoneParams, ZoneStatus, ZoneTemperature,
};
use crate::worker::{InternalMessage, SatelCommunicationWorker};
use crate::zone_catalog::ZoneReaction;
use chrono::Local;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
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
    pub(crate) auto_read_dirty: Arc<AtomicBool>,
    pub(crate) session_zone_type: Arc<AtomicU8>,
    pub(crate) session_output_type: Arc<AtomicU8>,
    pub(crate) session_partition_type: Arc<AtomicU8>,
}

impl SatelIntegra {
    /// Creates a new `SatelIntegra` instance with the given configuration.
    pub fn new(config: Config) -> Self {
        crate::init_logging();
        let state = Arc::new(std::sync::RwLock::new(SatelState::new()));
        let (tx, rx) = mpsc::channel(100);
        let (event_tx, _) = broadcast::channel(1024);
        let extended = config.extended_name_read;
        let config_arc = Arc::new(RwLock::new(config));
        let auto_read_dirty = Arc::new(AtomicBool::new(false));
        let session_zone_type = Arc::new(AtomicU8::new(if extended { 5 } else { 1 }));
        let session_output_type = Arc::new(AtomicU8::new(if extended { 17 } else { 4 }));
        let session_partition_type = Arc::new(AtomicU8::new(if extended { 19 } else { 0 }));

        let worker = SatelCommunicationWorker {
            config: config_arc.clone(),
            state: state.clone(),
            rx,
            stream: None,
            state_worker_tx: None,
            auto_read_dirty: auto_read_dirty.clone(),
        };

        Self {
            tx,
            state,
            config: config_arc,
            worker: Arc::new(Mutex::new(Some(worker))),
            event_tx,
            auto_read_dirty,
            session_zone_type,
            session_output_type,
            session_partition_type,
        }
    }

    /// Returns a copy of the current configuration.
    pub fn get_config(&self) -> Config {
        self.config.read().unwrap().clone()
    }

    /// Connects to the panel and spawns background tasks (actor worker, auto-requester, poller).
    pub async fn connect(&self) -> Result<(), SatelError> {
        self.config.read().unwrap().validate()?;
        {
            let extended = self.config.read().unwrap().extended_name_read;
            self.session_zone_type.store(if extended { 5 } else { 1 }, Ordering::SeqCst);
            self.session_output_type.store(if extended { 17 } else { 4 }, Ordering::SeqCst);
            self.session_partition_type.store(if extended { 19 } else { 0 }, Ordering::SeqCst);
        }

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
    ///
    /// If auto-read categories (`auto_read_*`) have changed, the 0x7F push notification mask
    /// is automatically re-sent to the panel on the next keep-alive tick without reconnecting.
    pub fn hot_reload_config(&self, mut new_config: Config) -> Result<(), SatelError> {
        new_config.validate()?;
        let (mask_changed, extended_changed, new_extended) = {
            let mut guard = self.config.write().unwrap();
            // Preserve connection parameters
            new_config.connection = guard.connection.clone();
            new_config.encryption = guard.encryption;
            new_config.integration_key = guard.integration_key.clone();

            let old_mask = SatelCommunicationWorker::satel_connection_worker_connect_build_push_mask(&guard, true);
            let new_mask = SatelCommunicationWorker::satel_connection_worker_connect_build_push_mask(&new_config, true);
            let changed = old_mask != new_mask;

            let ext_changed = guard.extended_name_read != new_config.extended_name_read;
            let ext = new_config.extended_name_read;

            *guard = new_config;
            (changed, ext_changed, ext)
        };
        if mask_changed {
            self.auto_read_dirty.store(true, Ordering::SeqCst);
        }
        if extended_changed {
            self.session_zone_type.store(if new_extended { 5 } else { 1 }, Ordering::SeqCst);
            self.session_output_type.store(if new_extended { 17 } else { 4 }, Ordering::SeqCst);
            self.session_partition_type.store(if new_extended { 19 } else { 0 }, Ordering::SeqCst);
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

    /// Returns a snapshot of connection statistics and telemetry counters.
    pub fn statistics(&self) -> ConnectionStatistics {
        let guard = self.state.read().unwrap();
        guard.telemetry.statistics()
    }

    /// Resets connection statistics and telemetry counters.
    pub fn reset_statistics(&self) {
        let mut guard = self.state.write().unwrap();
        guard.telemetry.reset();
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

    async fn query_zone_internal(&self, zone_id: u16) -> Result<(ZoneName, Option<ZoneParams>), SatelError> {
        let extended = self.config.read().unwrap().extended_name_read;
        let session_type = self.session_zone_type.load(Ordering::SeqCst);
        let types_to_try: Vec<u8> = if extended {
            if session_type == 5 {
                vec![5, 1]
            } else {
                vec![1]
            }
        } else {
            vec![1]
        };

        let device_id: u8 = if zone_id == 256 { 0 } else { zone_id as u8 };

        for &dev_type in &types_to_try {
            let cmd = vec![
                SatelCommand::ReadDeviceName.to_byte(),
                dev_type,
                device_id,
            ];
            let response = self.exchange(cmd, None, None).await?;

            if !response.is_empty() && response[0] == SatelCommand::ResultCode.to_byte() {
                // Centrala odrzuciła zapytanie (0xEF) — próbujemy niższy typ w łańcuchu
                continue;
            }

            if !response.is_empty() && response[0] == SatelCommand::ReadDeviceName.to_byte() {
                if dev_type < session_type {
                    self.session_zone_type.store(dev_type, Ordering::SeqCst);
                    tracing::info!("Session zone query type downgraded to {}", dev_type);
                }

                let (id, s_name, params) = process_zone_response(&response)?;

                {
                    let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
                    if let Some(zone) = state.zones.get_mut((id.wrapping_sub(1) % 256) as usize) {
                        zone.zone_name = s_name.name.clone();
                        zone.zone_name_read_at = s_name.read_at;
                        zone.reaction = Some(params.reaction);
                        zone.partition = params.partition;
                        zone.params_read_at = Some(params.read_at);
                    }
                }

                return Ok((s_name, Some(params)));
            } else {
                return Err(SatelError::InvalidFrame);
            }
        }

        // Wszystkie próbowane typy zwróciły ResultCode (0xEF) — pozycja nieskonfigurowana
        let now = chrono::Local::now();
        let empty_name = ZoneName {
            name: String::new(),
            read_at: now,
        };
        {
            let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
            if let Some(zone) = state.zones.get_mut((zone_id.wrapping_sub(1) % 256) as usize) {
                zone.zone_name = String::new();
                zone.zone_name_read_at = now;
                zone.reaction = None;
                zone.partition = None;
                zone.params_read_at = Some(now);
            }
        }
        Ok((empty_name, None))
    }

    /// Queries the UTF-8 name of a zone (0xEE).
    /// If `extended_name_read` is enabled, also emits `SatelEvent::ZoneParamsReceived`.
    pub async fn get_zone_name(&self, zone_id: u16) -> Result<ZoneName, SatelError> {
        tracing::info!("Querying zone name #{}", zone_id);
        let (s_name, maybe_params) = self.query_zone_internal(zone_id).await?;
        if let Some(params) = maybe_params {
            let _ = self.event_tx.send(SatelEvent::ZoneNameReceived {
                id: zone_id,
                name: s_name.name.clone(),
            });
            if self.config.read().unwrap().extended_name_read {
                let _ = self.event_tx.send(SatelEvent::ZoneParamsReceived {
                    id: zone_id,
                    params,
                });
            }
        }
        tracing::info!("Retrieved zone #{} name: {}", zone_id, s_name.name);
        Ok(s_name)
    }

    /// Queries the parameters of a zone (reaction type, partition).
    /// Emits `SatelEvent::ZoneNameReceived` and `SatelEvent::ZoneParamsReceived`.
    pub async fn get_zone_params(&self, zone_id: u16) -> Result<ZoneParams, SatelError> {
        tracing::info!("Querying zone params #{}", zone_id);
        let (s_name, maybe_params) = self.query_zone_internal(zone_id).await?;
        if let Some(params) = maybe_params {
            let _ = self.event_tx.send(SatelEvent::ZoneNameReceived {
                id: zone_id,
                name: s_name.name,
            });
            let _ = self.event_tx.send(SatelEvent::ZoneParamsReceived {
                id: zone_id,
                params: params.clone(),
            });
            Ok(params)
        } else {
            Ok(ZoneParams {
                zone_id,
                reaction: ZoneReaction::from_code(0),
                partition: None,
                read_at: s_name.read_at,
            })
        }
    }

    /// Returns the cached zone name from memory.
    pub fn get_cached_zone_name(&self, zone_id: u16) -> Result<Option<ZoneName>, SatelError> {
        let state = self.state.read().map_err(|_| SatelError::StatePoisoned)?;
        Ok(state
            .zones
            .get((zone_id.wrapping_sub(1) % 256) as usize)
            .map(|z| z.to_zone_name()))
    }

    /// Returns the cached zone parameters from memory.
    pub fn get_cached_zone_params(&self, zone_id: u16) -> Result<Option<ZoneParams>, SatelError> {
        let state = self.state.read().map_err(|_| SatelError::StatePoisoned)?;
        Ok(state
            .zones
            .get((zone_id.wrapping_sub(1) % 256) as usize)
            .and_then(|z| z.to_zone_params()))
    }

    /// Returns the most recently configured auto-read push notification report from memory.
    pub fn auto_read_report(&self) -> Result<Option<AutoReadReport>, SatelError> {
        let state = self.state.read().map_err(|_| SatelError::StatePoisoned)?;
        Ok(state.auto_read_report.clone())
    }

    /// Queries the UTF-8 names of all zones configured in the panel.
    /// Emits `SatelEvent::SyncStarted`, `SatelEvent::SyncProgress` per item, and `SatelEvent::SyncFinished`.
    /// When `extended_name_read` is enabled, also emits `SatelEvent::ZoneParamsReceived` for each zone.
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

    async fn query_output_internal(&self, output_id: u16) -> Result<(OutputName, Option<OutputParams>), SatelError> {
        let extended = self.config.read().unwrap().extended_name_read;
        let session_type = self.session_output_type.load(Ordering::SeqCst);
        let types_to_try: Vec<u8> = if extended {
            if session_type == 17 {
                vec![17, 4]
            } else {
                vec![4]
            }
        } else {
            vec![4]
        };

        let device_id: u8 = if output_id == 256 { 0 } else { output_id as u8 };

        for &dev_type in &types_to_try {
            let cmd = vec![
                SatelCommand::ReadDeviceName.to_byte(),
                dev_type,
                device_id,
            ];
            let response = self.exchange(cmd, None, None).await?;

            if !response.is_empty() && response[0] == SatelCommand::ResultCode.to_byte() {
                continue;
            }

            if !response.is_empty() && response[0] == SatelCommand::ReadDeviceName.to_byte() {
                if dev_type < session_type {
                    self.session_output_type.store(dev_type, Ordering::SeqCst);
                    tracing::info!("Session output query type downgraded to {}", dev_type);
                }

                let (id, s_name, params) = process_output_response(&response)?;

                {
                    let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
                    if let Some(output) = state.outputs.get_mut((id.wrapping_sub(1) % 256) as usize) {
                        output.name = s_name.name.clone();
                        output.name_read_at = s_name.read_at;
                        output.function = Some(params.function);
                        output.duration = params.duration;
                        output.control = Some(params.control.clone());
                        output.params_read_at = Some(params.read_at);
                    }
                }

                return Ok((s_name, Some(params)));
            } else {
                return Err(SatelError::InvalidFrame);
            }
        }

        let now = chrono::Local::now();
        let empty_name = OutputName {
            name: String::new(),
            read_at: now,
        };
        {
            let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
            if let Some(output) = state.outputs.get_mut((output_id.wrapping_sub(1) % 256) as usize) {
                output.name = String::new();
                output.name_read_at = now;
                output.function = None;
                output.duration = None;
                output.control = None;
                output.params_read_at = Some(now);
            }
        }
        Ok((empty_name, None))
    }

    /// Queries the UTF-8 name of an output (0xEE).
    /// If `extended_name_read` is enabled, also emits `SatelEvent::OutputParamsReceived`.
    pub async fn get_output_name(&self, output_id: u16) -> Result<OutputName, SatelError> {
        tracing::info!("Querying output name #{}", output_id);
        let (s_name, maybe_params) = self.query_output_internal(output_id).await?;
        if let Some(params) = maybe_params {
            let _ = self.event_tx.send(SatelEvent::OutputNameReceived {
                id: output_id,
                name: s_name.name.clone(),
            });
            if self.config.read().unwrap().extended_name_read {
                let _ = self.event_tx.send(SatelEvent::OutputParamsReceived {
                    id: output_id,
                    params,
                });
            }
        }
        tracing::info!("Retrieved output #{} name: {}", output_id, s_name.name);
        Ok(s_name)
    }

    /// Queries the parameters of an output (function, duration, control capability).
    /// Emits `SatelEvent::OutputNameReceived` and `SatelEvent::OutputParamsReceived`.
    pub async fn get_output_params(&self, output_id: u16) -> Result<OutputParams, SatelError> {
        tracing::info!("Querying output params #{}", output_id);
        let (s_name, maybe_params) = self.query_output_internal(output_id).await?;
        if let Some(params) = maybe_params {
            let _ = self.event_tx.send(SatelEvent::OutputNameReceived {
                id: output_id,
                name: s_name.name,
            });
            let _ = self.event_tx.send(SatelEvent::OutputParamsReceived {
                id: output_id,
                params: params.clone(),
            });
            Ok(params)
        } else {
            Ok(OutputParams {
                output_id,
                function: OutputFunction::Unused,
                duration: None,
                control: OutputControl::None,
                read_at: s_name.read_at,
            })
        }
    }

    /// Returns the cached output name from memory.
    pub fn get_cached_output_name(&self, output_id: u16) -> Result<Option<OutputName>, SatelError> {
        let state = self.state.read().map_err(|_| SatelError::StatePoisoned)?;
        Ok(state
            .outputs
            .get((output_id.wrapping_sub(1) % 256) as usize)
            .map(|o| o.to_output_name()))
    }

    /// Returns the cached output parameters from memory.
    pub fn get_cached_output_params(&self, output_id: u16) -> Result<Option<OutputParams>, SatelError> {
        let state = self.state.read().map_err(|_| SatelError::StatePoisoned)?;
        Ok(state
            .outputs
            .get((output_id.wrapping_sub(1) % 256) as usize)
            .and_then(|o| o.to_output_params()))
    }

    /// Returns the cached output control capability from memory.
    /// Returns `None` if output parameters have not been read yet.
    pub fn output_control(&self, output_id: u16) -> Option<OutputControl> {
        let state = self.state.read().ok()?;
        state
            .outputs
            .get((output_id.wrapping_sub(1) % 256) as usize)
            .and_then(|o| o.to_output_params())
            .map(|p| p.control)
    }

    /// Returns whether the output is controllable based on cached parameters.
    /// Returns `None` if output parameters have not been read yet.
    pub fn is_output_controllable(&self, output_id: u16) -> Option<bool> {
        self.output_control(output_id).map(|c| c.is_controllable())
    }

    /// Queries the UTF-8 names of all outputs configured in the panel.
    /// Emits `SatelEvent::SyncStarted`, `SatelEvent::SyncProgress` per item, and `SatelEvent::SyncFinished`.
    /// When `extended_name_read` is enabled, also emits `SatelEvent::OutputParamsReceived` for each output.
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

    async fn query_partition_internal(&self, partition_id: u16) -> Result<(PartitionName, Option<PartitionParams>), SatelError> {
        let extended = self.config.read().unwrap().extended_name_read;
        let session_type = self.session_partition_type.load(Ordering::SeqCst);
        let types_to_try: Vec<u8> = if extended {
            match session_type {
                19 => vec![19, 18, 16, 0],
                18 => vec![18, 16, 0],
                16 => vec![16, 0],
                _ => vec![0],
            }
        } else {
            vec![0]
        };

        let device_id: u8 = partition_id as u8;

        for &dev_type in &types_to_try {
            let cmd = vec![
                SatelCommand::ReadDeviceName.to_byte(),
                dev_type,
                device_id,
            ];
            let response = self.exchange(cmd, None, None).await?;

            if !response.is_empty() && response[0] == SatelCommand::ResultCode.to_byte() {
                continue;
            }

            if !response.is_empty() && response[0] == SatelCommand::ReadDeviceName.to_byte() {
                if dev_type < session_type {
                    self.session_partition_type.store(dev_type, Ordering::SeqCst);
                    tracing::info!("Session partition query type downgraded to {}", dev_type);
                }

                let (id, s_name, params) = process_partition_response(&response)?;

                {
                    let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
                    if let Some(partition) = state.partitions.get_mut((id.wrapping_sub(1) % 32) as usize) {
                        partition.name = s_name.name.clone();
                        partition.name_read_at = s_name.read_at;
                        partition.partition_type = Some(params.partition_type);
                        partition.object_number = params.object_number;
                        partition.options = params.options;
                        partition.auto_arm_defer = params.auto_arm_defer;
                        partition.dependent_partitions = params.dependent_partitions;
                        partition.params_read_at = Some(params.read_at);
                    }
                }

                return Ok((s_name, Some(params)));
            } else {
                return Err(SatelError::InvalidFrame);
            }
        }

        let now = chrono::Local::now();
        let empty_name = PartitionName {
            name: String::new(),
            read_at: now,
        };
        {
            let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
            if let Some(partition) = state.partitions.get_mut((partition_id.wrapping_sub(1) % 32) as usize) {
                partition.name = String::new();
                partition.name_read_at = now;
                partition.partition_type = None;
                partition.object_number = None;
                partition.options = None;
                partition.auto_arm_defer = None;
                partition.dependent_partitions = None;
                partition.params_read_at = Some(now);
            }
        }
        Ok((empty_name, None))
    }

    /// Queries the UTF-8 name of a partition (0xEE).
    /// If `extended_name_read` is enabled, also emits `SatelEvent::PartitionParamsReceived`.
    pub async fn get_partition_name(&self, partition_id: u16) -> Result<PartitionName, SatelError> {
        tracing::info!("Querying partition name #{}", partition_id);
        let (s_name, maybe_params) = self.query_partition_internal(partition_id).await?;
        if let Some(params) = maybe_params {
            let _ = self.event_tx.send(SatelEvent::PartitionNameReceived {
                id: partition_id,
                name: s_name.name.clone(),
            });
            if self.config.read().unwrap().extended_name_read {
                let _ = self.event_tx.send(SatelEvent::PartitionParamsReceived {
                    id: partition_id,
                    params,
                });
            }
        }
        tracing::info!("Retrieved partition #{} name: {}", partition_id, s_name.name);
        Ok(s_name)
    }

    /// Queries the parameters of a partition (type, object, options, timers, dependencies).
    /// Emits `SatelEvent::PartitionNameReceived` and `SatelEvent::PartitionParamsReceived`.
    pub async fn get_partition_params(&self, partition_id: u16) -> Result<PartitionParams, SatelError> {
        tracing::info!("Querying partition params #{}", partition_id);
        let (s_name, maybe_params) = self.query_partition_internal(partition_id).await?;
        if let Some(params) = maybe_params {
            let _ = self.event_tx.send(SatelEvent::PartitionNameReceived {
                id: partition_id,
                name: s_name.name,
            });
            let _ = self.event_tx.send(SatelEvent::PartitionParamsReceived {
                id: partition_id,
                params: params.clone(),
            });
            Ok(params)
        } else {
            Ok(PartitionParams {
                partition_id,
                partition_type: PartitionType::Normal,
                object_number: None,
                options: None,
                auto_arm_defer: None,
                dependent_partitions: None,
                read_at: s_name.read_at,
            })
        }
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

    /// Returns the cached partition parameters from memory.
    pub fn get_cached_partition_params(
        &self,
        partition_id: u16,
    ) -> Result<Option<PartitionParams>, SatelError> {
        let state = self.state.read().map_err(|_| SatelError::StatePoisoned)?;
        Ok(state
            .partitions
            .get((partition_id.wrapping_sub(1) % 32) as usize)
            .and_then(|p| p.to_partition_params()))
    }

    /// Queries the UTF-8 names of all partitions configured in the panel.
    /// Emits `SatelEvent::SyncStarted`, `SatelEvent::SyncProgress` per item, and `SatelEvent::SyncFinished`.
    /// When `extended_name_read` is enabled, also emits `SatelEvent::PartitionParamsReceived` for each partition.
    /// Returns `Err(SatelError::PanelVersionUnknown)` if Integra version has not been retrieved yet.
    /// Returns `Ok(true)` after querying all partitions (1..=partition_count).
    pub async fn get_all_partition_names(&self) -> Result<bool, SatelError> {
        let version = self.get_cached_version()?.ok_or(SatelError::PanelVersionUnknown)?;
        if version.io_count == 0 || version.partition_count == 0 {
            return Err(SatelError::PanelVersionUnknown);
        }

        let total = version.partition_count;
        let _ = self.event_tx.send(SatelEvent::SyncStarted {
            category: SyncCategory::Partitions,
            total,
        });

        tracing::info!("Querying all partition names (1..={total})...");
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
        self.get_zone_temperature_raw_timeout(zone_id, None).await
    }

    /// Queries the zone temperature directly over the network with an optional custom timeout.
    pub async fn get_zone_temperature_raw_timeout(
        &self,
        zone_id: u16,
        timeout: Option<Duration>,
    ) -> Result<ZoneTemperature, SatelError> {
        tracing::info!("Querying zone #{} temperature (0x7D)", zone_id);

        let cmd = vec![
            SatelCommand::ReadZoneTemperature.to_byte(),
            if zone_id == 256 { 0 } else { zone_id as u8 },
        ];

        let temp_read_timeout = timeout.unwrap_or_else(|| {
            Duration::from_millis(self.config.read().unwrap().temp_read_timeout_ms)
        });

        let response_result = self
            .exchange(
                cmd,
                None,
                Some(temp_read_timeout),
            )
            .await;

        match response_result {
            Ok(response) => match process_zone_temperature(&response) {
                Ok((id, temp)) => {
                    let old_temp = self.update_temperature_success_internal(id, temp)?;

                    if self.config.read().unwrap().emit_unchanged_temperatures || (old_temp - temp).abs() > 0.01 {
                        let _ = self.event_tx.send(SatelEvent::ZoneTemperatureChanged {
                            id,
                            temperature: temp,
                        });
                    }

                    tracing::info!("Retrieved zone #{} temperature: {}°C", id, temp);
                    self.get_cached_zone_temperature(id)?.ok_or(SatelError::InvalidFrame)
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

    /// Scans all configured zones (1..=io_count) to discover temperature probes.
    /// Emits `SatelEvent::SyncStarted`, `SatelEvent::SyncProgress` per zone, and `SatelEvent::SyncFinished`.
    /// When a sensor responds, `SatelEvent::ZoneTemperatureChanged` is emitted automatically.
    /// Returns `Ok(true)` on successful completion.
    pub async fn get_all_zone_temperatures(
        &self,
        probe_timeout: Option<Duration>,
    ) -> Result<bool, SatelError> {
        let version = self.get_cached_version()?.ok_or(SatelError::PanelVersionUnknown)?;
        if version.io_count == 0 {
            return Err(SatelError::PanelVersionUnknown);
        }

        let total = version.io_count;
        let _ = self.event_tx.send(SatelEvent::SyncStarted {
            category: SyncCategory::Temperatures,
            total,
        });

        tracing::info!(
            "Discovering temperature sensors across all zones (1..={}) with timeout {:?}...",
            total,
            probe_timeout
        );

        let mut success_count = 0;
        let mut last_error: Option<String> = None;

        for zone_id in 1..=total {
            let cached_name = self
                .get_cached_zone_name(zone_id)
                .ok()
                .flatten()
                .map(|z| z.name.trim().to_string())
                .filter(|s| !s.is_empty());

            match self.get_zone_temperature_raw_timeout(zone_id, probe_timeout).await {
                Ok(res) => {
                    success_count += 1;
                    let _ = self.event_tx.send(SatelEvent::SyncProgress {
                        category: SyncCategory::Temperatures,
                        current: zone_id,
                        total,
                        name: format!("{} - {:.1} °C", zone_id, res.temperature),
                    });
                }
                Err(SatelError::TemperatureSensorError) => {
                    // Sensor is physically present in the panel but reporting 0xFFFF (disconnected/damaged probe)
                    tracing::warn!("Zone #{} has a temperature sensor with error (0xFFFF)", zone_id);
                    let _ = self.event_tx.send(SatelEvent::SyncProgress {
                        category: SyncCategory::Temperatures,
                        current: zone_id,
                        total,
                        name: format!("{} - Błąd", zone_id),
                    });
                }
                Err(SatelError::TemperatureNotSupportedOrTimeOut) | Err(SatelError::Timeout) => {
                    // No sensor configured on this zone (normal timeout)
                    let no_temp_str = match &cached_name {
                        Some(name) => format!("{} - {}", zone_id, name),
                        None => format!("{}", zone_id),
                    };
                    let _ = self.event_tx.send(SatelEvent::SyncProgress {
                        category: SyncCategory::Temperatures,
                        current: zone_id,
                        total,
                        name: no_temp_str,
                    });
                }
                Err(e) => {
                    tracing::warn!("Failed to query zone #{} temperature: {:?}", zone_id, e);
                    let err_str = e.to_string();
                    last_error = Some(err_str.clone());
                    let _ = self.event_tx.send(SatelEvent::SyncProgress {
                        category: SyncCategory::Temperatures,
                        current: zone_id,
                        total,
                        name: format!("{} - Błąd ({})", zone_id, err_str),
                    });
                }
            }
        }

        let _ = self.event_tx.send(SatelEvent::SyncFinished {
            category: SyncCategory::Temperatures,
            total,
            success_count,
            error: last_error,
        });

        Ok(true)
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

    /// Resets the temperature sensor error counters and status.
    pub fn reset_temperature_sensor(&self, zone_id: u16) -> Result<(), SatelError> {
        if zone_id == 0 || zone_id > 256 {
            return Err(SatelError::InvalidConfig(format!("Invalid zone id: {}", zone_id)));
        }
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        let zone = state
            .zones
            .get_mut((zone_id.wrapping_sub(1) % 256) as usize)
            .ok_or(SatelError::InvalidFrame)?;
        
        zone.temperature_status = TemperatureSensorStatus::NoRead;
        zone.temperature_timeout_errors_current = 0;
        zone.temperature_sensor_errors_current = 0;
        zone.temperature_blocked_cycles = 0;
        
        Ok(())
    }

    /// Internal method to apply state updates after successful temperature read without network
    pub(crate) fn update_temperature_success_internal(&self, id: u16, temp: f32) -> Result<f32, SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        let zone = state
            .zones
            .get_mut((id.wrapping_sub(1) % 256) as usize)
            .ok_or(SatelError::InvalidFrame)?;

        let old_temp = zone.temperature_value;
        zone.temperature_value = temp;
        zone.temperature_read_at = Local::now();

        zone.temperature_status = TemperatureSensorStatus::Ok;
        zone.temperature_blocked_cycles = 0;

        if zone.temperature_timeout_errors_current > 0 {
            zone.temperature_timeout_errors_current -= 1;
        }
        if zone.temperature_sensor_errors_current > 0 {
            zone.temperature_sensor_errors_current -= 1;
        }

        Ok(old_temp)
    }

    /// Queries the zone temperature with automatic error blocking for faulty probes.
    pub async fn get_zone_temperature_with_blocking(
        &self,
        zone_id: u16,
    ) -> Result<ZoneTemperature, SatelError> {
        let limits = self.temp_limits(zone_id);

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
                || info.temperature_timeout_errors_current >= limits.max_timeout_errors
                || info.temperature_sensor_errors_current >= limits.max_sensor_errors;

            if is_blocked {
                let new_cycles = info.temperature_blocked_cycles + 1;
                
                if limits.unblock_enabled && new_cycles >= limits.unblock_after_cycles {
                    let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
                    if let Some(zone) = state.zones.get_mut((zone_id.wrapping_sub(1) % 256) as usize) {
                        if zone.temperature_timeout_errors_current >= limits.max_timeout_errors {
                            zone.temperature_timeout_errors_current = zone.temperature_timeout_errors_current.saturating_sub(1);
                        }
                        if zone.temperature_sensor_errors_current >= limits.max_sensor_errors {
                            zone.temperature_sensor_errors_current = zone.temperature_sensor_errors_current.saturating_sub(1);
                        }
                        zone.temperature_status = TemperatureSensorStatus::RetryRead;
                        zone.temperature_blocked_cycles = 0;
                    }
                    return Err(SatelError::TempTooManyErrors);
                }

                let status = if info.temperature_status == TemperatureSensorStatus::BlockSensorMissing
                    || info.temperature_timeout_errors_current >= limits.max_timeout_errors
                {
                    TemperatureSensorStatus::BlockSensorMissing
                } else {
                    TemperatureSensorStatus::BlockCommunicationError
                };

                {
                    let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
                    if let Some(zone) = state.zones.get_mut((zone_id.wrapping_sub(1) % 256) as usize) {
                        zone.temperature_status = status;
                        zone.temperature_blocked_cycles = new_cycles;
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
        self.update_troubles_internal(cmd, &response[1..])?;
        Ok(parsed)
    }

    /// Queries Troubles Part 1 (0x1B: technical zones, panel & expander power, buses, ETHM).
    pub async fn get_troubles_part1(&self) -> Result<TroublesPart1Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesPart1.to_byte()], None, None).await?;
        let parsed = process_troubles_part1(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesPart1, &response[1..])?;
        Ok(parsed)
    }

    /// Queries Troubles Memory Part 1 (0x20).
    pub async fn get_troubles_memory_part1(&self) -> Result<TroublesPart1Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesMemoryPart1.to_byte()], None, None).await?;
        let parsed = process_troubles_part1(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesMemoryPart1, &response[1..])?;
        Ok(parsed)
    }

    /// Queries Troubles Part 2 (0x1C: card readers, ACU synchro, power supplies, KNX).
    pub async fn get_troubles_part2(&self) -> Result<TroublesPart2Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesPart2.to_byte()], None, None).await?;
        let parsed = process_troubles_part2(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesPart2, &response[1..])?;
        Ok(parsed)
    }

    /// Queries Troubles Memory Part 2 (0x21).
    pub async fn get_troubles_memory_part2(&self) -> Result<TroublesMemoryPart2Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesMemoryPart2.to_byte()], None, None).await?;
        let parsed = process_troubles_memory_part2(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesMemoryPart2, &response[1..])?;
        Ok(parsed)
    }

    /// Queries Troubles Part 3 (0x1D: ACU jamming, ABAX wireless sensor batteries & comm 1..120).
    pub async fn get_troubles_part3(&self) -> Result<TroublesPart3Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesPart3.to_byte()], None, None).await?;
        let parsed = process_troubles_part3(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesPart3, &response[1..])?;
        Ok(parsed)
    }

    /// Queries Troubles Memory Part 3 (0x22).
    pub async fn get_troubles_memory_part3(&self) -> Result<TroublesMemoryPart3Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesMemoryPart3.to_byte()], None, None).await?;
        let parsed = process_troubles_memory_part3(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesMemoryPart3, &response[1..])?;
        Ok(parsed)
    }

    /// Queries Troubles Part 4 (0x1E: communication & tamper of expanders 1..64 and keypads 1..8).
    pub async fn get_troubles_part4(&self) -> Result<TroublesPart4Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesPart4.to_byte()], None, None).await?;
        let parsed = process_troubles_part4(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesPart4, &response[1..])?;
        Ok(parsed)
    }

    /// Queries Troubles Memory Part 4 (0x23).
    pub async fn get_troubles_memory_part4(&self) -> Result<TroublesPart4Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesMemoryPart4.to_byte()], None, None).await?;
        let parsed = process_troubles_part4(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesMemoryPart4, &response[1..])?;
        Ok(parsed)
    }

    /// Queries Troubles Part 5 (0x1F: user 1..240 and master key fobs low battery).
    pub async fn get_troubles_part5(&self) -> Result<TroublesPart5Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesPart5.to_byte()], None, None).await?;
        let parsed = process_troubles_part5(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesPart5, &response[1..])?;
        Ok(parsed)
    }

    /// Queries Troubles Memory Part 5 (0x24).
    pub async fn get_troubles_memory_part5(&self) -> Result<TroublesMemoryPart5Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesMemoryPart5.to_byte()], None, None).await?;
        let parsed = process_troubles_memory_part5(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesMemoryPart5, &response[1..])?;
        Ok(parsed)
    }

    /// Queries Troubles Part 6 (0x2C: wireless devices 121..240 - Integra 256).
    pub async fn get_troubles_part6(&self) -> Result<TroublesPart6Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesPart6.to_byte()], None, None).await?;
        let parsed = process_troubles_part6(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesPart6, &response[1..])?;
        Ok(parsed)
    }

    /// Queries Troubles Memory Part 6 (0x2E).
    pub async fn get_troubles_memory_part6(&self) -> Result<TroublesPart6Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesMemoryPart6.to_byte()], None, None).await?;
        let parsed = process_troubles_part6(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesMemoryPart6, &response[1..])?;
        Ok(parsed)
    }

    /// Queries Troubles Part 7 (0x2D: technical zones 129..256 - Integra 256).
    pub async fn get_troubles_part7(&self) -> Result<TroublesPart7Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesPart7.to_byte()], None, None).await?;
        let parsed = process_troubles_part7(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesPart7, &response[1..])?;
        Ok(parsed)
    }

    /// Queries Troubles Memory Part 7 (0x2F).
    pub async fn get_troubles_memory_part7(&self) -> Result<TroublesMemoryPart7Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesMemoryPart7.to_byte()], None, None).await?;
        let parsed = process_troubles_memory_part7(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesMemoryPart7, &response[1..])?;
        Ok(parsed)
    }

    /// Queries Troubles Part 8 (0x30: INT-GSM modules addresses 0..7).
    pub async fn get_troubles_part8(&self) -> Result<TroublesPart8Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesPart8.to_byte()], None, None).await?;
        let parsed = process_troubles_part8(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesPart8, &response[1..])?;
        Ok(parsed)
    }

    /// Queries Troubles Memory Part 8 (0x31).
    pub async fn get_troubles_memory_part8(&self) -> Result<TroublesPart8Data, SatelError> {
        let response = self.exchange(vec![SatelCommand::TroublesMemoryPart8.to_byte()], None, None).await?;
        let parsed = process_troubles_part8(&response)?;
        self.update_troubles_internal(SatelCommand::TroublesMemoryPart8, &response[1..])?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, TemperatureProbe};
    use crate::state::TemperatureSensorStatus;

    #[tokio::test]
    async fn test_temp_limits() {
        let mut config = Config::default();
        config.temp_max_timeout_errors = 5;
        config.temp_max_sensor_errors = 3;
        config.temperature_probes = vec![TemperatureProbe {
            zone_id: 5,
            max_timeout_errors: 10,
            max_sensor_errors: 8,
            interval_minutes: 1,
            unblock_enabled: true,
            unblock_after_cycles: 15,
        }];
        let client = SatelIntegra::new(config);
        
        // zone with custom limit
        let limits5 = client.temp_limits(5);
        assert_eq!(limits5.max_timeout_errors, 10);
        assert_eq!(limits5.max_sensor_errors, 8);
        assert_eq!(limits5.unblock_enabled, true);
        assert_eq!(limits5.unblock_after_cycles, 15);

        // zone without custom limit
        let limits2 = client.temp_limits(2);
        assert_eq!(limits2.max_timeout_errors, 5);
        assert_eq!(limits2.max_sensor_errors, 3);
        assert_eq!(limits2.unblock_enabled, false);
    }

    #[tokio::test]
    async fn test_unblocking_logic() {
        let mut config = Config::default();
        config.temp_blocking_enabled = true;
        config.temperature_probes = vec![TemperatureProbe {
            zone_id: 5,
            max_timeout_errors: 3,
            max_sensor_errors: 10,
            interval_minutes: 1,
            unblock_enabled: true,
            unblock_after_cycles: 10,
        }];
        let client = SatelIntegra::new(config);

        // set state directly
        {
            let handle = client.state_handle();
            let mut state = handle.write().unwrap();
            let zone = state.zones.get_mut(4).unwrap();
            zone.temperature_status = TemperatureSensorStatus::BlockSensorMissing;
            zone.temperature_timeout_errors_current = 3;
            zone.temperature_sensor_errors_current = 0;
        }

        // 9 calls
        for i in 1..=9 {
            let res = client.get_zone_temperature_with_blocking(5).await;
            assert!(matches!(res, Err(SatelError::TempTooManyErrors)));
            let handle = client.state_handle();
            let state = handle.read().unwrap();
            let zone = state.zones.get(4).unwrap();
            assert_eq!(zone.temperature_status, TemperatureSensorStatus::BlockSensorMissing);
            assert_eq!(zone.temperature_blocked_cycles, i);
        }

        // 10th call (unblock)
        let res = client.get_zone_temperature_with_blocking(5).await;
        assert!(matches!(res, Err(SatelError::TempTooManyErrors)));
        {
            let handle = client.state_handle();
            let state = handle.read().unwrap();
            let zone = state.zones.get(4).unwrap();
            assert_eq!(zone.temperature_status, TemperatureSensorStatus::RetryRead);
            assert_eq!(zone.temperature_blocked_cycles, 0);
            assert_eq!(zone.temperature_timeout_errors_current, 2);
            assert_eq!(zone.temperature_sensor_errors_current, 0);
        }

        // test with both counters at threshold
        {
            let handle = client.state_handle();
            let mut state = handle.write().unwrap();
            let zone = state.zones.get_mut(4).unwrap();
            zone.temperature_status = TemperatureSensorStatus::BlockSensorMissing;
            zone.temperature_timeout_errors_current = 3;
            zone.temperature_sensor_errors_current = 10;
            zone.temperature_blocked_cycles = 9;
        }

        let res = client.get_zone_temperature_with_blocking(5).await;
        assert!(matches!(res, Err(SatelError::TempTooManyErrors)));
        {
            let handle = client.state_handle();
            let state = handle.read().unwrap();
            let zone = state.zones.get(4).unwrap();
            assert_eq!(zone.temperature_status, TemperatureSensorStatus::RetryRead);
            assert_eq!(zone.temperature_blocked_cycles, 0);
            assert_eq!(zone.temperature_timeout_errors_current, 2);
            assert_eq!(zone.temperature_sensor_errors_current, 9);
        }

        // test unblock_enabled = false
        {
            let mut config2 = Config::default();
            config2.temp_blocking_enabled = true;
            config2.temperature_probes = vec![TemperatureProbe {
                zone_id: 6,
                max_timeout_errors: 3,
                max_sensor_errors: 10,
                interval_minutes: 1,
                unblock_enabled: false,
                unblock_after_cycles: 10,
            }];
            let client2 = SatelIntegra::new(config2);
            {
                let handle = client2.state_handle();
                let mut state = handle.write().unwrap();
                let zone = state.zones.get_mut(5).unwrap();
                zone.temperature_status = TemperatureSensorStatus::BlockSensorMissing;
                zone.temperature_timeout_errors_current = 3;
                zone.temperature_sensor_errors_current = 0;
            }

            for i in 1..=50 {
                let res = client2.get_zone_temperature_with_blocking(6).await;
                assert!(matches!(res, Err(SatelError::TempTooManyErrors)));
                let handle = client2.state_handle();
                let state = handle.read().unwrap();
                let zone = state.zones.get(5).unwrap();
                assert_eq!(zone.temperature_status, TemperatureSensorStatus::BlockSensorMissing);
                assert_eq!(zone.temperature_blocked_cycles, i);
                assert_eq!(zone.temperature_timeout_errors_current, 3);
            }
        }
    }

    #[tokio::test]
    async fn test_reset_temperature_sensor() {
        let client = SatelIntegra::new(Config::default());
        {
            let handle = client.state_handle();
            let mut state = handle.write().unwrap();
            let zone = state.zones.get_mut(4).unwrap();
            zone.temperature_status = TemperatureSensorStatus::BlockSensorMissing;
            zone.temperature_timeout_errors_current = 5;
            zone.temperature_timeout_errors_total = 10;
            zone.temperature_sensor_errors_current = 2;
            zone.temperature_sensor_errors_total = 2;
            zone.temperature_blocked_cycles = 15;
        }

        client.reset_temperature_sensor(5).unwrap();

        {
            let handle = client.state_handle();
            let state = handle.read().unwrap();
            let zone = state.zones.get(4).unwrap();
            assert_eq!(zone.temperature_status, TemperatureSensorStatus::NoRead);
            assert_eq!(zone.temperature_timeout_errors_current, 0);
            assert_eq!(zone.temperature_timeout_errors_total, 10);
            assert_eq!(zone.temperature_sensor_errors_current, 0);
            assert_eq!(zone.temperature_sensor_errors_total, 2);
            assert_eq!(zone.temperature_blocked_cycles, 0);
        }

        let err = client.reset_temperature_sensor(0);
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_update_temperature_success_internal() {
        let client = SatelIntegra::new(Config::default());
        {
            let handle = client.state_handle();
            let mut state = handle.write().unwrap();
            let zone = state.zones.get_mut(4).unwrap();
            zone.temperature_status = TemperatureSensorStatus::RetryRead;
            zone.temperature_timeout_errors_current = 2;
            zone.temperature_sensor_errors_current = 1;
            zone.temperature_blocked_cycles = 0;
        }

        let _ = client.update_temperature_success_internal(5, 22.5).unwrap();

        {
            let handle = client.state_handle();
            let state = handle.read().unwrap();
            let zone = state.zones.get(4).unwrap();
            assert_eq!(zone.temperature_status, TemperatureSensorStatus::Ok);
            assert_eq!(zone.temperature_timeout_errors_current, 1);
            assert_eq!(zone.temperature_sensor_errors_current, 0);
            assert_eq!(zone.temperature_blocked_cycles, 0);
            assert_eq!(zone.temperature_value, 22.5);
        }
    }

    #[tokio::test]
    async fn test_reset_statistics() {
        let client = SatelIntegra::new(Config::default());
        {
            let handle = client.state_handle();
            let mut s = handle.write().unwrap();
            s.telemetry.bytes_sent.store(100, std::sync::atomic::Ordering::Relaxed);
            s.telemetry.bytes_received.store(200, std::sync::atomic::Ordering::Relaxed);
            s.telemetry.connections_established.store(2, std::sync::atomic::Ordering::Relaxed);
            s.telemetry.reconnect_attempts.store(3, std::sync::atomic::Ordering::Relaxed);
            s.telemetry.connections_lost.store(1, std::sync::atomic::Ordering::Relaxed);
            s.telemetry.timeouts.store(4, std::sync::atomic::Ordering::Relaxed);
            s.telemetry.crc_errors.store(5, std::sync::atomic::Ordering::Relaxed);
            s.telemetry.rejected_by_panel.store(6, std::sync::atomic::Ordering::Relaxed);
            s.telemetry.io_errors.store(7, std::sync::atomic::Ordering::Relaxed);
            s.telemetry.total_connected_before = std::time::Duration::from_secs(300);
        }

        client.reset_statistics();

        let stats = client.statistics();
        assert_eq!(stats.bytes_sent, 0);
        assert_eq!(stats.bytes_received, 0);
        assert_eq!(stats.connections_established, 0);
        assert_eq!(stats.reconnect_attempts, 0);
        assert_eq!(stats.connections_lost, 0);
        assert_eq!(stats.timeouts, 0);
        assert_eq!(stats.crc_errors, 0);
        assert_eq!(stats.rejected_by_panel, 0);
        assert_eq!(stats.io_errors, 0);
        assert_eq!(stats.total_connected, std::time::Duration::ZERO);
        assert_eq!(stats.connected_since, None);
    }

    #[tokio::test]
    async fn test_total_connected_sums_closed_and_current_session() {
        let client = SatelIntegra::new(Config::default());
        {
            let handle = client.state_handle();
            let mut s = handle.write().unwrap();
            s.telemetry.status.state = crate::state::ConnectionState::Connected;
            s.telemetry.total_connected_before = std::time::Duration::from_secs(60);
            s.telemetry.connected_since = Some(chrono::Local::now() - chrono::Duration::seconds(10));
        }

        let stats = client.statistics();
        assert_eq!(stats.state, crate::state::ConnectionState::Connected);
        assert!(stats.connected_since.is_some());
        assert!(
            stats.total_connected >= std::time::Duration::from_secs(69)
                && stats.total_connected <= std::time::Duration::from_secs(75),
            "Expected total_connected around 70s, got {:?}",
            stats.total_connected
        );
    }

    #[test]
    fn test_hot_reload_config_identical_auto_read_flag_unchanged() {
        let client = SatelIntegra::new(Config::default());
        assert_eq!(client.auto_read_dirty.load(Ordering::SeqCst), false);
        client.hot_reload_config(Config::default()).unwrap();
        assert_eq!(client.auto_read_dirty.load(Ordering::SeqCst), false);
    }

    #[test]
    fn test_hot_reload_config_auto_read_zones_violation_flag_set() {
        let client = SatelIntegra::new(Config::default());
        assert_eq!(client.auto_read_dirty.load(Ordering::SeqCst), false);
        let mut new_config = Config::default();
        new_config.auto_read_zones_violation = true;
        client.hot_reload_config(new_config).unwrap();
        assert_eq!(client.auto_read_dirty.load(Ordering::SeqCst), true);
    }

    #[test]
    fn test_hot_reload_config_unrelated_param_change_flag_unchanged() {
        let client = SatelIntegra::new(Config::default());
        assert_eq!(client.auto_read_dirty.load(Ordering::SeqCst), false);
        let mut new_config = client.get_config();
        new_config.temperature_probes = vec![TemperatureProbe {
            zone_id: 1,
            max_timeout_errors: 5,
            max_sensor_errors: 3,
            interval_minutes: 2,
            unblock_enabled: true,
            unblock_after_cycles: 10,
        }];
        client.hot_reload_config(new_config).unwrap();
        assert_eq!(client.auto_read_dirty.load(Ordering::SeqCst), false);
    }

    #[test]
    fn test_build_push_mask_empty_config_is_all_zeros() {
        let config = Config::default();
        let mask12 = SatelCommunicationWorker::satel_connection_worker_connect_build_push_mask(&config, false);
        let mask14 = SatelCommunicationWorker::satel_connection_worker_connect_build_push_mask(&config, true);
        assert_eq!(mask12.len(), 12);
        assert_eq!(mask12, vec![0u8; 12]);
        assert_eq!(mask14.len(), 14);
        assert_eq!(mask14, vec![0u8; 14]);
    }

    #[test]
    fn test_hot_reload_extended_name_read_resets_session_types() {
        let client = SatelIntegra::new(Config::default());
        assert_eq!(client.session_zone_type.load(Ordering::SeqCst), 5);
        assert_eq!(client.session_output_type.load(Ordering::SeqCst), 17);
        assert_eq!(client.session_partition_type.load(Ordering::SeqCst), 19);

        // Simulate downgrade in session
        client.session_zone_type.store(1, Ordering::SeqCst);
        client.session_output_type.store(4, Ordering::SeqCst);
        client.session_partition_type.store(0, Ordering::SeqCst);

        // Hot reload disabling extended_name_read
        let mut cfg = client.get_config();
        cfg.extended_name_read = false;
        client.hot_reload_config(cfg).unwrap();

        assert_eq!(client.session_zone_type.load(Ordering::SeqCst), 1);
        assert_eq!(client.session_output_type.load(Ordering::SeqCst), 4);
        assert_eq!(client.session_partition_type.load(Ordering::SeqCst), 0);

        // Hot reload re-enabling extended_name_read resets session types to highest
        let mut cfg2 = client.get_config();
        cfg2.extended_name_read = true;
        client.hot_reload_config(cfg2).unwrap();

        assert_eq!(client.session_zone_type.load(Ordering::SeqCst), 5);
        assert_eq!(client.session_output_type.load(Ordering::SeqCst), 17);
        assert_eq!(client.session_partition_type.load(Ordering::SeqCst), 19);
    }

    fn create_mock_client(config: Config) -> (SatelIntegra, mpsc::Receiver<InternalMessage>) {
        let state = Arc::new(RwLock::new(SatelState::new()));
        let (tx, rx) = mpsc::channel(100);
        let (event_tx, _) = broadcast::channel(1024);
        let extended = config.extended_name_read;
        let config_arc = Arc::new(RwLock::new(config));
        let auto_read_dirty = Arc::new(AtomicBool::new(false));
        let session_zone_type = Arc::new(AtomicU8::new(if extended { 5 } else { 1 }));
        let session_output_type = Arc::new(AtomicU8::new(if extended { 17 } else { 4 }));
        let session_partition_type = Arc::new(AtomicU8::new(if extended { 19 } else { 0 }));

        let client = SatelIntegra {
            tx,
            state,
            config: config_arc,
            worker: Arc::new(Mutex::new(None)),
            event_tx,
            auto_read_dirty,
            session_zone_type,
            session_output_type,
            session_partition_type,
        };
        (client, rx)
    }

    fn pad_test_name(s: &str) -> [u8; 16] {
        let (encoded, _, _) = encoding_rs::WINDOWS_1250.encode(s);
        let mut buf = [b' '; 16];
        let len = encoded.len().min(16);
        buf[..len].copy_from_slice(&encoded[..len]);
        buf
    }

    #[tokio::test]
    async fn test_zone_fallback_5_to_1() {
        let (client, mut rx) = create_mock_client(Config::default());
        let mut events = client.subscribe();

        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if let InternalMessage::ExchangeStandard { data, response_tx, .. } = msg {
                    let dev_type = data[1];
                    let dev_id = data[2];
                    if dev_type == 5 {
                        // Centrala odrzuca typ 5
                        let _ = response_tx.send(Ok(vec![0xEF, 0xFF]));
                    } else if dev_type == 1 {
                        // Centrala akceptuje typ 1
                        let mut resp = vec![0xEE, 1, dev_id, 3]; // reaction 3: InteriorDelayed
                        resp.extend_from_slice(&pad_test_name("Kuchnia"));
                        let _ = response_tx.send(Ok(resp));
                    }
                }
            }
        });

        // 1. Pierwsze zapytanie — powinno spróbować 5, dostać 0xEF, zejść do 1 i zapamiętać 1
        let res = client.get_zone_name(1).await.unwrap();
        assert_eq!(res.name, "Kuchnia");
        assert_eq!(client.session_zone_type.load(Ordering::SeqCst), 1);

        // Zdarzenia: ZoneNameReceived oraz ZoneParamsReceived (bo extended_name_read = true)
        let ev1 = events.recv().await.unwrap();
        assert!(matches!(ev1, SatelEvent::ZoneNameReceived { id: 1, ref name } if name == "Kuchnia"));
        let ev2 = events.recv().await.unwrap();
        assert!(matches!(ev2, SatelEvent::ZoneParamsReceived { id: 1, ref params } if params.reaction == ZoneReaction::InteriorDelayed && params.partition == None));

        // 2. Kolejne zapytanie (np. zone 2) powinno od razu użyć typu 1 bez próbowania 5
        let res2 = client.get_zone_name(2).await.unwrap();
        assert_eq!(res2.name, "Kuchnia");
        assert_eq!(client.session_zone_type.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_output_fallback_17_to_4() {
        let (client, mut rx) = create_mock_client(Config::default());

        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if let InternalMessage::ExchangeStandard { data, response_tx, .. } = msg {
                    let dev_type = data[1];
                    let dev_id = data[2];
                    if dev_type == 17 {
                        let _ = response_tx.send(Ok(vec![0xEF, 0xFF]));
                    } else if dev_type == 4 {
                        let mut resp = vec![0xEE, 4, dev_id, 24]; // MonoSwitch
                        resp.extend_from_slice(&pad_test_name("Syrena"));
                        let _ = response_tx.send(Ok(resp));
                    }
                }
            }
        });

        let out = client.get_output_name(1).await.unwrap();
        assert_eq!(out.name, "Syrena");
        assert_eq!(client.session_output_type.load(Ordering::SeqCst), 4);

        let cached_params = client.get_cached_output_params(1).unwrap().unwrap();
        assert_eq!(cached_params.function, OutputFunction::MonoSwitch);
        assert_eq!(cached_params.duration, None);
        assert_eq!(cached_params.control, OutputControl::Timed { duration: None });
    }

    #[tokio::test]
    async fn test_partition_fallback_19_to_16() {
        let (client, mut rx) = create_mock_client(Config::default());

        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if let InternalMessage::ExchangeStandard { data, response_tx, .. } = msg {
                    let dev_type = data[1];
                    let dev_id = data[2];
                    if dev_type == 19 || dev_type == 18 {
                        let _ = response_tx.send(Ok(vec![0xEF, 0xFF]));
                    } else if dev_type == 16 {
                        let mut resp = vec![0xEE, 16, dev_id, 1]; // TimedBlocking
                        resp.extend_from_slice(&pad_test_name("Strefa 1"));
                        resp.push(3); // object 3
                        let _ = response_tx.send(Ok(resp));
                    }
                }
            }
        });

        let part = client.get_partition_name(1).await.unwrap();
        assert_eq!(part.name, "Strefa 1");
        assert_eq!(client.session_partition_type.load(Ordering::SeqCst), 16);

        let params = client.get_cached_partition_params(1).unwrap().unwrap();
        assert_eq!(params.partition_type, PartitionType::TimedBlocking);
        assert_eq!(params.object_number, Some(3));
        assert_eq!(params.options, None);
    }

    #[tokio::test]
    async fn test_unconfigured_item_does_not_downgrade_session_type() {
        let (client, mut rx) = create_mock_client(Config::default());

        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if let InternalMessage::ExchangeStandard { response_tx, .. } = msg {
                    // Wszystkie typy zwracają 0xEF
                    let _ = response_tx.send(Ok(vec![0xEF, 0xFF]));
                }
            }
        });

        assert_eq!(client.session_zone_type.load(Ordering::SeqCst), 5);
        let res = client.get_zone_name(100).await.unwrap();
        assert_eq!(res.name, "");
        // Typ sesji nie powinien zostać obniżony, bo odmowa dotyczyła nieistniejącej strefy
        assert_eq!(client.session_zone_type.load(Ordering::SeqCst), 5);
        assert_eq!(client.get_cached_zone_params(100).unwrap(), None);
    }

    #[tokio::test]
    async fn test_extended_name_read_false_behavior() {
        let mut cfg = Config::default();
        cfg.extended_name_read = false;
        let (client, mut rx) = create_mock_client(cfg);
        let mut events = client.subscribe();

        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if let InternalMessage::ExchangeStandard { data, response_tx, .. } = msg {
                    let dev_type = data[1];
                    let dev_id = data[2];
                    assert_eq!(dev_type, 1, "Should only query type 1 when extended_name_read is false");
                    let mut resp = vec![0xEE, 1, dev_id, 0];
                    resp.extend_from_slice(&pad_test_name("Wejscie 1"));
                    let _ = response_tx.send(Ok(resp));
                }
            }
        });

        let res = client.get_zone_name(1).await.unwrap();
        assert_eq!(res.name, "Wejscie 1");

        // Powinno nadejść tylko ZoneNameReceived, BEZ ZoneParamsReceived
        let ev = events.recv().await.unwrap();
        assert!(matches!(ev, SatelEvent::ZoneNameReceived { id: 1, .. }));
        assert!(events.try_recv().is_err());
    }

    #[test]
    fn test_cached_output_control_helpers() {
        let (client, _) = create_mock_client(Config::default());

        // 1. Przed odczytem parametrów: None
        assert_eq!(client.output_control(1), None);
        assert_eq!(client.is_output_controllable(1), None);

        // 2. Wyjście monostabilne (24, czasowe)
        {
            let mut state = client.state.write().unwrap();
            let out = &mut state.outputs[0];
            out.function = Some(OutputFunction::MonoSwitch);
            out.duration = Some(Duration::from_millis(3000));
            out.control = Some(OutputControl::Timed { duration: Some(Duration::from_millis(3000)) });
        }
        assert_eq!(
            client.output_control(1),
            Some(OutputControl::Timed { duration: Some(Duration::from_millis(3000)) })
        );
        assert_eq!(client.is_output_controllable(1), Some(true));

        // 3. Wyjście bistabilne (25)
        {
            let mut state = client.state.write().unwrap();
            let out = &mut state.outputs[1];
            out.function = Some(OutputFunction::BiSwitch);
            out.duration = None;
            out.control = Some(OutputControl::Bistable);
        }
        assert_eq!(client.output_control(2), Some(OutputControl::Bistable));
        assert_eq!(client.is_output_controllable(2), Some(true));

        // 4. Wyjście niesterowalne (alarm włamania 1)
        {
            let mut state = client.state.write().unwrap();
            let out = &mut state.outputs[2];
            out.function = Some(OutputFunction::BurglaryAlarm);
            out.duration = None;
            out.control = Some(OutputControl::None);
        }
        assert_eq!(client.output_control(3), Some(OutputControl::None));
        assert_eq!(client.is_output_controllable(3), Some(false));
    }
}
