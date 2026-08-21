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
    process_rtc_and_status, process_troubles, process_zone_name, process_zone_temperature,
    process_zones_alarm, process_zones_alarm_memory, process_zones_bypass,
    process_zones_long_violation_trouble, process_zones_no_violation_trouble, process_zones_tamper,
    process_zones_tamper_alarm, process_zones_tamper_alarm_memory, process_zones_violation,
};
use crate::state::{
    EthmVersion, IntegraVersion, OutputName, PartitionName, SatelState, SatelStateHandle,
    SystemStatus, TemperatureSensorStatus, ZoneName, ZoneStatus, ZoneTemperature,
};
use crate::worker::{InternalMessage, SatelCommunicationWorker};
use chrono::Local;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, oneshot};

/// Główny uchwyt (klient) do komunikacji z centralą Satel Integra.
/// Można go dowolnie klonować — każda kopia współdzieli to samo połączenie.
#[derive(Clone)]
pub struct SatelIntegra {
    pub(crate) tx: mpsc::Sender<InternalMessage>,
    pub(crate) state: SatelStateHandle,
    pub(crate) config: Config,
    pub(crate) worker: Arc<Mutex<Option<SatelCommunicationWorker>>>,
    pub(crate) event_tx: broadcast::Sender<SatelEvent>,
}

impl SatelIntegra {
    /// Tworzy nową instancję `SatelIntegra`.
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

    /// Uruchamia połączenie i workera w tle.
    /// Jeśli worker już działa, próbuje wymusić ponowne połączenie.
    pub async fn connect(&self) -> Result<(), SatelError> {
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

            tokio::spawn(async move {
                worker.run_with_initial_connect(connect_tx).await;
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

    /// Zamyka połączenie i zatrzymuje automatyczne próby łączenia.
    pub async fn disconnect(&self) -> Result<(), SatelError> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send(InternalMessage::Disconnect { response_tx: tx })
            .await
            .map_err(|_| SatelError::WorkerDropped)?;

        rx.await.map_err(|_| SatelError::WorkerDropped)?
    }

    /// Zwraca uchwyt do współdzielonego stanu.
    pub fn state_handle(&self) -> SatelStateHandle {
        self.state.clone()
    }

    /// Subskrybuje zdarzenia systemowe.
    pub fn subscribe(&self) -> broadcast::Receiver<SatelEvent> {
        self.event_tx.subscribe()
    }

    /// Wykonuje operację wymiany danych (wyślij i odbierz).
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

    /// Wykonuje operację wymiany danych z priorytetem (np. podczas handshake).
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

    /// Pobiera informacje o wersji centrali.
    pub async fn get_integra_version(&self) -> Result<IntegraVersion, SatelError> {
        tracing::info!("Pobieranie wersji centrali...");

        let cmd = vec![SatelCommand::IntegraVersion.to_byte()];
        let response = self.exchange(cmd, None, None).await?;

        let version = process_integra_version(&response)?;
        self.update_integra_version_internal(version.clone())?;

        Ok(version)
    }

    /// Pobiera informacje o wersji modułu ETHM/INT-RS.
    pub async fn get_ethm_version(&self) -> Result<EthmVersion, SatelError> {
        tracing::info!("Pobieranie wersji modułu ETHM/INT-RS...");

        let cmd = vec![SatelCommand::ModuleVersion.to_byte()];
        let response = self.exchange(cmd, None, None).await?;

        let version = process_ethm_version(&response)?;
        self.update_ethm_version_internal(version.clone())?;

        Ok(version)
    }

    /// Zwraca informacje o wersji centrali przechowywane w stanie.
    pub fn get_cached_version(&self) -> Result<Option<IntegraVersion>, SatelError> {
        let state = self.state.read().map_err(|_| SatelError::StatePoisoned)?;
        Ok(state.integra_version.clone())
    }

    /// Pobiera nazwę wejścia (zony) z centrali.
    pub async fn get_zone_name(&self, zone_id: u16) -> Result<ZoneName, SatelError> {
        tracing::info!("Pobieranie nazwy wejścia {}", zone_id);

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

        tracing::info!("Pobrano nazwę wejścia {}: {}", id, s_name.name);
        Ok(s_name)
    }

    /// Pobiera nazwę wejścia z cache.
    pub fn get_cached_zone_name(&self, zone_id: u16) -> Result<Option<ZoneName>, SatelError> {
        let state = self.state.read().map_err(|_| SatelError::StatePoisoned)?;
        Ok(state
            .zones
            .get((zone_id.wrapping_sub(1) % 256) as usize)
            .map(|z| z.to_zone_name()))
    }

    /// Pobiera nazwę wyjścia z centrali.
    pub async fn get_output_name(&self, output_id: u16) -> Result<OutputName, SatelError> {
        tracing::info!("Pobieranie nazwy wyjścia {}", output_id);

        let device_type: u8 = 4;
        let device_id: u8 = if output_id == 256 { 0 } else { output_id as u8 };

        let cmd = vec![
            SatelCommand::ReadDeviceName.to_byte(),
            device_type,
            device_id,
        ];
        let response = self.exchange(cmd, None, None).await?;

        if !response.is_empty() && response[0] == SatelCommand::ResultCode.to_byte() {
            // Panel returned 0xEF (ResultCode) indicating the requested output is not configured or unassigned
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

        tracing::info!("Pobrano nazwę wyjścia {}: {}", id, s_name.name);
        Ok(s_name)
    }

    /// Pobiera nazwę wyjścia z cache.
    pub fn get_cached_output_name(&self, output_id: u16) -> Result<Option<OutputName>, SatelError> {
        let state = self.state.read().map_err(|_| SatelError::StatePoisoned)?;
        Ok(state
            .outputs
            .get((output_id.wrapping_sub(1) % 256) as usize)
            .map(|o| o.to_output_name()))
    }

    /// Pobiera nazwę strefy (partycji) z centrali.
    pub async fn get_partition_name(&self, partition_id: u16) -> Result<PartitionName, SatelError> {
        tracing::info!("Pobieranie nazwy strefy {}", partition_id);

        let device_type: u8 = 0;
        let device_id: u8 = partition_id as u8;

        let cmd = vec![
            SatelCommand::ReadDeviceName.to_byte(),
            device_type,
            device_id,
        ];
        let response = self.exchange(cmd, None, None).await?;

        if !response.is_empty() && response[0] == SatelCommand::ResultCode.to_byte() {
            // Panel returned 0xEF (ResultCode) indicating the requested partition is not configured or unassigned
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

        tracing::info!("Pobrano nazwę strefy {}: {}", id, partition_name.name);
        Ok(partition_name)
    }

    /// Zwraca nazwę strefy z cache.
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

    /// Pobiera temperaturę wejścia (zony) z centrali.
    pub async fn get_zone_temperature(&self, zone_id: u16) -> Result<ZoneTemperature, SatelError> {
        tracing::info!("Pobieranie temperatury wejścia {}", zone_id);

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

                    if (old_temp - temp).abs() > 0.01 {
                        let _ = self.event_tx.send(SatelEvent::ZoneTemperatureChanged {
                            id: zone.id,
                            temperature: temp,
                        });
                    }

                    tracing::info!("Pobrano temperaturę wejścia {}: {}°C", id, temp);
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

    /// Pobiera temperaturę wejścia z cache.
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

    /// Pobiera temperaturę wejścia z mechanizmem blokowania wadliwych czujników.
    pub async fn get_zone_temperature_with_blocking(
        &self,
        zone_id: u16,
    ) -> Result<ZoneTemperature, SatelError> {
        if !self.config.temp_blocking_enabled {
            return self.get_zone_temperature(zone_id).await;
        }

        let zone_info = {
            let state = self.state.read().map_err(|_| SatelError::StatePoisoned)?;
            state
                .zones
                .get((zone_id.wrapping_sub(1) % 256) as usize)
                .cloned()
        };

        if let Some(info) = zone_info {
            if info.temperature_status != TemperatureSensorStatus::Ok
                && info.temperature_status != TemperatureSensorStatus::NoRead
            {
                return Err(SatelError::TempTooManyErrors);
            }

            if info.temperature_timeout_errors_current >= self.config.temp_max_timeout_errors {
                {
                    let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
                    if let Some(zone) = state.zones.get_mut((zone_id.wrapping_sub(1) % 256) as usize) {
                        zone.temperature_status = TemperatureSensorStatus::SensorMissing;
                    }
                }
                return Err(SatelError::TempTooManyErrors);
            }

            if info.temperature_sensor_errors_current >= self.config.temp_max_sensor_errors {
                {
                    let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
                    if let Some(zone) = state.zones.get_mut((zone_id.wrapping_sub(1) % 256) as usize) {
                        zone.temperature_status = TemperatureSensorStatus::CommunicationError;
                    }
                }
                return Err(SatelError::TempTooManyErrors);
            }
        }

        self.get_zone_temperature(zone_id).await
    }

    /// Pobiera zagregowany status pojedynczego wejścia z cache.
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

    /// Pobiera stan sabotaży wszystkich wejść z centrali i aktualizuje cache.
    pub async fn get_zones_tamper(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanu sabotaży wszystkich wejść...");
        let cmd = vec![SatelCommand::ZonesTamper.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_tamper(&response, &self.config.io_tamper_invert)?;
        self.update_zones_tamper_internal(result)?;
        tracing::info!("Zaktualizowano stan sabotaży");
        Ok(())
    }

    /// Pobiera stan alarmów wszystkich wejść z centrali i aktualizuje cache.
    pub async fn get_zones_alarm(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanu alarmów wszystkich wejść...");
        let cmd = vec![SatelCommand::ZonesAlarm.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_alarm(&response, &self.config.io_alarm_invert)?;
        self.update_zones_alarm_internal(result)?;
        tracing::info!("Zaktualizowano stan alarmów");
        Ok(())
    }

    /// Pobiera stan naruszeń wszystkich wejść z centrali i aktualizuje cache.
    pub async fn get_zones_violation(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanu naruszeń wszystkich wejść...");
        let cmd = vec![SatelCommand::ZonesViolation.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_violation(&response, &self.config.io_violation_invert)?;
        self.update_zones_violation_internal(result)?;
        tracing::info!("Zaktualizowano stan naruszeń");
        Ok(())
    }

    /// Pobiera stan alarmów sabotażowych wszystkich wejść z centrali i aktualizuje cache.
    pub async fn get_zones_tamper_alarm(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanu alarmów sabotażowych wszystkich wejść...");
        let cmd = vec![SatelCommand::ZonesTamperAlarm.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_tamper_alarm(&response, &self.config.io_tamper_alarm_invert)?;
        self.update_zones_tamper_alarm_internal(result)?;
        tracing::info!("Zaktualizowano stan alarmów sabotażowych");
        Ok(())
    }

    /// Pobiera stan pamięci alarmów wszystkich wejść z centrali i aktualizuje cache.
    pub async fn get_zones_alarm_memory(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanu pamięci alarmów wszystkich wejść...");
        let cmd = vec![SatelCommand::ZonesAlarmMemory.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_alarm_memory(&response, &self.config.io_alarm_memory_invert)?;
        self.update_zones_alarm_memory_internal(result)?;
        tracing::info!("Zaktualizowano stan pamięci alarmów");
        Ok(())
    }

    /// Pobiera stan pamięci alarmów sabotażowych wszystkich wejść z centrali i aktualizuje cache.
    pub async fn get_zones_tamper_alarm_memory(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanu pamięci alarmów sabotażowych wszystkich wejść...");
        let cmd = vec![SatelCommand::ZonesTamperAlarmMemory.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_tamper_alarm_memory(&response, &self.config.io_tamper_alarm_memory_invert)?;
        self.update_zones_tamper_alarm_memory_internal(result)?;
        tracing::info!("Zaktualizowano stan pamięci alarmów sabotażowych");
        Ok(())
    }

    /// Pobiera stan blokad (bypass) wszystkich wejść z centrali i aktualizuje cache.
    pub async fn get_zones_bypass(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanu blokad (bypass) wszystkich wejść...");
        let cmd = vec![SatelCommand::ZonesBypass.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_bypass(&response, &self.config.io_bypass_invert)?;
        self.update_zones_bypass_internal(result)?;
        tracing::info!("Zaktualizowano stan blokad");
        Ok(())
    }

    /// Pobiera stan awarii "brak naruszenia" wszystkich wejść z centrali i aktualizuje cache.
    pub async fn get_zones_no_violation_trouble(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanu awarii 'brak naruszenia' wszystkich wejść...");
        let cmd = vec![SatelCommand::ZonesNoViolationTrouble.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_no_violation_trouble(&response, &self.config.io_no_violation_trouble_invert)?;
        self.update_zones_no_violation_trouble_internal(result)?;
        tracing::info!("Zaktualizowano stan awarii 'brak naruszenia'");
        Ok(())
    }

    /// Pobiera stan awarii "długie naruszenie" wszystkich wejść z centrali i aktualizuje cache.
    pub async fn get_zones_long_violation_trouble(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanu awarii 'długie naruszenie' wszystkich wejść...");
        let cmd = vec![SatelCommand::ZonesLongViolationTrouble.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_zones_long_violation_trouble(&response, &self.config.io_long_violation_trouble_invert)?;
        self.update_zones_long_violation_trouble_internal(result)?;
        tracing::info!("Zaktualizowano stan awarii 'długie naruszenie'");
        Ok(())
    }

    /// Pobiera stan uzbrojenia stref (suppressed) z centrali i aktualizuje cache.
    pub async fn get_partitions_armed_suppressed(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanu uzbrojenia stref (suppressed)...");
        let cmd = vec![SatelCommand::ArmedPartitionsSuppressed.to_byte()];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_partitions_armed_suppressed(&response)?;
        self.update_partitions_armed_internal(result)?;
        tracing::info!("Zaktualizowano stan uzbrojenia (suppressed)");
        Ok(())
    }

    /// Pobiera faktyczny stan uzbrojenia stref z centrali i aktualizuje cache.
    pub async fn get_partitions_armed_really(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie faktycznego stanu uzbrojenia stref...");
        let cmd = vec![SatelCommand::ArmedPartitionsReally.to_byte()];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_partitions_armed_really(&response)?;
        self.update_partitions_armed_really_internal(result)?;
        tracing::info!("Zaktualizowano faktyczny stan uzbrojenia");
        Ok(())
    }

    /// Pobiera stan alarmów stref z centrali i aktualizuje cache.
    pub async fn get_partitions_alarm(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanu alarmów stref...");
        let cmd = vec![SatelCommand::PartitionsAlarm.to_byte()];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_partitions_alarm(&response)?;
        self.update_partitions_alarm_internal(result)?;
        tracing::info!("Zaktualizowano stan alarmów stref");
        Ok(())
    }

    /// Pobiera stan pamięci alarmów stref z centrali i aktualizuje cache.
    pub async fn get_partitions_alarm_memory(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanu pamięci alarmów stref...");
        let cmd = vec![SatelCommand::PartitionsAlarmMemory.to_byte()];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_partitions_alarm_memory(&response)?;
        self.update_partitions_alarm_memory_internal(result)?;
        tracing::info!("Zaktualizowano stan pamięci alarmów stref");
        Ok(())
    }

    /// Pobiera stany liczników czasu na wejście/wyjście stref z centrali i aktualizuje cache.
    pub async fn get_partitions_times(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanów liczników czasu stref...");

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

        tracing::info!("Zaktualizowano stany liczników czasu stref");
        Ok(())
    }

    /// Pobiera stan wszystkich wyjść z centrali i aktualizuje cache.
    pub async fn get_outputs_state(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanu wszystkich wyjść...");
        let cmd = vec![SatelCommand::OutputsState.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_outputs_state(&response)?;
        self.update_outputs_state_internal(result)?;
        tracing::info!("Zaktualizowano stan wyjść");
        Ok(())
    }

    /// Pobiera ogólny status systemu i aktualizuje cache.
    pub async fn get_system_status(&self) -> Result<SystemStatus, SatelError> {
        tracing::info!("Pobieranie statusu systemu (RTC)...");
        let cmd = vec![SatelCommand::RtcAndBasicStatusBits.to_byte()];
        let response = self.exchange(cmd, None, None).await?;
        let status = process_rtc_and_status(&response)?;
        self.update_system_status_internal(status)?;
        tracing::info!("Zaktualizowano status systemu");
        Ok(status)
    }

    /// Pobiera aktualny czas z centrali.
    pub async fn get_satel_time(&self) -> Result<chrono::DateTime<chrono::Local>, SatelError> {
        let status = self.get_system_status().await?;
        Ok(status.rtc)
    }

    /// Pobiera konkretną część awarii i aktualizuje cache, emitując zdarzenia dla zmian.
    pub async fn get_system_troubles(&self, cmd: SatelCommand) -> Result<(), SatelError> {
        tracing::info!("Pobieranie awarii systemu: {:02X?}", cmd);
        let response = self.exchange(vec![cmd.to_byte()], None, None).await?;
        let states = process_troubles(&response)?;
        self.update_troubles_internal(cmd, states)?;
        Ok(())
    }

    // --- Sterowanie ---

    /// Uzbraja wybraną strefę w podanym trybie z opcją forsowania.
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
        tracing::info!("Uzbrajanie strefy {} w trybie {} (force: {})...", partition_id, mode, force);

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

    /// Uzbraja strefę w trybie pełnym (Mode 0).
    pub async fn arm_full(&self, partition_id: u16, code: Option<&str>) -> Result<(), SatelError> {
        self.arm(partition_id, 0, false, code).await
    }

    /// Uzbraja strefę w trybie STAY (Mode 1).
    pub async fn arm_stay(&self, partition_id: u16, code: Option<&str>) -> Result<(), SatelError> {
        self.arm(partition_id, 1, false, code).await
    }

    /// Uzbraja strefę w trybie STAY bez opóźnienia (Mode 2).
    pub async fn arm_stay_delay0(&self, partition_id: u16, code: Option<&str>) -> Result<(), SatelError> {
        self.arm(partition_id, 2, false, code).await
    }

    /// Uzbraja strefę w trybie STAY bez czasu na wyjście (Mode 3).
    pub async fn arm_stay_no_exit(&self, partition_id: u16, code: Option<&str>) -> Result<(), SatelError> {
        self.arm(partition_id, 3, false, code).await
    }

    /// Uzbraja strefę w trybie pełnym z forsowaniem.
    pub async fn force_arm_full(&self, partition_id: u16, code: Option<&str>) -> Result<(), SatelError> {
        self.arm(partition_id, 0, true, code).await
    }

    /// Uzbraja strefę w trybie STAY z forsowaniem.
    pub async fn force_arm_stay(&self, partition_id: u16, code: Option<&str>) -> Result<(), SatelError> {
        self.arm(partition_id, 1, true, code).await
    }

    /// Rozbraja wybraną strefę.
    pub async fn disarm(&self, partition_id: u16, code: Option<&str>) -> Result<(), SatelError> {
        if !(1..=32).contains(&partition_id) {
            return Err(SatelError::NoAccess);
        }

        let resolved_code = self.resolve_code(code)?;
        tracing::info!("Rozbrajanie strefy {}...", partition_id);

        let mut data = vec![SatelCommand::Disarm.to_byte()];
        data.extend_from_slice(&Self::format_user_code(&resolved_code));

        let mut mask = [0u8; 4];
        let idx = (partition_id - 1) as usize;
        mask[idx / 8] |= 1 << (idx % 8);
        data.extend_from_slice(&mask);

        let response = self.exchange(data, None, None).await?;
        Self::handle_result_code(&response)
    }

    /// Kasuje alarm w wybranej strefie.
    pub async fn clear_alarm(&self, partition_id: u16, code: Option<&str>) -> Result<(), SatelError> {
        if !(1..=32).contains(&partition_id) {
            return Err(SatelError::NoAccess);
        }

        let resolved_code = self.resolve_code(code)?;
        tracing::info!("Kasowanie alarmu w strefie {}...", partition_id);

        let mut data = vec![SatelCommand::ClearAlarm.to_byte()];
        data.extend_from_slice(&Self::format_user_code(&resolved_code));

        let mut mask = [0u8; 4];
        let idx = (partition_id - 1) as usize;
        mask[idx / 8] |= 1 << (idx % 8);
        data.extend_from_slice(&mask);

        let response = self.exchange(data, None, None).await?;
        Self::handle_result_code(&response)
    }

    /// Ustawia czas (RTC) w centrali.
    pub async fn set_satel_time(
        &self,
        datetime: chrono::DateTime<chrono::Local>,
        code: Option<&str>,
    ) -> Result<(), SatelError> {
        let resolved_code = self.resolve_code(code)?;
        tracing::info!("Ustawianie czasu w centrali na {}...", datetime);

        let mut data = vec![SatelCommand::SetRtcClock.to_byte()];
        data.extend_from_slice(&Self::format_user_code(&resolved_code));

        let time_str = datetime.format("%Y%m%d%H%M%S").to_string();
        data.extend_from_slice(time_str.as_bytes());

        let response = self.exchange(data, None, None).await?;
        Self::handle_result_code(&response)
    }

    /// Włącza wybrane wyjście.
    pub async fn set_output_on(&self, output_id: u16, code: Option<&str>) -> Result<(), SatelError> {
        self.set_output(output_id, true, code).await
    }

    /// Wyłącza wybrane wyjście.
    pub async fn set_output_off(&self, output_id: u16, code: Option<&str>) -> Result<(), SatelError> {
        self.set_output(output_id, false, code).await
    }

    /// Przełącza stan wyjścia na przeciwny (toggle).
    pub async fn set_output_toggle(
        &self,
        output_id: u16,
        code: Option<&str>,
    ) -> Result<(), SatelError> {
        if !(1..=256).contains(&output_id) {
            return Err(SatelError::NoAccess);
        }

        let resolved_code = self.resolve_code(code)?;
        tracing::info!("Przełączanie stanu wyjścia {}...", output_id);

        let mut data = vec![SatelCommand::OutputsSwitch.to_byte()];
        data.extend_from_slice(&Self::format_user_code(&resolved_code));

        let mut mask = [0u8; 32];
        let idx = (output_id - 1) as usize;
        mask[idx / 8] |= 1 << (idx % 8);
        data.extend_from_slice(&mask);

        let response = self.exchange(data, None, None).await?;
        Self::handle_result_code(&response)
    }

    // --- Prywatne helpery ---

    /// Steruje wyjściem (włącza/wyłącza).
    async fn set_output(&self, output_id: u16, state: bool, code: Option<&str>) -> Result<(), SatelError> {
        if !(1..=256).contains(&output_id) {
            return Err(SatelError::NoAccess);
        }

        let resolved_code = self.resolve_code(code)?;
        tracing::info!("Ustawianie wyjścia {} na {}...", output_id, state);

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

    /// Formatuje kod użytkownika do 8 bajtów (BCD z paddingiem 0xFF).
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

    /// Przetwarza odpowiedź 0xEF z centrali.
    fn handle_result_code(response: &[u8]) -> Result<(), SatelError> {
        tracing::info!("Odebrano ramkę odpowiedzi: {:02X?}", response);

        if response.is_empty() {
            tracing::error!("Pusta odpowiedź z centrali");
            return Err(SatelError::InvalidFrame);
        }

        if response[0] != SatelCommand::ResultCode.to_byte() {
            tracing::error!("Oczekiwano ramki wyniku (0xEF), otrzymano: {:02X?}", response[0]);
            return Err(SatelError::InvalidFrame);
        }

        let code = response.get(1).cloned().unwrap_or(0xFF);
        match code {
            0x00 => {
                tracing::debug!("Centrala: OK (0x00)");
                Ok(())
            }
            0x01 => {
                tracing::warn!("Centrala: Błędny kod użytkownika (0x01)");
                Err(SatelError::InvalidUserCode)
            }
            0x02 => {
                tracing::warn!("Centrala: Brak dostępu (0x02)");
                Err(SatelError::NoAccess)
            }
            0x11 | 0x12 => {
                tracing::warn!("Centrala: Nie można uzbroić (0x{:02X})", code);
                Err(SatelError::CanNotArm)
            }
            0xFF => {
                tracing::debug!("Centrala: Polecenie zaakceptowane / operacja w toku (0xFF)");
                Ok(())
            }
            _ => {
                tracing::error!("Centrala: Nieznany błąd (0x{:02X})", code);
                Err(SatelError::IntegraResultError(code))
            }
        }
    }

    /// Rozwiązuje kod użytkownika (podany lub z konfiguracji).
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
