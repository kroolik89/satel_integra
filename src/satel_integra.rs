use crate::satel_integra_data::{
    Config, ConnectionConfig, ConnectionState, ConnectionType, IntegraVersion, OutputName,
    PartitionName, SatelCommand, SatelEvent, SatelResult, SatelState, SatelStateHandle, ZoneName,
    ZoneStatus, ZoneTemperature,
};
use crate::satel_integra_process::{
    process_integra_version, process_output_name, process_outputs_state, process_partition_name,
    process_partitions_alarm, process_partitions_alarm_memory, process_partitions_armed_really,
    process_partitions_armed_suppressed, process_partitions_entry_time,
    process_partitions_exit_time_gt_10s, process_partitions_exit_time_lt_10s, process_zone_name,
    process_zone_temperature,
    process_zones_alarm, process_zones_alarm_memory, process_zones_bypass,
    process_zones_long_violation_trouble, process_zones_no_violation_trouble, process_zones_tamper,
    process_zones_tamper_alarm, process_zones_tamper_alarm_memory, process_zones_violation,
};
use bytes::{Buf, BytesMut};
use chrono::Local;
use futures::{SinkExt, StreamExt};
use std::io;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::{sleep, timeout};
use tokio_serial::SerialPortBuilderExt;
use tokio_util::codec::{Decoder, Encoder, Framed};

/// Trait pomocniczy, łączący AsyncRead i AsyncWrite, aby ułatwić tworzenie trait object.
trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncReadWrite for T {}

/// Typ strumienia I/O, opakowany w `Framed` z naszym kodekiem.
type FramedStream = Framed<Box<dyn AsyncReadWrite>, SatelCodec>;

/// Zdarzenia dotyczące stanu połączenia.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionEvent {
    Connected,
    ConnectionLost,
    Disconnected,
}

/// Wiadomości przesyłane do SatelAutoRequester.
enum StateWorkerMessage {
    /// Ramka otrzymana z centrali (np. Push).
    Frame(Vec<u8>),
    /// Zmiana stanu połączenia.
    StatusChanged(ConnectionState),
}

/// Wewnętrzna wiadomość przesyłane między klientem a workerem.
enum InternalMessage {
    /// Standardowa wymiana danych (tylko w stanie Connected).
    ExchangeStandard {
        data: Vec<u8>,
        write_timeout: Duration,
        read_timeout: Duration,
        created_at: Instant,
        max_queue_time: Duration,
        response_tx: oneshot::Sender<Result<Vec<u8>, SatelError>>,
    },
    /// Priorytetowa wymiana danych (dozwolona w Connecting, Handshake, Connected).
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

/// Główny uchwyt (klient) do komunikacji z centralą.
/// Można go dowolnie klonować.
#[derive(Clone)]
pub struct SatelIntegra {
    tx: mpsc::Sender<InternalMessage>,
    state: SatelStateHandle,
    config: Config,
    worker: Arc<Mutex<Option<SatelCommunicationWorker>>>,
    event_tx: broadcast::Sender<SatelEvent>,
}

/// Błędy, które mogą wystąpić podczas komunikacji.
#[derive(Error, Debug)]
pub enum SatelError {
    #[error("Błąd wejścia/wyjścia: {0}")]
    Io(#[from] io::Error),
    #[error("Błąd portu szeregowego: {0}")]
    Serial(#[from] tokio_serial::Error),
    #[error("Przekroczono czas oczekiwania (timeout)")]
    Timeout,
    #[error("Połączenie nie jest aktywne")]
    NotConnected,
    #[error("Strumień został zamknięty przez drugą stronę")]
    StreamClosed,
    #[error("Połączenie zostało utracone, próba ponowienia może być możliwa")]
    ConnectionLost,
    #[error("Nieprawidłowa suma kontrolna ramki")]
    InvalidCrc,
    #[error("Nieprawidłowy format ramki (np. zła stopka)")]
    InvalidFrame,
    #[error("Wiadomość przedawniła się w kolejce")]
    MessageExpired,
    #[error("Worker został zatrzymany")]
    WorkerDropped,
    #[error("Połączenie/Worker zostało już uruchomione")]
    AlreadyConnected,
    #[error("Błąd czujnika temperatury (0xFFFF)")]
    TemperatureSensorError,
    #[error("Prawdopodobny brak czujnika temperatury lub TimeOut")]
    TemperatureNotSupportedOrTimeOut,
    #[error("Zbyt wiele błędów odczytu czujnika temperatury - odczyt zablokowany")]
    TempTooManyErrors,
    #[error("Stan wewnętrzny biblioteki został uszkodzony (poisoned lock)")]
    StatePoisoned,
    #[error("Błędny kod użytkownika")]
    InvalidUserCode,
    #[error("Brak dostępu / Nieprawidłowe ID")]
    NoAccess,
    #[error("Nie można uzbroić (wymagane forsowanie)")]
    CanNotArm,
    #[error("Nieznany błąd centrali (0xEF): {0}")]
    IntegraResultError(u8),
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
        let mut worker_lock = self.worker.lock().unwrap();
        if let Some(mut worker) = worker_lock.take() {
            let (state_worker_tx, state_worker_rx) = mpsc::channel(100);
            worker.state_worker_tx = Some(state_worker_tx);

            let mut state_worker = SatelAutoRequester {
                integra: self.clone(),
                rx: state_worker_rx,
            };

            // Kanał do powiadomienia o sukcesie początkowego połączenia
            let (connect_tx, connect_rx) = oneshot::channel();

            // Uruchamiamy StateWorkera
            tokio::spawn(async move {
                state_worker.run().await;
            });

            // Uruchamiamy CommunicationWorkera (przejmuje self)
            tokio::spawn(async move {
                worker.run_with_initial_connect(connect_tx).await;
            });

            // Czekamy na wynik pierwszego połączenia
            return connect_rx.await.map_err(|_| SatelError::WorkerDropped)?;
        }

        // Jeśli worker już działa, wysyłamy komendę Connect przez kanał
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

        {
            let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
            state.integra_version = Some(version.clone());
        }

        tracing::info!(
            "Pobrano wersję centrali: {} (v{}, język: {})",
            version.model,
            version.firmware_version,
            version.language
        );

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

        let device_type: u8 = 1; // 1 = zone
        let device_id: u8 = if zone_id == 256 { 0 } else { zone_id as u8 };

        let cmd = vec![
            SatelCommand::ReadDeviceName.to_byte(),
            device_type,
            device_id,
        ];
        let response = self.exchange(cmd, None, None).await?;

        if !response.is_empty() && response[0] == SatelCommand::ResultCode.to_byte() {
            tracing::error!(
                "Centrala zwróciła błąd (0xEF) dla nazwy wejścia {}: {:?}",
                zone_id,
                response.get(1)
            );
            return Err(SatelError::InvalidFrame);
        }

        let (id, s_name) = process_zone_name(&response).map_err(|e| {
            tracing::error!(
                "Błąd podczas przetwarzania nazwy wejścia {}: {:?}",
                zone_id,
                e
            );
            e
        })?;

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

        let device_type: u8 = 4; // 4 = output
        let device_id: u8 = if output_id == 256 { 0 } else { output_id as u8 };

        let cmd = vec![
            SatelCommand::ReadDeviceName.to_byte(),
            device_type,
            device_id,
        ];
        let response = self.exchange(cmd, None, None).await?;

        if !response.is_empty() && response[0] == SatelCommand::ResultCode.to_byte() {
            tracing::error!(
                "Centrala zwróciła błąd (0xEF) dla nazwy wyjścia {}: {:?}",
                output_id,
                response.get(1)
            );
            return Err(SatelError::InvalidFrame);
        }

        let (id, s_name) = process_output_name(&response).map_err(|e| {
            tracing::error!(
                "Błąd podczas przetwarzania nazwy wyjścia {}: {:?}",
                output_id,
                e
            );
            e
        })?;

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

        let device_type: u8 = 0; // 0 = partition/zone
        let device_id: u8 = partition_id as u8;

        let cmd = vec![
            SatelCommand::ReadDeviceName.to_byte(),
            device_type,
            device_id,
        ];
        let response = self.exchange(cmd, None, None).await?;

        if !response.is_empty() && response[0] == SatelCommand::ResultCode.to_byte() {
            tracing::error!(
                "Centrala zwróciła błąd (0xEF) dla nazwy strefy {}: {:?}",
                partition_id,
                response.get(1)
            );
            return Err(SatelError::InvalidFrame);
        }

        let (id, partition_name) = process_partition_name(&response).map_err(|e| {
            tracing::error!(
                "Błąd podczas przetwarzania nazwy strefy {}: {:?}",
                partition_id,
                e
            );
            e
        })?;

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

                    // Logika: NoRead -> Ok
                    if zone.temperature_status
                        == crate::satel_integra_data::TemperatureSensorStatus::NoRead
                    {
                        zone.temperature_status =
                            crate::satel_integra_data::TemperatureSensorStatus::Ok;
                    }

                    // Dekrementacja liczników current
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
                    return Ok(zone.to_zone_temperature());
                }
                Err(e) => {
                    self.update_temp_error(zone_id, &e)?;
                    return Err(e);
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

    fn update_temp_error(&self, zone_id: u16, error: &SatelError) -> Result<(), SatelError> {
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
            tracing::error!(
                "Oczekiwano ramki wyniku (0xEF), otrzymano: {:02X?}",
                response[0]
            );
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
                tracing::debug!("Centrala: Operacja w toku (0xFF)");
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

    /// Uzbraja wybraną strefę w podanym trybie z opcją forsowania.
    pub async fn arm(
        &self,
        partition_id: u16,
        mode: u8,
        force: bool,
        code: Option<&str>,
    ) -> Result<(), SatelError> {
        if partition_id < 1 || partition_id > 32 {
            return Err(SatelError::NoAccess);
        }

        let resolved_code = self.resolve_code(code)?;
        tracing::info!(
            "Uzbrajanie strefy {} w trybie {} (force: {})...",
            partition_id,
            mode,
            force
        );

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

        // Maska stref (4 bajty)
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
    pub async fn arm_stay_delay0(
        &self,
        partition_id: u16,
        code: Option<&str>,
    ) -> Result<(), SatelError> {
        self.arm(partition_id, 2, false, code).await
    }

    /// Uzbraja strefę w trybie STAY bez czasu na wyjście (Mode 3).
    pub async fn arm_stay_no_exit(
        &self,
        partition_id: u16,
        code: Option<&str>,
    ) -> Result<(), SatelError> {
        self.arm(partition_id, 3, false, code).await
    }

    /// Uzbraja strefę w trybie pełnym z forsowaniem (Force Mode 0).
    pub async fn force_arm_full(
        &self,
        partition_id: u16,
        code: Option<&str>,
    ) -> Result<(), SatelError> {
        self.arm(partition_id, 0, true, code).await
    }

    /// Uzbraja strefę w trybie STAY z forsowaniem (Force Mode 1).
    pub async fn force_arm_stay(
        &self,
        partition_id: u16,
        code: Option<&str>,
    ) -> Result<(), SatelError> {
        self.arm(partition_id, 1, true, code).await
    }

    /// Rozbraja wybraną strefę.
    pub async fn disarm(&self, partition_id: u16, code: Option<&str>) -> Result<(), SatelError> {
        if partition_id < 1 || partition_id > 32 {
            return Err(SatelError::NoAccess);
        }

        let resolved_code = self.resolve_code(code)?;
        tracing::info!("Rozbrajanie strefy {}...", partition_id);

        let mut data = vec![SatelCommand::Disarm.to_byte()];
        data.extend_from_slice(&Self::format_user_code(&resolved_code));

        // Maska stref (4 bajty)
        let mut mask = [0u8; 4];
        let idx = (partition_id - 1) as usize;
        mask[idx / 8] |= 1 << (idx % 8);
        data.extend_from_slice(&mask);

        let response = self.exchange(data, None, None).await?;
        Self::handle_result_code(&response)
    }

    /// Kasuje alarm w wybranej strefie.
    pub async fn clear_alarm(&self, partition_id: u16, code: Option<&str>) -> Result<(), SatelError> {
        if partition_id < 1 || partition_id > 32 {
            return Err(SatelError::NoAccess);
        }

        let resolved_code = self.resolve_code(code)?;
        tracing::info!("Kasowanie alarmu w strefie {}...", partition_id);

        let mut data = vec![SatelCommand::ClearAlarm.to_byte()];
        data.extend_from_slice(&Self::format_user_code(&resolved_code));

        // Maska stref (4 bajty)
        let mut mask = [0u8; 4];
        let idx = (partition_id - 1) as usize;
        mask[idx / 8] |= 1 << (idx % 8);
        data.extend_from_slice(&mask);

        let response = self.exchange(data, None, None).await?;
        Self::handle_result_code(&response)
    }

    /// Steruje wyjściem (włącza/wyłącza). Metoda wewnętrzna.
    async fn set_output(&self, output_id: u16, state: bool, code: Option<&str>) -> Result<(), SatelError> {
        if output_id < 1 || output_id > 256 {
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

        // Maska wyjść (32 bajty dla 256 wyjść)
        let mut mask = [0u8; 32];
        let idx = (output_id - 1) as usize;
        mask[idx / 8] |= 1 << (idx % 8);
        data.extend_from_slice(&mask);

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
        if output_id < 1 || output_id > 256 {
            return Err(SatelError::NoAccess);
        }

        let resolved_code = self.resolve_code(code)?;
        tracing::info!("Przełączanie stanu wyjścia {}...", output_id);

        let mut data = vec![SatelCommand::OutputsSwitch.to_byte()];
        data.extend_from_slice(&Self::format_user_code(&resolved_code));

        // Maska wyjść (32 bajty dla 256 wyjść)
        let mut mask = [0u8; 32];
        let idx = (output_id - 1) as usize;
        mask[idx / 8] |= 1 << (idx % 8);
        data.extend_from_slice(&mask);

        let response = self.exchange(data, None, None).await?;
        Self::handle_result_code(&response)
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
        use crate::satel_integra_data::TemperatureSensorStatus;

        // 1. Sprawdzenie czy blokowanie jest w ogóle włączone
        if !self.config.temp_blocking_enabled {
            return self.get_zone_temperature(zone_id).await;
        }

        // 2. Pobranie aktualnego stanu z cache (bezpieczna obsługa locka)
        let zone_info = {
            let state = self.state.read().map_err(|_| SatelError::StatePoisoned)?;
            state
                .zones
                .get((zone_id.wrapping_sub(1) % 256) as usize)
                .cloned()
        };

        if let Some(info) = zone_info {
            // A) Jeśli stan jest już zablokowany (inny niż Ok lub NoRead)
            if info.temperature_status != TemperatureSensorStatus::Ok
                && info.temperature_status != TemperatureSensorStatus::NoRead
            {
                return Err(SatelError::TempTooManyErrors);
            }

            // B) Sprawdzenie progów na licznikach bieżących (current)
            if info.temperature_timeout_errors_current >= self.config.temp_max_timeout_errors {
                {
                    let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
                    if let Some(zone) = state.zones.get_mut((zone_id.wrapping_sub(1) % 256) as usize)
                    {
                        zone.temperature_status = TemperatureSensorStatus::SensorMissing;
                    }
                }
                return Err(SatelError::TempTooManyErrors);
            }

            if info.temperature_sensor_errors_current >= self.config.temp_max_sensor_errors {
                {
                    let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
                    if let Some(zone) = state.zones.get_mut((zone_id.wrapping_sub(1) % 256) as usize)
                    {
                        zone.temperature_status = TemperatureSensorStatus::CommunicationError;
                    }
                }
                return Err(SatelError::TempTooManyErrors);
            }
        }

        // 3. Jeśli przeszło przez filtry -> wywołaj właściwy odczyt
        self.get_zone_temperature(zone_id).await
    }

    /// Pobiera stan sabotaży wszystkich wejść z centrali i aktualizuje cache.
    pub async fn get_zones_tamper(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanu sabotaży wszystkich wejść...");

        // Komenda 0x01, wysyłamy 2 bajty (0x01 + 0x00), aby wymusić odpowiedź 32-bajtową (256 wejść)
        let cmd = vec![SatelCommand::ZonesTamper.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;

        let result = process_zones_tamper(&response, &self.config.io_tamper_invert)?;
        self.update_zones_tamper_internal(result)?;

        tracing::info!("Zaktualizowano stan sabotaży");
        Ok(())
    }

    fn update_zones_tamper_internal(
        &self,
        result: crate::satel_integra_data::ZonesTamperData,
    ) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(zone) = state.zones.get_mut(i) {
                if zone.tamper_state != new_state {
                    zone.tamper_state = new_state;
                    zone.tamper_read_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::ZoneTamper {
                        id: zone.id,
                        state: new_state,
                    });
                }
            }
        }
        Ok(())
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

    /// Pobiera stan alarmów wszystkich wejść z centrali i aktualizuje cache.
    pub async fn get_zones_alarm(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanu alarmów wszystkich wejść...");

        // Komenda 0x02, wysyłamy 2 bajty (0x02 + 0x00), aby wymusić odpowiedź 32-bajtową (256 wejść)
        let cmd = vec![SatelCommand::ZonesAlarm.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;

        let result = process_zones_alarm(&response, &self.config.io_alarm_invert)?;
        self.update_zones_alarm_internal(result)?;

        tracing::info!("Zaktualizowano stan alarmów");
        Ok(())
    }

    fn update_zones_alarm_internal(
        &self,
        result: crate::satel_integra_data::ZonesAlarmData,
    ) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(zone) = state.zones.get_mut(i) {
                if zone.alarm_state != new_state {
                    zone.alarm_state = new_state;
                    zone.alarm_read_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::ZoneAlarm {
                        id: zone.id,
                        state: new_state,
                    });
                }
            }
        }
        Ok(())
    }

    /// Pobiera stan naruszeń wszystkich wejść z centrali i aktualizuje cache.
    pub async fn get_zones_violation(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanu naruszeń wszystkich wejść...");

        // Komenda 0x00, wysyłamy 2 bajty (0x00 + 0x00), aby wymusić odpowiedź 32-bajtową (256 wejść)
        let cmd = vec![SatelCommand::ZonesViolation.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;

        let result = process_zones_violation(&response, &self.config.io_violation_invert)?;
        self.update_zones_violation_internal(result)?;

        tracing::info!("Zaktualizowano stan naruszeń");
        Ok(())
    }

    fn update_zones_violation_internal(
        &self,
        result: crate::satel_integra_data::ZonesViolationData,
    ) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(zone) = state.zones.get_mut(i) {
                if zone.violation_state != new_state {
                    zone.violation_state = new_state;
                    zone.violation_read_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::ZoneViolation {
                        id: zone.id,
                        state: new_state,
                    });
                }
            }
        }
        Ok(())
    }

    /// Pobiera stan alarmów sabotażowych wszystkich wejść z centrali i aktualizuje cache.
    pub async fn get_zones_tamper_alarm(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanu alarmów sabotażowych wszystkich wejść...");

        // Komenda 0x03, wysyłamy 2 bajty (0x03 + 0x00), aby wymusić odpowiedź 32-bajtową (256 wejść)
        let cmd = vec![SatelCommand::ZonesTamperAlarm.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;

        let result = process_zones_tamper_alarm(&response, &self.config.io_tamper_alarm_invert)?;
        self.update_zones_tamper_alarm_internal(result)?;

        tracing::info!("Zaktualizowano stan alarmów sabotażowych");
        Ok(())
    }

    fn update_zones_tamper_alarm_internal(
        &self,
        result: crate::satel_integra_data::ZonesTamperAlarmData,
    ) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(zone) = state.zones.get_mut(i) {
                if zone.tamper_alarm_state != new_state {
                    zone.tamper_alarm_state = new_state;
                    zone.tamper_alarm_read_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::ZoneTamperAlarm {
                        id: zone.id,
                        state: new_state,
                    });
                }
            }
        }
        Ok(())
    }

    /// Pobiera stan pamięci alarmów wszystkich wejść z centrali i aktualizuje cache.
    pub async fn get_zones_alarm_memory(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanu pamięci alarmów wszystkich wejść...");

        // Komenda 0x04, wysyłamy 2 bajty (0x04 + 0x00), aby wymusić odpowiedź 32-bajtową (256 wejść)
        let cmd = vec![SatelCommand::ZonesAlarmMemory.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;

        let result = process_zones_alarm_memory(&response, &self.config.io_alarm_memory_invert)?;
        self.update_zones_alarm_memory_internal(result)?;

        tracing::info!("Zaktualizowano stan pamięci alarmów");
        Ok(())
    }

    fn update_zones_alarm_memory_internal(
        &self,
        result: crate::satel_integra_data::ZonesAlarmMemoryData,
    ) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(zone) = state.zones.get_mut(i) {
                if zone.alarm_memory_state != new_state {
                    zone.alarm_memory_state = new_state;
                    zone.alarm_memory_read_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::ZoneAlarmMemory {
                        id: zone.id,
                        state: new_state,
                    });
                }
            }
        }
        Ok(())
    }

    /// Pobiera stan pamięci alarmów sabotażowych wszystkich wejść z centrali i aktualizuje cache.
    pub async fn get_zones_tamper_alarm_memory(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanu pamięci alarmów sabotażowych wszystkich wejść...");

        // Komenda 0x05, wysyłamy 2 bajty (0x05 + 0x00), aby wymusić odpowiedź 32-bajtową (256 wejść)
        let cmd = vec![SatelCommand::ZonesTamperAlarmMemory.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;

        let result = process_zones_tamper_alarm_memory(
            &response,
            &self.config.io_tamper_alarm_memory_invert,
        )?;
        self.update_zones_tamper_alarm_memory_internal(result)?;

        tracing::info!("Zaktualizowano stan pamięci alarmów sabotażowych");
        Ok(())
    }

    fn update_zones_tamper_alarm_memory_internal(
        &self,
        result: crate::satel_integra_data::ZonesTamperAlarmMemoryData,
    ) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(zone) = state.zones.get_mut(i) {
                if zone.tamper_alarm_memory_state != new_state {
                    zone.tamper_alarm_memory_state = new_state;
                    zone.tamper_alarm_memory_read_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::ZoneTamperAlarmMemory {
                        id: zone.id,
                        state: new_state,
                    });
                }
            }
        }
        Ok(())
    }

    /// Pobiera stan blokad (bypass) wszystkich wejść z centrali i aktualizuje cache.
    pub async fn get_zones_bypass(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanu blokad (bypass) wszystkich wejść...");

        // Komenda 0x06, wysyłamy 2 bajty (0x06 + 0x00), aby wymusić odpowiedź 32-bajtową (256 wejść)
        let cmd = vec![SatelCommand::ZonesBypass.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;

        let result = process_zones_bypass(&response, &self.config.io_bypass_invert)?;
        self.update_zones_bypass_internal(result)?;

        tracing::info!("Zaktualizowano stan blokad");
        Ok(())
    }

    fn update_zones_bypass_internal(
        &self,
        result: crate::satel_integra_data::ZonesBypassData,
    ) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(zone) = state.zones.get_mut(i) {
                if zone.bypass_state != new_state {
                    zone.bypass_state = new_state;
                    zone.bypass_read_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::ZoneBypass {
                        id: zone.id,
                        state: new_state,
                    });
                }
            }
        }
        Ok(())
    }

    /// Pobiera stan awarii "brak naruszenia" wszystkich wejść z centrali i aktualizuje cache.
    pub async fn get_zones_no_violation_trouble(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanu awarii 'brak naruszenia' wszystkich wejść...");

        // Komenda 0x07, wysyłamy 2 bajty (0x07 + 0x00), aby wymusić odpowiedź 32-bajtową (256 wejść)
        let cmd = vec![SatelCommand::ZonesNoViolationTrouble.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;

        let result = process_zones_no_violation_trouble(
            &response,
            &self.config.io_no_violation_trouble_invert,
        )?;
        self.update_zones_no_violation_trouble_internal(result)?;

        tracing::info!("Zaktualizowano stan awarii 'brak naruszenia'");
        Ok(())
    }

    fn update_zones_no_violation_trouble_internal(
        &self,
        result: crate::satel_integra_data::ZonesNoViolationTroubleData,
    ) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(zone) = state.zones.get_mut(i) {
                if zone.no_violation_trouble_state != new_state {
                    zone.no_violation_trouble_state = new_state;
                    zone.no_violation_trouble_read_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::ZoneNoViolationTrouble {
                        id: zone.id,
                        state: new_state,
                    });
                }
            }
        }
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

    fn update_partitions_armed_internal(
        &self,
        result: crate::satel_integra_data::PartitionsArmedData,
    ) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(partition) = state.partitions.get_mut(i) {
                if partition.armed_suppressed != new_state {
                    partition.armed_suppressed = new_state;
                    partition.armed_suppressed_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::PartitionArmed {
                        id: partition.id,
                        state: new_state,
                    });
                }
            }
        }
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

    fn update_partitions_armed_really_internal(
        &self,
        result: crate::satel_integra_data::PartitionsData,
    ) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(partition) = state.partitions.get_mut(i) {
                if partition.armed_really != new_state {
                    partition.armed_really = new_state;
                    partition.armed_really_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::PartitionArmedReally {
                        id: partition.id,
                        state: new_state,
                    });
                }
            }
        }
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

    fn update_partitions_alarm_internal(
        &self,
        result: crate::satel_integra_data::PartitionsData,
    ) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(partition) = state.partitions.get_mut(i) {
                if partition.alarm != new_state {
                    partition.alarm = new_state;
                    partition.alarm_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::PartitionAlarm {
                        id: partition.id,
                        state: new_state,
                    });
                }
            }
        }
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

    fn update_partitions_alarm_memory_internal(
        &self,
        result: crate::satel_integra_data::PartitionsData,
    ) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(partition) = state.partitions.get_mut(i) {
                if partition.alarm_memory != new_state {
                    partition.alarm_memory = new_state;
                    partition.alarm_memory_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::PartitionAlarmMemory {
                        id: partition.id,
                        state: new_state,
                    });
                }
            }
        }
        Ok(())
    }

    /// Pobiera stany liczników czasu na wejście/wyjście stref z centrali i aktualizuje cache.
    pub async fn get_partitions_times(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanów liczników czasu stref...");

        // Entry time
        let cmd = vec![SatelCommand::PartitionsEntryTime.to_byte()];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_partitions_entry_time(&response)?;
        self.update_partitions_entry_time_internal(result)?;

        // Exit time > 10s
        let cmd = vec![SatelCommand::PartitionsExitTimeMore10s.to_byte()];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_partitions_exit_time_gt_10s(&response)?;
        self.update_partitions_exit_time_gt_10s_internal(result)?;

        // Exit time < 10s
        let cmd = vec![SatelCommand::PartitionsExitTimeLess10s.to_byte()];
        let response = self.exchange(cmd, None, None).await?;
        let result = process_partitions_exit_time_lt_10s(&response)?;
        self.update_partitions_exit_time_lt_10s_internal(result)?;

        tracing::info!("Zaktualizowano stany liczników czasu stref");
        Ok(())
    }

    fn update_partitions_entry_time_internal(
        &self,
        result: crate::satel_integra_data::PartitionsData,
    ) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(partition) = state.partitions.get_mut(i) {
                if partition.entry_time != new_state {
                    partition.entry_time = new_state;
                    partition.entry_time_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::PartitionEntryTime {
                        id: partition.id,
                        state: new_state,
                    });
                }
            }
        }
        Ok(())
    }

    fn update_partitions_exit_time_gt_10s_internal(
        &self,
        result: crate::satel_integra_data::PartitionsData,
    ) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(partition) = state.partitions.get_mut(i) {
                if partition.exit_time_gt_10s != new_state {
                    partition.exit_time_gt_10s = new_state;
                    partition.exit_time_gt_10s_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::PartitionExitTimeGt10s {
                        id: partition.id,
                        state: new_state,
                    });
                }
            }
        }
        Ok(())
    }

    fn update_partitions_exit_time_lt_10s_internal(
        &self,
        result: crate::satel_integra_data::PartitionsData,
    ) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(partition) = state.partitions.get_mut(i) {
                if partition.exit_time_lt_10s != new_state {
                    partition.exit_time_lt_10s = new_state;
                    partition.exit_time_lt_10s_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::PartitionExitTimeLt10s {
                        id: partition.id,
                        state: new_state,
                    });
                }
            }
        }
        Ok(())
    }

    /// Pobiera stan awarii "długie naruszenie" wszystkich wejść z centrali i aktualizuje cache.
    pub async fn get_zones_long_violation_trouble(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanu awarii 'długie naruszenie' wszystkich wejść...");

        // Komenda 0x08, wysyłamy 2 bajty (0x08 + 0x00), aby wymusić odpowiedź 32-bajtową (256 wejść)
        let cmd = vec![SatelCommand::ZonesLongViolationTrouble.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;

        let result = process_zones_long_violation_trouble(
            &response,
            &self.config.io_long_violation_trouble_invert,
        )?;
        self.update_zones_long_violation_trouble_internal(result)?;

        tracing::info!("Zaktualizowano stan awarii 'długie naruszenie'");
        Ok(())
    }

    /// Pobiera stan wszystkich wyjść z centrali i aktualizuje cache.
    pub async fn get_outputs_state(&self) -> Result<(), SatelError> {
        tracing::info!("Pobieranie stanu wszystkich wyjść...");

        // Komenda 0x17, wysyłamy 2 bajty (0x17 + 0x00), aby wymusić odpowiedź 32-bajtową (256 wyjść)
        let cmd = vec![SatelCommand::OutputsState.to_byte(), 0x00];
        let response = self.exchange(cmd, None, None).await?;

        let result = process_outputs_state(&response)?;
        self.update_outputs_state_internal(result)?;

        tracing::info!("Zaktualizowano stan wyjść");
        Ok(())
    }

    fn update_outputs_state_internal(
        &self,
        result: crate::satel_integra_data::OutputsStateData,
    ) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(output) = state.outputs.get_mut(i) {
                if output.state != new_state {
                    output.state = new_state;
                    output.state_read_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::OutputChanged {
                        id: output.id,
                        state: new_state,
                    });
                }
            }
        }
        Ok(())
    }

    fn update_zones_long_violation_trouble_internal(
        &self,
        result: crate::satel_integra_data::ZonesLongViolationTroubleData,
    ) -> Result<(), SatelError> {
        let mut state = self.state.write().map_err(|_| SatelError::StatePoisoned)?;
        for (i, &new_state) in result.states.iter().enumerate() {
            if let Some(zone) = state.zones.get_mut(i) {
                if zone.long_violation_trouble_state != new_state {
                    zone.long_violation_trouble_state = new_state;
                    zone.long_violation_trouble_read_at = result.read_at;
                    let _ = self.event_tx.send(SatelEvent::ZoneLongViolationTrouble {
                        id: zone.id,
                        state: new_state,
                    });
                }
            }
        }
        Ok(())
    }
}

/// SatelAutoRequester odpowiada za przetwarzanie danych Push oraz automatyczne zapytania (Keep-Alive).
pub struct SatelAutoRequester {
    integra: SatelIntegra,
    rx: mpsc::Receiver<StateWorkerMessage>,
}

impl SatelAutoRequester {
    pub async fn run(&mut self) {
        tracing::info!("SatelAutoRequester uruchomiony");

        loop {
            tokio::select! {
                maybe_msg = self.rx.recv() => {
                    if let Some(msg) = maybe_msg {
                        match msg {
                            StateWorkerMessage::Frame(frame) => {
                                self.handle_auto_frame(&frame);
                            }
                            StateWorkerMessage::StatusChanged(state) => {
                                tracing::info!("SatelAutoRequester: Zmiana stanu połączenia -> {:?}", state);
                                let _ = self.integra.event_tx.send(SatelEvent::ConnectionChanged(state));
                            }
                        }
                    } else {
                        break;
                    }
                }
            }
        }
        tracing::info!("SatelAutoRequester zatrzymany");
    }

    fn handle_auto_frame(&mut self, frame: &[u8]) {
        if frame.is_empty() {
            return;
        }

        match frame[0] {
            0x00 => {
                if let Ok(d) =
                    process_zones_violation(frame, &self.integra.config.io_violation_invert)
                {
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
                if let Ok(d) =
                    process_zones_tamper_alarm(frame, &self.integra.config.io_tamper_alarm_invert)
                {
                    let _ = self.integra.update_zones_tamper_alarm_internal(d);
                }
            }
            0x04 => {
                if let Ok(d) =
                    process_zones_alarm_memory(frame, &self.integra.config.io_alarm_memory_invert)
                {
                    let _ = self.integra.update_zones_alarm_memory_internal(d);
                }
            }
            0x05 => {
                if let Ok(d) = process_zones_tamper_alarm_memory(
                    frame,
                    &self.integra.config.io_tamper_alarm_memory_invert,
                ) {
                    let _ = self.integra.update_zones_tamper_alarm_memory_internal(d);
                }
            }
            0x06 => {
                if let Ok(d) = process_zones_bypass(frame, &self.integra.config.io_bypass_invert) {
                    let _ = self.integra.update_zones_bypass_internal(d);
                }
            }
            0x07 => {
                if let Ok(d) = process_zones_no_violation_trouble(
                    frame,
                    &self.integra.config.io_no_violation_trouble_invert,
                ) {
                    let _ = self.integra.update_zones_no_violation_trouble_internal(d);
                }
            }
            0x08 => {
                if let Ok(d) = process_zones_long_violation_trouble(
                    frame,
                    &self.integra.config.io_long_violation_trouble_invert,
                ) {
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

/// Worker zarządzający fizycznym połączeniem w tle.
pub struct SatelCommunicationWorker {
    config: Config,
    state: SatelStateHandle,
    rx: mpsc::Receiver<InternalMessage>,
    stream: Option<FramedStream>,
    state_worker_tx: Option<mpsc::Sender<StateWorkerMessage>>,
}

impl SatelCommunicationWorker {
    /// Główna pętla workera z obsługą początkowego połączenia.
    async fn run_with_initial_connect(mut self, on_connect: oneshot::Sender<Result<(), SatelError>>) {
        let result = self.satel_connection_worker_connect().await;
        if let Err(e) = &result {
            tracing::error!("Początkowe połączenie nieudane: {:?}", e);
        }
        let _ = on_connect.send(result);
        self.run().await;
    }

    /// Główna pętla workera.
    pub async fn run(mut self) {
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
                        tracing::error!("Błąd podczas odbierania danych Push / Stream: {:?}", e);
                        self.satel_connection_worker_connection_lost().await;
                    }
                }

                _ = ping_interval.tick() => {
                    let (current_state, last_send, last_event) = {
                        let s = self.state.read().unwrap();
                        (s.telemetry.status.state, s.telemetry.last_send_at, s.telemetry.status.last_event_at)
                    };

                    // 1. Watchdog dla Connecting / Handshake
                    if (current_state == ConnectionState::Connecting || current_state == ConnectionState::Handshake)
                        && last_event.elapsed() > Duration::from_secs(15) {
                        tracing::warn!("Watchdog: Przekroczono czas łączenia/handshake (15s)");
                        self.satel_connection_worker_connection_lost().await;
                    }

                    // 2. Ping (Keep-alive)
                    if current_state == ConnectionState::Connected && last_send.elapsed() >= Duration::from_secs(2) {
                        // Wysyłamy zapytanie o wersję dla podtrzymania połączenia
                        let cmd = vec![SatelCommand::IntegraVersion.to_byte()];
                        let _ = self.satel_connection_worker_exchange(cmd, 0x7E, Duration::from_millis(500), Duration::from_millis(500)).await;
                    }

                    // 3. Podtrzymywanie połączenia (Auto-reconnect)
                    if should_reconnect && current_state == ConnectionState::ConnectionLost {
                        if last_event.elapsed() >= self.calculate_backoff() {
                            tracing::info!("Auto-reconnect: Podejmowanie próby połączenia...");
                            if let Ok(s) = self.state.read() {
                                s.telemetry.reconnect_count.fetch_add(1, Ordering::Relaxed);
                            }
                            let _ = self.satel_connection_worker_connect().await;
                        }
                    }
                }
            }
        }
        tracing::info!("SatelCommunicationWorker zatrzymany");
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
                if state == ConnectionState::Connected {
                    let _ = response_tx.send(Err(SatelError::AlreadyConnected));
                } else if state == ConnectionState::Connecting || state == ConnectionState::Handshake {
                    let _ = response_tx.send(Err(match state {
                        ConnectionState::Connecting => SatelError::AlreadyConnected, // W trakcie łączenia
                        _ => SatelError::AlreadyConnected,
                    }));
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
                s.telemetry
                    .bytes_sent
                    .fetch_add(data.len(), Ordering::Relaxed);
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
                        s.telemetry
                            .bytes_received
                            .fetch_add(frame.len(), Ordering::Relaxed);
                    }
                    if frame.is_empty() {
                        continue;
                    }

                    // Powiadamiamy AutoRequestera o ramkach Push oraz o kodach wyników (0xEF).
                    // Pomijamy kod 0xFF (zaakceptowano), ponieważ jest to tylko techniczne potwierdzenie
                    // bieżącej komendy i nie powinno być traktowane jako asynchroniczne powiadomienie.
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
        self.stream = None; // Kasuje strumień i bufor dekodera
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
        tracing::info!("Podejmowanie próby połączenia fizycznego...");

        let stream_result = timeout(conn_timeout, async {
            match &self.config.connection {
                ConnectionConfig::Tcp { host, port } => {
                    let stream = TcpStream::connect((host.as_str(), *port))
                        .await
                        .map_err(SatelError::from)?;
                    let boxed: Box<dyn AsyncReadWrite> = Box::new(stream);
                    Ok::<Box<dyn AsyncReadWrite>, SatelError>(boxed)
                }
                ConnectionConfig::Uart { path, baud_rate } => {
                    let stream = tokio_serial::new(path, *baud_rate)
                        .open_native_async()
                        .map_err(SatelError::from)?;
                    let boxed: Box<dyn AsyncReadWrite> = Box::new(stream);
                    Ok::<Box<dyn AsyncReadWrite>, SatelError>(boxed)
                }
            }
        })
        .await;

        let stream = match stream_result {
            Ok(Ok(s)) => s,
            _ => {
                self.satel_connection_worker_connection_lost().await;
                return Err(SatelError::Timeout);
            }
        };

        self.stream = Some(Framed::new(stream, SatelCodec::default()));
        self.set_state_handshake().await;

        // Handshake
        sleep(Duration::from_millis(200)).await;

        // 1. Pytamy o wersję (używając priority exchange)
        let cmd_version = vec![SatelCommand::IntegraVersion.to_byte()];
        match self.satel_connection_worker_exchange(cmd_version, 0x7E, conn_timeout, conn_timeout).await {
            Ok(response) => {
                let version = process_integra_version(&response)?;
                let mut s = self.state.write().unwrap();
                s.integra_version = Some(version);
            }
            Err(e) => {
                tracing::warn!("Handshake: błąd podczas pobierania wersji: {:?}", e);
                self.satel_connection_worker_connection_lost().await;
                return Err(e);
            }
        }

        // 2. Jeśli autoodczyt jest włączony, konfigurujemy Push
        if self.config.is_auto_read_enabled() {
            let mut auto_push_data = vec![SatelCommand::ListOfNewData.to_byte()];
            let mut mask_on_change = [0u8; 6];
            if self.config.auto_read_zones_violation { mask_on_change[0] |= 1 << 0; }
            if self.config.auto_read_zones_tamper { mask_on_change[0] |= 1 << 1; }
            if self.config.auto_read_zones_alarm { mask_on_change[0] |= 1 << 2; }
            if self.config.auto_read_zones_tamper_alarm { mask_on_change[0] |= 1 << 3; }
            if self.config.auto_read_zones_alarm_memory { mask_on_change[0] |= 1 << 4; }
            if self.config.auto_read_zones_tamper_alarm_memory { mask_on_change[0] |= 1 << 5; }
            if self.config.auto_read_zones_bypass { mask_on_change[0] |= 1 << 6; }
            if self.config.auto_read_zones_no_violation_trouble { mask_on_change[0] |= 1 << 7; }
            if self.config.auto_read_zones_long_violation_trouble { mask_on_change[1] |= 1 << 0; }
            if self.config.auto_read_partitions_armed_suppressed { mask_on_change[1] |= 1 << 1; }
            if self.config.auto_read_partitions_armed_really { mask_on_change[1] |= 1 << 2; }
            if self.config.auto_read_partitions_alarm { mask_on_change[2] |= 1 << 3; }
            if self.config.auto_read_partitions_alarm_memory { mask_on_change[2] |= 1 << 5; }
            if self.config.auto_read_partitions_entry_time { mask_on_change[1] |= 1 << 6; } // 0x0E
            if self.config.auto_read_partitions_exit_time {
                mask_on_change[1] |= 1 << 7; // 0x0F
                mask_on_change[2] |= 1 << 0; // 0x10
            }
            if self.config.auto_read_outputs_state {
                mask_on_change[2] |= 1 << 7; // 0x17
            }
            auto_push_data.extend_from_slice(&mask_on_change);
            auto_push_data.extend_from_slice(&[0x00; 6]);

            match self.satel_connection_worker_exchange(auto_push_data, 0x7F, conn_timeout, conn_timeout).await {
                Ok(_) => { tracing::info!("Handshake: konfiguracja Push zakończona"); }
                Err(e) => {
                    tracing::warn!("Handshake: błąd podczas konfiguracji Push: {:?}", e);
                    self.satel_connection_worker_connection_lost().await;
                    return Err(e);
                }
            }
        }

        self.set_state_connected().await;
        tracing::info!("Połączenie i Handshake zakończone pomyślnie");
        Ok(())
    }

    fn calculate_backoff(&self) -> Duration {
        let attempts = self.state.read().unwrap().telemetry.status.failed_attempts;
        if attempts == 0 { return Duration::from_millis(500); }
        
        let ms = 500 * (2u64.pow(attempts.saturating_sub(1).min(7))); // 500, 1000, 2000, 4000, 8000, 16000, 32000, 64000
        Duration::from_millis(ms.min(60000))
    }

    async fn notify_state_worker(
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
                    s.telemetry
                        .bytes_received
                        .fetch_add(frame.len(), Ordering::Relaxed);
                }

                // Nie przekazujemy dalej technicznego potwierdzenia [EF, FF] jako Push
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

fn calculate_crc(data: &[u8]) -> u16 {
    let mut crc: u16 = 0x147A;
    for &byte in data {
        crc = crc.rotate_left(1);
        crc ^= 0xFFFF;
        crc = crc.wrapping_add((crc >> 8) as u16);
        crc = crc.wrapping_add(byte as u16);
    }
    crc
}

#[derive(Default)]
pub struct SatelCodec;

impl Decoder for SatelCodec {
    type Item = Vec<u8>;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            if src.len() < 2 {
                return Ok(None);
            }
            if let Some(pos) = src.windows(2).position(|w| w == [0xFE, 0xFE]) {
                if pos > 0 {
                    src.advance(pos);
                }
                break;
            } else {
                let to_advance = if src.last() == Some(&0xFE) {
                    src.len() - 1
                } else {
                    src.len()
                };
                src.advance(to_advance);
                return Ok(None);
            }
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
