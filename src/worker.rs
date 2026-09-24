use crate::codec::SatelCodec;
use crate::command::SatelCommand;
use crate::config::{Config, ConnectionConfig};
use crate::counting_stream::CountingStream;
use crate::error::SatelError;
use crate::parsers::{process_auto_read_response, process_ethm_version, process_integra_version};
use crate::state::{ConnectionState, ConnectionType, SatelStateHandle};
use chrono::Local;
use futures::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicBool, Ordering};
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
    pub config: std::sync::Arc<std::sync::RwLock<crate::config::Config>>,
    pub state: SatelStateHandle,
    pub rx: mpsc::Receiver<InternalMessage>,
    pub stream: Option<FramedStream>,
    pub state_worker_tx: Option<mpsc::Sender<StateWorkerMessage>>,
    pub auto_read_dirty: std::sync::Arc<AtomicBool>,
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
            let should_reconnect = self.config.read().unwrap().auto_reconnect;

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
                    let (current_state, last_event) = {
                        let s = self.state.read().unwrap();
                        (s.telemetry.status.state, s.telemetry.status.last_event_at)
                    };

                    // 1. Watchdog for Connecting / Handshake
                    if (current_state == ConnectionState::Connecting || current_state == ConnectionState::Handshake)
                        && last_event.elapsed() > Duration::from_secs(15) {
                        tracing::warn!("Watchdog: Connection/handshake timeout exceeded (15s)");
                        self.satel_connection_worker_connection_lost().await;
                    }

                    // 1b. Re-send 0x7F push mask when auto_read config changed
                    if current_state == ConnectionState::Connected
                        && self.auto_read_dirty.swap(false, Ordering::SeqCst)
                    {
                        let conn_timeout = Duration::from_millis(self.config.read().unwrap().read_timeout_ms);
                        if let Err(e) = self.satel_connection_worker_send_push_mask(conn_timeout).await {
                            tracing::warn!("Error re-sending push notification configuration (0x7F): {:?}", e);
                            self.auto_read_dirty.store(true, Ordering::SeqCst);
                        }
                    }

                    // 2. Ping keep-alive
                    let last_send = self.state.read().unwrap().telemetry.last_send_at;
                    if current_state == ConnectionState::Connected && last_send.elapsed() >= Duration::from_secs(2) {
                        let cmd = vec![SatelCommand::IntegraVersion.to_byte()];
                        let _ = self.satel_connection_worker_exchange(cmd, 0x7E, Duration::from_millis(500), Duration::from_millis(500), true).await;
                    }

                    // 3. Automatic reconnect with exponential backoff
                    if should_reconnect
                        && current_state == ConnectionState::ConnectionLost
                        && last_event.elapsed() >= self.calculate_backoff()
                    {
                        tracing::info!("Auto-reconnect: Attempting reconnection...");
                        if let Ok(s) = self.state.read() {
                            s.telemetry.reconnect_attempts.fetch_add(1, Ordering::Relaxed);
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
                    .satel_connection_worker_exchange(data, cmd_byte, write_timeout, read_timeout, false)
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
                    .satel_connection_worker_exchange(data, cmd_byte, write_timeout, read_timeout, false)
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
        is_ping: bool,
    ) -> Result<Vec<u8>, SatelError> {
        let stream = self.stream.as_mut().ok_or(SatelError::NotConnected)?;

        // Send
        match timeout(write_timeout, stream.send(data.clone())).await {
            Ok(Ok(_)) => {
                let mut s = self.state.write().unwrap();
                if !is_ping {
                    s.telemetry.non_ping_frames.fetch_add(1, Ordering::Relaxed);
                }
                s.telemetry.last_send_at = Instant::now();
            }
            Ok(Err(e)) => {
                self.state.read().unwrap().telemetry.io_errors.fetch_add(1, Ordering::Relaxed);
                self.satel_connection_worker_connection_lost().await;
                return Err(SatelError::Io(e));
            }
            Err(_) => {
                self.state.read().unwrap().telemetry.timeouts.fetch_add(1, Ordering::Relaxed);
                return Err(SatelError::Timeout);
            }
        }

        // Receive with filtering Push notifications
        let start = Instant::now();
        while start.elapsed() < read_timeout {
            let remaining = read_timeout.saturating_sub(start.elapsed());
            let stream = self.stream.as_mut().unwrap();
            match timeout(remaining, stream.next()).await {
                Ok(Some(Ok(frame))) => {
                    if frame.is_empty() {
                        continue;
                    }

                    let is_result_code = frame[0] == 0xEF;
                    let is_accepted = is_result_code && frame.get(1) == Some(&0xFF);

                    if is_result_code && !is_accepted {
                        self.state.read().unwrap().telemetry.rejected_by_panel.fetch_add(1, Ordering::Relaxed);
                    }

                    let is_target_response = frame[0] == expected_cmd || frame[0] == 0xEF;
                    if !is_ping || !is_target_response {
                        self.state.read().unwrap().telemetry.non_ping_frames.fetch_add(1, Ordering::Relaxed);
                    }

                    let is_name_response_ef = expected_cmd == 0xEE && is_result_code;
                    if (frame[0] != expected_cmd || is_result_code) && !is_accepted && !is_name_response_ef {
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
                    self.state.read().unwrap().telemetry.io_errors.fetch_add(1, Ordering::Relaxed);
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
        self.state.read().unwrap().telemetry.timeouts.fetch_add(1, Ordering::Relaxed);
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
            s.telemetry.connections_established.fetch_add(1, Ordering::Relaxed);
            s.telemetry.connected_since = Some(Local::now());
            s.telemetry.status.state = ConnectionState::Connected;
            s.telemetry.status.last_event_at = Instant::now();
            s.telemetry.status.failed_attempts = 0;
            s.telemetry.last_connected_at = Some(SystemTime::now());
            s.connection_type = Some(match &self.config.read().unwrap().connection {
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
            let prev = s.telemetry.status.state;
            if prev == ConnectionState::Connected {
                s.telemetry.connections_lost.fetch_add(1, Ordering::Relaxed);
                let now = Local::now();
                if let Some(since) = s.telemetry.connected_since.take() {
                    let session_dur = (now - since).to_std().unwrap_or(Duration::ZERO);
                    s.telemetry.total_connected_before = s.telemetry.total_connected_before.saturating_add(session_dur);
                }
            }
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
            let now = Local::now();
            if let Some(since) = s.telemetry.connected_since.take() {
                let session_dur = (now - since).to_std().unwrap_or(Duration::ZERO);
                s.telemetry.total_connected_before = s.telemetry.total_connected_before.saturating_add(session_dur);
            }
            s.telemetry.status.state = ConnectionState::Disconnected;
            s.telemetry.status.last_event_at = Instant::now();
            s.telemetry.status.failed_attempts = 0;
        }
        Self::notify_state_worker(&self.state_worker_tx, StateWorkerMessage::StatusChanged(ConnectionState::Disconnected)).await;
    }

    async fn satel_connection_worker_connect(&mut self) -> Result<(), SatelError> {
        self.set_state_connecting().await;

        let conn_timeout = Duration::from_millis(self.config.read().unwrap().read_timeout_ms);

        self.auto_read_dirty.store(false, Ordering::SeqCst);

        // STEP 1: Physical transport connection
        let stream = match self.satel_connection_worker_connect_physical(conn_timeout).await {
            Ok(s) => s,
            Err(e) => {
                self.satel_connection_worker_connection_lost().await;
                return Err(e);
            }
        };

        let crc_errors = {
            let s = self.state.read().unwrap();
            s.telemetry.crc_errors.clone()
        };
        self.stream = Some(Framed::new(stream, SatelCodec::new(crc_errors)));
        self.set_state_handshake().await;

        sleep(Duration::from_millis(200)).await;

        // STEP 2: Protocol Handshake
        self.satel_connection_worker_connect_handshake(conn_timeout).await?;

        // STEP 3: Configure Auto-read push notifications
        if self.config.read().unwrap().is_auto_read_enabled() {
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

        let connection_config = self.config.read().unwrap().connection.clone();
        let encryption = self.config.read().unwrap().encryption;
        let integration_key = self.config.read().unwrap().integration_key.clone();
        let (bytes_sent, bytes_received) = {
            let s = self.state.read().unwrap();
            (
                s.telemetry.bytes_sent.clone(),
                s.telemetry.bytes_received.clone(),
            )
        };

        let stream_result = timeout(conn_timeout, async move {
            match connection_config {
                ConnectionConfig::Tcp { host, port } => {
                    let stream = TcpStream::connect((host.as_str(), port))
                        .await
                        .map_err(SatelError::from)?;
                    let counted = CountingStream::new(stream, bytes_sent, bytes_received);
                    let boxed: Box<dyn AsyncReadWrite> = if encryption {
                        let key_str = integration_key.as_deref().ok_or_else(|| {
                            SatelError::InvalidIntegrationKey(
                                "encryption is enabled but integration_key is not set".into(),
                            )
                        })?;
                        let aes_key = crate::encryption::derive_aes_key(key_str);
                        tracing::info!("TCP connection established with AES-192 encryption");
                        Box::new(crate::encryption::EncryptedStream::new(counted, aes_key))
                    } else {
                        tracing::info!("TCP connection established (plaintext)");
                        Box::new(counted)
                    };
                    Ok::<Box<dyn AsyncReadWrite>, SatelError>(boxed)
                }
                ConnectionConfig::Uart { path, baud_rate } => {
                    let stream = tokio_serial::new(path, baud_rate)
                        .open_native_async()
                        .map_err(SatelError::from)?;
                    let counted = CountingStream::new(stream, bytes_sent, bytes_received);
                    let boxed: Box<dyn AsyncReadWrite> = Box::new(counted);
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
            .satel_connection_worker_exchange(cmd_ethm, 0x7C, conn_timeout, conn_timeout, false)
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
            .satel_connection_worker_exchange(cmd_integra, 0x7E, conn_timeout, conn_timeout, false)
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

    async fn satel_connection_worker_send_push_mask(
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

        let mask = {
            let config = self.config.read().unwrap();
            Self::satel_connection_worker_connect_build_push_mask(&*config, support_14_byte_mask)
        };
        let mut auto_push_data = vec![SatelCommand::ListOfNewData.to_byte()];
        auto_push_data.extend_from_slice(&mask);

        let response = self
            .satel_connection_worker_exchange(auto_push_data, 0x7F, conn_timeout, conn_timeout, false)
            .await?;

        tracing::info!("Push notification configuration (0x7F) successful");
        let report = {
            let config = self.config.read().unwrap();
            process_auto_read_response(&*config, support_14_byte_mask, &response)
        };
        Self::notify_state_worker(&self.state_worker_tx, StateWorkerMessage::AutoReadReport(report)).await;
        Ok(())
    }

    async fn satel_connection_worker_connect_auto_read(
        &mut self,
        conn_timeout: Duration,
    ) -> Result<(), SatelError> {
        match self.satel_connection_worker_send_push_mask(conn_timeout).await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::warn!("Handshake: error during push notification configuration: {:?}", e);
                self.satel_connection_worker_connection_lost().await;
                Err(e)
            }
        }
    }

    pub(crate) fn satel_connection_worker_connect_build_push_mask(config: &Config, support_14_byte_mask: bool) -> Vec<u8> {
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
        let Some(stream) = stream_opt else {
            return Ok(());
        };
        match timeout(Duration::from_millis(100), stream.next()).await {
            Ok(Some(Ok(frame))) => {
                {
                    let s = state.read().map_err(|_| SatelError::StatePoisoned)?;
                    s.telemetry.non_ping_frames.fetch_add(1, Ordering::Relaxed);
                }

                let is_accepted = frame[0] == 0xEF && frame.get(1) == Some(&0xFF);
                if !is_accepted {
                    Self::notify_state_worker(state_worker_tx, StateWorkerMessage::Frame(frame)).await;
                }
                Ok(())
            }
            Ok(Some(Err(e))) => {
                if let Ok(s) = state.read() {
                    s.telemetry.io_errors.fetch_add(1, Ordering::Relaxed);
                }
                Err(SatelError::Io(e))
            }
            Ok(None) => Err(SatelError::StreamClosed),
            Err(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::SatelState;
    use std::sync::{Arc, RwLock};

    #[tokio::test]
    async fn test_disconnect_does_not_reset_counters() {
        let state = Arc::new(RwLock::new(SatelState::new()));
        {
            let mut s = state.write().unwrap();
            s.telemetry.status.state = ConnectionState::Connected;
            s.telemetry.connected_since = Some(Local::now() - chrono::Duration::seconds(5));
            s.telemetry.bytes_sent.store(100, Ordering::Relaxed);
            s.telemetry.bytes_received.store(200, Ordering::Relaxed);
            s.telemetry.connections_established.store(1, Ordering::Relaxed);
            s.telemetry.reconnect_attempts.store(2, Ordering::Relaxed);
            s.telemetry.connections_lost.store(3, Ordering::Relaxed);
            s.telemetry.timeouts.store(4, Ordering::Relaxed);
            s.telemetry.crc_errors.store(5, Ordering::Relaxed);
            s.telemetry.rejected_by_panel.store(6, Ordering::Relaxed);
            s.telemetry.io_errors.store(7, Ordering::Relaxed);
        }

        let (_tx, rx) = mpsc::channel(1);
        let mut worker = SatelCommunicationWorker {
            config: Arc::new(RwLock::new(Config::default())),
            state: state.clone(),
            rx,
            stream: None,
            state_worker_tx: None,
            auto_read_dirty: Arc::new(AtomicBool::new(false)),
        };

        worker.satel_connection_worker_disconnect().await;

        let s = state.read().unwrap();
        assert_eq!(s.telemetry.status.state, ConnectionState::Disconnected);
        assert_eq!(s.telemetry.bytes_sent.load(Ordering::Relaxed), 100);
        assert_eq!(s.telemetry.bytes_received.load(Ordering::Relaxed), 200);
        assert_eq!(s.telemetry.connections_established.load(Ordering::Relaxed), 1);
        assert_eq!(s.telemetry.reconnect_attempts.load(Ordering::Relaxed), 2);
        assert_eq!(s.telemetry.connections_lost.load(Ordering::Relaxed), 3);
        assert_eq!(s.telemetry.timeouts.load(Ordering::Relaxed), 4);
        assert_eq!(s.telemetry.crc_errors.load(Ordering::Relaxed), 5);
        assert_eq!(s.telemetry.rejected_by_panel.load(Ordering::Relaxed), 6);
        assert_eq!(s.telemetry.io_errors.load(Ordering::Relaxed), 7);
        assert_eq!(s.telemetry.connected_since, None);
        assert!(
            s.telemetry.total_connected_before >= Duration::from_secs(4),
            "Expected session to close and add ~5s, got {:?}",
            s.telemetry.total_connected_before
        );
    }

    #[tokio::test]
    async fn test_connections_lost_only_increments_when_previously_connected() {
        let state = Arc::new(RwLock::new(SatelState::new()));
        let (_tx, rx) = mpsc::channel(1);
        let mut worker = SatelCommunicationWorker {
            config: Arc::new(RwLock::new(Config::default())),
            state: state.clone(),
            rx,
            stream: None,
            state_worker_tx: None,
            auto_read_dirty: Arc::new(AtomicBool::new(false)),
        };

        // 1. From Disconnected -> ConnectionLost: connections_lost should NOT increment
        {
            let mut s = state.write().unwrap();
            s.telemetry.status.state = ConnectionState::Disconnected;
        }
        worker.satel_connection_worker_connection_lost().await;
        assert_eq!(state.read().unwrap().telemetry.connections_lost.load(Ordering::Relaxed), 0);

        // 2. From Connecting -> ConnectionLost: connections_lost should NOT increment
        {
            let mut s = state.write().unwrap();
            s.telemetry.status.state = ConnectionState::Connecting;
        }
        worker.satel_connection_worker_connection_lost().await;
        assert_eq!(state.read().unwrap().telemetry.connections_lost.load(Ordering::Relaxed), 0);

        // 3. From Handshake -> ConnectionLost: connections_lost should NOT increment
        {
            let mut s = state.write().unwrap();
            s.telemetry.status.state = ConnectionState::Handshake;
        }
        worker.satel_connection_worker_connection_lost().await;
        assert_eq!(state.read().unwrap().telemetry.connections_lost.load(Ordering::Relaxed), 0);

        // 4. From ConnectionLost -> ConnectionLost: connections_lost should NOT increment
        {
            let mut s = state.write().unwrap();
            s.telemetry.status.state = ConnectionState::ConnectionLost;
        }
        worker.satel_connection_worker_connection_lost().await;
        assert_eq!(state.read().unwrap().telemetry.connections_lost.load(Ordering::Relaxed), 0);

        // 5. From Connected -> ConnectionLost: connections_lost MUST increment by 1 and close session
        {
            let mut s = state.write().unwrap();
            s.telemetry.status.state = ConnectionState::Connected;
            s.telemetry.connected_since = Some(Local::now() - chrono::Duration::seconds(10));
        }
        worker.satel_connection_worker_connection_lost().await;
        assert_eq!(state.read().unwrap().telemetry.connections_lost.load(Ordering::Relaxed), 1);
        assert_eq!(state.read().unwrap().telemetry.connected_since, None);
        assert!(
            state.read().unwrap().telemetry.total_connected_before >= Duration::from_secs(9),
            "Expected session duration around 10s, got {:?}",
            state.read().unwrap().telemetry.total_connected_before
        );
    }

    #[tokio::test]
    async fn test_worker_exchange_name_0xef_not_notified() {
        use futures::{SinkExt, StreamExt};
        use std::sync::atomic::AtomicU64;

        let state = Arc::new(RwLock::new(SatelState::new()));
        let (_tx, rx) = mpsc::channel(1);
        let (state_worker_tx, mut state_worker_rx) = mpsc::channel(10);
        let (client_io, server_io) = tokio::io::duplex(1024);

        let crc1 = Arc::new(AtomicU64::new(0));
        let codec = crate::codec::SatelCodec::new(crc1);
        let framed: tokio_util::codec::Framed<Box<dyn AsyncReadWrite>, _> =
            tokio_util::codec::Framed::new(Box::new(client_io), codec);

        let mut worker = SatelCommunicationWorker {
            config: Arc::new(RwLock::new(Config::default())),
            state: state.clone(),
            rx,
            stream: Some(framed),
            state_worker_tx: Some(state_worker_tx),
            auto_read_dirty: Arc::new(AtomicBool::new(false)),
        };

        // Background task simulating central responses
        tokio::spawn(async move {
            let crc2 = Arc::new(AtomicU64::new(0));
            let mut s_framed = tokio_util::codec::Framed::new(server_io, crate::codec::SatelCodec::new(crc2));
            // 1. Read command 0xEE and reply with [0xEF, 0x08]
            if let Some(Ok(_cmd)) = s_framed.next().await {
                s_framed.send(vec![0xEF, 0x08]).await.unwrap();
            }
            // 2. Read command 0x80 and reply with [0xEF, 0x01]
            if let Some(Ok(_cmd)) = s_framed.next().await {
                s_framed.send(vec![0xEF, 0x01]).await.unwrap();
            }
        });

        // 1. Exchange with expected_cmd = 0xEE -> reply is 0xEF 0x08
        let res = worker.satel_connection_worker_exchange(
            vec![0xEE, 5, 1],
            0xEE,
            Duration::from_secs(1),
            Duration::from_secs(1),
            false,
        ).await.unwrap();
        assert_eq!(res, vec![0xEF, 0x08]);

        // State worker should NOT have received any message for 0xEE + 0xEF!
        assert!(state_worker_rx.try_recv().is_err());

        // 2. Exchange with expected_cmd = 0x80 -> reply is 0xEF 0x01
        let res2 = worker.satel_connection_worker_exchange(
            vec![0x80, 1],
            0x80,
            Duration::from_secs(1),
            Duration::from_secs(1),
            false,
        ).await.unwrap();
        assert_eq!(res2, vec![0xEF, 0x01]);

        // State worker SHOULD receive the frame for 0x80!
        let msg = state_worker_rx.try_recv().expect("Should notify state worker for non-0xEE command");
        assert!(matches!(msg, StateWorkerMessage::Frame(f) if f == vec![0xEF, 0x01]));
    }
}
