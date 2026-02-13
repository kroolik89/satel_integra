use crate::satel_integra_data::{
    Config, ConnectionConfig, ConnectionStatus, ConnectionType, SatelState, SatelStateHandle,
};
use bytes::{Buf, BytesMut};
use futures::{SinkExt, StreamExt};
use std::io;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, timeout};
use tokio_serial::SerialPortBuilderExt;
use tokio_util::codec::{Decoder, Encoder, Framed};

/// Trait pomocniczy, łączący AsyncRead i AsyncWrite, aby ułatwić tworzenie trait object.
trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncReadWrite for T {}

/// Typ strumienia I/O, opakowany w `Framed` z naszym kodekiem.
type FramedStream = Framed<Box<dyn AsyncReadWrite>, SatelCodec>;

/// Wewnętrzna wiadomość przesyłana między klientem a workerem.
enum InternalMessage {
    Exchange {
        data: Vec<u8>,
        write_timeout: Duration,
        read_timeout: Duration,
        created_at: Instant,
        max_queue_time: Duration,
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
    worker: Arc<Mutex<Option<SatelWorker>>>,
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
}

impl SatelIntegra {
    /// Tworzy nową instancję `SatelIntegra`.
    pub fn new(config: Config) -> Self {
        crate::init_logging();
        let state = Arc::new(std::sync::RwLock::new(SatelState::new()));
        let (tx, rx) = mpsc::channel(100);

        let worker = SatelWorker {
            config: config.clone(),
            state: state.clone(),
            rx,
            stream: None,
            consecutive_failures: 0,
        };

        Self {
            tx,
            state,
            config,
            worker: Arc::new(Mutex::new(Some(worker))),
        }
    }

    /// Uruchamia połączenie i workera w tle.
    /// Jeśli worker już działa, próbuje wymusić ponowne połączenie.
    pub async fn connect(&self) -> Result<(), SatelError> {
        let (tx, rx) = {
            let mut worker_lock = self.worker.lock().unwrap();
            if let Some(mut worker) = worker_lock.take() {
                let (tx, rx) = oneshot::channel();
                tokio::spawn(async move {
                    worker.run_with_initial_connect(tx).await;
                });
                return rx.await.map_err(|_| SatelError::WorkerDropped)?;
            }
            // Jeśli worker został już zabrany, oznacza to że pętla działa
            let (tx, rx) = oneshot::channel();
            (tx, rx)
        };

        self.tx
            .send(InternalMessage::Connect { response_tx: tx })
            .await
            .map_err(|_| SatelError::WorkerDropped)?;

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

    /// Wykonuje operację wymiany danych (wyślij i odbierz).
    pub async fn exchange(
        &self,
        data: Vec<u8>,
        write_timeout: Option<Duration>,
        read_timeout: Option<Duration>,
    ) -> Result<Vec<u8>, SatelError> {
        {
            let state = self.state.read().unwrap();
            let status = state.telemetry.status;
            let can_exchange = status == ConnectionStatus::Connected || 
                (status == ConnectionStatus::ConnectionLost && self.config.auto_reconnect);
            
            if !can_exchange {
                return Err(match status {
                    ConnectionStatus::Disconnected => SatelError::NotConnected,
                    ConnectionStatus::ConnectionLost => SatelError::ConnectionLost,
                    ConnectionStatus::Connected => unreachable!(),
                });
            }
        }

        let (response_tx, response_rx) = oneshot::channel();

        let msg = InternalMessage::Exchange {
            data,
            write_timeout: write_timeout.unwrap_or(Duration::from_secs(self.config.write_timeout)),
            read_timeout: read_timeout.unwrap_or(Duration::from_secs(self.config.read_timeout)),
            created_at: Instant::now(),
            max_queue_time: Duration::from_secs(30), // Domyślny TTL wiadomości w kolejce
            response_tx,
        };

        self.tx
            .send(msg)
            .await
            .map_err(|_| SatelError::WorkerDropped)?;

        response_rx.await.map_err(|_| SatelError::WorkerDropped)?
    }
}

/// Worker zarządzający fizycznym połączeniem w tle.
pub struct SatelWorker {
    config: Config,
    state: SatelStateHandle,
    rx: mpsc::Receiver<InternalMessage>,
    stream: Option<FramedStream>,
    consecutive_failures: usize,
}

impl SatelWorker {
    /// Główna pętla workera z obsługą początkowego połączenia.
    async fn run_with_initial_connect(&mut self, initial_response_tx: oneshot::Sender<Result<(), SatelError>>) {
        let result = self.connect().await;
        let _ = initial_response_tx.send(result);
        self.run().await;
    }

    /// Główna pętla workera.
    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                // Obsługa przychodzących komunikatów
                Some(msg) = self.rx.recv() => {
                    self.handle_message(msg).await;
                }
                
                // Pasywne monitorowanie i próby połączenia w tle
                _ = sleep(Duration::from_millis(100)), if self.stream.is_none() && self.config.auto_reconnect => {
                     if self.should_retry_connection() {
                         let _ = self.ensure_connected().await;
                     }
                }
            }
        }
    }

    fn should_retry_connection(&self) -> bool {
        let state = self.state.read().unwrap();
        state.telemetry.status == ConnectionStatus::ConnectionLost
    }

    async fn handle_message(&mut self, msg: InternalMessage) {
        match msg {
            InternalMessage::Exchange {
                data,
                write_timeout,
                read_timeout,
                created_at,
                max_queue_time,
                response_tx,
            } => {
                // 1. Sprawdź TTL wiadomości
                if created_at.elapsed() > max_queue_time {
                    tracing::info!("Odrzucono wiadomość z kolejki: przekroczono czas TTL ({:?})", max_queue_time);
                    let _ = response_tx.send(Err(SatelError::MessageExpired));
                    return;
                }

                // 2. Zapewnij połączenie
                if let Err(e) = self.ensure_connected().await {
                    let _ = response_tx.send(Err(e));
                    return;
                }

                // 3. Wykonaj exchange
                let result = self.perform_exchange(data, write_timeout, read_timeout).await;
                let _ = response_tx.send(result);
            }
            InternalMessage::Connect { response_tx } => {
                let status = self.state.read().unwrap().telemetry.status;
                if status == ConnectionStatus::Connected {
                    let _ = response_tx.send(Err(SatelError::AlreadyConnected));
                } else {
                    let result = self.connect().await;
                    let _ = response_tx.send(result);
                }
            }
            InternalMessage::Disconnect { response_tx } => {
                let status = self.state.read().unwrap().telemetry.status;
                if status == ConnectionStatus::Disconnected {
                    let _ = response_tx.send(Err(SatelError::NotConnected));
                } else {
                    self.stream = None;
                    let mut state = self.state.write().unwrap();
                    state.telemetry.status = ConnectionStatus::Disconnected;
                    let _ = response_tx.send(Ok(()));
                }
            }
        }
    }

    async fn perform_exchange(
        &mut self,
        data: Vec<u8>,
        write_timeout: Duration,
        read_timeout: Duration,
    ) -> Result<Vec<u8>, SatelError> {
        let stream = self.stream.as_mut().ok_or(SatelError::NotConnected)?;

        // Send
        match timeout(write_timeout, stream.send(data.clone())).await {
            Ok(Ok(_)) => {
                self.state
                    .read()
                    .unwrap()
                    .telemetry
                    .bytes_sent
                    .fetch_add(data.len(), Ordering::Relaxed);
            }
            Ok(Err(e)) => {
                self.set_connection_lost();
                return Err(SatelError::Io(e));
            }
            Err(_) => {
                tracing::info!("Przekroczono czas oczekiwania na wysłanie danych (timeout: {:?})", write_timeout);
                return Err(SatelError::Timeout);
            }
        }

        // Receive
        match timeout(read_timeout, stream.next()).await {
            Ok(Some(Ok(frame))) => {
                self.state
                    .read()
                    .unwrap()
                    .telemetry
                    .bytes_received
                    .fetch_add(frame.len(), Ordering::Relaxed);
                Ok(frame)
            }
            Ok(Some(Err(e))) => {
                self.set_connection_lost();
                Err(SatelError::Io(e))
            }
            Ok(None) => {
                self.set_connection_lost();
                Err(SatelError::StreamClosed)
            }
            Err(_) => {
                tracing::info!("Przekroczono czas oczekiwania na odpowiedź z centrali (timeout: {:?})", read_timeout);
                Err(SatelError::Timeout)
            }
        }
    }

    async fn connect(&mut self) -> Result<(), SatelError> {
        let conn_timeout = Duration::from_secs(self.config.read_timeout);

        tracing::info!("Podejmowanie próby połączenia z centralą...");

        let stream_result = timeout(conn_timeout, async {
            match &self.config.connection {
                ConnectionConfig::Tcp { host, port } => {
                    let stream: Box<dyn AsyncReadWrite> =
                        Box::new(TcpStream::connect((host.as_str(), *port)).await?);
                    Ok(stream)
                }
                ConnectionConfig::Uart { path, baud_rate } => {
                    let stream: Box<dyn AsyncReadWrite> =
                        Box::new(tokio_serial::new(path, *baud_rate).open_native_async()?);
                    Ok(stream)
                }
            }
        })
        .await;

        let stream: Box<dyn AsyncReadWrite> = match stream_result {
            Ok(Ok(s)) => {
                self.consecutive_failures = 0;
                s
            }
            Ok(Err(e)) => {
                self.consecutive_failures += 1;
                let mut state = self.state.write().unwrap();
                state.telemetry.status = if self.config.auto_reconnect {
                    ConnectionStatus::ConnectionLost
                } else {
                    ConnectionStatus::Disconnected
                };
                let err = SatelError::Io(e);
                tracing::error!("Błąd podczas łączenia się z centralą: {}", err);
                return Err(err);
            }
            Err(_) => {
                self.consecutive_failures += 1;
                let mut state = self.state.write().unwrap();
                state.telemetry.status = if self.config.auto_reconnect {
                    ConnectionStatus::ConnectionLost
                } else {
                    ConnectionStatus::Disconnected
                };
                tracing::error!("Przekroczono czas oczekiwania na połączenie z centralą (timeout: {:?})", conn_timeout);
                return Err(SatelError::Timeout);
            }
        };

        let framed = Framed::new(stream, SatelCodec::default());
        self.stream = Some(framed);

        let mut state = self.state.write().unwrap();
        state.telemetry.status = ConnectionStatus::Connected;
        state.telemetry.last_connected_at = Some(SystemTime::now());

        let conn_type = match &self.config.connection {
            ConnectionConfig::Tcp { host, port } => ConnectionType::Tcp(host.clone(), *port),
            ConnectionConfig::Uart { path, .. } => ConnectionType::Uart(path.clone()),
        };
        state.connection_type = Some(conn_type);
        state.telemetry.bytes_sent.store(0, Ordering::Relaxed);
        state.telemetry.bytes_received.store(0, Ordering::Relaxed);

        tracing::info!("Połączono pomyślnie!");
        Ok(())
    }

    async fn ensure_connected(&mut self) -> Result<(), SatelError> {
        if self.stream.is_some() {
            return Ok(());
        }

        tracing::info!("Wykryto brak aktywnego połączenia. Próba nawiązania...");

        {
            let state = self.state.read().unwrap();
            if state.telemetry.status == ConnectionStatus::Disconnected {
                return Err(SatelError::NotConnected);
            }
        }

        if !self.config.auto_reconnect && self.consecutive_failures > 0 {
             return Err(SatelError::NotConnected);
        }

        if self.consecutive_failures > 0 {
            let delay = self.calculate_backoff();
            sleep(delay).await;
        }

        if let Ok(state) = self.state.read() {
            state.telemetry.reconnect_count.fetch_add(1, Ordering::Relaxed);
        }

        self.connect().await
    }

    fn calculate_backoff(&self) -> Duration {
        if self.consecutive_failures <= 10 {
            Duration::from_millis(500)
        } else {
            let additional_ms = (self.consecutive_failures as u64 - 10) * 500;
            let total_ms = 500 + additional_ms;
            let max_ms = 120 * 1000;
            Duration::from_millis(total_ms.min(max_ms))
        }
    }

    fn set_connection_lost(&mut self) {
        self.stream = None;
        let mut state = self.state.write().unwrap();
        if state.telemetry.status == ConnectionStatus::Connected {
            state.telemetry.status = ConnectionStatus::ConnectionLost;
        }
    }
}

// --- Koder/Dekoder (bez zmian) ---

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
        // 1. Synchronizacja: Szukamy początku ramki [FE, FE]
        loop {
            if src.len() < 2 {
                return Ok(None);
            }
            if let Some(pos) = src.windows(2).position(|w| w == [0xFE, 0xFE]) {
                if pos > 0 {
                    src.advance(pos); // Usuwamy wszystko przed FE FE
                }
                break; // Znaleźliśmy początek
            } else {
                // Nie ma FE FE w całym buforze. 
                // Zostawiamy tylko ostatni bajt (jeśli to 0xFE), resztę usuwamy.
                let to_advance = if src.last() == Some(&0xFE) {
                    src.len() - 1
                } else {
                    src.len()
                };
                src.advance(to_advance);
                return Ok(None);
            }
        }

        // 2. Szukamy końca ramki [FE, 0D]
        if let Some(pos) = src.windows(2).position(|window| window == [0xFE, 0x0D]) {
            // Wycinamy całą ramkę (z nagłówkiem i stopką) z głównego bufora
            let frame_raw = src.split_to(pos + 2).to_vec();

            // 3. Unescape i wyciąganie danych (pomiń FE FE na początku i FE 0D na końcu)
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
                // Ramka zbyt krótka nawet na CRC, szukamy dalej (ramka już usunięta z src przez split_to)
                return self.decode(src);
            }

            // 4. Weryfikacja CRC
            let received_crc_low = data_with_crc.pop().unwrap();
            let received_crc_high = data_with_crc.pop().unwrap();
            let received_crc = u16::from_be_bytes([received_crc_high, received_crc_low]);

            let calculated_crc = calculate_crc(&data_with_crc);

            if received_crc == calculated_crc {
                return Ok(Some(data_with_crc));
            } else {
                tracing::warn!(
                    "Invalid CRC. Got: {:04X}, calculated: {:04X}. Frame discarded.",
                    received_crc, calculated_crc
                );
                // CRC błędne, ale ramka już skonsumowana. Szukamy kolejnej w reszcie bufora.
                return self.decode(src);
            }
        }

        // Nie znaleźliśmy jeszcze końca ramki
        Ok(None)
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
        let crc_bytes = crc.to_be_bytes();

        for &byte in &crc_bytes {
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
