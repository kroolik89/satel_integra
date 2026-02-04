use crate::satel_integra_data::{
    Config, ConnectionConfig, ConnectionStatus, ConnectionType, SatelState, SatelStateHandle,
};
use bytes::{Buf, BytesMut};
use futures::{SinkExt, StreamExt};
use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_serial::{SerialPortBuilderExt};
use tokio_util::codec::{Decoder, Encoder, Framed};

/// Trait pomocniczy, łączący AsyncRead i AsyncWrite, aby ułatwić tworzenie trait object.
trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncReadWrite for T {}

/// Typ strumienia I/O, opakowany w `Framed` z naszym kodekiem.
type FramedStream = Framed<Box<dyn AsyncReadWrite>, SatelCodec>;

/// Główna struktura do zarządzania połączeniem z centralą.
pub struct SatelIntegra {
    config: Config,
    state: SatelStateHandle,
    stream: Option<FramedStream>,
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
}

/// Implementacja logiki `SatelIntegra`.
impl SatelIntegra {
    /// Tworzy nową instancję `SatelIntegra` z podaną konfiguracją.
    pub fn new(config: Config) -> Self {
        Self {
            config,
            state: Arc::new(std::sync::RwLock::new(SatelState::new())),
            stream: None,
        }
    }

    /// Zwraca uchwyt do współdzielonego stanu.
    pub fn state_handle(&self) -> SatelStateHandle {
        self.state.clone()
    }

    /// Inicjalizuje połączenie na podstawie konfiguracji przekazanej w konstruktorze.
    pub async fn connect(&mut self) -> Result<(), SatelError> {
        // Użyj timeoutu na połączenie z konfiguracji
        let conn_timeout = Duration::from_secs(self.config.read_timeout);

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
        }).await;

        let stream: Box<dyn AsyncReadWrite> = match stream_result {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(SatelError::Timeout),
        };
        
        let framed = Framed::new(stream, SatelCodec::default());
        self.stream = Some(framed);

        let mut state = self.state.write().unwrap();
        state.status = ConnectionStatus::Connected;
        
        // Zapisz typ połączenia do stanu
        let conn_type = match &self.config.connection {
            ConnectionConfig::Tcp { host, port } => ConnectionType::Tcp(host.clone(), *port),
            ConnectionConfig::Uart { path, .. } => ConnectionType::Uart(path.clone()),
        };
        state.connection_type = Some(conn_type);
        state.metrics.reset();

        Ok(())
    }

    /// Wysyła dane do centrali z opcjonalnym, specyficznym czasem oczekiwania.
    pub async fn send(&mut self, data: Vec<u8>, timeout_duration: Option<Duration>) -> Result<(), SatelError> {
        let op_timeout = timeout_duration.unwrap_or_else(|| Duration::from_secs(self.config.write_timeout));
        self.check_connection_lost(op_timeout).await?;

        let stream = self.stream.as_mut().ok_or(SatelError::NotConnected)?;
        let data_len = data.len();

        match timeout(op_timeout, stream.send(data)).await {
            Ok(Ok(_)) => {
                self.state.read().unwrap().metrics.bytes_sent.fetch_add(data_len, Ordering::Relaxed);
                Ok(())
            }
            Ok(Err(e)) => {
                self.set_connection_lost();
                Err(SatelError::Io(e))
            }
            Err(_) => Err(SatelError::Timeout),
        }
    }

    /// Odbiera dane z centrali z opcjonalnym, specyficznym czasem oczekiwania.
    pub async fn receive(&mut self, timeout_duration: Option<Duration>) -> Result<Vec<u8>, SatelError> {
        let op_timeout = timeout_duration.unwrap_or_else(|| Duration::from_secs(self.config.read_timeout));
        self.check_connection_lost(op_timeout).await?;

        let stream = self.stream.as_mut().ok_or(SatelError::NotConnected)?;

        match timeout(op_timeout, stream.next()).await {
            Ok(Some(Ok(frame))) => {
                let frame_len = frame.len();
                self.state.read().unwrap().metrics.bytes_received.fetch_add(frame_len, Ordering::Relaxed);
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
            Err(_) => Err(SatelError::Timeout),
        }
    }

    /// Wykonuje operację `send`, a następnie `receive`.
    pub async fn exchange(&mut self, data: Vec<u8>, write_timeout_duration: Option<Duration>, read_timeout_duration: Option<Duration>) -> Result<Vec<u8>, SatelError> {
        self.send(data, write_timeout_duration).await?;
        self.receive(read_timeout_duration).await
    }
    
    /// Sprawdza, czy połączenie nie zostało utracone.
    async fn check_connection_lost(&self, _wait_duration: Duration) -> Result<(), SatelError> {
        let state = self.state.read().unwrap();
        if state.status == ConnectionStatus::ConnectionLost {
            return Err(SatelError::ConnectionLost);
        }
        Ok(())
    }

    /// Ustawia stan na `ConnectionLost`.
    fn set_connection_lost(&mut self) {
        self.stream = None;
        let mut state = self.state.write().unwrap();
        if state.status == ConnectionStatus::Connected {
            state.status = ConnectionStatus::ConnectionLost;
        }
    }
}


// --- Koder/Dekoder (bez zmian) ---

/// Oblicza 16-bitową sumę kontrolną.
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
        while let Some(pos) = src.iter().position(|&b| b == 0xFE) {
            if src.get(pos + 1) == Some(&0xFE) {
                src.advance(pos);
                break;
            } else {
                src.advance(pos + 1);
            }
        }

        if let Some(pos) = src.windows(2).position(|window| window == [0xFE, 0x0D]) {
            let frame_with_header_and_footer = src.split_to(pos + 2).to_vec();
            
            let mut data_with_crc = Vec::new();
            let mut i = 2;
            while i < frame_with_header_and_footer.len() - 2 {
                if frame_with_header_and_footer[i] == 0xFE && frame_with_header_and_footer.get(i + 1) == Some(&0xF0) {
                    data_with_crc.push(0xFE);
                    i += 2;
                } else {
                    data_with_crc.push(frame_with_header_and_footer[i]);
                    i += 1;
                }
            }
            
            if data_with_crc.len() < 2 {
                 return Ok(None);
            }

            let received_crc_low = data_with_crc.pop().unwrap();
            let received_crc_high = data_with_crc.pop().unwrap();
            let received_crc = u16::from_be_bytes([received_crc_high, received_crc_low]);
            
            let calculated_crc = calculate_crc(&data_with_crc);

            if received_crc == calculated_crc {
                return Ok(Some(data_with_crc));
            } else {
                eprintln!("Invalid CRC. Got: {:04X}, calculated: {:04X}", received_crc, calculated_crc);
            }
        }
        
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