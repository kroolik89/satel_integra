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
}
