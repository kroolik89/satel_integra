use std::io;
use thiserror::Error;

/// Błędy, które mogą wystąpić podczas komunikacji z centralą Satel Integra.
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
