//! Biblioteka do komunikacji z centralami alarmowymi Satel Integra
//! poprzez protokół ETHM-1 Plus lub UART.
//!
//! # Przykład użycia
//!
//! ```no_run
//! // TODO: Zaktualizuj przykład, aby używał nowej struktury Config
//! ```

// Deklaracja modułów
pub mod satel_integra;
pub mod satel_integra_data;
pub mod satel_integra_process;

// Re-eksport najważniejszych publicznych typów dla wygody użytkownika biblioteki.
pub use satel_integra::{SatelError, SatelIntegra};
pub use satel_integra_data::{
    Config, ConnectionConfig, ConnectionMetrics, ConnectionStatus, ConnectionType, SatelCommand,
    SatelState, SatelStateHandle,
};
