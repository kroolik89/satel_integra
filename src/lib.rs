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
    Config, ConnectionConfig, ConnectionStatus, ConnectionTelemetry, ConnectionType, SatelCommand,
    SatelState, SatelStateHandle,
};

use std::sync::Once;

static LOG_INIT: Once = Once::new();

/// Inicjalizuje system logowania w formacie dostosowanym do biblioteki Satel Integra.
/// Wywoływana automatycznie przy tworzeniu obiektu `SatelIntegra`.
pub fn init_logging() {
    LOG_INIT.call_once(|| {
        use tracing_subscriber::fmt::format::Writer;
        use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
        use tracing::Event;
        use core::fmt;

        struct CustomFormatter;

        impl<S, N> FormatEvent<S, N> for CustomFormatter
        where
            S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
            N: for<'a> FormatFields<'a> + 'static,
        {
            fn format_event(
                &self,
                _ctx: &FmtContext<'_, S, N>,
                mut writer: Writer<'_>,
                event: &Event<'_>,
            ) -> fmt::Result {
                let now = chrono::Local::now();
                let meta = event.metadata();

                // Skracanie nazwy pliku
                let file = meta.file().unwrap_or("unknown");
                let file_short = std::path::Path::new(file)
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or(file);

                // Wyciąganie nazwy funkcji (jeśli dostępna przez instrument) lub modułu
                let target = meta.module_path().unwrap_or(meta.target());
                
                // Linia 1: [CZAS] | [LEVEL] | [PLIK] | [FUNKCJA] |
                write!(
                    writer,
                    "{} | {:5} | {} | {} |\n",
                    now.format("%H:%M:%S:%3f %d.%m.%Y"),
                    meta.level().to_string(),
                    file_short,
                    target,
                )?;

                // Linia 2: --> [TREŚĆ]
                write!(writer, "--> ")?;
                _ctx.format_fields(writer.by_ref(), event)?;
                writeln!(writer)
            }
        }

        use tracing::Level;
        tracing_subscriber::fmt()
            .event_format(CustomFormatter)
            .with_max_level(Level::WARN)
            .init();
    });
}
