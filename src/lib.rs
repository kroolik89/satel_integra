//! Biblioteka do komunikacji z centralami alarmowymi Satel Integra
//! poprzez protokół ETHM-1 Plus lub UART.
//!
//! # Przykład użycia
//!
//! ```no_run
//! use satel_integra::{SatelIntegra, Config, ConnectionConfig};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = Config {
//!         connection: ConnectionConfig::Tcp {
//!             host: "192.168.1.100".to_string(),
//!             port: 7094,
//!         },
//!         ..Default::default()
//!     };
//!
//!     let satel = SatelIntegra::new(config);
//!     satel.connect().await?;
//!
//!     let mut events = satel.subscribe();
//!     while let Ok(event) = events.recv().await {
//!         println!("{:?}", event);
//!     }
//!
//!     Ok(())
//! }
//! ```

// --- Moduły wewnętrzne (ukryte) ---
pub(crate) mod auto_requester;
pub(crate) mod client_internal;
pub(crate) mod worker;

// --- Moduły publiczne ---
pub mod client;
pub mod codec;
pub mod command;
pub mod config;
pub mod error;
pub mod event;
pub mod parsers;
pub mod state;



// --- Re-eksport najważniejszych publicznych typów ---
pub use client::SatelIntegra;
pub use codec::SatelCodec;
pub use command::{SatelCommand, SatelResult};
pub use config::{Config, ConnectionConfig};
pub use error::SatelError;
pub use event::SatelEvent;
pub use state::{
    AutoReadItemState, AutoReadItemStatus, AutoReadReport, ConnectionState, ConnectionStatus,
    ConnectionTelemetry, ConnectionType, EthmCapabilities, EthmVersion, IntegraVersion,
    Output, OutputName, Partition, PartitionName, SatelState, SatelStateHandle, SystemStatus,
    TroubleType, Zone, ZoneName, ZoneStatus, ZoneTemperature,
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

                let file = meta.file().unwrap_or("unknown");
                let file_short = std::path::Path::new(file)
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or(file);

                let target = meta.module_path().unwrap_or(meta.target());

                write!(
                    writer,
                    "{} | {:5} | {} | {} |\n",
                    now.format("%H:%M:%S:%3f %d.%m.%Y"),
                    meta.level().to_string(),
                    file_short,
                    target,
                )?;

                write!(writer, "--> ")?;
                _ctx.format_fields(writer.by_ref(), event)?;
                writeln!(writer)
            }
        }

        use tracing::Level;
        let _ = tracing_subscriber::fmt()
            .event_format(CustomFormatter)
            .with_max_level(Level::WARN)
            .try_init();
    });
}
