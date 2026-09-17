//! Asynchronous Rust client for Satel Integra alarm control panels
//! communicating via ETHM-1 Plus (TCP/IP) or UART (RS-232).
//!
//! # Quick Example
//!
//! ```no_run
//! use satel_integra::{SatelIntegra, Config, ConnectionConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
//!     let mut events = satel.subscribe_events();
//!     while let Ok(event) = events.recv().await {
//!         println!("{:?}", event);
//!     }
//!
//!     Ok(())
//! }
//! ```

// --- Internal modules (private) ---
pub(crate) mod auto_requester;
pub(crate) mod client_internal;
pub(crate) mod encryption;
pub(crate) mod polling_worker;
pub(crate) mod worker;

// --- Public modules ---
pub mod client;
pub mod codec;
pub mod command;
pub mod config;
pub mod error;
pub mod event;
pub mod parsers;
pub mod state;
pub mod trouble_catalog;

// --- Re-exports of primary public types ---
pub use client::SatelIntegra;
pub use codec::SatelCodec;
pub use command::{SatelCommand, SatelResult};
pub use config::{Config, ConnectionConfig, TemperatureProbe, MIN_UNBLOCK_AFTER_CYCLES};
pub use error::SatelError;
pub use event::{SatelEvent, SyncCategory};
pub use state::{
    AutoReadItemState, AutoReadItemStatus, AutoReadReport, ConnectionState, ConnectionStatistics,
    ConnectionStatus, ConnectionTelemetry, ConnectionType, EthmCapabilities, EthmPtsaTroubles,
    EthmVersion, GsmModuleTroubles, IntegraVersion, MainBoardTroubles, Output, OutputName,
    Partition, PartitionName, SatelState, SatelStateHandle, SystemStatus, TemperatureSensorStatus,
    TroubleType, TroublesData, TroublesPart1Data, TroublesPart2Data, TroublesPart3Data,
    TroublesPart4Data, TroublesPart5Data, TroublesPart6Data, TroublesPart7Data, TroublesPart8Data,
    Zone, ZoneName, ZoneStatus, ZoneTemperature,
};
pub use trouble_catalog::{TroubleAddress, TroubleAddressing, TroubleDescriptor, TroubleDomain};

use std::sync::Once;

static LOG_INIT: Once = Once::new();

/// Initializes the tracing subscriber with a custom formatted output tailored for Satel Integra.
/// Invoked automatically upon creating a `SatelIntegra` instance.
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

                writeln!(
                    writer,
                    "{} | {:5} | {} | {} |",
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
