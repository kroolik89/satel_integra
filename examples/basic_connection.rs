use satel_integra::satel_integra_data::{Config, ConnectionConfig};
use satel_integra::SatelIntegra;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = "10.20.30.5";
    let port = 7094;

    // Konfiguracja z nowymi timeoutami w ms
    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.to_string(),
            port,
        },
        read_timeout_ms: 2000,
        write_timeout_ms: 500,
        temp_read_timeout_ms: 6000,
        buffer_timeout_ms: 10000,
        user_code: Some("123456".to_string()),
        auto_reconnect: true,
    };

    println!("Inicjalizacja połączenia z {}:{}...", host, port);
    let satel = SatelIntegra::new(config);

    match satel.connect().await {
        Ok(_) => println!("Połączono pomyślnie!"),
        Err(e) => {
            eprintln!("Błąd połączenia: {}", e);
            return Err(e.into());
        }
    }

    // Pozwól na chwilę działania, aby worker mógł nawiązać i utrzymać sesję
    sleep(Duration::from_secs(2)).await;

    println!("Rozłączanie...");
    satel.disconnect().await?;

    Ok(())
}
