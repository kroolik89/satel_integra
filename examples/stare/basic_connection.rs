use satel_integra::{Config, ConnectionConfig};
use satel_integra::SatelIntegra;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = "10.20.30.5";
    let port = 7094;

    // Konfiguracja z użyciem wartości domyślnych (Config::default())
    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.to_string(),
            port,
        },
        user_code: Some("123456".to_string()),
        ..Config::default()
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
