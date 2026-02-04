use satel_integra::{Config, ConnectionConfig, SatelCommand, SatelError};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), SatelError> {
    // Parametry połączenia
    let ip = "10.20.30.5";
    let port = 7094;
    let read_timeout_seconds = 2;
    let write_timeout_seconds = 1;

    println!(
        "Przygotowywanie konfiguracji dla {}:{}, timeout odczytu: {}s, timeout zapisu: {}s",
        ip, port, read_timeout_seconds, write_timeout_seconds
    );

    // 1. Tworzenie konfiguracji bezpośrednio w kodzie.
    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: ip.to_string(),
            port,
        },
        read_timeout: read_timeout_seconds,
        write_timeout: write_timeout_seconds,
        user_code: None,
    };

    // 2. Tworzenie nowej instancji klienta.
    let mut integra = satel_integra::SatelIntegra::new(config);

    // 3. Nawiązywanie połączenia.
    println!("Łączenie z Satel Integra...");
    integra.connect().await?;

    println!("Połączono pomyślnie!");

    // 4. Przygotowanie i wysłanie komendy.
    let command = vec![SatelCommand::IntegraVersion.to_byte()];
    println!("Wysyłanie komendy: 0x{:02X?} (IntegraVersion)...", command);

    // 5. Wymiana danych z użyciem timeoutów z konfiguracji (przekazując None).
    let response = integra.exchange(command, None, None).await?;
    
    // Przykład z własnymi timeoutami:
    // let write_timeout = Some(Duration::from_millis(500));
    // let read_timeout = Some(Duration::from_secs(3));
    // let response = integra.exchange(command, write_timeout, read_timeout).await?;

    // 6. Wydrukowanie odpowiedzi.
    println!("Otrzymano odpowiedź (hex): {:02X?}", response);

    Ok(())
}