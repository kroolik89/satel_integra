use satel_integra::{Config, ConnectionConfig, SatelCommand, SatelError};
use std::time::Duration;

#[tokio::main]
#[tracing::instrument]
async fn main() -> Result<(), SatelError> {
    // Parametry połączenia
    let ip = "10.20.30.5";
    let port = 7094;
    let read_timeout_seconds = 2;
    let write_timeout_seconds = 1;

    // 1. Tworzenie konfiguracji bezpośrednio w kodzie.
    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: ip.to_string(),
            port,
        },
        read_timeout: read_timeout_seconds,
        write_timeout: write_timeout_seconds,
        user_code: None,
        auto_reconnect: true,
    };

    // 2. Tworzenie nowej instancji klienta.
    // Inicjalizacja logowania następuje automatycznie wewnątrz SatelIntegra::new()
    let integra = satel_integra::SatelIntegra::new(config);

    // 3. Nawiązywanie połączenia i uruchomienie workera w tle.
    tracing::info!("Łączenie z Satel Integra...");
    if let Err(e) = integra.connect().await {
        tracing::error!("Wystąpił błąd podczas pierwszego połączenia (worker będzie próbował dalej w tle): {}", e);
    }

    // 4. Przygotowanie komendy.
    let command = vec![SatelCommand::IntegraVersion.to_byte()];
    
    loop {
        tracing::info!("--- Zapytanie o wersję ---");
        let start_time = std::time::Instant::now();

        // 5. Wymiana danych z użyciem timeoutów z konfiguracji.
        match integra.exchange(command.clone(), None, None).await {
            Ok(response) => {
                let duration = start_time.elapsed();
                tracing::info!("Otrzymano odpowiedź (hex): {:02X?}", response);
                tracing::info!("Czas trwania zapytania: {}ms", duration.as_millis());
            }
            Err(e) => {
                let duration = start_time.elapsed();
                tracing::error!("BŁĄD ZAPYTANIA po {:?}: {}", duration, e);
                tracing::info!("Czas trwania zapytania: {}ms", duration.as_millis());
            }
        }

        // Czekaj 2 sekundy przed kolejnym zapytaniem
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
