use satel_integra::satel_integra_data::{Config, ConnectionConfig};
use satel_integra::SatelIntegra;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Konfiguracja
    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: "10.20.30.5".to_string(),
            port: 7094,
        },
        user_code: Some("1234".to_string()),
        ..Config::default()
    };

    // 2. Tworzenie instancji
    let satel = SatelIntegra::new(config);

    // 3. Połączenie
    println!("Próba połączenia...");
    satel.connect().await?;

    // 4. Pobranie wersji centrali (Nowa funkcja)
    match satel.get_integra_version().await {
        Ok(ver) => {
            println!("--- Dane Centrali ---");
            println!("Model: {}", ver.model);
            println!("Firmware: {}", ver.firmware_version);
            println!("Ilość IO: {}", ver.io_count);
            println!("Język: {}", ver.language);
            println!("Zapisane we FLASH: {}", ver.stored_in_flash);
            println!("Data odczytu: {}", ver.read_at);
        }
        Err(e) => println!("Błąd podczas pobierania wersji: {:?}", e),
    }

    // 5. Sprawdzenie danych w cache
    if let Ok(Some(cached)) = satel.get_cached_version() {
        println!("Dane z cache: {} v{}", cached.model, cached.firmware_version);
    }

    // Pozwól na chwilę działania, aby zobaczyć logi z workera
    sleep(Duration::from_secs(2)).await;

    println!("Rozłączanie...");
    satel.disconnect().await?;

    Ok(())
}
