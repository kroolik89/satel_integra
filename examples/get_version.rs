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

    // 4. Pobranie wersji centrali
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
        Err(e) => println!("Błąd podczas pobierania wersji centrali: {:?}", e),
    }

    // 5. Pobranie wersji modułu ETHM/INT-RS
    match satel.get_ethm_version().await {
        Ok(ver) => {
            println!("--- Dane Modułu ---");
            println!("Wersja: {}", ver.version_raw);
            println!("Możliwości: {:?}", ver.capabilities);
            println!("Data odczytu: {}", ver.read_at);
        }
        Err(e) => println!("Błąd podczas pobierania wersji modułu: {:?}", e),
    }

    // 6. Pobranie statusu systemu
    satel.get_system_status().await?;
    {
        let handle = satel.state_handle();
        let state = handle.read().unwrap();
        if let Some(status) = &state.system_status {
            println!("--- Status Systemu ---");
            println!("Czas RTC: {}", status.rtc);
            println!("Tryb serwisowy: {}", status.service_mode);
            println!("Awarie: {}", status.troubles_present);
        }
    }

    // 7. Sprawdzenie danych w cache wersji
    if let Ok(Some(cached)) = satel.get_cached_version() {
        println!("Cache centrali: {} v{}", cached.model, cached.firmware_version);
    }

    // Pozwól na chwilę działania, aby zobaczyć logi z workera
    sleep(Duration::from_secs(2)).await;

    println!("Rozłączanie...");
    satel.disconnect().await?;

    Ok(())
}
