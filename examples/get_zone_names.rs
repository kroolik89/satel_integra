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
        user_code: Some("123456".to_string()),
        ..Config::default()
    };

    // 2. Tworzenie instancji
    let satel = SatelIntegra::new(config);

    // 3. Połączenie
    println!("Próba połączenia...");
    satel.connect().await?;

    // 4. Pobranie nazw kilku wejść
    for zone_id in 1..=5 {
        match satel.get_zone_name(zone_id).await {
            Ok(zone) => {
                println!("Wejście {}: {} (pobrano: {})", zone_id, zone.name, zone.read_at);
            }
            Err(e) => println!("Błąd podczas pobierania nazwy wejścia {}: {:?}", zone_id, e),
        }
    }

    // Pozwól na chwilę działania
    sleep(Duration::from_secs(1)).await;

    println!("Rozłączanie...");
    satel.disconnect().await?;

    Ok(())
}
