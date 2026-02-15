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
        read_timeout_ms: 2000,
        write_timeout_ms: 500,
        temp_read_timeout_ms: 6000,
        buffer_timeout_ms: 10000,
        user_code: Some("123456".to_string()), 
        auto_reconnect: true,
    };

    // 2. Tworzenie instancji
    let satel = SatelIntegra::new(config);

    // 3. Połączenie
    println!("Próba połączenia...");
    satel.connect().await?;

    // 4. Pobranie nazw stref (partycji)
    println!("\n--- Nazwy Stref (Partycji) ---");
    for partition_id in 1..=32 {
        match satel.get_partition_name(partition_id).await {
            Ok(part) => {
                println!("Strefa {}: {} (pobrano: {})", partition_id, part.name, part.read_at);
            }
            Err(_) => {
                // Centrala często zwraca błąd dla nieistniejących stref, pomijamy milcząco w pętli
            }
        }
    }

    // Pozwól na chwilę działania
    sleep(Duration::from_secs(1)).await;

    println!("Rozłączanie...");
    satel.disconnect().await?;

    Ok(())
}
