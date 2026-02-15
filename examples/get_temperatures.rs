use satel_integra::satel_integra_data::{Config, ConnectionConfig};
use satel_integra::SatelIntegra;
use std::time::{Duration, Instant};
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
        temp_read_timeout_ms: 2000,
        buffer_timeout_ms: 10000,
        user_code: Some("123456".to_string()), 
        auto_reconnect: true,
        temp_blocking_enabled: true,
        temp_max_timeout_errors: 4,
        temp_max_sensor_errors: 10,
    };

    // 2. Tworzenie instancji
    let satel = SatelIntegra::new(config);

    // 3. Połączenie
    println!("Próba połączenia...");
    satel.connect().await?;

    println!("\n--- Ciągły Odczyt Temperatury (Wejście 34) ---");
    let zone_id = 34;

    loop {
        let start = Instant::now();
        
        match satel.get_zone_temperature(zone_id).await {
            Ok(temp) => {
                let duration = start.elapsed();
                println!(
                    "[{:?}] Temp: {:.1}°C | Czas zapytania: {:?} | Status: {:?} | T-Err: {} | S-Err: {}", 
                    Local::now().format("%H:%M:%S"),
                    temp.temperature, 
                    duration,
                    temp.status,
                    temp.timeout_errors_current,
                    temp.sensor_errors_current
                );
            }
            Err(e) => {
                let duration = start.elapsed();
                println!(
                    "[{:?}] BŁĄD: {:?} | Czas zapytania: {:?}", 
                    Local::now().format("%H:%M:%S"),
                    e,
                    duration
                );
            }
        }

        // Czekaj 2 sekundy przed kolejnym zapytaniem
        sleep(Duration::from_secs(2)).await;
    }
}

// Musimy zaimportować Local do formatowania czasu w pętli
use chrono::Local;
