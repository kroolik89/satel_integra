use satel_integra::satel_integra_data::{Config, ConnectionConfig};
use satel_integra::SatelIntegra;
use std::time::Duration;
use chrono::Local;

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
    println!("Połączono pomyślnie!\n");

    let test_zones = vec![5, 25, 29];
    println!("Rozpoczynam ciągły test BLOKOWANIA dla wejść {:?} (co 1s)...\n", test_zones);

    loop {
        for &zone_id in &test_zones {
            // Pobieranie nazwy (może być z cache po pierwszym odczycie)
            let zone_name = match satel.get_zone_name(zone_id).await {
                Ok(zn) => zn.name,
                Err(_) => "Nieznane".to_string(),
            };

            // Używamy nowej funkcji z blokowaniem
            match satel.get_zone_temperature_with_blocking(zone_id).await {
                Ok(temp) => {
                    println!(
                        "[{}] Wejście {:2} ({:18}): OK | Temp: {:5.1}°C | Status: {:?} | T-Err(Tot/Cur): {}/{} | S-Err(Tot/Cur): {}/{}", 
                        Local::now().format("%H:%M:%S"),
                        zone_id,
                        zone_name,
                        temp.temperature,
                        temp.status,
                        temp.timeout_errors_total,
                        temp.timeout_errors_current,
                        temp.sensor_errors_total,
                        temp.sensor_errors_current
                    );
                }
                Err(e) => {
                    // Pobieramy stan z cache (bezpiecznie), aby zobaczyć co się dzieje z licznikami przy błędzie
                    let cached = satel.get_cached_zone_temperature(zone_id).unwrap_or(None);
                    if let Some(temp) = cached {
                        println!(
                            "[{}] Wejście {:2} ({:18}): BŁĄD ({}) | Status: {:?} | T-Err(Tot/Cur): {}/{} | S-Err(Tot/Cur): {}/{}", 
                            Local::now().format("%H:%M:%S"),
                            zone_id,
                            zone_name,
                            e,
                            temp.status,
                            temp.timeout_errors_total,
                            temp.timeout_errors_current,
                            temp.sensor_errors_total,
                            temp.sensor_errors_current
                        );
                    } else {
                        println!("[{}] Wejście {:2}: BŁĄD ({}) - brak danych w cache", Local::now().format("%H:%M:%S"), zone_id, e);
                    }
                }
            }
        }
        println!("---");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
