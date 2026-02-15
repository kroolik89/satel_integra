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
        user_code: Some("123456".to_string()),
        ..Config::default()
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

            // Pobieramy pełne dane z unikalnego cache'u (za pomocą uchwytu stanu)
            let (status, t_err_tot, t_err_cur, s_err_tot, s_err_cur) = {
                let state = satel.state_handle();
                let s = state.read().unwrap();
                let z = &s.zones[(zone_id.wrapping_sub(1) % 256) as usize];
                (z.temperature_status, z.temperature_timeout_errors_total, z.temperature_timeout_errors_current, z.temperature_sensor_errors_total, z.temperature_sensor_errors_current)
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
                        status,
                        t_err_tot,
                        t_err_cur,
                        s_err_tot,
                        s_err_cur
                    );
                }
                Err(e) => {
                    println!(
                        "[{}] Wejście {:2} ({:18}): BŁĄD ({}) | Status: {:?} | T-Err(Tot/Cur): {}/{} | S-Err(Tot/Cur): {}/{}", 
                        Local::now().format("%H:%M:%S"),
                        zone_id,
                        zone_name,
                        e,
                        status,
                        t_err_tot,
                        t_err_cur,
                        s_err_tot,
                        s_err_cur
                    );
                }
            }
        }
        println!("---");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
