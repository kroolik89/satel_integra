use satel_integra::{Config, ConnectionConfig, SatelIntegra};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Konfiguracja połączenia z inwersją WSZYSTKICH parametrów dla wejść 1 i 2
    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: "10.20.30.5".to_string(), // Zmień na IP swojego modułu ETHM
            port: 7094,
        },
        io_violation_invert: vec![1, 2],
        io_tamper_invert: vec![1, 2],
        io_alarm_invert: vec![1, 2],
        io_tamper_alarm_invert: vec![1, 2],
        io_alarm_memory_invert: vec![1, 2],
        io_tamper_alarm_memory_invert: vec![1, 2],
        io_bypass_invert: vec![1, 2],
        io_no_violation_trouble_invert: vec![1, 2],
        io_long_violation_trouble_invert: vec![1, 2],
        ..Default::default()
    };

    let satel = SatelIntegra::new(config);

    println!("Próba połączenia z centralą...");
    satel.connect().await?;
    println!("Połączono!");

    // Pobieramy wersję, aby wiedzieć ile wejść ma ta centrala
    let version = satel.get_integra_version().await?;
    println!("Model centrali: {}, Liczba wejść: {}", version.model, version.io_count);

    // Odświeżamy wszystkie stany wejść (0x00 do 0x08)
    println!("Odświeżanie stanów wejść (z uwzględnieniem inwersji z Config)...");
    satel.get_zones_violation().await?;
    satel.get_zones_tamper().await?;
    satel.get_zones_alarm().await?;
    satel.get_zones_tamper_alarm().await?;
    satel.get_zones_alarm_memory().await?;
    satel.get_zones_tamper_alarm_memory().await?;
    satel.get_zones_bypass().await?;
    satel.get_zones_no_violation_trouble().await?;
    satel.get_zones_long_violation_trouble().await?;

    // Odczytujemy zagregowany status dla wszystkich wejść z cache
    println!("\nAktualny stan wejść:");
    println!("{:-<125}", "");
    println!("{: <4} | {: <7} | {: <7} | {: <7} | {: <7} | {: <7} | {: <7} | {: <7} | {: <7} | {: <7}", 
             "ID", "Narusz.", "Sabot.", "Alarm", "Al.Sab.", "Pam.Al.", "Pam.Sab.", "Bypass", "Aw.Nar.", "Aw.Dłu.");
    println!("{:-<125}", "");

    for i in 1..=version.io_count {
        if let Ok(Some(status)) = satel.get_cached_zone_status(i) {
            // Wypisujemy wejścia, które są w jakikolwiek sposób aktywne
            if status.violation_state || status.tamper_state || 
               status.alarm_state || status.tamper_alarm_state || 
               status.alarm_memory_state || status.tamper_alarm_memory_state ||
               status.bypass_state || status.no_violation_trouble_state || 
               status.long_violation_trouble_state {
                println!(
                    "{: <4} | {: <7} | {: <7} | {: <7} | {: <7} | {: <7} | {: <7} | {: <7} | {: <7} | {: <7}",
                    status.id,
                    if status.violation_state { "TAK" } else { "ok" },
                    if status.tamper_state { "TAK" } else { "ok" },
                    if status.alarm_state { "ALARM" } else { "ok" },
                    if status.tamper_alarm_state { "ALARM" } else { "ok" },
                    if status.alarm_memory_state { "PAMIĘĆ" } else { "ok" },
                    if status.tamper_alarm_memory_state { "PAMIĘĆ" } else { "ok" },
                    if status.bypass_state { "BLOK" } else { "ok" },
                    if status.no_violation_trouble_state { "AWARIA" } else { "ok" },
                    if status.long_violation_trouble_state { "AWARIA" } else { "ok" }
                );
            }
        }
    }

    satel.disconnect().await?;
    Ok(())
}
