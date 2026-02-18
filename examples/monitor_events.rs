use satel_integra::satel_integra::SatelIntegra;
use satel_integra::satel_integra_data::{Config, ConnectionConfig, SatelEvent};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Konfiguracja połączenia
    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: "10.20.30.5".to_string(), // Zmień na IP swojego modułu ETHM-1
            port: 7094,
        },
        auto_reconnect: true,
        // Włączamy automatyczny odczyt dla różnych typów danych przez Push (0x7F)
        auto_read_zones_violation: true,
        auto_read_zones_tamper: true,
        auto_read_partitions_armed_suppressed: true,
        ..Default::default()
    };

    println!("Łączenie z centralą Satel na 10.20.30.5:7094...");
    let satel = SatelIntegra::new(config);
    
    // 2. Subskrypcja zdarzeń PRZED połączeniem, aby nie przegapić zdarzenia Connected
    let mut event_rx = satel.subscribe();
    
    // Uruchamiamy zadanie w tle do obsługi zdarzeń
    tokio::spawn(async move {
        println!("Odbiornik zdarzeń uruchomiony.");
        while let Ok(event) = event_rx.recv().await {
            match event {
                SatelEvent::ConnectionChanged(status) => {
                    println!("\n[SYSTEM] Status połączenia zmieniony na: {:?}", status);
                }
                SatelEvent::ZoneViolation { id, state } => {
                    println!("[WEJŚCIE] {} - Naruszenie: {}", id, if state { "TAK" } else { "NIE" });
                }
                SatelEvent::ZoneTamper { id, state } => {
                    println!("[WEJŚCIE] {} - Sabotaż: {}", id, if state { "TAK" } else { "NIE" });
                }
                SatelEvent::PartitionArmed { id, state } => {
                    println!("[STREFA] {} - Uzbrojenie: {}", id, if state { "TAK" } else { "NIE" });
                }
                SatelEvent::ZoneTemperatureChanged { id, temperature } => {
                    println!("[TEMPERATURA] Wejście {}: {:.1}°C", id, temperature);
                }
                SatelEvent::ZoneNameReceived { id, name } => {
                    println!("[NAZWA] Wejście {}: {}", id, name);
                }
                SatelEvent::OutputNameReceived { id, name } => {
                    println!("[NAZWA] Wyjście {}: {}", id, name);
                }
                SatelEvent::PartitionNameReceived { id, name } => {
                    println!("[NAZWA] Strefa {}: {}", id, name);
                }
                _ => {
                    // Pozostałe zdarzenia: alarmy, pamięć alarmów, blokady itp.
                }
            }
        }
    });

    // 3. Połączenie (uruchamia workery w tle)
    satel.connect().await?;
    println!("Wywołano connect(). Czekanie na zdarzenia...");

    // Przykład: Pobranie nazw (każde wywoła zdarzenie ...NameReceived)
    println!("\nPobieranie nazw dla kilku wejść...");
    for i in 1..=5 {
        let _ = satel.get_zone_name(i).await;
    }

    // Przykład: Ręczne wymuszenie odczytu po 5 sekundach
    // Jeśli stan w centrali różni się od tego w cache, zostaną wyemitowane zdarzenia.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    println!("\n--- Ręczne wymuszenie odczytu naruszeń (sprawdzenie spójności) ---");
    let _ = satel.get_zones_violation().await;

    // Trzymamy program przy życiu, aby odbierać powiadomienia Push
    tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
    
    Ok(())
}
