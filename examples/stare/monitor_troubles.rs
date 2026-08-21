use satel_integra::SatelIntegra;
use satel_integra::{Config, ConnectionConfig, SatelEvent};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Konfiguracja połączenia
    // Włączamy automatyczny odczyt awarii i ich pamięci
    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: "10.20.30.5".to_string(), // Zmień na IP swojego modułu ETHM-1
            port: 7094,
        },
        auto_reconnect: true,
        auto_read_system_troubles: true,
        auto_read_troubles_memory: true,
        ..Default::default()
    };

    println!("Łączenie z centralą Satel (Monitoring Awarii)...");
    let satel = SatelIntegra::new(config);
    
    // 2. Subskrypcja zdarzeń
    let mut event_rx = satel.subscribe();
    
    tokio::spawn(async move {
        println!("Odbiornik awarii uruchomiony.");
        while let Ok(event) = event_rx.recv().await {
            match event {
                SatelEvent::Trouble(kind, state) => {
                    println!("[AWARIA] {:?} -> {}", kind, if state { "WYSTĄPIŁA" } else { "USTĄPIŁA" });
                }
                SatelEvent::TroubleMemory(kind, state) => {
                    println!("[PAMIĘĆ AWARII] {:?} -> {}", kind, if state { "W PAMIĘCI" } else { "SKASOWANA" });
                }
                SatelEvent::SystemStatusChanged(status) => {
                    println!("[STATUS] ServiceMode: {}, Troubles: {}, Memory: {}, Time: {}", 
                        status.service_mode, status.troubles_present, status.troubles_memory, status.rtc);
                }
                SatelEvent::ConnectionChanged(state) => {
                    println!("[POŁĄCZENIE] Status: {:?}", state);
                }
                _ => {}
            }
        }
    });

    // 3. Połączenie
    satel.connect().await?;
    println!("Połączono. Czekanie na zmiany w awariach...");

    // Trzymamy program przy życiu
    tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
    
    Ok(())
}
