use satel_integra::satel_integra::SatelIntegra;
use satel_integra::satel_integra_data::{Config, ConnectionConfig};
use std::time::Duration;
use tokio::time::sleep;
use chrono::{DateTime, Local};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Konfiguracja połączenia
    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: "10.20.30.5".to_string(),
            port: 7094,
        },
        auto_reconnect: true,
        // Włączamy automatyczny odczyt uzbrojenia stref (0x09) przez Push (0x7F)
        auto_read_partitions_armed_suppressed: true,
        ..Default::default()
    };

    println!("Łączenie z centralą Satel na 10.20.30.5:7094...");
    let satel = SatelIntegra::new(config);
    satel.connect().await?;
    println!("Połączono pomyślnie!");

    // Pobranie wersji dla potwierdzenia komunikacji
    if let Ok(version) = satel.get_integra_version().await {
        println!("Model centrali: {}, Wersja: {}", version.model, version.firmware_version);
    }

    // 2. Ręczny odczyt nazw i stanu początkowego stref
    println!("\nPobieranie nazw i stanu początkowego stref (1-32)...");
    for i in 1..=32 {
        let _ = satel.get_partition_name(i as u16).await;
    }

    // Ręczne wymuszenie odczytu uzbrojenia (0x09)
    println!("Pobieranie stanu uzbrojenia stref (ręczne 0x09)...");
    satel.get_partitions_armed_suppressed().await?;

    // Mapa do śledzenia zmian czasu aktualizacji stref
    let mut last_update_times: HashMap<u16, DateTime<Local>> = HashMap::new();

    // Wyświetlenie stanu początkowego i zainicjowanie czasów
    println!("\nStan początkowy stref:");
    {
        let state_handle = satel.state_handle();
        let state = state_handle.read().unwrap();
        for partition in &state.partitions {
            println!(
                "Strefa {}: {} - Status: {}",
                partition.id,
                if partition.name.is_empty() { "Brak nazwy" } else { &partition.name },
                if partition.armed_suppressed { "UZBROJONA (Suppressed)" } else { "CZUWANIE WYŁ." }
            );
            last_update_times.insert(partition.id, partition.armed_suppressed_at);
        }
    }

    println!("\nRozpoczęto monitorowanie automatyczne (Push 0x09)...");
    println!("Wskazówka: Zmień stan uzbrojenia w strefie, aby zobaczyć powiadomienie Push.");

    // 3. Pętla sprawdzająca aktualizacje automatyczne w cache
    loop {
        {
            let state_handle = satel.state_handle();
            let state = state_handle.read().unwrap();
            
            for partition in &state.partitions {
                let last_time = last_update_times.get(&partition.id).cloned().unwrap();
                
                if partition.armed_suppressed_at > last_time {
                    println!(
                        "[{}] POWIADOMIENIE AUTOMATYCZNE (0x09) - Strefa {}: {} -> {}",
                        partition.armed_suppressed_at.format("%H:%M:%S"),
                        partition.id,
                        if partition.name.is_empty() { "Strefa" } else { &partition.name },
                        if partition.armed_suppressed { "UZBROJONA" } else { "WYŁĄCZONA" }
                    );
                    
                    // Aktualizacja czasu
                    last_update_times.insert(partition.id, partition.armed_suppressed_at);
                }
            }
        }

        sleep(Duration::from_millis(200)).await;
    }
}
