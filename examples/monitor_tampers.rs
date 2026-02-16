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
        ..Default::default()
    };

    println!("Łączenie z centralą Satel na 10.20.30.5:7094...");
    let satel = SatelIntegra::new(config);
    satel.connect().await?;
    println!("Połączono pomyślnie!");

    // 2. Pobranie wersji dla potwierdzenia komunikacji
    let version = satel.get_integra_version().await?;
    println!("Model centrali: {}, Wersja: {}", version.model, version.firmware_version);

    // 3. Aktywacja automatycznego wysyłania ramek Push dla sabotaży (komenda 0x7F)
    println!("Aktywacja automatycznego wysyłania ramek Push dla sabotaży (0x01)...");
    let mut auto_push_data = vec![0x7F]; // Komenda 0x7F
    
    // Bajty dla ramek wysyłanych przy zmianie (6 bajtów)
    // 0x01 dla sabotaży (bit 0 w pierwszym bajcie)
    auto_push_data.push(0b1111_1111); // Aktywuj 0x01 (Zones Tamper)
    auto_push_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00]); // Pozostałe 5 bajtów na 0x00

    // Bajty dla ramek wysyłanych przy każdym odebraniu (6 bajtów - wszystkie na 0x00)
    auto_push_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);

    // Wysłanie komendy 0x7F
    satel.exchange(auto_push_data, None, None).await?;
    println!("Komenda 0x7F wysłana. Oczekiwanie na inicjalny stan sabotaży...");

    // Poczekaj chwilę, aby centrala mogła wysłać inicjalny stan po aktywacji
    sleep(Duration::from_secs(1)).await;

    // 4. Ręczne wywołanie odczytu sabotaży, aby wypełnić cache (może nie być konieczne, jeśli 0x7F od razu wysyła stan)
    // Pozostawiamy dla pewności, w razie gdyby centrala nie wysyłała inicjalnego stanu od razu po 0x7F.
    println!("Pobieranie początkowego stanu sabotaży...");
    satel.get_zones_tamper().await?;

    // Mapa do przechowywania czasu ostatniej znanej aktualizacji dla każdego wejścia
    let mut last_update_times: HashMap<u16, DateTime<Local>> = HashMap::new();

    // Inicjalizacja mapy czasów
    for i in 1..=256 {
        if let Ok(Some(status)) = satel.get_cached_zone_status(i) {
            last_update_times.insert(i, status.tamper_at);
        }
    }

    println!("Rozpoczęto monitorowanie zmian sabotaży (pętla nieskończona)...");
    println!("Wskazówka: Centrala powinna teraz automatycznie wysyłać zmiany sabotaży, a ja będę co 2 sekundy odpytywał o wersję.");

    let mut counter = 0;

    // 5. Pętla monitorująca zmiany w cache i utrzymująca połączenie
    loop {
        counter += 1;

        if counter % 20 == 0 { // Co 20 obiegów pętli (co ~2 sekundy)
            match satel.get_integra_version().await {
                Ok(version) => {
                    println!("[{}] Odświeżono wersję centrali: {} (v{})", Local::now().format("%H:%M:%S%.3f"), version.model, version.firmware_version);
                }
                Err(e) => {
                    eprintln!("[{}] Błąd podczas odczytu wersji: {}", Local::now().format("%H:%M:%S%.3f"), e);
                }
            }
        }

        for i in 1..=256 {
            if let Ok(Some(status)) = satel.get_cached_zone_status(i) {
                let last_time = last_update_times.get(&i).cloned().unwrap_or(status.tamper_at);

                // Sprawdzamy czy czas aktualizacji się zmienił
                if status.tamper_at > last_time {
                    println!(
                        "[{}] ZMIANA na wejściu {}: Sabotaż = {} (Poprzednia aktualizacja: {})",
                        status.tamper_at.format("%H:%M:%S%.3f"),
                        i,
                        if status.tamper_state { "AKTYWNY" } else { "OK" },
                        last_time.format("%H:%M:%S%.3f")
                    );
                    
                    // Aktualizujemy czas w naszej lokalnej mapie
                    last_update_times.insert(i, status.tamper_at);
                }
            }
        }

        // Krótki sleep, aby nie blokować CPU, ale reagować szybko
        sleep(Duration::from_millis(100)).await;
    }
}