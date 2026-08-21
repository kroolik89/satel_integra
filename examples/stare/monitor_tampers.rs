use satel_integra::SatelIntegra;
use satel_integra::{Config, ConnectionConfig};
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
        auto_read_zones_violation: true,
        auto_read_zones_tamper: true,
        auto_read_zones_alarm: true,
        auto_read_zones_tamper_alarm: true,
        auto_read_zones_alarm_memory: true,
        auto_read_zones_tamper_alarm_memory: true,
        auto_read_zones_bypass: true,
        auto_read_zones_no_violation_trouble: true,
        auto_read_zones_long_violation_trouble: true,
        ..Default::default()
    };

    println!("Łączenie z centralą Satel na 10.20.30.5:7094...");
    let satel = SatelIntegra::new(config);
    satel.connect().await?;
    println!("Połączono pomyślnie!");

    // 2. Pobranie wersji dla potwierdzenia komunikacji.
    // Używamy bezpiecznej obsługi, bo system Auto-Reconnect może właśnie pracować w tle.
    match satel.get_integra_version().await {
        Ok(version) => println!("Model centrali: {}, Wersja: {}", version.model, version.firmware_version),
        Err(e) => eprintln!("Początkowe pobranie wersji nieudane (Auto-Reconnect w toku?): {}", e),
    }

    // Poczekaj chwilę na ewentualną stabilizację i automatyczną konfigurację Push (0x7F)
    sleep(Duration::from_secs(1)).await;

    // 3. Ręczne wywołanie odczytu sabotaży (bezpieczna obsługa błędu)
    println!("Próba pobrania początkowego stanu sabotaży...");
    if let Err(e) = satel.get_zones_tamper().await {
        eprintln!("Początkowe pobranie sabotaży nieudane: {}", e);
    }

    // Mapa do przechowywania czasu ostatniej znanej aktualizacji dla każdego wejścia
    // Klucz: zone_id, Wartość: (violation_at, tamper_at)
    let mut last_update_times: HashMap<u16, (DateTime<Local>, DateTime<Local>)> = HashMap::new();

    // Inicjalizacja mapy czasów
    for i in 1..=256 {
        if let Ok(Some(status)) = satel.get_cached_zone_status(i) {
            last_update_times.insert(i, (status.violation_at, status.tamper_at));
        }
    }

    println!("Rozpoczęto monitorowanie zmian naruszeń i sabotaży (pętla nieskończona)...");
    println!("Wskazówka: Centrala powinna teraz automatycznie wysyłać zmiany. Biblioteka automatycznie podtrzymuje połączenie w tle.");

    // 4. Pętla monitorująca zmiany w cache
    loop {
        for i in 1..=256 {
            if let Ok(Some(status)) = satel.get_cached_zone_status(i) {
                let (last_violation_time, last_tamper_time) = last_update_times
                    .get(&i)
                    .cloned()
                    .unwrap_or((status.violation_at, status.tamper_at));

                // Sprawdzamy czy czas naruszenia się zmienił
                if status.violation_at > last_violation_time {
                    println!(
                        "[{}] ZMIANA (Naruszenie) na wejściu {}: Stan = {} (Poprzednia: {})",
                        status.violation_at.format("%H:%M:%S%.3f"),
                        i,
                        if status.violation_state { "NARUSZONE" } else { "OK" },
                        last_violation_time.format("%H:%M:%S%.3f")
                    );
                }

                // Sprawdzamy czy czas sabotażu się zmienił
                if status.tamper_at > last_tamper_time {
                    println!(
                        "[{}] ZMIANA (Sabotaż) na wejściu {}: Stan = {} (Poprzednia: {})",
                        status.tamper_at.format("%H:%M:%S%.3f"),
                        i,
                        if status.tamper_state { "AKTYWNY" } else { "OK" },
                        last_tamper_time.format("%H:%M:%S%.3f")
                    );
                }

                if status.violation_at > last_violation_time || status.tamper_at > last_tamper_time {
                    // Aktualizujemy czas w naszej lokalnej mapie
                    last_update_times.insert(i, (status.violation_at, status.tamper_at));
                }
            }
        }

        // Krótki sleep, aby nie blokować CPU, ale reagować szybko
        sleep(Duration::from_millis(100)).await;
    }
}
