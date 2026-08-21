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
        // Włączamy automatyczny odczyt różnych stanów stref przez Push (0x7F)
        auto_read_partitions_armed_suppressed: true,
        auto_read_partitions_armed_really: true,
        auto_read_partitions_alarm: true,
        auto_read_partitions_alarm_memory: true,
        auto_read_partitions_entry_time: true,
        auto_read_partitions_exit_time: true,
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

    // Ręczne wymuszenie odczytu wszystkich stanów stref
    println!("Inicjalizacja cache (pobieranie stanów 0x09, 0x0A, 0x13, 0x15)...");
    satel.get_partitions_armed_suppressed().await?;
    satel.get_partitions_armed_really().await?;
    satel.get_partitions_alarm().await?;
    satel.get_partitions_alarm_memory().await?;
    satel.get_partitions_times().await?;

    // Struktura do śledzenia zmian czasu aktualizacji różnych stanów stref
    struct PartitionUpdateTimes {
        armed_suppressed_at: DateTime<Local>,
        armed_really_at: DateTime<Local>,
        alarm_at: DateTime<Local>,
        alarm_memory_at: DateTime<Local>,
        entry_time_at: DateTime<Local>,
        exit_time_gt_10s_at: DateTime<Local>,
        exit_time_lt_10s_at: DateTime<Local>,
    }

    let mut last_update_times: HashMap<u16, PartitionUpdateTimes> = HashMap::new();

    // Wyświetlenie stanu początkowego i zainicjowanie czasów
    println!("\nStan początkowy stref:");
    {
        let state_handle = satel.state_handle();
        let state = state_handle.read().unwrap();
        for partition in &state.partitions {
            let name = if partition.name.is_empty() { "Brak nazwy" } else { &partition.name };
            
            let armed_status = if partition.armed_really {
                "UZBROJONA"
            } else if partition.armed_suppressed {
                "UZBROJONA (Tłumiona)"
            } else {
                "CZUWANIE WYŁ."
            };

            let alarm_status = if partition.alarm {
                " !!! ALARM !!!"
            } else if partition.alarm_memory {
                " (Pamięć alarmu)"
            } else {
                ""
            };

            let time_status = if partition.entry_time {
                " [CZAS NA WEJŚCIE]"
            } else if partition.exit_time_lt_10s {
                " [WYJŚCIE <10s]"
            } else if partition.exit_time_gt_10s {
                " [WYJŚCIE >10s]"
            } else {
                ""
            };

            println!(
                "Strefa {:2}: {:20} | {:18} | {}{}",
                partition.id, name, armed_status, alarm_status, time_status
            );

            last_update_times.insert(partition.id, PartitionUpdateTimes {
                armed_suppressed_at: partition.armed_suppressed_at,
                armed_really_at: partition.armed_really_at,
                alarm_at: partition.alarm_at,
                alarm_memory_at: partition.alarm_memory_at,
                entry_time_at: partition.entry_time_at,
                exit_time_gt_10s_at: partition.exit_time_gt_10s_at,
                exit_time_lt_10s_at: partition.exit_time_lt_10s_at,
            });
        }
    }

    println!("\nRozpoczęto monitorowanie automatyczne (Push)...");
    println!("Wskazówka: Zmień stan uzbrojenia w strefie, aby zobaczyć powiadomienie Push.");

    // 3. Pętla sprawdzająca aktualizacje automatyczne w cache
    loop {
        {
            let state_handle = satel.state_handle();
            let state = state_handle.read().unwrap();
            
            for partition in &state.partitions {
                let last = last_update_times.get_mut(&partition.id).unwrap();
                let name = if partition.name.is_empty() { "Strefa" } else { &partition.name };
                
                // 1. Zmiana uzbrojenia (suppressed)
                if partition.armed_suppressed_at > last.armed_suppressed_at {
                    println!(
                        "[{}] PUSH: Strefa {} ({}) - Stan Suppressed: {}",
                        partition.armed_suppressed_at.format("%H:%M:%S"),
                        partition.id, name,
                        if partition.armed_suppressed { "TAK" } else { "NIE" }
                    );
                    last.armed_suppressed_at = partition.armed_suppressed_at;
                }

                // 2. Zmiana uzbrojenia (really)
                if partition.armed_really_at > last.armed_really_at {
                    println!(
                        "[{}] PUSH: Strefa {} ({}) - Stan Uzbrojenia: {}",
                        partition.armed_really_at.format("%H:%M:%S"),
                        partition.id, name,
                        if partition.armed_really { "UZBROJONA" } else { "ROZBROJONA" }
                    );
                    last.armed_really_at = partition.armed_really_at;
                }

                // 3. Zmiana alarmu
                if partition.alarm_at > last.alarm_at {
                    println!(
                        "[{}] PUSH: Strefa {} ({}) - !!! ALARM !!!: {}",
                        partition.alarm_at.format("%H:%M:%S"),
                        partition.id, name,
                        if partition.alarm { "AKTYWNY" } else { "SKASOWANY" }
                    );
                    last.alarm_at = partition.alarm_at;
                }

                // 4. Zmiana pamięci alarmu
                if partition.alarm_memory_at > last.alarm_memory_at {
                    println!(
                        "[{}] PUSH: Strefa {} ({}) - Pamięć alarmu: {}",
                        partition.alarm_memory_at.format("%H:%M:%S"),
                        partition.id, name,
                        if partition.alarm_memory { "OBECNA" } else { "WYCZYSZCZONA" }
                    );
                    last.alarm_memory_at = partition.alarm_memory_at;
                }

                // 5. Zmiana czasu na wejście
                if partition.entry_time_at > last.entry_time_at {
                    println!(
                        "[{}] PUSH: Strefa {} ({}) - Odliczanie NA WEJŚCIE: {}",
                        partition.entry_time_at.format("%H:%M:%S"),
                        partition.id, name,
                        if partition.entry_time { "START" } else { "KONIEC" }
                    );
                    last.entry_time_at = partition.entry_time_at;
                }

                // 6. Zmiana czasu na wyjście > 10s
                if partition.exit_time_gt_10s_at > last.exit_time_gt_10s_at {
                    println!(
                        "[{}] PUSH: Strefa {} ({}) - Odliczanie NA WYJŚCIE (>10s): {}",
                        partition.exit_time_gt_10s_at.format("%H:%M:%S"),
                        partition.id, name,
                        if partition.exit_time_gt_10s { "START" } else { "KONIEC" }
                    );
                    last.exit_time_gt_10s_at = partition.exit_time_gt_10s_at;
                }

                // 7. Zmiana czasu na wyjście < 10s
                if partition.exit_time_lt_10s_at > last.exit_time_lt_10s_at {
                    println!(
                        "[{}] PUSH: Strefa {} ({}) - Odliczanie NA WYJŚCIE (<10s): {}",
                        partition.exit_time_lt_10s_at.format("%H:%M:%S"),
                        partition.id, name,
                        if partition.exit_time_lt_10s { "START" } else { "KONIEC" }
                    );
                    last.exit_time_lt_10s_at = partition.exit_time_lt_10s_at;
                }
            }
        }

        sleep(Duration::from_millis(200)).await;
    }
}
