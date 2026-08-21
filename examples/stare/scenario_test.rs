use satel_integra::SatelIntegra;
use satel_integra::{Config, ConnectionConfig, SatelEvent};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ==========================================================
    // KONFIGURACJA TESTU
    // ==========================================================
    let user_pin = "1111";          // <--- TUTAJ WPISZ SWÓJ PIN
    let integra_ip = "10.20.30.5";   // <--- TUTAJ WPISZ IP CENTRALI
    // ==========================================================

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: integra_ip.to_string(),
            port: 7094,
        },
        user_code: Some(user_pin.to_string()),
        auto_reconnect: true,
        // Włączamy automatyczny odczyt stanów uzbrojenia i alarmów (Push 0x7F)
        auto_read_partitions_armed_really: true,
        auto_read_partitions_alarm: true,
        ..Default::default()
    };

    println!("--- SCENARIUSZ TESTOWY: Uzbrajanie -> Czekanie -> Rozbrajanie ---");
    println!("Łączenie z centralą {}...", integra_ip);
    
    let satel = SatelIntegra::new(config);
    satel.connect().await?;
    println!("Połączono pomyślnie!");

    // Pobranie subskrypcji zdarzeń przed wykonaniem akcji
    let mut event_rx = satel.subscribe();

    // 1. UZBRAJANIE
    println!("\n[1/3] Uzbrajam strefę 1 i 2...");
    
    if let Err(e) = satel.arm_full(1, Some(user_pin)).await {
        eprintln!("Błąd uzbrajania strefy 1: {:?}", e);
    } else {
        println!("Komenda uzbrojenia strefy 1 wysłana.");
    }

    if let Err(e) = satel.arm_full(2, Some(user_pin)).await {
        eprintln!("Błąd uzbrajania strefy 2: {:?}", e);
    } else {
        println!("Komenda uzbrojenia strefy 2 wysłana.");
    }

    // 2. MONITOROWANIE PRZEZ 60 SEKUND
    println!("\n[2/3] Monitorowanie stref przez 60 sekund...");
    println!("(Jeśli w tym czasie wystąpi ALARM w dowolnej strefie, rozbroję system natychmiast)");

    let timer = sleep(Duration::from_secs(60));
    tokio::pin!(timer);

    let mut alarm_detected = false;

    loop {
        tokio::select! {
            _ = &mut timer => {
                println!("\nMinęła 1 minuta. Przechodzę do planowego rozbrajania.");
                break;
            }
            Ok(event) = event_rx.recv() => {
                match event {
                    SatelEvent::PartitionArmedReally { id, state } => {
                        println!("[PUSH] Strefa {}: {}", id, if state { "UZBROJONA" } else { "ROZBROJONA" });
                    }
                    SatelEvent::PartitionAlarm { id, state: true } => {
                        println!("\n[!!!] WYKRYTO ALARM W STREFIE {}!", id);
                        println!("NATYCHMIASTOWA REAKCJA: ROZBRAJANIE.");
                        alarm_detected = true;
                        break;
                    }
                    SatelEvent::PartitionAlarm { id, state: false } => {
                        println!("[PUSH] Alarm w strefie {} skasowany.", id);
                    }
                    _ => {} // Ignorujemy inne zdarzenia w tym teście
                }
            }
        }
    }

    // 3. ROZBRAJANIE
    println!("\n[3/3] Rozbrajanie stref 1 i 2...");
    
    if let Err(e) = satel.disarm(1, Some(user_pin)).await {
        eprintln!("Błąd rozbrajania strefy 1: {:?}", e);
    } else {
        println!("Strefa 1: Komenda rozbrojenia wysłana.");
    }

    if let Err(e) = satel.disarm(2, Some(user_pin)).await {
        eprintln!("Błąd rozbrajania strefy 2: {:?}", e);
    } else {
        println!("Strefa 2: Komenda rozbrojenia wysłana.");
    }

    if alarm_detected {
        println!("\nTest zakończony przedwcześnie z powodu alarmu.");
    } else {
        println!("\nTest zakończony pomyślnie po upływie czasu.");
    }

    // Krótkie czekanie na ostatnie powiadomienia Push przed zamknięciem
    sleep(Duration::from_secs(2)).await;
    
    Ok(())
}
