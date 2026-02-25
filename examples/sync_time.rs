use satel_integra::SatelIntegra;
use satel_integra::satel_integra_data::{Config, ConnectionConfig, SatelEvent};
use chrono::Local;
use std::io::{self, Write};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Konfiguracja generowana bezpośrednio w kodzie
    let host = "10.20.30.5";
    let port = 7094;

    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.to_string(),
            port,
        },
        // Włączamy autoodczyt systemowych informacji (w tym 0x1A), aby widzieć eventy w tle
        auto_read_system_troubles: true,
        auto_read_zones_violation: true,
        user_code: Some("1234".to_string()),
        auto_reconnect: true,
        ..Config::default()
    };

    println!("Inicjalizacja połączenia z {}:{}...", host, port);
    let integra = SatelIntegra::new(config);
    
    // 2. Subskrypcja zdarzeń w tle przed połączeniem
    let mut events = integra.subscribe();
    tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            match event {
                SatelEvent::SystemStatusChanged(status) => {
                    println!("🔔 [EVENT] Status systemu: RTC={}, Service={}", status.rtc.format("%H:%M:%S"), status.service_mode);
                }
                SatelEvent::ZoneViolation { id, state } => {
                    println!("🔔 [EVENT] Naruszenie wejścia {}: {}", id, if state { "TAK" } else { "NIE" });
                }
                _ => {}
            }
        }
    });

    println!("Nawiązywanie połączenia...");
    integra.connect().await?;
    println!("✅ Połączono.\n");

    // 3. Pobranie czasu z centrali (używamy poprawionej metody zwracającej wynik bezpośrednio)
    println!("Pobieranie czasu z centrali...");
    let panel_time = integra.get_satel_time().await?;
    let computer_time = Local::now();
    
    let diff = computer_time.signed_duration_since(panel_time);
    let seconds_diff = diff.num_seconds();

    println!("--------------------------------------------------");
    println!("Czas w centrali:  {}", panel_time.format("%Y-%m-%d %H:%M:%S"));
    println!("Czas komputera:   {}", computer_time.format("%Y-%m-%d %H:%M:%S"));
    println!("Różnica:          {} sekund", seconds_diff);
    println!("--------------------------------------------------");

    if seconds_diff.abs() < 2 {
        println!("✅ Czas jest zsynchronizowany (różnica mniejsza niż 2s).");
    } else {
        if seconds_diff > 0 {
            println!("⚠️ Czas w centrali spóźnia się o {} sekund.", seconds_diff);
        } else {
            println!("⚠️ Czas w centrali spieszy się o {} sekund.", seconds_diff.abs());
        }
    }

    // 4. Interakcja z użytkownikiem
    print!("\nCzy zaktualizować czas w centrali do czasu komputera? [t/N]: ");
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    if input.trim().to_lowercase() == "t" {
        let new_time = Local::now();
        println!("🚀 Wysyłanie nowego czasu: {}...", new_time.format("%H:%M:%S"));
        
        match integra.set_satel_time(new_time, None).await {
            Ok(_) => {
                println!("✅ Czas został pomyślnie ustawiony.");
                // Dajemy chwilę na odebranie eventu z nowym czasem
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => println!("❌ Błąd podczas ustawiania czasu: {:?}", e),
        }
    } else {
        println!("Pominięto aktualizację czasu.");
    }

    println!("\nZamykanie połączenia...");
    integra.disconnect().await?;
    println!("👋 Koniec.");

    Ok(())
}
