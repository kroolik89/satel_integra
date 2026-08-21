use satel_integra::{Config, ConnectionConfig, SatelEvent};
use satel_integra::SatelIntegra;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --- KONFIGURACJA UŻYTKOWNIKA ---
    let host = "10.20.30.5"; // WPISZ ADRES IP CENTRALY
    let pin = "3258";           // WPISZ SWÓJ PIN
    // --------------------------------

    // Inicjalizacja logowania nastąpi automatycznie przez SatelIntegra::new()
    // lub możesz ją skonfigurować ręcznie przed utworzeniem obiektu.

    // Konfiguracja połączenia
    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.to_string(),
            port: 7094,
        },
        user_code: Some(pin.to_string()),
        auto_read_outputs_state: true, // Włączamy automatyczne monitorowanie wyjść
        ..Default::default()
    };

    // Tworzymy instancję SatelIntegra
    let satel = SatelIntegra::new(config);

    // Subskrybujemy zdarzenia przed połączeniem
    let mut rx = satel.subscribe();

    // Łączymy się z centralą
    println!("Łączenie z centralą {}...", host);
    satel.connect().await?;
    println!("Połączono!");

    // Pobieramy początkowy stan wszystkich wyjść
    println!("Pobieranie początkowego stanu wyjść...");
    satel.get_outputs_state().await?;

    // Wyświetlamy aktualny stan wyjść z cache
    {
        let state = satel.state_handle();
        let state_read = state.read().unwrap();
        println!("--- Aktualny stan wyjść (używanych/aktywnych) ---");
        for output in &state_read.outputs {
            if !output.name.is_empty() || output.state {
                println!("Wyjście {}: {} (Stan: {})", 
                    output.id, 
                    if output.name.is_empty() { "Brak nazwy" } else { &output.name },
                    if output.state { "ON" } else { "OFF" }
                );
            }
        }
        println!("--------------------------------------------------");
    }

    // SCENARIUSZ TESTOWY URUCHOMIONY W TLE
    let satel_clone = satel.clone();
    tokio::spawn(async move {
        println!("\n[TEST] Rozpoczęcie scenariusza za 3 sekundy...");
        sleep(Duration::from_secs(3)).await;

        let test_outputs = vec![41, 2, 8];

        for &id in &test_outputs {
            // WŁĄCZANIE
            println!("\n[TEST] >>> Wysyłam: WŁĄCZ wyjście {}", id);
            match satel_clone.set_output_on(id, None).await {
                Ok(_) => println!("[TEST] centrala ZAAKCEPTOWAŁA komendę włączenia wyjścia {}", id),
                Err(e) => println!("[TEST] centrala ODRZUCIŁA komendę włączenia wyjścia {}: {:?}", id, e),
            }
            
            sleep(Duration::from_secs(3)).await;

            // WYŁĄCZANIE
            println!("\n[TEST] >>> Wysyłam: WYŁĄCZ wyjście {}", id);
            match satel_clone.set_output_off(id, None).await {
                Ok(_) => println!("[TEST] centrala ZAAKCEPTOWAŁA komendę wyłączenia wyjścia {}", id),
                Err(e) => println!("[TEST] centrala ODRZUCIŁA komendę wyłączenia wyjścia {}: {:?}", id, e),
            }

            sleep(Duration::from_secs(3)).await;

            // PRZEŁĄCZANIE (Toggle)
            println!("\n[TEST] >>> Wysyłam: PRZEŁĄCZ wyjście {}", id);
            match satel_clone.set_output_toggle(id, None).await {
                Ok(_) => println!("[TEST] centrala ZAAKCEPTOWAŁA komendę przełączenia wyjścia {}", id),
                Err(e) => println!("[TEST] centrala ODRZUCIŁA komendę przełączenia wyjścia {}: {:?}", id, e),
            }

            sleep(Duration::from_secs(3)).await;
        }

        println!("\n[TEST] Scenariusz zakończony.");
            println!("\n[TEST] >>> Wysyłam: WYŁĄCZ wyjście {}", 41);
            match satel_clone.set_output_off(41, None).await {
                Ok(_) => println!("[TEST] centrala ZAAKCEPTOWAŁA komendę wyłączenia wyjścia {}", 41),
                Err(e) => println!("[TEST] centrala ODRZUCIŁA komendę wyłączenia wyjścia {}: {:?}", 41, e),
            }
    });

    println!("\nOczekiwanie na zdarzenia (naciśnij Ctrl+C, aby przerwać)...");

    // Obsługa zdarzeń w pętli głównej
    while let Ok(event) = rx.recv().await {
        match event {
            SatelEvent::OutputChanged { id, state } => {
                let name = if let Ok(Some(o)) = satel.get_cached_output_name(id) {
                    o.name
                } else {
                    "Nieznane".to_string()
                };
                
                println!(">>> POWIADOMIENIE Z CENTRALY: Wyjście {} ({}) zmieniło stan na: {}", 
                    id, 
                    if name.is_empty() { "Brak nazwy" } else { &name },
                    if state { "WŁĄCZONE" } else { "WYŁĄCZONE" }
                );
            }
            SatelEvent::PanelMessage(result) => {
                println!(">>> KOMUNIKAT Z PANELU: {} (kod: {:?})", 
                    result.to_description(),
                    result
                );
            }
            SatelEvent::ConnectionChanged(state) => {
                println!("!!! Stan połączenia: {:?}", state);
            }
            _ => {} 
        }
    }

    Ok(())
}
