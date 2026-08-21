use satel_integra::{Config, ConnectionConfig, SatelEvent};
use satel_integra::SatelIntegra;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = "10.20.30.5";
    let port = 7094;

    // Pełna konfiguracja autoodczytu wszystkiego
    let config = Config {
        connection: ConnectionConfig::Tcp {
            host: host.to_string(),
            port,
        },
        auto_read_zones_violation: true,
        auto_read_zones_tamper: true,
        auto_read_zones_alarm: true,
        auto_read_zones_tamper_alarm: true,
        auto_read_zones_alarm_memory: true,
        auto_read_zones_tamper_alarm_memory: true,
        auto_read_zones_bypass: true,
        auto_read_zones_no_violation_trouble: true,
        auto_read_zones_long_violation_trouble: true,
        auto_read_partitions_armed_suppressed: true,
        auto_read_partitions_armed_really: true,
        auto_read_partitions_alarm: true,
        auto_read_partitions_alarm_memory: true,
        auto_read_partitions_entry_time: true,
        auto_read_partitions_exit_time: true,
        auto_read_outputs_state: true,
        auto_read_system_troubles: true,
        auto_read_troubles_memory: true,
        user_code: Some("1234".to_string()), // Przykładowy kod
        auto_reconnect: true,
        ..Config::default()
    };

    println!("Inicjalizacja testu połączenia z {}:{}...", host, port);
    let satel = SatelIntegra::new(config);

    // Subskrypcja kanału Push przed połączeniem
    let mut rx = satel.subscribe();

    // Zadanie nasłuchujące na zdarzenia w tle
    tokio::spawn(async move {
        println!("Oczekiwanie na zdarzenia z centrali...");
        while let Ok(event) = rx.recv().await {
            match event {
                SatelEvent::IntegraVersionReceived(v) => {
                    println!(">>> PUSH: Otrzymano wersję centrali: {} v{}", v.model, v.firmware_version);
                }
                SatelEvent::EthmVersionReceived(v) => {
                    println!(">>> PUSH: Otrzymano wersję modułu ETHM: {}", v.version_raw);
                    println!("    Obsługa ramek 32B: {}", v.capabilities.support_32_byte_frames);
                    println!(
                        "    Obsługa 8 grup awarii: {}",
                        v.capabilities.support_8_troubles_groups
                    );
                }
                SatelEvent::AutoReadConfigured(report) => {
                    println!("\n   === RAPORT AUTO-ODCZYTU (PUSH) ===");
                    println!(
                        "   Skuteczność: {}/{}",
                        report.success_count, report.total_requested
                    );
                    for item in report.items {
                        use satel_integra::AutoReadItemState;
                        let status_icon = match item.state {
                            AutoReadItemState::Active => "✅",
                            AutoReadItemState::NotRequested => "⚪",
                            AutoReadItemState::UnsupportedByHardware => "⚠️",
                            AutoReadItemState::RejectedByPanel(_) => "❌",
                        };
                        if item.state != AutoReadItemState::NotRequested {
                            println!(
                                "   {} {:<30} -> {}",
                                status_icon,
                                item.name,
                                item.state.to_description()
                            );
                        }
                    }
                    println!("   ===================================\n");
                }
                SatelEvent::SystemStatusChanged(s) => {
                    println!(">>> PUSH: Zmiana statusu systemu:");
                    println!("    Czas RTC: {}", s.rtc);
                    println!("    Tryb serwisowy: {}", s.service_mode);
                    println!("    Obecne awarie: {}", s.troubles_present);
                    println!("    Pamięć awarii: {}", s.troubles_memory);
                }
                other => {
                    println!(">>> EVENT: {:?}", other);
                }
            }
        }
    });

    // Próba połączenia
    println!("Nawiązywanie połączenia (ETHM -> Centrala -> Autoodczyt)...");
    if let Err(e) = satel.connect().await {
        eprintln!("KRYTYCZNY BŁĄD POŁĄCZENIA: {}", e);
        return Err(e.into());
    }

    println!("POŁĄCZONO! Wszystkie poniższe dane pochodzą wyłącznie z kanału subscribe()...");

    // Pozostawiamy program uruchomiony, aby obserwować przychodzące zdarzenia
    println!("Program będzie działał przez 60 sekund. Obserwuj napływające dane...");
    sleep(Duration::from_secs(180)).await;

    println!("Kończenie testu...");
    satel.disconnect().await?;

    Ok(())
}
