use satel_integra::command::SatelCommand;
use satel_integra::{Config, ConnectionConfig, SatelEvent, SatelIntegra};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // =========================================================================
    // 0. KONFIGURACJA POŁĄCZENIA I AUTOODCZYTU
    // =========================================================================
    let host = std::env::var("SATEL_HOST").unwrap_or_else(|_| "10.20.30.5".to_string());
    let port = std::env::var("SATEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7094);
    let user_code = std::env::var("SATEL_PIN").ok().or_else(|| Some("123456".to_string()));

    println!("=================================================================");
    println!("     SATEL INTEGRA - KOMPLEKSOWY TEST WSZYSTKICH FUNKCJI         ");
    println!("=================================================================");
    println!("Adres: {}:{}", host, port);
    println!("Kod użytkownika: {:?}", user_code);

    let config = Config {
        connection: ConnectionConfig::Tcp { host, port },
        user_code,
        auto_reconnect: true,
        read_timeout_ms: 3000,
        write_timeout_ms: 2000,
        temp_read_timeout_ms: 2000,
        temp_blocking_enabled: true,
        // Włączenie wszystkich flag autoodczytu (Push 0x7F)
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
        ..Default::default()
    };

    let satel = SatelIntegra::new(config);

    // =========================================================================
    // 1. WŁĄCZENIE ODCZYTU KANAŁAMI (BROADCAST SUBSCRIBER)
    // =========================================================================
    println!("\n[KROK 1] Inicjalizacja subskrypcji kanału zdarzeń (SatelEvent)...");
    let mut event_rx = satel.subscribe();

    tokio::spawn(async move {
        println!("--> [Kanał Zdarzeń] Odbiornik zdarzeń w tle aktywny.");
        while let Ok(event) = event_rx.recv().await {
            match event {
                SatelEvent::ConnectionChanged(state) => {
                    println!("  [EVENT::POŁĄCZENIE] Stan -> {:?}", state);
                }
                SatelEvent::ZoneViolation { id, state } => {
                    println!("  [EVENT::WEJŚCIE] Wejście {:2} -> {}", id, if state { "NARUSZONE [!]" } else { "OK" });
                }
                SatelEvent::ZoneTamper { id, state } => {
                    println!("  [EVENT::WEJŚCIE] Wejście {:2} Sabotaż -> {}", id, if state { "SABOTAŻ [!]" } else { "OK" });
                }
                SatelEvent::ZoneAlarm { id, state } => {
                    println!("  [EVENT::WEJŚCIE] Wejście {:2} ALARM -> {}", id, if state { "ALARM [!!!]" } else { "OK" });
                }
                SatelEvent::ZoneTamperAlarm { id, state } => {
                    println!("  [EVENT::WEJŚCIE] Wejście {:2} Alarm Sabotażowy -> {}", id, if state { "ALARM SABOTAŻOWY [!]" } else { "OK" });
                }
                SatelEvent::ZoneTemperatureChanged { id, temperature } => {
                    println!("  [EVENT::TEMP] Wejście {:2} Temperatura -> {:.1}°C", id, temperature);
                }
                SatelEvent::PartitionArmedReally { id, state } => {
                    println!("  [EVENT::STREFA] Strefa {:2} Faktyczne Uzbrojenie -> {}", id, if state { "UZBROJONA" } else { "ROZBROJONA" });
                }
                SatelEvent::PartitionAlarm { id, state } => {
                    println!("  [EVENT::STREFA] Strefa {:2} ALARM -> {}", id, if state { "ALARM [!!!]" } else { "OK" });
                }
                SatelEvent::PartitionEntryTime { id, state } => {
                    println!("  [EVENT::STREFA] Strefa {:2} Czas na wejście -> {}", id, if state { "ODLICZANIE" } else { "KONIEC" });
                }
                SatelEvent::OutputChanged { id, state } => {
                    println!("  [EVENT::WYJŚCIE] Wyjście {:2} -> {}", id, if state { "WŁĄCZONE (ON)" } else { "WYŁĄCZONE (OFF)" });
                }
                SatelEvent::Trouble(trouble, state) => {
                    println!("  [EVENT::AWARIA] {:?} -> {}", trouble, if state { "WYSTĄPIŁA [!]" } else { "USTĄPIŁA" });
                }
                SatelEvent::TroubleMemory(trouble, state) => {
                    println!("  [EVENT::PAMIĘĆ AWARII] {:?} -> {}", trouble, if state { "W PAMIĘCI" } else { "SKASOWANA" });
                }
                SatelEvent::SystemStatusChanged(status) => {
                    println!("  [EVENT::STATUS RTC] Czas: {}, Serwis: {}, Awarie: {}", status.rtc, status.service_mode, status.troubles_present);
                }
                SatelEvent::AutoReadConfigured(report) => {
                    println!("  [EVENT::AUTOREAD] Konfiguracja Push zakończona: {}/{} aktywnych", report.success_count, report.total_requested);
                }
                SatelEvent::PanelMessage(result) => {
                    println!("  [EVENT::KOMUNIKAT] Odpowiedź panelu: {:?}", result);
                }
                _ => {}
            }
        }
    });

    // Łączenie z centralą
    println!("\nŁączenie z centralą...");
    satel.connect().await?;
    println!("Połączono i wykonano handshake pomyślnie!\n");

    // =========================================================================
    // 2. ODCZYT WSZYSTKICH DOSTĘPNYCH PARAMETRÓW
    // =========================================================================
    println!("=================================================================");
    println!("[KROK 2] PEŁNY ODCZYT WSZYSTKICH PARAMETRÓW CENTRALI");
    println!("=================================================================");

    // 2.1. Wersja Centrali i Modułu
    println!("\n--- [2.1] WERSJE URZĄDZEŃ ---");
    let integra_ver = satel.get_integra_version().await?;
    println!("Model Centrali: {}", integra_ver.model);
    println!("Wersja Firmware: {}", integra_ver.firmware_version);
    println!("Liczba Wejść/Wyjść: {}", integra_ver.io_count);
    println!("Język: {}", integra_ver.language);
    println!("Pamięć FLASH: {}", integra_ver.stored_in_flash);

    let ethm_ver = satel.get_ethm_version().await?;
    println!("Wersja ETHM/INT-RS: {}", ethm_ver.version_raw);
    println!("Możliwości modułu: 32B_ramki={}, 8GrupAwarii={}, ExtArm={}",
        ethm_ver.capabilities.support_32_byte_frames,
        ethm_ver.capabilities.support_8_troubles_groups,
        ethm_ver.capabilities.support_extended_arming_commands
    );

    // 2.2. Status Systemu i Zegar RTC
    println!("\n--- [2.2] STATUS SYSTEMU I ZEGAR RTC ---");
    let sys_status = satel.get_system_status().await?;
    println!("Czas RTC centrali: {}", sys_status.rtc);
    println!("Tryb serwisowy: {}", if sys_status.service_mode { "AKTYWNY" } else { "nieaktywny" });
    println!("Awarie w systemie: {}", if sys_status.troubles_present { "WYSTĘPUJĄ" } else { "brak" });
    println!("Pamięć awarii: {}", if sys_status.troubles_memory { "TAK" } else { "brak" });

    // 2.3. Stan Stref (Partitions) - Pełny odczyt i prezentacja stanów
    println!("\n--- [2.3] STREFY (PARTITIONS) - STAN I NAZWY ---");
    println!("Pobieranie stanów stref z centrali...");
    satel.get_partitions_armed_suppressed().await?;
    satel.get_partitions_armed_really().await?;
    satel.get_partitions_alarm().await?;
    satel.get_partitions_alarm_memory().await?;
    satel.get_partitions_times().await?;

    println!("Pobieranie nazw stref (1..32)...");
    for part_id in 1..=32 {
        let _ = satel.get_partition_name(part_id).await;
    }

    println!("\n{:-<95}", "");
    println!("{: <4} | {: <22} | {: <10} | {: <10} | {: <7} | {: <8} | {: <8}",
        "ID", "Nazwa Strefy", "Uzbrojona", "Suppressed", "Alarm", "Pamięć", "Czas We.");
    println!("{:-<95}", "");

    {
        let handle = satel.state_handle();
        let state = handle.read().unwrap();
        for partition in &state.partitions {
            // Wyświetlamy nazwane strefy lub strefy w aktywnym stanie
            let has_name = !partition.name.trim().is_empty();
            let is_active = partition.armed_really || partition.armed_suppressed || partition.alarm || partition.entry_time;
            if has_name || is_active {
                println!(
                    "{: <4} | {: <22} | {: <10} | {: <10} | {: <7} | {: <8} | {: <8}",
                    partition.id,
                    if has_name { partition.name.as_str() } else { "-- brak nazwy --" },
                    if partition.armed_really { "TAK" } else { "nie" },
                    if partition.armed_suppressed { "TAK" } else { "nie" },
                    if partition.alarm { "ALARM!" } else { "ok" },
                    if partition.alarm_memory { "PAMIĘĆ" } else { "brak" },
                    if partition.entry_time { "ODLICZA" } else { "nie" },
                );
            }
        }
    }
    println!("{:-<95}", "");

    // 2.4. Stan Wejść (Zones) - Pełny odczyt i prezentacja stanów
    println!("\n--- [2.4] WEJŚCIA (ZONES) - STAN I NAZWY ---");
    println!("Pobieranie stanów wejść (0x00 do 0x08)...");
    satel.get_zones_violation().await?;
    satel.get_zones_tamper().await?;
    satel.get_zones_alarm().await?;
    satel.get_zones_tamper_alarm().await?;
    satel.get_zones_alarm_memory().await?;
    satel.get_zones_tamper_alarm_memory().await?;
    satel.get_zones_bypass().await?;
    satel.get_zones_no_violation_trouble().await?;
    satel.get_zones_long_violation_trouble().await?;

    let max_zones = integra_ver.io_count.min(128);
    println!("Pobieranie nazw wejść 1..{}...", max_zones);
    for zone_id in 1..=max_zones {
        let _ = satel.get_zone_name(zone_id).await;
    }

    println!("\n{:-<110}", "");
    println!("{: <4} | {: <22} | {: <8} | {: <8} | {: <7} | {: <8} | {: <8} | {: <8}",
        "ID", "Nazwa Wejścia", "Narusz.", "Sabotaż", "Alarm", "Al.Sab.", "Pam.Al.", "Bypass");
    println!("{:-<110}", "");

    let mut active_zones_count = 0;
    for zone_id in 1..=integra_ver.io_count {
        if let Ok(Some(status)) = satel.get_cached_zone_status(zone_id) {
            let has_name = !status.name.trim().is_empty();
            let is_active = status.violation_state || status.tamper_state || status.alarm_state
                || status.tamper_alarm_state || status.alarm_memory_state || status.bypass_state
                || status.no_violation_trouble_state || status.long_violation_trouble_state;

            if has_name || is_active {
                active_zones_count += 1;
                println!(
                    "{: <4} | {: <22} | {: <8} | {: <8} | {: <7} | {: <8} | {: <8} | {: <8}",
                    status.id,
                    if has_name { status.name.as_str() } else { "-- brak nazwy --" },
                    if status.violation_state { "NARUSZ" } else { "ok" },
                    if status.tamper_state { "SABOTAŻ" } else { "ok" },
                    if status.alarm_state { "ALARM!" } else { "ok" },
                    if status.tamper_alarm_state { "ALARM!" } else { "ok" },
                    if status.alarm_memory_state { "PAMIĘĆ" } else { "brak" },
                    if status.bypass_state { "BLOKADA" } else { "nie" },
                );
            }
        }
    }
    println!("{:-<110}", "");
    println!("Liczba skonfigurowanych/aktywnych wejść: {}", active_zones_count);

    // 2.5. Odczyt Czujników Temperatury (używamy ID z testów get_temperatures: 34, 5, 25, 29)
    println!("\n--- [2.5] CZUJNIKI TEMPERATURY ---");
    let temp_sensor_zones = vec![34, 5, 25, 29];
    println!("Odpytywanie czujników temperatury na wejściach {:?}...", temp_sensor_zones);

    for &zone_id in &temp_sensor_zones {
        let zone_name = satel.get_cached_zone_name(zone_id)
            .ok()
            .flatten()
            .map(|n| n.name)
            .unwrap_or_else(|| "Wejście".to_string());

        match satel.get_zone_temperature_with_blocking(zone_id).await {
            Ok(temp) => {
                println!("  Wejście {:2} ({:16}) -> OK | Temperatura: {:.1}°C", zone_id, zone_name, temp.temperature);
            }
            Err(e) => {
                println!("  Wejście {:2} ({:16}) -> Brak odczytu / błąd: {:?}", zone_id, zone_name, e);
            }
        }
    }

    // 2.6. Stan Wyjść (Outputs)
    println!("\n--- [2.6] WYJŚCIA (OUTPUTS) ---");
    satel.get_outputs_state().await?;

    let max_outputs = integra_ver.io_count.min(64);
    println!("Pobieranie nazw wyjść 1..{}...", max_outputs);
    for out_id in 1..=max_outputs {
        let _ = satel.get_output_name(out_id).await;
    }

    println!("\n{:-<50}", "");
    println!("{: <4} | {: <25} | {: <10}", "ID", "Nazwa Wyjścia", "Stan");
    println!("{:-<50}", "");
    {
        let handle = satel.state_handle();
        let state = handle.read().unwrap();
        for output in state.outputs.iter().take(max_outputs as usize) {
            let has_name = !output.name.trim().is_empty();
            if has_name || output.state {
                println!("{: <4} | {: <25} | {: <10}",
                    output.id,
                    if has_name { output.name.as_str() } else { "-- brak nazwy --" },
                    if output.state { "WŁĄCZONE (ON)" } else { "wyłączone" }
                );
            }
        }
    }
    println!("{:-<50}", "");

    // 2.7. Awarie Systemu (Troubles)
    println!("\n--- [2.7] AWARIE SYSTEMU ---");
    let trouble_cmds = [
        SatelCommand::TroublesPart1,
        SatelCommand::TroublesPart2,
        SatelCommand::TroublesPart3,
        SatelCommand::TroublesPart4,
        SatelCommand::TroublesPart5,
        SatelCommand::TroublesPart6,
        SatelCommand::TroublesPart7,
        SatelCommand::TroublesPart8,
    ];
    for cmd in trouble_cmds {
        let _ = satel.get_system_troubles(cmd).await;
    }

    let (active_troubles, memory_troubles) = {
        let handle = satel.state_handle();
        let state = handle.read().unwrap();
        let mut act = Vec::new();
        let mut mem = Vec::new();
        for (i, &is_trouble) in state.troubles.iter().enumerate() {
            if is_trouble {
                act.push(satel_integra::parsers::map_trouble_bit(i as u16));
            }
        }
        for (i, &is_mem) in state.troubles_memory.iter().enumerate() {
            if is_mem {
                mem.push(satel_integra::parsers::map_trouble_bit(i as u16));
            }
        }
        (act, mem)
    };

    println!("Aktywne awarie ({}): {:?}", active_troubles.len(), active_troubles);
    println!("Awarie w pamięci ({}): {:?}", memory_troubles.len(), memory_troubles);

    // =========================================================================
    // 3. NASŁUCH NA ŻYWO - AUTOODCZYT (PUSH 0x7F)
    // =========================================================================
    println!("\n=================================================================");
    println!("[KROK 3] AUTOODCZYT PUSH - NASŁUCH ZDARZEŃ W CZASIE RZECZYWISTYM");
    println!("=================================================================");
    println!("Centrala wysyła teraz zdarzenia natychmiast po ich wystąpieniu.");
    println!("Test nasłuchuje przez 30 sekund (lub naciśnij Ctrl+C)...");
    println!("Możesz teraz naruszyć czujkę, otworzyć strefę lub zmienić stan wyjścia.");

    tokio::select! {
        _ = sleep(Duration::from_secs(30)) => {
            println!("\nCzas testu autoodczytu (30s) minął.");
        }
        _ = tokio::signal::ctrl_c() => {
            println!("\nOtrzymano sygnał przerwania Ctrl+C.");
        }
    }

    println!("\nZamykanie połączenia z centralą...");
    satel.disconnect().await?;
    println!("Połączenie zamknięte. Test zakończony sukcesem!");

    Ok(())
}
