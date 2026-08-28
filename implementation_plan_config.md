# Plan Wdrożenia: Bezpośrednia Aktualizacja Konfiguracji w `satel_integra`

## Cel
Dodanie do biblioteki `satel_integra` funkcji `update_config(&self, new_config: Config) -> Result<(), SatelError>`, która pozwala na **bezpośrednią aktualizację konfiguracji w pamięci RAM w locie (Hot-Reload)** bez rozłączania socketu TCP/UART i bez przesyłania payloadu konfiguracji przez kanały. Przez kanał zdarzeń `SatelEvent` wysyłana jest wyłącznie informacja o fakcie przeładowania (`SatelEvent::ConfigUpdated`).

---

## Proponowane Zmiany

### Biblioteka `satel_integra`

#### [MODIFY] `satel_integra/src/event.rs`
- Dopisanie nowego wariantu do enumu `SatelEvent`:
  ```rust
  /// Configuration has been dynamically updated in place.
  ConfigUpdated,
  ```

#### [MODIFY] `satel_integra/src/client.rs`
- Zmiana typu pola `config` w `SatelIntegra` ze zwykłego `Config` na współdzielony wskaźnik wątkowo-bezpieczny:
  ```rust
  pub struct SatelIntegra {
      pub(crate) tx: mpsc::Sender<InternalMessage>,
      pub(crate) state: SatelStateHandle,
      pub(crate) config: Arc<std::sync::RwLock<Config>>,
      pub(crate) worker: Arc<Mutex<Option<SatelCommunicationWorker>>>,
      pub(crate) event_tx: broadcast::Sender<SatelEvent>,
  }
  ```
- Dodanie metod publicznych na strukturze `SatelIntegra`:
  - `pub fn config(&self) -> Config` – bezpieczny odczyt aktualnej migawki konfiguracji.
  - `pub fn update_config(&self, new_config: Config) -> Result<(), SatelError>`:
    1. Walidacja `new_config.validate()?`.
    2. Bezpośredni zapis do pamięci: `*self.config.write().unwrap() = new_config;`.
    3. Emisja zdarzenia: `let _ = self.event_tx.send(SatelEvent::ConfigUpdated);`.
    4. Zwrócenie `Ok(())`.

#### [MODIFY] `satel_integra/src/worker.rs`
- Zmiana pola `config` w `SatelCommunicationWorker` na `Arc<std::sync::RwLock<Config>>`.
- Odczytywanie parametrów (`read_timeout_ms`, `write_timeout_ms`, `buffer_timeout_ms`, `auto_reconnect`, `io_tamper_invert`) bezpośrednio z `self.config.read().unwrap()`.
- Dzięki temu worker natychmiast widzi zaktualizowane parametry przy kolejnej ramce.

#### [MODIFY] `satel_integra/src/polling_worker.rs`
- Dynamiczny odczyt listy stref i interwałów odpytywania temperatur z `integra.config()`.

#### [NEW] `satel_integra/tests/test_config_reload.rs`
- Test jednostkowy weryfikujący:
  1. Utworzenie klienta `SatelIntegra::new(cfg1)`.
  2. Zmianę konfiguracji przez `integra.update_config(cfg2)`.
  3. Potwierdzenie, że `integra.config()` zwraca nowe wartości.
  4. Potwierdzenie, że na kanale zdarzeń `subscribe()` odebrano `SatelEvent::ConfigUpdated`.

---

## Plan Weryfikacji

### Testy automatyczne
1. Uruchomienie testów biblioteki `satel_integra`:
   ```bash
   cargo test --package satel_integra
   ```
2. Sprawdzenie kompilacji całego workspace:
   ```bash
   cargo check --workspace
   ```
