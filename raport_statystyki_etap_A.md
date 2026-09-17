# Raport: Statystyki połączenia w bibliotece satel_integra — Etap A

## Przebieg 1

### 1. Hash commitów i status push
- **Commit ze zmianami kodu (A1, A3 część, A4 część, A6 część):** `668d813`
  `Statystyki A1: model ConnectionStatistics, liczniki polaczen i bledow, disconnect nie zeruje, statistics()/reset_statistics()`
- **Status push:** Sukces (`To https://github.com/kroolik89/satel_integra.git  37c99b2..668d813 master -> master`)

### 2. Ostatnie linie komend walidacyjnych (dosłownie)

#### `cargo test` (w katalogu satel_integra):
```text
test worker::tests::test_connections_lost_only_increments_when_previously_connected ... ok
test worker::tests::test_disconnect_does_not_reset_counters ... ok
test parsers::system::tests::test_t3_catalog_consistency ... ok

test result: ok. 83 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s

   Doc-tests satel_integra

running 1 test
test src\lib.rs - (line 6) - compile ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

all doctests ran in 3.42s; merged doctests compilation took 2.65s
```

#### `cargo build --examples` (w katalogu satel_integra):
```text
   Compiling satel_integra v1.6.0 (D:\programowanie\rust\satel_integra)
warning: unused variable: `memory`
   --> src\parsers\system.rs:392:33
    |
392 |             custom(true, |data, memory| {
    |                                 ^^^^^^ help: if this is intentional, prefix it with an underscore: `_memory`
    |
    = note: `#[warn(unused_variables)]` (part of `#[warn(unused)]`) on by default

warning: `satel_integra` (lib) generated 1 warning (run `cargo fix --lib -p satel_integra` to apply 1 suggestion)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 1m 09s
```

#### `cargo check -p x2iot-app` (w katalogu D:\programowanie\rust\x2iot):
```text
warning: `x2iot-web` (lib) generated 17 warnings (run `cargo fix --lib -p x2iot-web` to apply 17 suggestions)
    Checking x2iot-app v0.1.0 (D:\programowanie\rust\x2iot\crates\app)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 31.81s
warning: the following packages contain code that will be rejected by a future version of Rust: proc-macro-error2 v2.0.1
note: to see what the problems were, use the option `--future-incompat-report`, or run `cargo report future-incompatibilities --id 1`
```

### 3. Fragmenty diffu

#### a) `satel_connection_worker_disconnect` (bez `telemetry.reset()`, zamknięcie sesji):
```rust
    async fn satel_connection_worker_disconnect(&mut self) {
        self.stream = None;
        {
            let mut s = self.state.write().unwrap();
            let now = Local::now();
            if let Some(since) = s.telemetry.connected_since.take() {
                let session_dur = (now - since).to_std().unwrap_or(Duration::ZERO);
                s.telemetry.total_connected_before = s.telemetry.total_connected_before.saturating_add(session_dur);
            }
            s.telemetry.status.state = ConnectionState::Disconnected;
            s.telemetry.status.last_event_at = Instant::now();
            s.telemetry.status.failed_attempts = 0;
        }
        Self::notify_state_worker(&self.state_worker_tx, StateWorkerMessage::StatusChanged(ConnectionState::Disconnected)).await;
    }
```

#### b) `satel_connection_worker_connection_lost` (warunek `Connected` + zamknięcie sesji):
```rust
    async fn satel_connection_worker_connection_lost(&mut self) {
        self.stream = None;
        {
            let mut s = self.state.write().unwrap();
            let prev = s.telemetry.status.state;
            if prev == ConnectionState::Connected {
                s.telemetry.connections_lost.fetch_add(1, Ordering::Relaxed);
                let now = Local::now();
                if let Some(since) = s.telemetry.connected_since.take() {
                    let session_dur = (now - since).to_std().unwrap_or(Duration::ZERO);
                    s.telemetry.total_connected_before = s.telemetry.total_connected_before.saturating_add(session_dur);
                }
            }
            s.telemetry.status.state = ConnectionState::ConnectionLost;
            s.telemetry.status.last_event_at = Instant::now();
            s.telemetry.status.failed_attempts += 1;
        }
        Self::notify_state_worker(&self.state_worker_tx, StateWorkerMessage::StatusChanged(ConnectionState::ConnectionLost)).await;
    }
```

#### c) `set_state_connected`:
```rust
    async fn set_state_connected(&mut self) {
        {
            let mut s = self.state.write().unwrap();
            s.telemetry.connections_established.fetch_add(1, Ordering::Relaxed);
            s.telemetry.connected_since = Some(Local::now());
            s.telemetry.status.state = ConnectionState::Connected;
            s.telemetry.status.last_event_at = Instant::now();
            s.telemetry.status.failed_attempts = 0;
            s.telemetry.last_connected_at = Some(SystemTime::now());
            s.connection_type = Some(match &self.config.read().unwrap().connection {
                ConnectionConfig::Tcp { host, port } => ConnectionType::Tcp(host.clone(), *port),
                ConnectionConfig::Uart { path, .. } => ConnectionType::Uart(path.clone()),
            });
        }
        Self::notify_state_worker(&self.state_worker_tx, StateWorkerMessage::StatusChanged(ConnectionState::Connected)).await;
    }
```

#### d) Miejsca `timeouts`, `rejected_by_panel`, `io_errors`:
W `satel_connection_worker_exchange`:
```rust
        // Send
        match timeout(write_timeout, stream.send(data.clone())).await {
            Ok(Ok(_)) => {
                let mut s = self.state.write().unwrap();
                s.telemetry.bytes_sent.fetch_add(data.len() as u64, Ordering::Relaxed);
                s.telemetry.last_send_at = Instant::now();
            }
            Ok(Err(e)) => {
                self.state.read().unwrap().telemetry.io_errors.fetch_add(1, Ordering::Relaxed);
                self.satel_connection_worker_connection_lost().await;
                return Err(SatelError::Io(e));
            }
            Err(_) => {
                self.state.read().unwrap().telemetry.timeouts.fetch_add(1, Ordering::Relaxed);
                return Err(SatelError::Timeout);
            }
        }
```
i w pętli odbioru exchange:
```rust
                    let is_result_code = frame[0] == 0xEF;
                    let is_accepted = is_result_code && frame.get(1) == Some(&0xFF);

                    if is_result_code && !is_accepted {
                        self.state.read().unwrap().telemetry.rejected_by_panel.fetch_add(1, Ordering::Relaxed);
                    }
...
                Ok(Some(Err(e))) => {
                    self.state.read().unwrap().telemetry.io_errors.fetch_add(1, Ordering::Relaxed);
                    self.satel_connection_worker_connection_lost().await;
                    return Err(SatelError::Io(e));
                }
...
        self.state.read().unwrap().telemetry.timeouts.fetch_add(1, Ordering::Relaxed);
        Err(SatelError::Timeout)
```
oraz w `receive_push_internal`:
```rust
            Ok(Some(Err(e))) => {
                if let Ok(s) = state.read() {
                    s.telemetry.io_errors.fetch_add(1, Ordering::Relaxed);
                }
                Err(SatelError::Io(e))
            }
```

#### e) `statistics()` i `reset_statistics()`:
Na `SatelIntegra` (`src/client.rs`):
```rust
    /// Returns a snapshot of connection statistics and telemetry counters.
    pub fn statistics(&self) -> ConnectionStatistics {
        let guard = self.state.read().unwrap();
        guard.telemetry.statistics()
    }

    /// Resets connection statistics and telemetry counters.
    pub fn reset_statistics(&self) {
        let mut guard = self.state.write().unwrap();
        guard.telemetry.reset();
    }
```
Na `ConnectionTelemetry` (`src/state.rs`):
```rust
    pub fn statistics(&self) -> ConnectionStatistics {
        let taken_at = Local::now();
        let current_session = if self.status.state == ConnectionState::Connected {
            self.connected_since
                .map(|since| (taken_at - since).to_std().unwrap_or(Duration::ZERO))
                .unwrap_or(Duration::ZERO)
        } else {
            Duration::ZERO
        };
        let total_connected = self.total_connected_before.saturating_add(current_session);
        let connected_since = if self.status.state == ConnectionState::Connected {
            self.connected_since
        } else {
            None
        };

        ConnectionStatistics {
            state: self.status.state,
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            connections_established: self.connections_established.load(Ordering::Relaxed),
            reconnect_attempts: self.reconnect_attempts.load(Ordering::Relaxed),
            connections_lost: self.connections_lost.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
            crc_errors: self.crc_errors.load(Ordering::Relaxed),
            rejected_by_panel: self.rejected_by_panel.load(Ordering::Relaxed),
            io_errors: self.io_errors.load(Ordering::Relaxed),
            connected_since,
            total_connected,
            taken_at,
        }
    }
```

### 4. Lista testów (nazwa + co sprawdza)
1. `client::tests::test_reset_statistics`
   - Sprawdza wywołanie `client.reset_statistics()`: wszystkie liczniki atomowe zerują się do 0, `total_connected` wynosi `Duration::ZERO`, `connected_since` jest `None` (gdy stan niepołączony).
2. `client::tests::test_total_connected_sums_closed_and_current_session`
   - Sprawdza poprawne sumowanie łącznego czasu połączenia: zakończona sesja (`total_connected_before` = 60 s) oraz aktywna sesja (`connected_since` = 10 s temu) dają łącznie w statystykach ~70 s.
3. `worker::tests::test_disconnect_does_not_reset_counters`
   - Sprawdza, że `satel_connection_worker_disconnect` zamyka bieżącą sesję (dodaje czas trwania do `total_connected_before`, ustawia `connected_since = None`), przechodzi w stan `Disconnected`, ale **nie zeruje** liczników bajtów, prób, timeoutów ani błędów IO, i nie inkrementuje `connections_lost`.
4. `worker::tests::test_connections_lost_only_increments_when_previously_connected`
   - Testuje przejścia `satel_connection_worker_connection_lost` bez sieci: ze stanów `Disconnected`, `Connecting`, `Handshake`, `ConnectionLost` licznik `connections_lost` pozostaje na 0; natomiast przy przejściu ze stanu `Connected` licznik `connections_lost` wzrasta o 1, a sesja zostaje zsumowana w `total_connected_before`.

### 5. Rozbieżności planu z kodem
Brak rozbieżności. Wszystkie założenia sekcji 3 dla Przebiegu 1 (A1, fragmenty A3, A4, A6) zostały zrealizowane dokładnie według planu.

### 6. Narzędzia edycji
Wszystkie zmiany w plikach zostały wprowadzone **wyłącznie** za pomocą narzędzia edycji IDE (`replace_file_content` / `write_to_file`). Nie użyto żadnych skryptów (Python, PowerShell), poleceń powłoki typu `Set-Content`, `copy`, `type >`, `sed`, ani wyrażeń regularnych na plikach.
