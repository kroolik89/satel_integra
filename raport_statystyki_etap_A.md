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

---

## Przebieg 2

### 1. Hash commitów i status push
- **Commit ze zmianami kodu (A2, reszta A3, reszta A4, A5, reszta A6):** `d3d0419`
  `1.7.0: statystyki A2 - bajty na kablu (CountingStream), licznik CRC, zdarzenie ConnectionStatistics co 30 s bez pingu`
- **Status push:** Sukces (`To https://github.com/kroolik89/satel_integra.git  d751321..d3d0419 master -> master`)

### 2. Ostatnie linie komend walidacyjnych (dosłownie)

#### `cargo test` (w katalogu satel_integra):
```text
test result: ok. 87 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.03s

   Doc-tests satel_integra

running 1 test
test src\lib.rs - (line 6) - compile ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

all doctests ran in 4.09s; merged doctests compilation took 2.81s
```

#### `cargo build --examples` (w katalogu satel_integra):
```text
warning: unused variable: `memory`
   --> src\parsers\system.rs:392:33
    |
392 |             custom(true, |data, memory| {
    |                                 ^^^^^^ help: if this is intentional, prefix it with an underscore: `_memory`
    |
    = note: `#[warn(unused_variables)]` (part of `#[warn(unused)]`) on by default

warning: `satel_integra` (lib) generated 1 warning (run `cargo fix --lib -p satel_integra` to apply 1 suggestion)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.31s
```

#### `cargo check -p x2iot-app` (w katalogu D:\programowanie\rust\x2iot):
```text
error[E0004]: non-exhaustive patterns: `&SatelEvent::ConnectionStatistics(_)` not covered
   --> crates\core\src\satel\event_mapping.rs:7:15
    |
  7 |         match event {
    |               ^^^^^ pattern `&SatelEvent::ConnectionStatistics(_)` not covered
    |
note: `SatelEvent` defined here
   --> D:\programowanie\rust\satel_integra\src\event.rs:36:1
    |
 36 | pub enum SatelEvent {
    | ^^^^^^^^^^^^^^^^^^^
...
 40 |     ConnectionStatistics(ConnectionStatistics),
    |     -------------------- not covered
    = note: the matched value is of type `&SatelEvent`
help: ensure that all possible cases are being handled by adding a match arm with a wildcard pattern or an explicit pattern as shown
    |
419 ~             },
420 +             &SatelEvent::ConnectionStatistics(_) => todo!()
    |

For more information about this error, try `rustc --explain E0004`.
warning: `x2iot-core` (lib) generated 4 warnings
error: could not compile `x2iot-core` (lib) due to 1 previous error; 4 warnings emitted
```

### 3. Fragmenty diffu

#### a) CountingStream (`poll_read`, `poll_write`):
```rust
impl<S: AsyncRead + Unpin> AsyncRead for CountingStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let len_before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let len_after = buf.filled().len();
                let n = len_after.saturating_sub(len_before);
                self.bytes_received.fetch_add(n as u64, Ordering::Relaxed);
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for CountingStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                self.bytes_sent.fetch_add(n as u64, Ordering::Relaxed);
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }
```

#### b) Miejsce owinięcia surowego strumienia w `satel_connection_worker_connect_physical`:
```rust
    async fn satel_connection_worker_connect_physical(
        &mut self,
        conn_timeout: Duration,
    ) -> Result<Box<dyn AsyncReadWrite>, SatelError> {
        tracing::info!("Attempting physical connection...");

        let connection_config = self.config.read().unwrap().connection.clone();
        let encryption = self.config.read().unwrap().encryption;
        let integration_key = self.config.read().unwrap().integration_key.clone();
        let (bytes_sent, bytes_received) = {
            let s = self.state.read().unwrap();
            (
                s.telemetry.bytes_sent.clone(),
                s.telemetry.bytes_received.clone(),
            )
        };

        let stream_result = timeout(conn_timeout, async move {
            match connection_config {
                ConnectionConfig::Tcp { host, port } => {
                    let stream = TcpStream::connect((host.as_str(), port))
                        .await
                        .map_err(SatelError::from)?;
                    let counted = CountingStream::new(stream, bytes_sent, bytes_received);
                    let boxed: Box<dyn AsyncReadWrite> = if encryption {
                        let key_str = integration_key.as_deref().ok_or_else(|| {
                            SatelError::InvalidIntegrationKey(
                                "encryption is enabled but integration_key is not set".into(),
                            )
                        })?;
                        let aes_key = crate::encryption::derive_aes_key(key_str);
                        tracing::info!("TCP connection established with AES-192 encryption");
                        Box::new(crate::encryption::EncryptedStream::new(counted, aes_key))
                    } else {
                        tracing::info!("TCP connection established (plaintext)");
                        Box::new(counted)
                    };
                    Ok::<Box<dyn AsyncReadWrite>, SatelError>(boxed)
                }
                ConnectionConfig::Uart { path, baud_rate } => {
                    let stream = tokio_serial::new(path, baud_rate)
                        .open_native_async()
                        .map_err(SatelError::from)?;
                    let counted = CountingStream::new(stream, bytes_sent, bytes_received);
                    let boxed: Box<dyn AsyncReadWrite> = Box::new(counted);
                    Ok::<Box<dyn AsyncReadWrite>, SatelError>(boxed)
                }
            }
        })
        .await;
```

#### c) Usunięte stare liczenie bajtów w `worker.rs`:
Usunięte ze wszystkich miejsc:
- w wysyłce (`satel_connection_worker_exchange`): usunięto `s.telemetry.bytes_sent.fetch_add(data.len() as u64, Ordering::Relaxed);`
- w odbiorze (`satel_connection_worker_exchange`): usunięto `s.telemetry.bytes_received.fetch_add(frame.len() as u64, Ordering::Relaxed);`
- w odbiorze push (`receive_push_internal`): usunięto `s.telemetry.bytes_received.fetch_add(frame.len() as u64, Ordering::Relaxed);`

#### d) Licznik CRC w kodeku:
```rust
#[derive(Clone, Debug)]
pub struct SatelCodec {
    pub crc_errors: Arc<AtomicU64>,
}
...
            if received_crc == calculate_crc(&data_with_crc) {
                Ok(Some(data_with_crc))
            } else {
                self.crc_errors.fetch_add(1, Ordering::Relaxed);
                self.decode(src)
            }
```
oraz przekazanie z telemetrii przy inicjalizacji Framed w `satel_connection_worker_connect`:
```rust
        let crc_errors = {
            let s = self.state.read().unwrap();
            s.telemetry.crc_errors.clone()
        };
        self.stream = Some(Framed::new(stream, SatelCodec::new(crc_errors)));
```

#### e) Oznaczenie pingu flagą `is_ping`:
Sygnatura:
```rust
    async fn satel_connection_worker_exchange(
        &mut self,
        data: Vec<u8>,
        expected_cmd: u8,
        write_timeout: Duration,
        read_timeout: Duration,
        is_ping: bool,
    ) -> Result<Vec<u8>, SatelError>
```
Zliczanie `non_ping_frames` przy wysyłce i odbiorze:
```rust
        // Send
        match timeout(write_timeout, stream.send(data.clone())).await {
            Ok(Ok(_)) => {
                let mut s = self.state.write().unwrap();
                if !is_ping {
                    s.telemetry.non_ping_frames.fetch_add(1, Ordering::Relaxed);
                }
                s.telemetry.last_send_at = Instant::now();
            }
...
        // Receive
                    let is_target_response = frame[0] == expected_cmd || frame[0] == 0xEF;
                    if !is_ping || !is_target_response {
                        self.state.read().unwrap().telemetry.non_ping_frames.fetch_add(1, Ordering::Relaxed);
                    }
```
Flaga w wywołaniu pingu z `ping_interval.tick()`:
```rust
let _ = self.satel_connection_worker_exchange(cmd, 0x7E, Duration::from_millis(500), Duration::from_millis(500), true).await;
```
Wszystkie pozostałe wywołania (handshake, wiadomości standardowe/priorytetowe): `is_ping = false`.

#### f) `statistics_changed`:
```rust
pub const STATS_MIN_NON_PING_FRAMES: u64 = 5;

pub fn statistics_changed(prev: &StatsMark, now: &StatsMark) -> bool {
    now.non_ping_frames.saturating_sub(prev.non_ping_frames) >= STATS_MIN_NON_PING_FRAMES
        || prev.connections_established != now.connections_established
        || prev.reconnect_attempts != now.reconnect_attempts
        || prev.connections_lost != now.connections_lost
        || prev.timeouts != now.timeouts
        || prev.crc_errors != now.crc_errors
        || prev.rejected_by_panel != now.rejected_by_panel
        || prev.io_errors != now.io_errors
}
```

#### g) Obie wysyłki zdarzenia `ConnectionStatistics`:
- **(a) w `src/auto_requester.rs` zaraz po `ConnectionChanged`:**
```rust
                            StateWorkerMessage::StatusChanged(state) => {
                                tracing::info!("SatelAutoRequester: Connection state transition -> {:?}", state);
                                let _ = self.integra.event_tx.send(SatelEvent::ConnectionChanged(state));
                                let stats = if let Ok(mut s) = self.integra.state.write() {
                                    s.telemetry.last_sent_stats_mark = s.telemetry.stats_mark();
                                    Some(s.telemetry.statistics())
                                } else {
                                    None
                                };
                                if let Some(stats) = stats {
                                    let _ = self.integra.event_tx.send(SatelEvent::ConnectionStatistics(stats));
                                }
                            }
```
- **(b) co 30 s z `SatelPollingWorker::run`:**
```rust
                if last_stats_check.elapsed() >= Duration::from_secs(30) {
                    last_stats_check = Instant::now();
                    let maybe_stats = {
                        if let Ok(mut state) = self.integra.state_handle().write() {
                            let current_mark = state.telemetry.stats_mark();
                            if crate::state::statistics_changed(
                                &state.telemetry.last_sent_stats_mark,
                                &current_mark,
                            ) {
                                state.telemetry.last_sent_stats_mark = current_mark;
                                Some(state.telemetry.statistics())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    };
                    if let Some(stats) = maybe_stats {
                        let _ = self.integra.event_tx.send(SatelEvent::ConnectionStatistics(stats));
                    }
                }
```

### 4. Lista testów (nazwa + co sprawdza)
1. `counting_stream::tests::test_counting_stream_duplex_write_10_read_7`
   - Sprawdza zapis 10 bajtów i odczyt 7 bajtów na `CountingStream` owijającym `tokio::io::duplex`. Weryfikuje precyzyjne działanie liczników `bytes_sent == 10` i `bytes_received == 7`.
2. `counting_stream::tests::test_framed_counting_stream_wire_bytes_with_0xfe`
   - Sprawdza `Framed<CountingStream<duplex>, SatelCodec>` dla payloadu z bajtem `0xFE` (wymagającym byte-stuffing `0xFE 0xF0`). Weryfikuje, że `bytes_sent` po wysłaniu ramki odpowiada dokładnie długości całej ramki na kablu (nagłówek `0xFE 0xFE`, ucieczki, CRC, stopka `0xFE 0x0D`).
3. `codec::tests::test_codec_crc_error_handling`
   - Sprawdza obsługę błędu CRC w `SatelCodec`: poprawna ramka pozostawia `crc_errors == 0`, uszkodzona ramka CRC zwiększa licznik `crc_errors` do 1, a kolejna poprawna ramka w strumieniu zostaje z sukcesem zdekodowana.
4. `state::tests::test_statistics_changed_logic`
   - Sprawdza logikę `statistics_changed`: +4 ramki `non_ping_frames` zwracają `false`, +5 zwracają `true`, zmiana `timeouts` przy 0 ramkach zwraca `true`, brak zmian parametrów zwraca `false`.

### 5. Rozbieżności planu z kodem
- Brak rozbieżności w implementacji biblioteki `satel_integra`.
- W pliku demonstracyjnym `examples/4_05_monitor_all_events.rs` dodano obsługę nowego zdarzenia `SatelEvent::ConnectionStatistics`, aby zachować pełną kompilowalność przykładów biblioteki.
- Zgodnie z wytycznymi, repozytorium `x2iot` nie było modyfikowane — brak obsługi `ConnectionStatistics` w `x2iot` został udokumentowany powyżej.

### 6. Narzędzia edycji
Wszystkie modyfikacje plików zostały wykonane **wyłącznie** przy użyciu dedykowanych narzędzi edycji (`replace_file_content` / `multi_replace_file_content`). Nie użyto żadnych skryptów (Python, PowerShell), ani komend powłoki typu `Set-Content`, `copy`, `type >`, `sed`.

