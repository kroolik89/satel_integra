use crate::client::SatelIntegra;
use crate::state::ConnectionState;
use futures::future::BoxFuture;
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Trait defining a scheduled periodic polling task.
pub(crate) trait PollingTask: Send + Sync {
    /// Task identifier name for logging.
    fn name(&self) -> &str;

    /// Execution recurrence interval.
    #[allow(dead_code)]
    fn interval(&self) -> Duration;

    /// Checks if the task is due for execution.
    fn is_due(&self) -> bool;

    /// Executes the task asynchronously.
    fn execute<'a>(&'a mut self, integra: &'a SatelIntegra) -> BoxFuture<'a, ()>;
}

/// Periodic background task querying configured zone temperature sensors.
pub(crate) struct TemperaturePollingTask {
    pub zones: Vec<u16>,
    pub interval: Duration,
    pub last_run: Option<Instant>,
}

impl TemperaturePollingTask {
    pub fn new(zones: Vec<u16>, interval: Duration) -> Self {
        Self {
            zones,
            interval,
            last_run: None,
        }
    }
}

impl PollingTask for TemperaturePollingTask {
    fn name(&self) -> &str {
        "TemperaturePollingTask"
    }

    fn interval(&self) -> Duration {
        self.interval
    }

    fn is_due(&self) -> bool {
        match self.last_run {
            None => true,
            Some(last) => last.elapsed() >= self.interval,
        }
    }

    fn execute<'a>(&'a mut self, integra: &'a SatelIntegra) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.last_run = Some(Instant::now());
            tracing::info!(
                "Polling worker: starting temperature polling cycle for {} zones",
                self.zones.len()
            );

            for &zone_id in &self.zones {
                // Check if connection is active
                let is_connected = {
                    if let Ok(state) = integra.state_handle().read() {
                        state.telemetry.status.state == ConnectionState::Connected
                    } else {
                        false
                    }
                };

                if !is_connected {
                    tracing::warn!(
                        "Polling worker: connection inactive, aborting current temperature polling cycle"
                    );
                    break;
                }

                // Query zone temperature using the public client API (handles smart blocking, cache & events)
                match integra.get_zone_temperature(zone_id).await {
                    Ok(temp) => {
                        tracing::debug!(
                            "Polling worker: retrieved zone #{:03} temperature: {}°C",
                            zone_id,
                            temp.temperature
                        );
                    }
                    Err(e) => {
                        tracing::debug!(
                            "Polling worker: error querying zone #{:03} temperature: {:?}",
                            zone_id,
                            e
                        );
                    }
                }

                // Brief pause between sensor queries (100ms) to avoid queue saturation
                sleep(Duration::from_millis(100)).await;
            }

            tracing::info!("Polling worker: completed temperature polling cycle");
        })
    }
}

/// Generic scheduler worker managing background polling tasks.
pub(crate) struct SatelPollingWorker {
    pub integra: SatelIntegra,
    pub tasks: Vec<Box<dyn PollingTask>>,
}

impl SatelPollingWorker {
    pub fn new(integra: SatelIntegra) -> Self {
        Self {
            integra,
            tasks: Vec::new(),
        }
    }

    #[allow(dead_code)]
    pub fn register(&mut self, task: Box<dyn PollingTask>) {
        self.tasks.push(task);
    }

    pub fn register_task(&mut self, task: Box<dyn PollingTask>) {
        self.tasks.push(task);
    }

    pub async fn run(mut self) {
        tracing::info!(
            "SatelPollingWorker started with {} registered tasks",
            self.tasks.len()
        );

        if self.tasks.is_empty() {
            tracing::info!("SatelPollingWorker: no tasks registered, stopping worker");
            return;
        }

        // Initial delay of 5 seconds post-connection to let handshake and auto-push stabilize
        let initial_delay = Duration::from_secs(5);

        tracing::info!(
            "SatelPollingWorker: waiting 5s post-connection before first polling cycle..."
        );
        sleep(initial_delay).await;

        loop {
            // Check if connection is active
            let is_connected = {
                if let Ok(state) = self.integra.state_handle().read() {
                    state.telemetry.status.state == ConnectionState::Connected
                } else {
                    false
                }
            };

            if is_connected {
                for task in &mut self.tasks {
                    if task.is_due() {
                        tracing::debug!("SatelPollingWorker: running task '{}'", task.name());
                        task.execute(&self.integra).await;
                    }
                }
            } else {
                tracing::debug!("SatelPollingWorker: waiting for connection...");
            }

            // Evaluate task schedule every 1 second
            sleep(Duration::from_secs(1)).await;
        }
    }
}
