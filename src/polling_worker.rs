use crate::client::SatelIntegra;
use crate::config::TemperatureProbe;
use crate::event::SatelEvent;
use crate::state::ConnectionState;
use futures::future::BoxFuture;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::time::sleep;

pub(crate) fn due_zones(
    probes: &[TemperatureProbe],
    last_run: &HashMap<u16, Instant>,
    now: Instant,
) -> Vec<u16> {
    let mut due = Vec::new();
    for probe in probes {
        let interval = Duration::from_secs(probe.interval_minutes.max(1) * 60);
        if let Some(&last) = last_run.get(&probe.zone_id) {
            if now.saturating_duration_since(last) >= interval {
                due.push(probe.zone_id);
            }
        } else {
            due.push(probe.zone_id);
        }
    }
    due.sort_unstable();
    due
}

/// Trait defining a scheduled periodic polling task.
pub(crate) trait PollingTask: Send + Sync {
    /// Task identifier name for logging.
    fn name(&self) -> &str;

    /// Checks if the task is due for execution.
    fn is_due(&self, integra: &SatelIntegra) -> bool;

    /// Executes the task asynchronously.
    fn execute<'a>(&'a mut self, integra: &'a SatelIntegra) -> BoxFuture<'a, ()>;
}

/// Periodic background task querying configured zone temperature sensors.
pub(crate) struct TemperaturePollingTask {
    pub last_run: HashMap<u16, Instant>,
}

impl TemperaturePollingTask {
    pub fn new() -> Self {
        Self {
            last_run: HashMap::new(),
        }
    }
}

impl PollingTask for TemperaturePollingTask {
    fn name(&self) -> &str {
        "TemperaturePollingTask"
    }

    fn is_due(&self, integra: &SatelIntegra) -> bool {
        let config = integra.config.read().unwrap();
        if !config.is_polling_enabled() {
            return false;
        }
        !due_zones(&config.temperature_probes, &self.last_run, Instant::now()).is_empty()
    }

    fn execute<'a>(&'a mut self, integra: &'a SatelIntegra) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let probes = integra.config.read().unwrap().temperature_probes.clone();
            
            // Clean up deleted probes
            let current_zone_ids: std::collections::HashSet<_> = probes.iter().map(|p| p.zone_id).collect();
            self.last_run.retain(|k, _| current_zone_ids.contains(k));

            let zones = due_zones(&probes, &self.last_run, Instant::now());
            if zones.is_empty() {
                return;
            }
            
            tracing::info!(
                "Polling worker: starting temperature polling cycle for {} zones",
                zones.len()
            );

            for &zone_id in &zones {
                self.last_run.insert(zone_id, Instant::now());
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

        // Initial delay of 5 seconds post-connection to let handshake and auto-push stabilize
        let initial_delay = Duration::from_secs(5);

        tracing::info!(
            "SatelPollingWorker: waiting 5s post-connection before first polling cycle..."
        );
        sleep(initial_delay).await;

        let mut last_stats_check = Instant::now();

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
                    if task.is_due(&self.integra) {
                        tracing::debug!("SatelPollingWorker: running task '{}'", task.name());
                        task.execute(&self.integra).await;
                    }
                }

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
            } else {
                tracing::debug!("SatelPollingWorker: waiting for connection...");
            }

            // Evaluate task schedule every 1 second
            sleep(Duration::from_secs(1)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_due_zones() {
        let now = Instant::now();
        let probes = vec![
            TemperatureProbe {
                zone_id: 1,
                interval_minutes: 1,
                max_timeout_errors: 4,
                max_sensor_errors: 10,
                unblock_enabled: false,
                unblock_after_cycles: 10,
            },
            TemperatureProbe {
                zone_id: 2,
                interval_minutes: 5,
                max_timeout_errors: 4,
                max_sensor_errors: 10,
                unblock_enabled: false,
                unblock_after_cycles: 10,
            },
            TemperatureProbe {
                zone_id: 3,
                interval_minutes: 0, // treated as 1
                max_timeout_errors: 4,
                max_sensor_errors: 10,
                unblock_enabled: false,
                unblock_after_cycles: 10,
            },
        ];

        let mut last_run = HashMap::new();

        // Empty map -> all
        let due = due_zones(&probes, &last_run, now);
        assert_eq!(due, vec![1, 2, 3]);

        // After setting last_run to now, none should be due
        last_run.insert(1, now);
        last_run.insert(2, now);
        last_run.insert(3, now);
        let due = due_zones(&probes, &last_run, now);
        assert!(due.is_empty());

        // After 61s, 1 and 3 are due, 2 is not
        let later_61s = now + Duration::from_secs(61);
        let due = due_zones(&probes, &last_run, later_61s);
        assert_eq!(due, vec![1, 3]);

        // After 301s, all are due
        let later_301s = now + Duration::from_secs(301);
        let due = due_zones(&probes, &last_run, later_301s);
        assert_eq!(due, vec![1, 2, 3]);
    }
}
