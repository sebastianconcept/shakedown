//! Resource budget assertions using diagnostics stats rings on [`crate::lifecycle::Context`].
//!
//! **What:** [`ResourceAssertBuilder`] evaluates peak memory and average CPU from collected
//! [`crate::event::InfraEvent::ResourceSample`] values for a container name.
//!
//! **Why:** scenarios get a typed, fail-fast way to assert infra stayed within bounds without parsing
//! `docker stats` or raw cgroup output.
//!
//! **How:** call [`crate::lifecycle::Context::assert_resource`] during run or assert phase when
//! diagnostics attached rings; failures return [`crate::error::ShakedownError::ResourceAssertionFailed`].

use crate::error::{InternalError, ShakedownError};
use crate::event::InfraEvent;
use crate::lifecycle::Context;
use crate::phase::Phase;

/// Fluent assertions for CPU and memory bounds using per-container resource samples.
///
/// Exists to chain budget checks (`peak_memory_under`, `avg_cpu_under`) after diagnostics have
/// populated [`crate::event::InfraEvent::ResourceSample`] rings for the scenario.
///
/// Obtained via [`Context::assert_resource`] when diagnostics enabled the stats rings; each method
/// consumes `self` and returns `Result<(), ShakedownError>` so callers compose checks in order.
pub struct ResourceAssertBuilder<'a> {
    context: &'a Context,
    container_name: &'a str,
}

impl<'a> ResourceAssertBuilder<'a> {
    pub(crate) fn new(context: &'a Context, container_name: &'a str) -> Self {
        Self {
            context,
            container_name,
        }
    }

    /// Assert peak resident memory for this container stayed under `threshold_bytes`.
    pub fn peak_memory_under(self, threshold_bytes: u64) -> Result<(), ShakedownError> {
        let samples = gather_samples(self.context, self.container_name)?;
        let Some((peak, phase)) = peak_memory_and_phase(&samples) else {
            return Err(ShakedownError::ResourceAssertionFailed {
                container: self.container_name.to_string(),
                metric: "peak_memory".to_string(),
                measured: "no samples".to_string(),
                threshold: format_bytes(threshold_bytes),
                phase: Phase::Run,
            });
        };
        if peak > threshold_bytes {
            return Err(ShakedownError::ResourceAssertionFailed {
                container: self.container_name.to_string(),
                metric: "peak_memory".to_string(),
                measured: format_bytes(peak),
                threshold: format_bytes(threshold_bytes),
                phase,
            });
        }
        Ok(())
    }

    /// Assert average CPU percentage for this container stayed under `limit_pct`.
    pub fn avg_cpu_under(self, limit_pct: f64) -> Result<(), ShakedownError> {
        let samples = gather_samples(self.context, self.container_name)?;
        if samples.is_empty() {
            return Err(ShakedownError::ResourceAssertionFailed {
                container: self.container_name.to_string(),
                metric: "avg_cpu".to_string(),
                measured: "no samples".to_string(),
                threshold: format!("{limit_pct:.1}%"),
                phase: Phase::Run,
            });
        }
        let sum: f64 = samples.iter().map(|sample| sample.cpu_pct).sum();
        let average = sum / samples.len() as f64;
        if average > limit_pct {
            let phase_at_max_cpu = samples
                .iter()
                .max_by(|left, right| {
                    left.cpu_pct
                        .partial_cmp(&right.cpu_pct)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .map(|sample| sample.phase)
                .unwrap_or(Phase::Run);
            return Err(ShakedownError::ResourceAssertionFailed {
                container: self.container_name.to_string(),
                metric: "avg_cpu".to_string(),
                measured: format!("{average:.1}%"),
                threshold: format!("{limit_pct:.1}%"),
                phase: phase_at_max_cpu,
            });
        }
        Ok(())
    }
}

fn gather_samples(
    context: &Context,
    container_name: &str,
) -> Result<Vec<ResourceSampleView>, ShakedownError> {
    let Some(rings) = context.resource_stats_rings() else {
        return Err(ShakedownError::ResourceAssertionFailed {
            container: container_name.to_string(),
            metric: "resource_stats".to_string(),
            measured:
                "no stats ring (run under Docker-backed resources or `services` with diagnostics)"
                    .to_string(),
            threshold: "attached stats ring".to_string(),
            phase: Phase::Run,
        });
    };
    let guard = rings.lock().map_err(|_| {
        ShakedownError::Internal(InternalError::PoisonedMutex(
            "resource stats rings (assertion)".to_string(),
        ))
    })?;
    let scenario_id = context.scenario_id().as_str();
    let mut out = Vec::new();
    for ring in guard.values() {
        for event in ring.iter() {
            if let InfraEvent::ResourceSample {
                container,
                phase,
                scenario_id: sample_scenario,
                cpu_pct,
                mem_bytes,
                ..
            } = event
            {
                if container == container_name
                    && (sample_scenario.is_empty() || sample_scenario == scenario_id)
                {
                    out.push(ResourceSampleView {
                        phase: *phase,
                        cpu_pct: *cpu_pct,
                        mem_bytes: *mem_bytes,
                    });
                }
            }
        }
    }
    Ok(out)
}

struct ResourceSampleView {
    phase: Phase,
    cpu_pct: f64,
    mem_bytes: u64,
}

fn peak_memory_and_phase(samples: &[ResourceSampleView]) -> Option<(u64, Phase)> {
    let mut best: Option<(u64, Phase)> = None;
    for sample in samples {
        match best {
            None => best = Some((sample.mem_bytes, sample.phase)),
            Some((peak, _)) if sample.mem_bytes > peak => {
                best = Some((sample.mem_bytes, sample.phase));
            }
            _ => {}
        }
    }
    best
}

fn format_bytes(bytes: u64) -> String {
    format!("{bytes} bytes")
}

#[cfg(test)]
mod tests {
    use super::super::ring_buffer::RingBuffer;
    use super::*;
    use crate::diagnostics::SharedResourceStatsRings;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn peak_memory_under_passes_when_below_threshold() {
        let rings: SharedResourceStatsRings = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let mut ring = RingBuffer::new(8);
        ring.push(InfraEvent::ResourceSample {
            container: "c1".to_string(),
            phase: Phase::Run,
            timestamp: 1,
            scenario_id: "scn".to_string(),
            cpu_pct: 1.0,
            mem_bytes: 100,
            mem_limit: 200,
            net_rx: 0,
            net_tx: 0,
        });
        rings.lock().expect("lock").insert("id1".to_string(), ring);

        let mut context = Context::new();
        context.set_scenario_id("scn");
        context.set_resource_stats_rings(Some(rings));

        context
            .assert_resource("c1")
            .peak_memory_under(500)
            .expect("peak_memory_under");
    }

    #[test]
    fn peak_memory_under_fails_when_over_threshold() {
        let rings: SharedResourceStatsRings = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let mut ring = RingBuffer::new(8);
        ring.push(InfraEvent::ResourceSample {
            container: "c1".to_string(),
            phase: Phase::Seed,
            timestamp: 1,
            scenario_id: "scn".to_string(),
            cpu_pct: 1.0,
            mem_bytes: 900,
            mem_limit: 1000,
            net_rx: 0,
            net_tx: 0,
        });
        rings.lock().expect("lock").insert("id1".to_string(), ring);

        let mut context = Context::new();
        context.set_scenario_id("scn");
        context.set_resource_stats_rings(Some(rings));

        let error = context
            .assert_resource("c1")
            .peak_memory_under(500)
            .expect_err("expected failure");
        assert!(matches!(
            error,
            ShakedownError::ResourceAssertionFailed {
                phase: Phase::Seed,
                ..
            }
        ));
    }

    #[test]
    fn avg_cpu_under_computes_mean() {
        let rings: SharedResourceStatsRings = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let mut ring = RingBuffer::new(8);
        for (index, cpu) in [10.0_f64, 20.0, 30.0].iter().enumerate() {
            ring.push(InfraEvent::ResourceSample {
                container: "c2".to_string(),
                phase: Phase::Run,
                timestamp: index as u64,
                scenario_id: "scn2".to_string(),
                cpu_pct: *cpu,
                mem_bytes: 1,
                mem_limit: 2,
                net_rx: 0,
                net_tx: 0,
            });
        }
        rings.lock().expect("lock").insert("id1".to_string(), ring);

        let mut context = Context::new();
        context.set_scenario_id("scn2");
        context.set_resource_stats_rings(Some(rings));

        context
            .assert_resource("c2")
            .avg_cpu_under(25.0)
            .expect("avg should be 20%");
        assert!(context.assert_resource("c2").avg_cpu_under(19.0).is_err());
    }
}
