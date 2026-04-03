//! Fluent performance assertions on [`super::metrics::LoadMetrics`] (US-003).

use std::cell::RefCell;
use std::io::IsTerminal;
use std::time::Duration;

use crate::error::{PerformanceAssertionDetails, ShakedownError};

use super::metrics::LoadMetrics;
use super::{AssertionVerdict, Phase};

/// Chainable SLO-style checks on steady-state [`LoadMetrics`] (warmup excluded when configured).
#[derive(Debug)]
pub struct LoadMetricsAssertBuilder<'a> {
    metrics: &'a LoadMetrics,
    load_name: String,
    correlation_phase: Phase,
    verdicts: RefCell<Vec<AssertionVerdict>>,
}

impl LoadMetrics {
    /// Start performance assertions on this snapshot (steady-state aggregates when warmup is configured).
    ///
    /// Failed checks return [`ShakedownError::PerformanceAssertionFailed`](crate::error::ShakedownError::PerformanceAssertionFailed)
    /// with `phase` set to [`Phase::pause`](Phase::pause)([`Duration::ZERO`]) for correlation. Use
    /// [`Self::assert_with_phase`](LoadMetrics::assert_with_phase) to attach a specific [`Phase`] label.
    pub fn assert<'a>(&'a self, load_name: impl Into<String>) -> LoadMetricsAssertBuilder<'a> {
        LoadMetricsAssertBuilder::new(self, load_name.into(), Phase::pause(Duration::ZERO))
    }

    /// Same as [`Self::assert`](LoadMetrics::assert) but records `phase` on assertion failures (e.g. the profile phase under test).
    pub fn assert_with_phase<'a>(
        &'a self,
        load_name: impl Into<String>,
        phase: Phase,
    ) -> LoadMetricsAssertBuilder<'a> {
        LoadMetricsAssertBuilder::new(self, load_name.into(), phase)
    }
}

impl<'a> LoadMetricsAssertBuilder<'a> {
    pub(crate) fn new(
        metrics: &'a LoadMetrics,
        load_name: String,
        correlation_phase: Phase,
    ) -> Self {
        Self {
            metrics,
            load_name,
            correlation_phase,
            verdicts: RefCell::new(Vec::new()),
        }
    }

    /// Clone of recorded verdicts (for summaries and reports).
    pub fn verdicts_snapshot(&self) -> Vec<AssertionVerdict> {
        self.verdicts.borrow().clone()
    }

    /// Copy verdicts onto the shared metrics accumulator so [`LoadMetrics::write_report`] includes an `assertions` array (US-005).
    pub fn store_for_report(&self) {
        let rows: Vec<super::ReportAssertion> = self
            .verdicts_snapshot()
            .iter()
            .map(super::ReportAssertion::from)
            .collect();
        self.metrics.set_report_assertions(rows);
    }

    /// Print load summary including assertion verdict rows to stderr when `SHAKEDOWN_LOAD_SUMMARY` is not `0` (US-004).
    pub fn print_summary_to_stderr(&self) {
        if !super::summary::load_summary_print_enabled() {
            return;
        }
        let use_color = std::io::stderr().is_terminal();
        let verdicts = self.verdicts_snapshot();
        let text = super::summary::render_summary_styled(
            &self.load_name,
            self.metrics,
            Some(&verdicts),
            use_color,
        );
        eprintln!("{text}");
    }

    fn fail(
        &self,
        metric: impl Into<String>,
        measured: String,
        threshold: String,
    ) -> ShakedownError {
        ShakedownError::PerformanceAssertionFailed(Box::new(PerformanceAssertionDetails {
            load_name: self.load_name.clone(),
            metric: metric.into(),
            measured,
            threshold,
            phase: self.correlation_phase.clone(),
        }))
    }

    fn record_pass(&self, metric: String, measured: String, threshold: String) {
        self.verdicts.borrow_mut().push(AssertionVerdict {
            metric,
            measured,
            threshold,
            passed: true,
        });
    }

    fn check(
        &self,
        metric: impl Into<String>,
        measured: String,
        threshold: String,
        ok: bool,
    ) -> Result<&Self, ShakedownError> {
        let metric = metric.into();
        if ok {
            self.record_pass(metric.clone(), measured.clone(), threshold.clone());
            Ok(self)
        } else {
            Err(self.fail(metric, measured, threshold))
        }
    }

    /// p99 latency must be at most `limit` (inclusive SLO ceiling).
    pub fn p99_under(&self, limit: Duration) -> Result<&Self, ShakedownError> {
        let measured = self.metrics.p99();
        let measured_s = format_duration(measured);
        let threshold_s = format_duration(limit);
        self.check(
            "p99",
            measured_s,
            format!("<= {threshold_s}"),
            measured <= limit,
        )
    }

    /// p95 latency must be at most `limit`.
    pub fn p95_under(&self, limit: Duration) -> Result<&Self, ShakedownError> {
        let measured = self.metrics.p95();
        let measured_s = format_duration(measured);
        let threshold_s = format_duration(limit);
        self.check(
            "p95",
            measured_s,
            format!("<= {threshold_s}"),
            measured <= limit,
        )
    }

    /// p50 latency must be at most `limit`.
    pub fn p50_under(&self, limit: Duration) -> Result<&Self, ShakedownError> {
        let measured = self.metrics.p50();
        let measured_s = format_duration(measured);
        let threshold_s = format_duration(limit);
        self.check(
            "p50",
            measured_s,
            format!("<= {threshold_s}"),
            measured <= limit,
        )
    }

    /// Completed iterations per second (steady-state window) must be at least `minimum`.
    pub fn throughput_above(&self, minimum: f64) -> Result<&Self, ShakedownError> {
        let measured = self.metrics.throughput();
        let measured_s = format_throughput(measured);
        let threshold_s = format_throughput(minimum);
        self.check(
            "throughput",
            measured_s,
            format!(">= {threshold_s}"),
            measured >= minimum,
        )
    }

    /// Failed / completed ratio must be at most `limit` (e.g. `0.01` for 1%).
    pub fn error_rate_under(&self, limit: f64) -> Result<&Self, ShakedownError> {
        let measured = self.metrics.error_rate();
        let measured_s = format_rate_pct(measured);
        let threshold_s = format_rate_pct(limit);
        self.check(
            "error_rate",
            measured_s,
            format!("<= {threshold_s}"),
            measured <= limit,
        )
    }

    /// No dropped iterations (open-model saturation drops).
    pub fn no_drops(&self) -> Result<&Self, ShakedownError> {
        let drops = self.metrics.dropped_iterations();
        self.check("drops", format!("{drops}"), "0".to_string(), drops == 0)
    }

    /// Dropped iterations must be at most `maximum` (inclusive).
    pub fn drops_under(&self, maximum: u64) -> Result<&Self, ShakedownError> {
        let drops = self.metrics.dropped_iterations();
        self.check(
            "drops",
            format!("{drops}"),
            format!("<= {maximum}"),
            drops <= maximum,
        )
    }

    /// When present, time from last failure to next success must be at most `limit`.
    /// If there were no failures, this check passes.
    pub fn recovery_under(&self, limit: Duration) -> Result<&Self, ShakedownError> {
        match self.metrics.recovery_time() {
            None => {
                self.record_pass(
                    "recovery".to_string(),
                    "n/a (no failure burst)".to_string(),
                    format!("<= {}", format_duration(limit)),
                );
                Ok(self)
            }
            Some(recovery) => {
                let measured_s = format_duration(recovery);
                let threshold_s = format_duration(limit);
                self.check(
                    "recovery",
                    measured_s,
                    format!("<= {threshold_s}"),
                    recovery <= limit,
                )
            }
        }
    }

    /// When present, longest consecutive failure burst must be at most `limit`.
    /// If there was no failure burst, this check passes.
    pub fn error_burst_under(&self, limit: Duration) -> Result<&Self, ShakedownError> {
        match self.metrics.error_burst_duration() {
            None => {
                self.record_pass(
                    "error_burst".to_string(),
                    "n/a (no burst)".to_string(),
                    format!("<= {}", format_duration(limit)),
                );
                Ok(self)
            }
            Some(burst) => {
                let measured_s = format_duration(burst);
                let threshold_s = format_duration(limit);
                self.check(
                    "error_burst",
                    measured_s,
                    format!("<= {threshold_s}"),
                    burst <= limit,
                )
            }
        }
    }

    /// Ratio p99/p50 must be at most `max_ratio` (requires p50 > 0 when p99 > 0).
    pub fn tail_ratio_under(&self, max_ratio: f64) -> Result<&Self, ShakedownError> {
        let p50 = self.metrics.p50();
        let p99 = self.metrics.p99();
        let ratio = tail_ratio(p50, p99);
        let measured_s = format!("{ratio:.3}");
        let threshold_s = format!("{max_ratio:.3}");
        let ok = ratio <= max_ratio && ratio.is_finite();
        self.check(
            "tail_ratio (p99/p50)",
            measured_s,
            format!("<= {threshold_s}"),
            ok,
        )
    }
}

fn tail_ratio(p50: Duration, p99: Duration) -> f64 {
    let p50_ns = p50.as_nanos().max(1) as f64;
    let p99_ns = p99.as_nanos() as f64;
    p99_ns / p50_ns
}

fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs_f64();
    if seconds >= 1.0 {
        format!("{seconds:.2}s")
    } else if duration.subsec_millis() > 0 || seconds >= 0.001 {
        format!("{:.2}ms", seconds * 1000.0)
    } else if duration.subsec_micros() > 0 {
        format!("{}µs", duration.subsec_micros())
    } else {
        format!("{duration:?}")
    }
}

fn format_throughput(ops_per_s: f64) -> String {
    format!("{ops_per_s:.2}/s")
}

fn format_rate_pct(rate: f64) -> String {
    format!("{:.2}%", rate * 100.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use super::super::metrics::LoadMetricsAccumulator;
    use super::super::Rate;

    fn sample_metrics() -> LoadMetrics {
        let epoch = std::time::Instant::now();
        let accumulator = Arc::new(LoadMetricsAccumulator::new_with_epoch(epoch));
        for index in 0..50u64 {
            let completion = epoch + Duration::from_millis(index);
            accumulator.record_success(completion, Duration::from_micros(500 + index * 10));
        }
        LoadMetrics::new(accumulator)
    }

    #[test]
    fn assertion_chain_passes() {
        let metrics = sample_metrics();
        let assertions = metrics.assert("checkout");
        assertions
            .p99_under(Duration::from_millis(50))
            .expect("p99")
            .p50_under(Duration::from_millis(5))
            .expect("p50")
            .throughput_above(1.0)
            .expect("throughput")
            .error_rate_under(0.5)
            .expect("error_rate")
            .no_drops()
            .expect("no_drops")
            .recovery_under(Duration::from_secs(10))
            .expect("recovery")
            .error_burst_under(Duration::from_secs(10))
            .expect("burst")
            .tail_ratio_under(100.0)
            .expect("tail");
        let verdicts = assertions.verdicts_snapshot();
        assert!(verdicts.iter().all(|verdict| verdict.passed));
    }

    #[test]
    fn assertion_chain_question_mark_operator() {
        fn run(metrics: &LoadMetrics) -> Result<(), ShakedownError> {
            metrics
                .assert("q")
                .p99_under(Duration::from_secs(1))?
                .error_rate_under(1.0)?;
            Ok(())
        }
        run(&sample_metrics()).expect("chain with ?");
    }

    #[test]
    fn p99_under_fails_with_structured_error() {
        let metrics = sample_metrics();
        let phase = Phase::constant(Duration::from_secs(1), Rate::per_second(1.0));
        let error = metrics
            .assert_with_phase("cart", phase.clone())
            .p99_under(Duration::from_nanos(1))
            .expect_err("expected failure");
        match error {
            ShakedownError::PerformanceAssertionFailed(details) => {
                assert_eq!(details.load_name, "cart");
                assert_eq!(details.metric, "p99");
                assert!(!details.measured.is_empty());
                assert!(details.threshold.contains('<'));
                assert_eq!(details.phase, phase);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn drops_under_records_failure() {
        let epoch = std::time::Instant::now();
        let accumulator = Arc::new(LoadMetricsAccumulator::new_with_epoch(epoch));
        accumulator.record_dropped(epoch);
        accumulator.record_dropped(epoch);
        let metrics = LoadMetrics::new(accumulator);
        let error = metrics.assert("x").drops_under(0).expect_err("drops");
        assert!(matches!(
            error,
            ShakedownError::PerformanceAssertionFailed(details) if details.metric == "drops"
        ));
    }
}
