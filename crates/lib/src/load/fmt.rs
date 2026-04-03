//! Shared human-readable formatting for load metrics (assertions, summaries).

use std::time::Duration;

pub(crate) fn format_duration(duration: Duration) -> String {
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

pub(crate) fn format_throughput(ops_per_s: f64) -> String {
    format!("{ops_per_s:.2}/s")
}

pub(crate) fn format_rate_pct(rate: f64) -> String {
    format!("{:.2}%", rate * 100.0)
}
