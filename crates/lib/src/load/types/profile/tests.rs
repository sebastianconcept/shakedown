use std::time::Duration;

use crate::load::error::LoadError;

use super::{LoadProfile, LoadRuntime, Phase, Rate, RecoveryConfig, SaturationPolicy};

#[test]
fn build_succeeds_for_valid_open_model_profile() {
    let profile = LoadProfile::open_model()
        .phase(Phase::ramp(
            Duration::from_secs(10),
            Rate::per_second(0.5),
            Rate::per_second(100.0),
        ))
        .phase(Phase::constant(Duration::from_secs(30), Rate::per_second(100.0)).named("baseline"))
        .phase(Phase::constant(
            Duration::from_secs(60),
            Rate::per_second(100.0),
        ))
        .workers(50)
        .max_workers(200)
        .on_saturation(SaturationPolicy::Drop)
        .build()
        .expect("valid profile");

    assert_eq!(profile.phases().len(), 3);
    assert_eq!(profile.workers(), 50);
    assert_eq!(profile.max_workers(), 200);
    assert_eq!(profile.saturation_policy(), SaturationPolicy::Drop);
    assert!(matches!(profile.runtime(), LoadRuntime::Shared));
    assert_eq!(profile.phases()[1].name.as_deref(), Some("baseline"));
}

#[test]
fn build_fails_without_phases() {
    let error = LoadProfile::open_model()
        .workers(2)
        .max_workers(2)
        .build()
        .expect_err("expected NoPhases");
    assert!(matches!(error, LoadError::NoPhases));
}

#[test]
fn build_fails_when_workers_zero() {
    let error = LoadProfile::open_model()
        .phase(Phase::pause(Duration::from_secs(1)))
        .workers(0)
        .max_workers(1)
        .build()
        .expect_err("expected InvalidWorkers");
    assert!(matches!(error, LoadError::InvalidWorkers { workers: 0 }));
}

#[test]
fn build_fails_when_max_workers_less_than_workers() {
    let error = LoadProfile::open_model()
        .phase(Phase::pause(Duration::from_secs(1)))
        .workers(10)
        .max_workers(5)
        .build()
        .expect_err("expected MaxWorkersLessThanWorkers");
    assert!(matches!(
        error,
        LoadError::MaxWorkersLessThanWorkers {
            workers: 10,
            max_workers: 5
        }
    ));
}

#[test]
fn default_worker_bounds_are_one_and_valid() {
    let profile = LoadProfile::open_model()
        .phase(Phase::constant(
            Duration::from_secs(1),
            Rate::per_second(1.0),
        ))
        .build()
        .expect("valid");
    assert_eq!(profile.workers(), 1);
    assert_eq!(profile.max_workers(), 1);
}

#[test]
fn saturation_queue_and_error_round_trip() {
    let queue = LoadProfile::open_model()
        .phase(Phase::pause(Duration::from_millis(1)))
        .on_saturation(SaturationPolicy::Queue)
        .build()
        .unwrap();
    assert_eq!(queue.saturation_policy(), SaturationPolicy::Queue);

    let error_policy = LoadProfile::open_model()
        .phase(Phase::pause(Duration::from_millis(1)))
        .on_saturation(SaturationPolicy::Error)
        .build()
        .unwrap();
    assert_eq!(error_policy.saturation_policy(), SaturationPolicy::Error);
}

#[test]
fn closed_model_phases_validate_positive_vus() {
    let ok = LoadProfile::open_model()
        .phase(Phase::constant_vus(Duration::from_secs(1), 5))
        .phase(Phase::ramp_vus(Duration::from_secs(2), 2, 8))
        .build()
        .expect("valid VU phases");
    assert_eq!(ok.phases().len(), 2);

    let vu_zero = LoadProfile::open_model()
        .phase(Phase::constant_vus(Duration::from_secs(1), 0))
        .build()
        .expect_err("invalid vu");
    assert!(matches!(vu_zero, LoadError::InvalidVuCount { count: 0 }));

    let vu_ramp_zero = LoadProfile::open_model()
        .phase(Phase::ramp_vus(Duration::from_secs(1), 0, 5))
        .build()
        .expect_err("invalid ramp");
    assert!(matches!(
        vu_ramp_zero,
        LoadError::InvalidVuRamp { from: 0, to: 5 }
    ));
}

#[test]
fn warmup_first_then_constant_builds() {
    let profile = LoadProfile::open_model()
        .phase(Phase::warmup(Duration::from_secs(2), Rate::per_second(10.0)).named("warm"))
        .phase(Phase::constant(
            Duration::from_secs(1),
            Rate::per_second(5.0),
        ))
        .build()
        .expect("valid profile with warmup");

    assert!(matches!(profile.phases()[0].phase, Phase::Warmup { .. }));
    assert_eq!(profile.phases()[0].name.as_deref(), Some("warm"));
    assert!(matches!(profile.phases()[1].phase, Phase::Constant { .. }));
}

#[test]
fn build_rejects_warmup_not_first() {
    let error = LoadProfile::open_model()
        .phase(Phase::constant(
            Duration::from_secs(1),
            Rate::per_second(1.0),
        ))
        .phase(Phase::warmup(Duration::from_secs(1), Rate::per_second(1.0)))
        .build()
        .expect_err("warmup must be first");
    assert!(matches!(error, LoadError::WarmupNotFirst));
}

#[test]
fn build_rejects_multiple_warmups() {
    let error = LoadProfile::open_model()
        .phase(Phase::warmup(Duration::from_secs(1), Rate::per_second(1.0)))
        .phase(Phase::warmup(Duration::from_secs(1), Rate::per_second(1.0)))
        .build()
        .expect_err("at most one warmup");
    assert!(matches!(error, LoadError::MultipleWarmups));
}

#[test]
fn warmup_zero_duration_invalid() {
    let error = LoadProfile::open_model()
        .phase(Phase::warmup(Duration::ZERO, Rate::per_second(1.0)))
        .build()
        .expect_err("warmup duration must be positive");
    assert!(matches!(error, LoadError::WarmupDurationInvalid));
}

#[test]
fn warmup_invalid_rate_rejected() {
    let error = LoadProfile::open_model()
        .phase(Phase::warmup(Duration::from_secs(1), Rate::per_second(0.0)))
        .build()
        .expect_err("invalid rate");
    assert!(matches!(error, LoadError::InvalidRate { rate } if rate == 0.0));
}

#[test]
fn recovery_timeout_round_trips_on_the_profile() {
    let configured = Duration::from_millis(250);
    let profile = LoadProfile::open_model()
        .phase(Phase::constant_vus(Duration::from_millis(10), 2))
        .recovery(RecoveryConfig::new(configured))
        .build()
        .expect("profile with recovery budget");
    assert_eq!(profile.recovery().timeout, configured);

    let defaulted = LoadProfile::open_model()
        .phase(Phase::pause(Duration::from_millis(1)))
        .build()
        .expect("default profile");
    assert_eq!(defaulted.recovery().timeout, Duration::from_secs(60));
}

#[test]
fn hdrhistogram_crate_is_linked_for_load_feature() {
    let mut histogram = hdrhistogram::Histogram::<u64>::new(3).expect("histogram");
    histogram.record(42).expect("record");
}
