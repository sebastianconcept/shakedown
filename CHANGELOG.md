# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(with Cargo’s `0.x` flexibility until 1.0 — see the workspace README).

## 0.1.0 — not published

`shakedown` 0.1.0 and `shakedown-macros` 0.1.0 have not been published to crates.io. This commit does not publish them. There is no GitHub release for `v0.1.0`.

### Added

- Crate surface prepared for **`shakedown`** and **`shakedown-macros`**. Still unpublished.
- Scenario lifecycle: provision → readiness → seed → run → assert → teardown via `run_scenario` and `#[shakedown::test]` / `#[shakedown::faulty_test]`. The assert phase logs that it started. It does not run user assertions; those stay in the scenario body.
- Opt-in Cargo features for real infrastructure (Kafka, Postgres, Redis, RabbitMQ, SQLite, and more), mock services (default), services under test, outbox/publication verification, fault injection, load, and cluster layouts (`kafka-cluster`, `postgres-cluster`, `rabbitmq-cluster`, `redis-sentinel`, `mongodb-cluster`).
- Structured errors separating scenario failures from infrastructure failures, plus failure timelines for CI evidence.
- Separate proc-macro crate with trybuild compile-fail coverage for attribute ergonomics. `#[shakedown::register_resource]` and `default_greeting!` are not part of the 0.1 contract.
- Load runs: closed-model `LoadPhase::constant_vus` virtual users repeat their work on the shared runtime until the phase ends; they ignore `workers`, `max_workers`, and the saturation policy. `LoadRuntime::Dedicated` is rejected with `LoadError::UnsupportedRuntime` before a handle or timeline row exists. A panicking virtual user fails the run with `LoadError::DriverFailed`. A virtual user still mid-iteration 5 seconds after the phase ends, shutdown arrives, or the run fails is aborted instead of holding the phase open.
- Load runs: opt-in recovery budget via `LoadProfileBuilder::recovery(RecoveryConfig)`. The driver stops with `LoadError::RecoveryTimeout` when the rolling error rate stays above the budget's threshold for its whole timeout, in open-model scheduling (including a `SaturationPolicy::Queue` wait) and closed-model runs. Paused time is not charged. Without the call a run is never stopped because of its error rate.
- Load runs: `LoadHandle::await_phase` returns `LoadError::DriverStopped` when the driver ends before reaching the phase (for example after a `RecoveryTimeout`) instead of never returning. Dropping a `LoadHandle` aborts a driver that has not stopped within 5 seconds, and no longer panics outside a Tokio runtime.
