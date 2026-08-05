# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(with Cargo’s `0.x` flexibility until 1.0 — see the workspace README).

## [0.1.0] — 2026-08-05

### Added

- Initial public release of **`shakedown`** and **`shakedown-macros`** on crates.io.
- Progressive confidence scenario lifecycle: provision → readiness → seed → run → assert → teardown via `run_scenario` and `#[shakedown::test]` / `#[shakedown::faulty_test]`.
- Opt-in Cargo features for real infrastructure (Kafka, Postgres, Redis, RabbitMQ, SQLite, and more), mock services (default), services under test, outbox/publication verification, fault injection, load, and cluster layouts.
- Structured errors separating scenario failures from infrastructure failures, plus failure timelines for CI evidence.
- Separate proc-macro crate with trybuild compile-fail coverage for attribute ergonomics.

[0.1.0]: https://github.com/sebastianconcept/shakedown/releases/tag/v0.1.0
