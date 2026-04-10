//! # End-to-end adoption: resources, declarative mock, service-under-test, assertions
//!
//! This file is the **macro-first** version teams usually keep in their own crate: it uses
//! [`#[shakedown::test]`](shakedown::test) with **`resources = [Postgres, Kafka]`** (Docker-backed
//! tests are auto-`#[ignore]` unless you set `ignore = false` on the attribute).
//!
//! **What it demonstrates**
//!
//! 1. **Resources** — Postgres + Kafka provisioned by the harness (topic prefixing, scenario DB).
//! 2. **Prelude** — register an [`shakedown::HttpMockService`] with [`respond_with`](shakedown::HttpMockServiceRoutePending::respond_with)
//!    and custom headers, then register [`shakedown::ServiceUnderTest`] with
//!    [`EnvMapping::kafka_topic`](shakedown::EnvMapping::kafka_topic) and
//!    [`EnvMapping::mock_service`](shakedown::EnvMapping::mock_service).
//! 3. **Body** — HTTP checks against `traefik/whoami` plus Postgres helper assertions on the same
//!    scenario database.
//!
//! With Docker available:
//!
//! ```text
//! cargo test -p shakedown-examples --test service_with_mock --features kafka,postgres,mock-services,services -- --ignored
//! ```

#[shakedown::test(
    resources = [Postgres, Kafka],
    prelude = {
        shakedown_examples::register_adoption_example_prelude(&mut ctx)
            .expect("register mock + service under test");
    }
)]
async fn service_with_mock(ctx: &mut shakedown::Context) {
    shakedown_examples::adoption_example_body(ctx).await;
}
