//! `pre_start` runs after seed and before the service container starts (`services` feature).
//!
//! ```text
//! cargo test -p shakedown --features services --test services_pre_start -- --ignored
//! ```

use std::sync::Arc;
use std::sync::Mutex;

use shakedown::{run_scenario, Context, EventLog, HealthCheck, ServiceUnderTest};

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features services --test services_pre_start -- --ignored"]
async fn pre_start_runs_before_container_in_declaration_order() {
    let observed = Arc::new(Mutex::new(Vec::<u8>::new()));

    let specification = ServiceUnderTest::docker("traefik/whoami:v1.10")
        .name("web")
        .port(80)
        .health_check(HealthCheck::http("/"))
        .auto_inject(false)
        .pre_start({
            let observed = observed.clone();
            move |_context| {
                let observed = observed.clone();
                async move {
                    observed.lock().unwrap().push(1);
                    Ok(())
                }
            }
        })
        .pre_start({
            let observed = observed.clone();
            move |_context| {
                let observed = observed.clone();
                async move {
                    observed.lock().unwrap().push(2);
                    Ok(())
                }
            }
        })
        .build()
        .unwrap_or_else(|error| panic!("spec must build: {error:?}"));

    let mut context = Context::new();
    context
        .register_service(specification)
        .unwrap_or_else(|error| panic!("register_service: {error:?}"));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut context,
        "services_pre_start_order",
        |_scenario_context| Box::pin(async move {}),
        &mut event_log,
    )
    .await
    .unwrap_or_else(|error| panic!("run_scenario: {error:?}"));

    let guard = observed.lock().unwrap();
    assert_eq!(
        guard.as_slice(),
        &[1, 2],
        "pre_start hooks run in declaration order before the container is up"
    );
}
