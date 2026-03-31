//! Service containers participate in bollard diagnostics (`services` feature).
//!
//! In-scenario [`Context::assert_resource`] is covered by unit tests in `diagnostics/assertions.rs`
//! (injected rings). This integration test asserts merged `ResourceSample` / `ContainerLog` names
//! after shutdown. Some Docker hosts omit stats samples until teardown or omit empty streams; logs
//! and the in-scenario label/correlation check still prove diagnostics wiring.
//!
//! ```text
//! cargo test -p shakedown --features services --test services_diagnostics -- --ignored
//! ```

#[cfg(not(feature = "no-diagnostics"))]
mod diagnostics_service_tests {
    use bollard::query_parameters::ListContainersOptionsBuilder;
    use shakedown::diagnostics::labels::{
        correlation_name_from_container_summary, SHAKEDOWN_DOCKER_LABEL_KEY,
        SHAKEDOWN_DOCKER_LABEL_VALUE, SHAKEDOWN_SERVICE_LABEL_KEY,
    };
    use shakedown::event::{InfraEvent, LogEntry};
    use shakedown::{
        render_failure_timeline, run_scenario, Context, EventLog, FailureTimelineOptions,
        HealthCheck, ServiceUnderTest,
    };
    use std::collections::HashMap;

    #[tokio::test]
    #[ignore = "requires Docker; run: cargo test -p shakedown --features services --test services_diagnostics -- --ignored"]
    async fn service_container_appears_in_merged_diagnostics() {
        // Given stats interval tightened and a labeled whoami service registered for the scenario
        std::env::set_var("SHAKEDOWN_STATS_INTERVAL_MS", "150");

        let specification = ServiceUnderTest::docker("traefik/whoami:v1.10")
            .name("web")
            .port(80)
            .health_check(HealthCheck::http("/"))
            .auto_inject(false)
            .build()
            .unwrap_or_else(|error| panic!("spec must build: {error:?}"));

        let mut context = Context::new();
        context
            .register_service(specification)
            .unwrap_or_else(|error| panic!("register_service: {error:?}"));
        let mut event_log = EventLog::new();

        run_scenario(
            &mut context,
            "services_diagnostics_whoami",
            |scenario_context| {
                Box::pin(async move {
                    let _url = scenario_context
                        .service_url("web")
                        .expect("service URL after readiness");
                    // Prove Docker lists `io.shakedown.service` and correlation matches the
                    // registration name (same rules as stats/logs/events).
                    let handle = shakedown::suite_diagnostics_handle()
                        .await
                        .expect("diagnostics handle");
                    let docker = handle.docker();
                    let list = docker
                        .list_containers(Some(
                            ListContainersOptionsBuilder::default()
                                .all(true)
                                .filters(&HashMap::from([
                                    (
                                        "label".to_string(),
                                        vec![format!(
                                            "{SHAKEDOWN_DOCKER_LABEL_KEY}={SHAKEDOWN_DOCKER_LABEL_VALUE}"
                                        )],
                                    ),
                                    ("status".to_string(), vec!["running".to_string()]),
                                ]))
                                .build(),
                        ))
                        .await
                        .expect("list");
                    let web_summary = list
                        .iter()
                        .find(|summary| {
                            summary.labels.as_ref().is_some_and(|labels| {
                                labels.get(SHAKEDOWN_SERVICE_LABEL_KEY).map(|value| value.as_str())
                                    == Some("web")
                            })
                        })
                        .expect("running service container with io.shakedown.service=web");
                    assert_eq!(
                        correlation_name_from_container_summary(web_summary),
                        "web",
                        "correlation name must match registered service name"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                })
            },
            &mut event_log,
        )
        .await
        .unwrap_or_else(|error| panic!("run_scenario: {error:?}"));

        // Then merged event log and timeline include the logical service name
        let mut saw_sample = false;
        let mut saw_log = false;
        for entry in event_log.entries() {
            if let LogEntry::Infrastructure(infra) = entry {
                match infra {
                    InfraEvent::ResourceSample { container, .. } if container == "web" => {
                        saw_sample = true;
                    }
                    InfraEvent::ContainerLog { container, .. } if container == "web" => {
                        saw_log = true;
                    }
                    _ => {}
                }
            }
        }
        assert!(
            saw_sample || saw_log,
            "expected merged ResourceSample and/or ContainerLog for logical service name `web` (stats streams vary by engine)"
        );

        let timeline = render_failure_timeline(
            &event_log,
            &FailureTimelineOptions::new("services_diagnostics_whoami"),
        );
        assert!(
            timeline.contains("web"),
            "expected failure timeline output to include logical service name `web` from merged diagnostics"
        );
    }
}

/// `assert_resource` is absent when `no-diagnostics` is enabled (e.g. `cargo test --all-features`).
#[cfg(feature = "no-diagnostics")]
#[test]
fn services_diagnostics_skipped_when_compiled_without_docker_observability() {
    // Compiles with `no-diagnostics` + `services` to ensure the integration test target stays valid.
}
