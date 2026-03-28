//! Integration tests for Docker-backed infrastructure diagnostics (US-010).
//!
//! **No Docker:** see `tests/diagnostics_graceful.rs` (runs under `cargo test -p shakedown --features kafka`).
//!
//! **Docker:** ignored tests require a working engine. Run:
//!
//! ```text
//! cargo test -p shakedown --features kafka --test diagnostics_integration -- --ignored
//! ```

use serial_test::serial;

#[cfg(not(feature = "no-diagnostics"))]
mod bollard_diagnostics_tests {
    use async_trait::async_trait;
    use bollard::query_parameters::{ListContainersOptionsBuilder, StopContainerOptionsBuilder};
    use serial_test::serial;
    use testcontainers::core::WaitFor;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{GenericImage, ImageExt};

    use shakedown::diagnostics::labels::{
        SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE,
    };
    use shakedown::error::{InfrastructureError, ResourceHandle, ScenarioId, ShakedownError};
    use shakedown::event::{DockerEventKind, InfraEvent, LogEntry};
    use shakedown::resources::Resource;
    use shakedown::{run_scenario, Context, EventLog};

    const STDOUT_MARKER: &str = "SHAKEDOWN_DIAG_STDOUT";

    /// Alpine container with Shakedown labels; prints [`STDOUT_MARKER`] then sleeps.
    #[derive(Default)]
    struct LabeledAlpineSleep {
        container: Option<testcontainers::ContainerAsync<GenericImage>>,
    }

    #[async_trait]
    impl Resource for LabeledAlpineSleep {
        async fn provision(
            &mut self,
            _scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            let container = GenericImage::new("alpine", "3.20")
                .with_wait_for(WaitFor::message_on_stdout(STDOUT_MARKER))
                .with_cmd(vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    format!("echo {STDOUT_MARKER} && sleep 600"),
                ])
                .with_label(SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE)
                .start()
                .await
                .map_err(|error| InfrastructureError::Provision {
                    resource: "LabeledAlpineSleep".to_string(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;
            self.container = Some(container);
            Ok(vec![ResourceHandle {
                id: "alpine".to_string(),
                connection_string: String::new(),
                metadata: None,
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if let Some(container) = self.container.take() {
                let _ = container.stop_with_timeout(Some(2)).await;
            }
            Ok(())
        }
    }

    fn infra_events(log: &EventLog) -> Vec<&InfraEvent> {
        log.entries()
            .iter()
            .filter_map(|entry| match entry {
                LogEntry::Infrastructure(infra) => Some(infra),
                _ => None,
            })
            .collect()
    }

    /// Wait until stats rings contain samples, then assert peak memory (avoids racing the stats poller).
    async fn assert_peak_memory_under_when_sampled(
        ctx: &mut Context,
        container_name: &str,
        max_bytes: u64,
    ) {
        use std::time::{Duration, Instant};

        let deadline = Instant::now() + Duration::from_secs(25);
        loop {
            match ctx
                .assert_resource(container_name)
                .peak_memory_under(max_bytes)
            {
                Ok(()) => return,
                Err(ShakedownError::ResourceAssertionFailed {
                    ref measured,
                    ref metric,
                    ..
                }) if metric == "peak_memory" && measured == "no samples" => {
                    if Instant::now() >= deadline {
                        panic!(
                            "timed out waiting for resource stats samples for container {container_name:?}"
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(400)).await;
                }
                Err(other) => panic!("peak_memory_under: {other}"),
            }
        }
    }

    async fn first_shakedown_container_id(docker: &bollard::Docker) -> Result<String, String> {
        let mut filters = std::collections::HashMap::new();
        filters.insert(
            "label".to_string(),
            vec![format!(
                "{SHAKEDOWN_DOCKER_LABEL_KEY}={SHAKEDOWN_DOCKER_LABEL_VALUE}"
            )],
        );
        filters.insert("status".to_string(), vec!["running".to_string()]);
        let options = ListContainersOptionsBuilder::default()
            .all(true)
            .filters(&filters)
            .build();
        let list = docker
            .list_containers(Some(options))
            .await
            .map_err(|error| error.to_string())?;
        let summary = list
            .into_iter()
            .next()
            .ok_or_else(|| "no labeled running container".to_string())?;
        summary.id.ok_or_else(|| "container id missing".to_string())
    }

    #[tokio::test]
    #[ignore = "requires Docker"]
    #[serial]
    async fn log_streaming_captures_container_stdout() {
        let mut ctx = Context::new();
        ctx.register_extension(LabeledAlpineSleep::default());
        let mut event_log = EventLog::new();

        run_scenario(
            &mut ctx,
            "diag_log_stream",
            |_: &mut Context| {
                Box::pin(async move { tokio::time::sleep(std::time::Duration::from_secs(3)).await })
            },
            &mut event_log,
        )
        .await
        .expect("scenario");

        let found = infra_events(&event_log).iter().any(|event| {
            matches!(
                event,
                InfraEvent::ContainerLog { line, .. } if line.contains(STDOUT_MARKER)
            )
        });
        assert!(
            found,
            "expected ContainerLog line containing {STDOUT_MARKER:?}, got {:?}",
            infra_events(&event_log)
        );
    }

    #[tokio::test]
    #[ignore = "requires Docker"]
    #[serial]
    async fn docker_stop_event_captured_in_event_log() {
        let mut ctx = Context::new();
        ctx.register_extension(LabeledAlpineSleep::default());
        let mut event_log = EventLog::new();

        run_scenario(
            &mut ctx,
            "diag_docker_stop",
            |_: &mut Context| {
                Box::pin(async move {
                    let handle = shakedown::suite_diagnostics_handle()
                        .await
                        .expect("diagnostics handle");
                    let docker = handle.docker();
                    let container_id = first_shakedown_container_id(docker)
                        .await
                        .expect("container id");
                    let options = StopContainerOptionsBuilder::default().t(10).build();
                    docker
                        .stop_container(&container_id, Some(options))
                        .await
                        .expect("stop container");
                    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
                })
            },
            &mut event_log,
        )
        .await
        .expect("scenario");

        let stop_recorded = infra_events(&event_log).iter().any(|event| {
            matches!(
                event,
                InfraEvent::DockerEvent {
                    kind: DockerEventKind::Other(stop),
                    ..
                } if stop == "stop"
            )
        });
        assert!(
            stop_recorded,
            "expected DockerEvent stop, got {:?}",
            infra_events(&event_log)
        );
    }

    #[tokio::test]
    #[ignore = "requires Docker"]
    #[serial]
    async fn resource_stats_collected_and_accessible_via_ring_buffer() {
        let mut ctx = Context::new();
        ctx.register_extension(LabeledAlpineSleep::default());
        let mut event_log = EventLog::new();

        run_scenario(
        &mut ctx,
        "diag_resource_stats",
        |ctx: &mut Context| {
            Box::pin(async move {
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                let handle = shakedown::suite_diagnostics_handle()
                    .await
                    .expect("diagnostics handle");
                let docker = handle.docker();
                let list = docker
                    .list_containers(Some(
                        ListContainersOptionsBuilder::default()
                            .all(true)
                            .filters(&std::collections::HashMap::from([
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
                let name = list
                    .first()
                    .and_then(|summary| summary.names.as_ref()?.first())
                    .map(|name| name.trim_start_matches('/').to_string())
                    .expect("container name");
                assert_peak_memory_under_when_sampled(ctx, &name, 512 * 1024 * 1024).await;
            })
        },
        &mut event_log,
    )
    .await
    .expect("scenario");

        let sample = infra_events(&event_log)
            .iter()
            .find_map(|event| match event {
                InfraEvent::ResourceSample { mem_bytes, .. } if *mem_bytes > 0 => Some(()),
                _ => None,
            });
        assert!(
            sample.is_some(),
            "expected at least one ResourceSample with memory, got {:?}",
            infra_events(&event_log)
        );
    }
}

#[tokio::test]
#[serial]
async fn try_connect_graceful_when_docker_unreachable() {
    let previous = std::env::var("DOCKER_HOST").ok();
    let socket_path = std::env::temp_dir().join(format!(
        "shakedown-unreachable-docker-{}.sock",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&socket_path);
    std::env::set_var("DOCKER_HOST", format!("unix://{}", socket_path.display()));
    let outcome = shakedown::try_connect().await;
    match previous {
        Some(value) => std::env::set_var("DOCKER_HOST", value),
        None => std::env::remove_var("DOCKER_HOST"),
    }
    assert!(
        outcome.is_none(),
        "expected None when Docker Engine is unreachable"
    );
}
