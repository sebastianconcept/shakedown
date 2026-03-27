//! Isolated binary: one test, fresh process so [`suite_diagnostics_handle`] is not warmed by Docker tests.
//!
//! Proves [`run_scenario`] completes successfully when the Docker socket is unreachable and
//! diagnostics degrade gracefully (no background monitors, no infra events).

use shakedown::{run_scenario, BuiltinResource, Context, EventLog, FakeResource, LogEntry};

#[tokio::test]
async fn run_scenario_succeeds_when_docker_socket_unreachable() {
    let previous = std::env::var("DOCKER_HOST").ok();
    let socket_path = std::env::temp_dir().join(format!(
        "shakedown-graceful-scenario-{}.sock",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&socket_path);
    std::env::set_var("DOCKER_HOST", format!("unix://{}", socket_path.display()));

    let mut context = Context::new();
    context.register_builtin(BuiltinResource::Fake(FakeResource::default()));
    let mut event_log = EventLog::new();

    let outcome = run_scenario(
        &mut context,
        "graceful_no_diagnostics",
        |_context: &mut Context| Box::pin(async {}),
        &mut event_log,
    )
    .await;

    match previous {
        Some(value) => std::env::set_var("DOCKER_HOST", value),
        None => std::env::remove_var("DOCKER_HOST"),
    }

    outcome.expect("scenario should succeed when diagnostics are unavailable");
    let infrastructure_entries: Vec<_> = event_log
        .entries()
        .iter()
        .filter(|entry| matches!(entry, LogEntry::Infrastructure(_)))
        .collect();
    assert!(
        infrastructure_entries.is_empty(),
        "expected no InfraEvent records when Docker is unreachable; got {infrastructure_entries:?}"
    );
}
