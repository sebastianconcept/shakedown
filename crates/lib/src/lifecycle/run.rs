//! Drive one scenario through provision, readiness, seed, run, assert, and teardown.
//!
//! This module is the ordered harness for [`run_scenario`].
//! This module is useful when you add a phase, change how a failure is classified, or wire a
//! new resource into the run.
//! This module does not define [`super::Context`] field accessors.

use crate::error::{InfrastructureError, ScenarioId, ShakedownError};
use crate::event::{EventLevel, EventLog};
use crate::resources::{BuiltinResource, FakeResource};

#[cfg(feature = "kafka-cluster")]
use super::cluster_registration::register_kafka_cluster_control;
#[cfg(feature = "mongodb-cluster")]
use super::cluster_registration::register_mongo_replica_set_cluster_control;
#[cfg(feature = "postgres-cluster")]
use super::cluster_registration::register_postgres_replica_set_cluster_control;
#[cfg(feature = "rabbitmq-cluster")]
use super::cluster_registration::register_rabbit_cluster_control;
#[cfg(feature = "redis-sentinel")]
use super::cluster_registration::register_redis_sentinel_cluster_control;
#[cfg(all(feature = "mock-services", feature = "grpc"))]
use super::context::mock_servers::shutdown_grpc_mock_service_servers_with_log;
#[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
use super::context::mock_servers::shutdown_tcp_mock_service_servers_with_log;
#[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
use super::context::mock_servers::shutdown_udp_mock_service_servers_with_log;
#[cfg(feature = "mock-services")]
use super::context::mock_servers::{
    reset_mock_services_at_run_start, shutdown_http_mock_service_servers_with_log,
    start_and_readiness_mock_services,
};
use super::context::Context;
#[cfg(all(feature = "fault-injection", feature = "kafka"))]
use super::fault_proxy::apply_fault_injection_proxy_rewrites;
#[cfg(feature = "fault-injection")]
use super::fault_proxy::install_fault_handle_if_toxiproxy_provisioned;
use super::phase::Phase;
use super::resource_bindings::*;
use super::run_diagnostics::LifecycleDiagnostics;
#[cfg(all(feature = "fault-injection", feature = "mongodb-cluster"))]
use super::teardown::prepare_mongo_replica_set_inter_node_defer;
use super::teardown::teardown_all_resources;

/// Final value produced by a [`run_scenario`] async body: `()` for legacy scenarios, or `Result<(), ShakedownError>` to fail without panicking (e.g. performance assertions).
///
/// Return `()` on success, or [`Result::Err`] with [`ShakedownError`] to fail the scenario without
/// panicking and to integrate with the failure timeline (for example `ShakedownError::PerformanceAssertionFailed`
/// when the `load` feature is enabled).
pub trait ScenarioRunOutcome: Send + 'static {
    /// Converts the scenario body's output into a harness result.
    fn into_shakedown_result(self) -> Result<(), ShakedownError>;
}

impl ScenarioRunOutcome for () {
    fn into_shakedown_result(self) -> Result<(), ShakedownError> {
        Ok(())
    }
}

impl ScenarioRunOutcome for Result<(), ShakedownError> {
    fn into_shakedown_result(self) -> Result<(), ShakedownError> {
        self
    }
}

fn emit_failure_timeline_on_infra_err(
    event_log: &EventLog,
    scenario_id: &ScenarioId,
    error: &InfrastructureError,
) {
    crate::timeline::emit_failure_timeline_to_stderr(
        event_log,
        scenario_id.as_str(),
        &error.to_string(),
    );
}

/// This function tears down resources, merges diagnostics, and returns the infrastructure error.
/// This function lets a failed phase leave a complete event log and timeline.
async fn abort_on_infrastructure_error(
    ctx: &mut Context,
    event_log: &mut EventLog,
    scenario_id: &ScenarioId,
    error: InfrastructureError,
    diagnostics: &mut LifecycleDiagnostics,
) -> Result<(), ShakedownError> {
    teardown_all_resources(ctx).await;
    diagnostics.merge_into(event_log).await?;
    emit_failure_timeline_on_infra_err(event_log, scenario_id, &error);
    Err(error.into())
}

/// Runs a scenario through all lifecycle phases.
///
/// This function executes the scenario in order:
/// - Provision resources (async)
/// - Check readiness (async)
/// - Seed data (async)
/// - Run scenario logic
/// - Assert outcomes
/// - Teardown resources (always attempted at the end, async)
///
/// # Errors
///
/// Returns an error if Provision or Readiness phases fail. The error type indicates the failure category:
/// - [`crate::error::ShakedownError`] (infrastructure category) for provisioning/readiness failures
///
/// If Provision or Readiness fails, teardown is attempted before returning the error.
///
/// # Lifecycle ordering
///
/// Phases always run in this order: provision → readiness → seed → run → assert → teardown
/// (summarized in the list at the start of this documentation).
///
/// # Event logging
///
/// Events emitted during the scenario run are appended to the provided [`EventLog`].
/// After completion, callers can retrieve all events for debugging or analysis.
///
/// If provision, readiness, or seed fails with an infrastructure error, diagnostics (when enabled)
/// are merged into the log first, then a human-readable failure timeline is written to stderr
/// (see [`crate::timeline::render_failure_timeline`]).
///
/// `scenario_fn` takes only `&mut Context` and must return a [`std::pin::Pin`]-boxed [`std::future::Future`]
/// so the run phase can borrow `Context` across `.await` while the harness keeps using
/// `event_log` after the scenario body completes.
///
/// # Example
///
/// ```rust,no_run
/// use shakedown::{Context, run_scenario, EventLog};
///
/// async fn my_scenario(ctx: &mut Context) {
///     // Scenario implementation
/// }
///
/// #[tokio::test]
/// async fn test_scenario() {
///     let mut ctx = Context::new();
///     let mut event_log = EventLog::new();
///
///     run_scenario(&mut ctx, "test-scenario", |c| Box::pin(my_scenario(c)), &mut event_log)
///         .await
///         .unwrap();
///
///     // Application-level events (`EventLog::entries` for the full log including infrastructure)
///     let _application_events: Vec<_> = event_log.events().collect();
/// }
/// ```
pub async fn run_scenario<F, O>(
    ctx: &mut Context,
    scenario_id: impl Into<String>,
    scenario_fn: F,
    event_log: &mut EventLog,
) -> Result<(), ShakedownError>
where
    F: for<'s> FnOnce(
        &'s mut Context,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = O> + Send + 's>>,
    O: ScenarioRunOutcome,
{
    // Update scenario ID
    ctx.set_scenario_id(scenario_id);

    #[cfg(feature = "mock-services")]
    reset_mock_services_at_run_start(ctx).await;

    #[cfg(all(
        any(
            feature = "kafka",
            feature = "rabbitmq",
            feature = "postgres",
            feature = "mariadb",
            feature = "mysql",
            feature = "redis",
            feature = "valkey",
            feature = "mongodb",
            feature = "nats",
            feature = "neo4j",
            feature = "elasticsearch",
            feature = "opensearch",
            feature = "qdrant",
            feature = "weaviate",
            feature = "influxdb",
            feature = "timescaledb",
            feature = "services",
        ),
        feature = "diagnostics",
        not(feature = "no-diagnostics"),
    ))]
    ctx.set_resource_stats_rings(None);

    let mut diagnostics = LifecycleDiagnostics::spawn(ctx).await;

    // Provision phase (async)
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting provision phase");

    #[cfg(all(feature = "fault-injection", feature = "mongodb-cluster"))]
    prepare_mongo_replica_set_inter_node_defer(ctx);

    let scenario_id = ctx.scenario_id().clone();
    let n = ctx.resources().len();
    for i in 0..n {
        let result = {
            let resource = &mut ctx.resources_mut()[i];
            resource.provision(&scenario_id).await
        };
        match result {
            // `mut` only needed when `apply_fault_injection_proxy_rewrites` is compiled.
            #[cfg_attr(
                not(all(feature = "fault-injection", feature = "kafka")),
                allow(unused_mut)
            )]
            Ok(mut handles) => {
                #[cfg(all(feature = "fault-injection", feature = "kafka"))]
                if let Err(e) = apply_fault_injection_proxy_rewrites(ctx, &mut handles).await {
                    event_log.append_raw(
                        EventLevel::Error,
                        "lifecycle",
                        format!("Provision failed (fault injection rewrite): {e}"),
                    );
                    return abort_on_infrastructure_error(
                        ctx,
                        event_log,
                        &scenario_id,
                        e,
                        &mut diagnostics,
                    )
                    .await;
                }
                apply_resource_handles_to_kafka_bindings(ctx, &handles);
                #[cfg(feature = "rabbitmq")]
                apply_resource_handles_to_rabbit_bindings(ctx, &handles);
                #[cfg(feature = "postgres")]
                apply_resource_handles_to_postgres_bindings(ctx, &handles);
                #[cfg(feature = "mariadb")]
                apply_resource_handles_to_mariadb_bindings(ctx, &handles);
                #[cfg(feature = "mysql")]
                apply_resource_handles_to_mysql_bindings(ctx, &handles);
                #[cfg(feature = "sqlite")]
                apply_resource_handles_to_sqlite_bindings(ctx, &handles);
                #[cfg(feature = "duckdb")]
                apply_resource_handles_to_duckdb_bindings(ctx, &handles);
                #[cfg(any(feature = "redis", feature = "valkey"))]
                apply_resource_handles_to_redis_bindings(ctx, &handles);
                #[cfg(feature = "mongodb")]
                apply_resource_handles_to_mongo_bindings(ctx, &handles);
                #[cfg(feature = "mongodb-cluster")]
                register_mongo_replica_set_cluster_control(ctx, &handles).await;
                #[cfg(feature = "kafka-cluster")]
                register_kafka_cluster_control(ctx, &handles).await;
                #[cfg(feature = "redis-sentinel")]
                register_redis_sentinel_cluster_control(ctx, &handles).await;
                #[cfg(feature = "postgres-cluster")]
                register_postgres_replica_set_cluster_control(ctx, &handles).await;
                #[cfg(feature = "rabbitmq-cluster")]
                register_rabbit_cluster_control(ctx, &handles).await;
                #[cfg(feature = "nats")]
                apply_resource_handles_to_nats_bindings(ctx, &handles);
                #[cfg(feature = "neo4j")]
                apply_resource_handles_to_neo4j_bindings(ctx, &handles);
                #[cfg(feature = "elasticsearch")]
                apply_resource_handles_to_elasticsearch_bindings(ctx, &handles);
                #[cfg(feature = "opensearch")]
                apply_resource_handles_to_opensearch_bindings(ctx, &handles);
                #[cfg(feature = "qdrant")]
                apply_resource_handles_to_qdrant_bindings(ctx, &handles);
                #[cfg(feature = "weaviate")]
                apply_resource_handles_to_weaviate_bindings(ctx, &handles);
                #[cfg(feature = "influxdb")]
                apply_resource_handles_to_influxdb_bindings(ctx, &handles);
                #[cfg(feature = "timescaledb")]
                apply_resource_handles_to_timescaledb_bindings(ctx, &handles);
                #[cfg(feature = "fault-injection")]
                install_fault_handle_if_toxiproxy_provisioned(ctx, &handles);
                event_log.append_raw(EventLevel::Debug, "lifecycle", "Provisioned resource(s)");
            }
            Err(e) => {
                event_log.append_raw(
                    EventLevel::Error,
                    "lifecycle",
                    format!("Provision failed: {e}"),
                );
                return abort_on_infrastructure_error(
                    ctx,
                    event_log,
                    &scenario_id,
                    e,
                    &mut diagnostics,
                )
                .await;
            }
        }
    }

    #[cfg(all(feature = "fault-injection", feature = "mongodb-cluster"))]
    {
        if let Err(error) =
            crate::cluster::mongo_replica_set::inter_node::finalize_mongo_replica_set_inter_node_mesh(ctx)
                .await
        {
            event_log.append_raw(
                EventLevel::Error,
                "lifecycle",
                format!("Provision failed (MongoDB inter-node mesh): {error}"),
            );
            return abort_on_infrastructure_error(
                    ctx,
                    event_log,
                    &scenario_id,
                    error,
                    &mut diagnostics,
                )
                .await;
        }
    }

    #[cfg(all(feature = "fault-injection", feature = "kafka"))]
    if ctx.kafka_bindings_apply_deferred() {
        let error = InfrastructureError::FaultInjection(
            crate::fault_injection::Error::KafkaFaultProxyIncomplete,
        );
        event_log.append_raw(
            EventLevel::Error,
            "lifecycle",
            format!("Provision incomplete: {error}"),
        );
        return abort_on_infrastructure_error(
            ctx,
            event_log,
            &scenario_id,
            error,
            &mut diagnostics,
        )
        .await;
    }

    // Readiness phase (async)
    diagnostics.push_phase(ctx, Phase::Readiness);
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting readiness phase");
    for resource in ctx.resources() {
        if let Err(e) = resource.check_ready().await {
            event_log.append_raw(
                EventLevel::Error,
                "lifecycle",
                format!("Readiness failed: {e}"),
            );
            return abort_on_infrastructure_error(
                ctx,
                event_log,
                &scenario_id,
                e,
                &mut diagnostics,
            )
            .await;
        }
    }

    // Seed phase (async)
    diagnostics.push_phase(ctx, Phase::Seed);
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting seed phase");
    for resource in ctx.resources_mut() {
        if let Err(e) = resource.apply_default_seed().await {
            event_log.append_raw(EventLevel::Error, "lifecycle", format!("Seed failed: {e}"));
            return abort_on_infrastructure_error(
                ctx,
                event_log,
                &scenario_id,
                e,
                &mut diagnostics,
            )
            .await;
        }
    }

    #[cfg(feature = "mock-services")]
    if let Err(error) = start_and_readiness_mock_services(ctx, event_log).await {
        let infrastructure_error = match &error {
            ShakedownError::Infrastructure(ref infrastructure) => Some(infrastructure),
            _ => None,
        };
        event_log.append_raw(
            EventLevel::Error,
            "lifecycle",
            format!("MockService phase failed: {error}"),
        );
        teardown_all_resources(ctx).await;
        diagnostics.merge_into(event_log).await?;
        if let Some(infrastructure) = infrastructure_error {
            emit_failure_timeline_on_infra_err(event_log, &scenario_id, infrastructure);
        }
        return Err(error);
    }

    #[cfg(feature = "services")]
    if let Err(error) =
        crate::service::lifecycle::start_and_readiness_services(ctx, event_log).await
    {
        event_log.append_raw(
            EventLevel::Error,
            "lifecycle",
            format!("Service container phase failed: {error}"),
        );
        return abort_on_infrastructure_error(
            ctx,
            event_log,
            &scenario_id,
            error,
            &mut diagnostics,
        )
        .await;
    }

    // Run phase
    diagnostics.push_phase(ctx, Phase::Run);
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting run phase");
    let (scenario_application_event_sender, scenario_application_event_receiver) =
        std::sync::mpsc::channel::<crate::event::Event>();
    ctx.set_scenario_application_event_sender(Some(scenario_application_event_sender));
    // Fault-injection timeline: `FaultHandle` sits on `Context` and scenario code calls it from
    // async paths (add/remove toxic, reset proxy). The harness owns `EventLog` separately; we need
    // those fault actions to show up in the same log as lifecycle and infra lines. You cannot hand
    // `FaultHandle` a `&mut EventLog` for the whole run: that would alias `event_log` with `ctx`
    // across the scenario body, and tying lifetimes through `Context` is awkward. Alternatives like
    // a raw pointer to `EventLog` would work only under `unsafe` with a brittle "call order"
    // contract.
    //
    // A bounded `SyncSender`/`Receiver` pair fixes this: the handle only holds the sender and
    // enqueues owned `Event` values with `try_send` (no shared mutable state with the log). The
    // harness keeps the receiver and drains after `scenario_fn` returns, appending into the
    // caller-owned `EventLog` in one place—same merge point as other diagnostics. Queue depth is
    // [`crate::fault_injection::FAULT_EVENT_CHANNEL_CAPACITY`]; if it fills, `try_send` fails and
    // that event is skipped (explicit back-pressure; same idea as a fixed-size buffer).
    #[cfg(feature = "fault-injection")]
    let fault_event_receiver = {
        let (sender, receiver) =
            std::sync::mpsc::sync_channel(crate::fault_injection::FAULT_EVENT_CHANNEL_CAPACITY);
        if let Some(handle) = ctx.fault_handle_mut() {
            handle.set_event_sender(sender);
        }
        receiver
    };
    #[cfg(feature = "mongodb-cluster")]
    let cluster_event_receiver = {
        use crate::cluster::mongo_replica_set::MongoReplicaSetMarker;
        use crate::cluster::CLUSTER_EVENT_CHANNEL_CAPACITY;
        use crate::event::Event;
        if ctx.cluster_handle::<MongoReplicaSetMarker>().is_some() {
            let (sender, receiver) =
                std::sync::mpsc::sync_channel::<Event>(CLUSTER_EVENT_CHANNEL_CAPACITY);
            if let Some(handle) = ctx.cluster_handle_mut::<MongoReplicaSetMarker>() {
                handle.set_event_sender(sender.clone());
            }
            if let Some(control) = ctx.mongo_replica_set_cluster.as_ref() {
                control.set_event_sender(sender);
            }
            Some(receiver)
        } else {
            None
        }
    };
    #[cfg(feature = "kafka-cluster")]
    let kafka_cluster_event_receiver = {
        use crate::cluster::kafka_cluster::KafkaClusterMarker;
        use crate::cluster::CLUSTER_EVENT_CHANNEL_CAPACITY;
        use crate::event::Event;
        if ctx.cluster_handle::<KafkaClusterMarker>().is_some() {
            let (sender, receiver) =
                std::sync::mpsc::sync_channel::<Event>(CLUSTER_EVENT_CHANNEL_CAPACITY);
            if let Some(handle) = ctx.cluster_handle_mut::<KafkaClusterMarker>() {
                handle.set_event_sender(sender.clone());
            }
            if let Some(control) = ctx.kafka_cluster_control.as_ref() {
                control.set_event_sender(sender);
            }
            Some(receiver)
        } else {
            None
        }
    };
    #[cfg(feature = "redis-sentinel")]
    let redis_sentinel_cluster_event_receiver = {
        use crate::cluster::redis_sentinel::RedisSentinelMarker;
        use crate::cluster::CLUSTER_EVENT_CHANNEL_CAPACITY;
        use crate::event::Event;
        if ctx.cluster_handle::<RedisSentinelMarker>().is_some() {
            let (sender, receiver) =
                std::sync::mpsc::sync_channel::<Event>(CLUSTER_EVENT_CHANNEL_CAPACITY);
            if let Some(handle) = ctx.cluster_handle_mut::<RedisSentinelMarker>() {
                handle.set_event_sender(sender.clone());
            }
            if let Some(control) = ctx.redis_sentinel_cluster.as_ref() {
                control.set_event_sender(sender);
            }
            Some(receiver)
        } else {
            None
        }
    };
    #[cfg(feature = "postgres-cluster")]
    let postgres_replica_set_cluster_event_receiver = {
        use crate::cluster::postgres_replica_set::PostgresReplicaSetMarker;
        use crate::cluster::CLUSTER_EVENT_CHANNEL_CAPACITY;
        use crate::event::Event;
        if ctx.cluster_handle::<PostgresReplicaSetMarker>().is_some() {
            let (sender, receiver) =
                std::sync::mpsc::sync_channel::<Event>(CLUSTER_EVENT_CHANNEL_CAPACITY);
            if let Some(handle) = ctx.cluster_handle_mut::<PostgresReplicaSetMarker>() {
                handle.set_event_sender(sender.clone());
            }
            if let Some(control) = ctx.postgres_replica_set_cluster.as_ref() {
                control.set_event_sender(sender);
            }
            Some(receiver)
        } else {
            None
        }
    };
    #[cfg(feature = "rabbitmq-cluster")]
    let rabbit_cluster_event_receiver = {
        use crate::cluster::rabbit_cluster::RabbitClusterMarker;
        use crate::cluster::CLUSTER_EVENT_CHANNEL_CAPACITY;
        use crate::event::Event;
        if ctx.cluster_handle::<RabbitClusterMarker>().is_some() {
            let (sender, receiver) =
                std::sync::mpsc::sync_channel::<Event>(CLUSTER_EVENT_CHANNEL_CAPACITY);
            if let Some(handle) = ctx.cluster_handle_mut::<RabbitClusterMarker>() {
                handle.set_event_sender(sender.clone());
            }
            if let Some(control) = ctx.rabbit_cluster_control.as_ref() {
                control.set_event_sender(sender);
            }
            Some(receiver)
        } else {
            None
        }
    };
    #[cfg(feature = "load")]
    let load_event_receiver = {
        use crate::event::Event;
        let (sender, receiver) = std::sync::mpsc::channel::<Event>();
        ctx.set_load_event_sender(Some(sender));
        receiver
    };

    // Execute the scenario function. If the body panics, we still merge load/cluster/fault events
    // into `event_log` before re-panicking so failure timelines correlate load and topology rows.
    #[cfg(any(
        feature = "load",
        feature = "mongodb-cluster",
        feature = "kafka-cluster",
        feature = "redis-sentinel",
        feature = "postgres-cluster",
        feature = "rabbitmq-cluster",
        feature = "fault-injection"
    ))]
    let scenario_result = {
        use futures_util::future::FutureExt;
        std::panic::AssertUnwindSafe(scenario_fn(ctx))
            .catch_unwind()
            .await
    };
    #[cfg(not(any(
        feature = "load",
        feature = "mongodb-cluster",
        feature = "kafka-cluster",
        feature = "redis-sentinel",
        feature = "postgres-cluster",
        feature = "rabbitmq-cluster",
        feature = "fault-injection"
    )))]
    let scenario_outcome = scenario_fn(ctx).await;

    ctx.set_scenario_application_event_sender(None);
    for event in scenario_application_event_receiver.try_iter() {
        event_log.append(event);
    }

    #[cfg(feature = "load")]
    {
        ctx.set_load_event_sender(None);
        for event in load_event_receiver.try_iter() {
            event_log.append(event);
        }
    }

    #[cfg(feature = "mongodb-cluster")]
    if let Some(receiver) = cluster_event_receiver {
        for event in receiver.try_iter() {
            event_log.append(event);
        }
    }

    #[cfg(feature = "kafka-cluster")]
    if let Some(receiver) = kafka_cluster_event_receiver {
        for event in receiver.try_iter() {
            event_log.append(event);
        }
    }

    #[cfg(feature = "redis-sentinel")]
    if let Some(receiver) = redis_sentinel_cluster_event_receiver {
        for event in receiver.try_iter() {
            event_log.append(event);
        }
    }

    #[cfg(feature = "postgres-cluster")]
    if let Some(receiver) = postgres_replica_set_cluster_event_receiver {
        for event in receiver.try_iter() {
            event_log.append(event);
        }
    }

    #[cfg(feature = "rabbitmq-cluster")]
    if let Some(receiver) = rabbit_cluster_event_receiver {
        for event in receiver.try_iter() {
            event_log.append(event);
        }
    }

    // Drain fault events into the caller-owned log after the scenario body and before assert/teardown,
    // so timelines and tests that read `event_log` after `run_scenario` see a complete run-phase slice.
    #[cfg(feature = "fault-injection")]
    for event in fault_event_receiver.try_iter() {
        event_log.append(event);
    }

    #[cfg(any(
        feature = "load",
        feature = "mongodb-cluster",
        feature = "kafka-cluster",
        feature = "redis-sentinel",
        feature = "postgres-cluster",
        feature = "rabbitmq-cluster",
        feature = "fault-injection"
    ))]
    let run_result = match scenario_result {
        Ok(outcome) => outcome.into_shakedown_result(),
        Err(panic_payload) => std::panic::resume_unwind(panic_payload),
    };
    #[cfg(not(any(
        feature = "load",
        feature = "mongodb-cluster",
        feature = "kafka-cluster",
        feature = "redis-sentinel",
        feature = "postgres-cluster",
        feature = "rabbitmq-cluster",
        feature = "fault-injection"
    )))]
    let run_result = scenario_outcome.into_shakedown_result();

    // Assert phase (in a real implementation, this would run assertions)
    diagnostics.push_phase(ctx, Phase::Assert);
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting assert phase");

    // Teardown phase
    diagnostics.push_phase(ctx, Phase::Teardown);
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting teardown phase");
    #[cfg(feature = "services")]
    if let Err(error) = crate::service::lifecycle::teardown_service_containers(ctx, event_log).await
    {
        event_log.append_raw(
            EventLevel::Error,
            "lifecycle",
            format!("Service container teardown failed: {error}"),
        );
    }
    #[cfg(all(feature = "mock-services", feature = "grpc"))]
    shutdown_grpc_mock_service_servers_with_log(ctx, event_log).await;
    #[cfg(feature = "mock-services")]
    shutdown_http_mock_service_servers_with_log(ctx, event_log).await;
    #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
    shutdown_tcp_mock_service_servers_with_log(ctx, event_log).await;
    #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
    shutdown_udp_mock_service_servers_with_log(ctx, event_log).await;
    teardown_all_resources(ctx).await;

    diagnostics.merge_into(event_log).await?;

    #[cfg(all(
        any(
            feature = "kafka",
            feature = "rabbitmq",
            feature = "postgres",
            feature = "mariadb",
            feature = "mysql",
            feature = "redis",
            feature = "valkey",
            feature = "mongodb",
            feature = "nats",
            feature = "neo4j",
            feature = "elasticsearch",
            feature = "opensearch",
            feature = "qdrant",
            feature = "weaviate",
            feature = "influxdb",
            feature = "timescaledb",
            feature = "services",
        ),
        feature = "diagnostics",
        not(feature = "no-diagnostics"),
    ))]
    ctx.set_resource_stats_rings(None);

    if let Err(ref error) = run_result {
        crate::timeline::emit_failure_timeline_for_shakedown_error(
            event_log,
            scenario_id.as_str(),
            error,
        );
    }

    run_result
}

/// Builds a [`Context`] with [`crate::resources::BuiltinResource::Fake`] already registered.
///
/// Convenience for tests that call [`run_scenario`] directly without `#[shakedown::test(resources = [Fake])]`.
pub fn new_context_with_fake_resource() -> Context {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Fake(FakeResource::default()));
    ctx
}
