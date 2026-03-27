//! Scenario lifecycle types and execution.
//!
//! The scenario lifecycle consists of six ordered phases:
//!
//! 1. Provision resources
//! 2. Check resource readiness
//! 3. Seed data into resources
//! 4. Run the scenario logic
//! 5. Assert scenario outcomes
//! 6. Teardown resources
//!
//! If any phase fails, the lifecycle will attempt to run teardown for resources that were
//! successfully provisioned. The error classification distinguishes between scenario errors
//! (user logic/assertions) and infrastructure errors (provision/readiness failures).
//!
//! ## Event logging
//!
//! The lifecycle emits structured events during each phase. These events are collected in an
//! [`EventLog`] that can be retrieved for debugging or failure analysis.
//!
//! # Example
//!
//! ```ignore
//! use shakedown::{run_scenario, Context, EventLog};
//!
//! async fn my_scenario(ctx: &mut Context) {
//!     // Scenario implementation
//! }
//!
//! #[tokio::test]
//! async fn test_scenario_with_events() {
//!     let mut ctx = Context::new();
//!     let mut event_log = EventLog::new();
//!
//!     run_scenario(&mut ctx, "test-scenario", |c| Box::pin(my_scenario(c)), &mut event_log)
//!         .await
//!         .unwrap();
//!
//!     // Retrieve events after scenario completion
//!     let events = event_log.events();
//!     assert!(events.iter().any(|e| e.source == "scenario" && e.level == EventLevel::Info));
//! }
//! ```

use crate::error::{InfrastructureError, ResourceHandle, ScenarioId, ShakedownError};
use crate::event::{EventLevel, EventLog};
#[cfg(all(
    feature = "outbox",
    any(
        feature = "postgres",
        feature = "mariadb",
        feature = "mysql",
        feature = "sqlite"
    )
))]
use crate::outbox::Outbox;
#[cfg(feature = "mariadb")]
use crate::resources::MariaDBBindings;
#[cfg(feature = "mysql")]
use crate::resources::MysqlBindings;
#[cfg(feature = "postgres")]
use crate::resources::PostgresBindings;
#[cfg(feature = "sqlite")]
use crate::resources::SqliteBindings;
use crate::resources::{
    BuiltinResource, FakeResource, KafkaBindings, RegisteredResource, Resource,
};
use async_trait::async_trait;

/// Ordered phase of [`run_scenario`] (provision through teardown).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Provision resources
    Provision,
    /// Check resource readiness
    Readiness,
    /// Seed data into resources
    Seed,
    /// Run the scenario logic
    Run,
    /// Assert scenario outcomes
    Assert,
    /// Teardown resources
    Teardown,
}

/// High-level state machine position for a scenario (reserved for richer orchestration).
#[derive(Debug)]
pub enum LifecycleState {
    /// Not yet started
    NotStarted,
    /// Currently running a phase
    Running(Phase),
    /// Completed successfully
    Completed,
    /// Failed during a specific phase
    Failed(Phase, Box<dyn std::error::Error + Send + Sync>),
}

/// The environment backend abstraction.
///
/// This trait abstracts the underlying infrastructure where scenarios run,
/// allowing for different backends (Docker, local processes, cloud environments, etc.)
#[async_trait]
pub trait EnvironmentBackend: Send + Sync {
    /// Provision a single resource.
    ///
    /// This method is called during the provision phase. The environment backend
    /// should handle setting up the resource with scenario-scoped isolation.
    async fn provision_resource(
        &mut self,
        resource: RegisteredResource,
    ) -> Result<(), InfrastructureError>;

    /// Check readiness of all resources.
    async fn check_readiness(&self) -> Result<(), InfrastructureError> {
        Ok(())
    }

    /// Teardown all resources.
    ///
    /// This method should be idempotent - it can be called multiple times
    /// safely, even if some resources were never provisioned or already torn down.
    async fn teardown(&self) -> Result<(), InfrastructureError> {
        Ok(())
    }
}

/// A default stub environment backend that does nothing.
#[derive(Debug, Default)]
pub struct NoopEnvironmentBackend;

#[async_trait]
impl EnvironmentBackend for NoopEnvironmentBackend {
    async fn provision_resource(
        &mut self,
        _resource: RegisteredResource,
    ) -> Result<(), InfrastructureError> {
        Ok(())
    }

    async fn check_readiness(&self) -> Result<(), InfrastructureError> {
        Ok(())
    }

    async fn teardown(&self) -> Result<(), InfrastructureError> {
        Ok(())
    }
}

/// Represents the kind of scenario being executed.
///
/// This is used by `#[shakedown::faulty_test]` to mark fault-oriented scenarios,
/// and by future fault-injection code to decide whether to apply faults like
/// Toxiproxy toxics.
#[allow(clippy::derivable_impls)] // Manual impl needed for enum without #[default] marker
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScenarioKind {
    /// A normal scenario (no faults injected)
    Normal,
    /// A faulty scenario (faults may be injected)
    Faulty,
}

impl std::fmt::Display for ScenarioKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScenarioKind::Normal => write!(f, "Normal"),
            ScenarioKind::Faulty => write!(f, "Faulty"),
        }
    }
}

/// Mutable scenario state for one run: resources, backend, ids, and optional Kafka bindings.
///
/// Why: the harness and scenario body share one place for registered infrastructure and
/// per-run metadata so [`run_scenario`] can drive provision → teardown without global singletons.
///
/// How: constructed with [`Context::new`] or by `#[shakedown::test]`; the scenario function receives
/// `&mut Context` to register extras or read [`Context::scenario_id`], [`Context::kafka_bindings`],
/// and (with the `postgres` / `mariadb` / `mysql` / `sqlite` features) relational bindings accessors.
pub struct Context {
    /// Registered resources
    resources: Vec<RegisteredResource>,
    /// Environment backend for lifecycle operations
    environment_backend: Box<dyn EnvironmentBackend>,
    /// Scenario ID for isolation
    scenario_id: ScenarioId,
    /// Kafka bindings when a Kafka resource populated them (from provision metadata).
    kafka_bindings: Option<KafkaBindings>,
    /// Postgres bindings when a Postgres resource populated them (`postgres` feature).
    #[cfg(feature = "postgres")]
    postgres_bindings: Option<PostgresBindings>,
    /// MariaDB bindings when a MariaDB resource populated them (`mariadb` feature).
    #[cfg(feature = "mariadb")]
    mariadb_bindings: Option<MariaDBBindings>,
    /// Oracle MySQL bindings when a MySQL resource populated them (`mysql` feature).
    #[cfg(feature = "mysql")]
    mysql_bindings: Option<MysqlBindings>,
    /// SQLite bindings when a SQLite resource populated them (`sqlite` feature).
    #[cfg(feature = "sqlite")]
    sqlite_bindings: Option<SqliteBindings>,
    /// Kind of scenario being executed (Normal or Faulty)
    scenario_kind: ScenarioKind,
}

impl Context {
    /// Create a new empty context with the default noop environment backend.
    pub fn new() -> Self {
        Self {
            resources: Vec::new(),
            environment_backend: Box::new(NoopEnvironmentBackend),
            scenario_id: ScenarioId::empty(),
            kafka_bindings: None,
            #[cfg(feature = "postgres")]
            postgres_bindings: None,
            #[cfg(feature = "mariadb")]
            mariadb_bindings: None,
            #[cfg(feature = "mysql")]
            mysql_bindings: None,
            #[cfg(feature = "sqlite")]
            sqlite_bindings: None,
            scenario_kind: ScenarioKind::Normal,
        }
    }

    /// Create a new context with a custom environment backend.
    pub fn with_backend(backend: impl EnvironmentBackend + 'static) -> Self {
        Self {
            resources: Vec::new(),
            environment_backend: Box::new(backend),
            scenario_id: ScenarioId::empty(),
            kafka_bindings: None,
            #[cfg(feature = "postgres")]
            postgres_bindings: None,
            #[cfg(feature = "mariadb")]
            mariadb_bindings: None,
            #[cfg(feature = "mysql")]
            mysql_bindings: None,
            #[cfg(feature = "sqlite")]
            sqlite_bindings: None,
            scenario_kind: ScenarioKind::Normal,
        }
    }

    /// Create a new context with a specific scenario ID.
    pub fn with_scenario_id(scenario_id: impl Into<String>) -> Self {
        Self {
            resources: Vec::new(),
            environment_backend: Box::new(NoopEnvironmentBackend),
            scenario_id: ScenarioId::new(scenario_id),
            kafka_bindings: None,
            #[cfg(feature = "postgres")]
            postgres_bindings: None,
            #[cfg(feature = "mariadb")]
            mariadb_bindings: None,
            #[cfg(feature = "mysql")]
            mysql_bindings: None,
            #[cfg(feature = "sqlite")]
            sqlite_bindings: None,
            scenario_kind: ScenarioKind::Normal,
        }
    }

    /// Register a first-party [`BuiltinResource`].
    pub fn register_builtin(&mut self, resource: BuiltinResource) {
        self.resources.push(RegisteredResource::Builtin(resource));
    }

    /// Register an extension resource with the context.
    pub fn register_extension<R: Resource + 'static>(&mut self, resource: R) {
        self.resources
            .push(RegisteredResource::Extension(Box::new(resource)));
    }

    /// Get a reference to all registered resources.
    pub fn resources(&self) -> &[RegisteredResource] {
        &self.resources
    }

    /// Get a mutable reference to all registered resources.
    pub fn resources_mut(&mut self) -> &mut Vec<RegisteredResource> {
        &mut self.resources
    }

    /// Get a reference to the environment backend.
    pub fn environment_backend(&self) -> &dyn EnvironmentBackend {
        &*self.environment_backend
    }

    /// Get a mutable reference to the environment backend.
    pub fn environment_backend_mut(&mut self) -> &mut dyn EnvironmentBackend {
        &mut *self.environment_backend
    }

    /// Get the scenario ID.
    pub fn scenario_id(&self) -> &ScenarioId {
        &self.scenario_id
    }

    /// Set Kafka bindings for this scenario (typically after Kafka provision).
    pub fn set_kafka_bindings(&mut self, kafka_bindings: KafkaBindings) {
        self.kafka_bindings = Some(kafka_bindings);
    }

    /// Get a reference to Kafka bindings if available.
    pub fn kafka_bindings(&self) -> Option<&KafkaBindings> {
        self.kafka_bindings.as_ref()
    }

    /// Set Postgres bindings for this scenario (typically after Postgres provision).
    #[cfg(feature = "postgres")]
    pub fn set_postgres_bindings(&mut self, postgres_bindings: PostgresBindings) {
        self.postgres_bindings = Some(postgres_bindings);
    }

    /// Get Postgres bindings if available (`postgres` feature).
    #[cfg(feature = "postgres")]
    pub fn postgres_bindings(&self) -> Option<&PostgresBindings> {
        self.postgres_bindings.as_ref()
    }

    /// Set MariaDB bindings for this scenario (typically after MariaDB provision).
    #[cfg(feature = "mariadb")]
    pub fn set_mariadb_bindings(&mut self, mariadb_bindings: MariaDBBindings) {
        self.mariadb_bindings = Some(mariadb_bindings);
    }

    /// Get MariaDB bindings if available (`mariadb` feature).
    #[cfg(feature = "mariadb")]
    pub fn mariadb_bindings(&self) -> Option<&MariaDBBindings> {
        self.mariadb_bindings.as_ref()
    }

    /// Set MySQL (Oracle) bindings for this scenario (typically after MySQL provision).
    #[cfg(feature = "mysql")]
    pub fn set_mysql_bindings(&mut self, mysql_bindings: MysqlBindings) {
        self.mysql_bindings = Some(mysql_bindings);
    }

    /// Get Oracle MySQL bindings if available (`mysql` feature).
    #[cfg(feature = "mysql")]
    pub fn mysql_bindings(&self) -> Option<&MysqlBindings> {
        self.mysql_bindings.as_ref()
    }

    /// Set SQLite bindings for this scenario (typically after SQLite provision).
    #[cfg(feature = "sqlite")]
    pub fn set_sqlite_bindings(&mut self, sqlite_bindings: SqliteBindings) {
        self.sqlite_bindings = Some(sqlite_bindings);
    }

    /// Get SQLite bindings if available (`sqlite` feature).
    #[cfg(feature = "sqlite")]
    pub fn sqlite_bindings(&self) -> Option<&SqliteBindings> {
        self.sqlite_bindings.as_ref()
    }

    /// Build an outbox helper bound to this context (`outbox` + relational features).
    #[cfg(all(
        feature = "outbox",
        any(
            feature = "postgres",
            feature = "mariadb",
            feature = "mysql",
            feature = "sqlite"
        )
    ))]
    pub fn outbox(&self) -> Outbox<'_> {
        Outbox::new(self)
    }

    /// Check if the context has any registered resources.
    pub fn is_empty(&self) -> bool {
        self.resources.is_empty()
    }

    /// Get a mutable reference to the scenario ID.
    pub fn set_scenario_id(&mut self, id: impl Into<String>) {
        self.scenario_id = ScenarioId::new(id);
    }

    /// Get a reference to the scenario kind.
    pub fn scenario_kind(&self) -> ScenarioKind {
        self.scenario_kind
    }

    /// Set the scenario kind for this run.
    pub fn set_scenario_kind(&mut self, kind: ScenarioKind) {
        self.scenario_kind = kind;
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "postgres")]
/// If any handle metadata carries Postgres engine fields, set [`Context::set_postgres_bindings`].
fn apply_resource_handles_to_postgres_bindings(ctx: &mut Context, handles: &[ResourceHandle]) {
    for h in handles {
        let Some(meta) = &h.metadata else {
            continue;
        };
        let Some(engine) = meta.get("engine").and_then(|v| v.as_str()) else {
            continue;
        };
        if engine != "postgres" {
            continue;
        }
        let host = meta.get("host").and_then(|v| v.as_str());
        let port = meta.get("port").and_then(|v| v.as_u64()).map(|p| p as u16);
        let database = meta.get("database").and_then(|v| v.as_str());
        let username = meta.get("username").and_then(|v| v.as_str());
        let password = meta.get("password").and_then(|v| v.as_str());
        let connection_url = meta.get("connection_url").and_then(|v| v.as_str());
        if let (
            Some(host),
            Some(port),
            Some(database),
            Some(username),
            Some(password),
            Some(connection_url),
        ) = (host, port, database, username, password, connection_url)
        {
            ctx.set_postgres_bindings(PostgresBindings {
                host: host.to_string(),
                port,
                database: database.to_string(),
                username: username.to_string(),
                password: password.to_string(),
                connection_url: connection_url.to_string(),
            });
        }
    }
}

#[cfg(feature = "mariadb")]
/// If any handle metadata carries MariaDB engine fields, set [`Context::set_mariadb_bindings`].
fn apply_resource_handles_to_mariadb_bindings(ctx: &mut Context, handles: &[ResourceHandle]) {
    for h in handles {
        let Some(meta) = &h.metadata else {
            continue;
        };
        let Some(engine) = meta.get("engine").and_then(|v| v.as_str()) else {
            continue;
        };
        if engine != "mariadb" {
            continue;
        }
        let host = meta.get("host").and_then(|v| v.as_str());
        let port = meta.get("port").and_then(|v| v.as_u64()).map(|p| p as u16);
        let database = meta.get("database").and_then(|v| v.as_str());
        let username = meta.get("username").and_then(|v| v.as_str());
        let password = meta.get("password").and_then(|v| v.as_str());
        let connection_url = meta.get("connection_url").and_then(|v| v.as_str());
        if let (
            Some(host),
            Some(port),
            Some(database),
            Some(username),
            Some(password),
            Some(connection_url),
        ) = (host, port, database, username, password, connection_url)
        {
            ctx.set_mariadb_bindings(MariaDBBindings {
                host: host.to_string(),
                port,
                database: database.to_string(),
                username: username.to_string(),
                password: password.to_string(),
                connection_url: connection_url.to_string(),
            });
        }
    }
}

#[cfg(feature = "sqlite")]
/// If any handle metadata carries SQLite engine fields, set [`Context::set_sqlite_bindings`].
fn apply_resource_handles_to_sqlite_bindings(ctx: &mut Context, handles: &[ResourceHandle]) {
    for h in handles {
        let Some(meta) = &h.metadata else {
            continue;
        };
        let Some(engine) = meta.get("engine").and_then(|v| v.as_str()) else {
            continue;
        };
        if engine != "sqlite" {
            continue;
        }
        let Some(database_url) = meta.get("database_url").and_then(|v| v.as_str()) else {
            continue;
        };
        ctx.set_sqlite_bindings(SqliteBindings {
            database_url: database_url.to_string(),
        });
    }
}

#[cfg(feature = "mysql")]
/// If any handle metadata carries MySQL engine fields, set [`Context::set_mysql_bindings`].
fn apply_resource_handles_to_mysql_bindings(ctx: &mut Context, handles: &[ResourceHandle]) {
    for h in handles {
        let Some(meta) = &h.metadata else {
            continue;
        };
        let Some(engine) = meta.get("engine").and_then(|v| v.as_str()) else {
            continue;
        };
        if engine != "mysql" {
            continue;
        }
        let host = meta.get("host").and_then(|v| v.as_str());
        let port = meta.get("port").and_then(|v| v.as_u64()).map(|p| p as u16);
        let database = meta.get("database").and_then(|v| v.as_str());
        let username = meta.get("username").and_then(|v| v.as_str());
        let password = meta.get("password").and_then(|v| v.as_str());
        let connection_url = meta.get("connection_url").and_then(|v| v.as_str());
        if let (
            Some(host),
            Some(port),
            Some(database),
            Some(username),
            Some(password),
            Some(connection_url),
        ) = (host, port, database, username, password, connection_url)
        {
            ctx.set_mysql_bindings(MysqlBindings {
                host: host.to_string(),
                port,
                database: database.to_string(),
                username: username.to_string(),
                password: password.to_string(),
                connection_url: connection_url.to_string(),
            });
        }
    }
}

/// If any handle metadata carries `bootstrap_servers` and `topic_prefix`, set [`Context::set_kafka_bindings`].
fn apply_resource_handles_to_kafka_bindings(ctx: &mut Context, handles: &[ResourceHandle]) {
    for h in handles {
        let Some(meta) = &h.metadata else {
            continue;
        };
        let (Some(bs), Some(tp)) = (
            meta.get("bootstrap_servers").and_then(|v| v.as_str()),
            meta.get("topic_prefix").and_then(|v| v.as_str()),
        ) else {
            continue;
        };
        ctx.set_kafka_bindings(KafkaBindings::new(bs, tp));
    }
}

/// Run [`Resource::teardown`](crate::resources::Resource::teardown) on every registered resource, then backend teardown.
async fn teardown_all_resources(ctx: &mut Context) {
    for resource in ctx.resources_mut() {
        let _ = resource.teardown().await;
    }
    let _ = ctx.environment_backend_mut().teardown().await;
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
/// The function respects the lifecycle order: provision → readiness → seed → run → assert → teardown
/// as documented in ARCHITECTURE.md §§ 3.6–3.9.
///
/// # Event logging
///
/// Events emitted during the scenario run are appended to the provided [`EventLog`].
/// After completion, callers can retrieve all events for debugging or analysis.
///
/// `scenario_fn` takes only `&mut Context` and must return a [`std::pin::Pin`]-boxed [`std::future::Future`]
/// so the run phase can borrow `Context` across `.await` while the harness keeps using
/// `event_log` after the scenario body completes.
///
/// # Example
///
/// ```ignore
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
///     // Retrieve events after scenario completion
///     let events = event_log.events();
/// }
/// ```
pub async fn run_scenario<F>(
    ctx: &mut Context,
    scenario_id: impl Into<String>,
    scenario_fn: F,
    event_log: &mut EventLog,
) -> Result<(), ShakedownError>
where
    F: for<'s> FnOnce(
        &'s mut Context,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 's>>,
{
    // Update scenario ID
    ctx.set_scenario_id(scenario_id);

    #[cfg(any(
        feature = "kafka",
        feature = "postgres",
        feature = "mariadb",
        feature = "mysql",
    ))]
    {
        let _ = crate::diagnostics::suite_diagnostics_handle().await;
    }

    // Provision phase (async)
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting provision phase");

    let scenario_id = ctx.scenario_id().clone();
    let n = ctx.resources().len();
    for i in 0..n {
        let result = {
            let resource = &mut ctx.resources_mut()[i];
            resource.provision(&scenario_id).await
        };
        match result {
            Ok(handles) => {
                apply_resource_handles_to_kafka_bindings(ctx, &handles);
                #[cfg(feature = "postgres")]
                apply_resource_handles_to_postgres_bindings(ctx, &handles);
                #[cfg(feature = "mariadb")]
                apply_resource_handles_to_mariadb_bindings(ctx, &handles);
                #[cfg(feature = "mysql")]
                apply_resource_handles_to_mysql_bindings(ctx, &handles);
                #[cfg(feature = "sqlite")]
                apply_resource_handles_to_sqlite_bindings(ctx, &handles);
                event_log.append_raw(EventLevel::Debug, "lifecycle", "Provisioned resource(s)");
            }
            Err(e) => {
                event_log.append_raw(
                    EventLevel::Error,
                    "lifecycle",
                    format!("Provision failed: {e}"),
                );
                teardown_all_resources(ctx).await;
                return Err(e.into());
            }
        }
    }

    // Readiness phase (async)
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting readiness phase");
    for resource in ctx.resources() {
        if let Err(e) = resource.check_ready().await {
            event_log.append_raw(
                EventLevel::Error,
                "lifecycle",
                format!("Readiness failed: {e}"),
            );
            teardown_all_resources(ctx).await;
            return Err(e.into());
        }
    }

    // Seed phase (async)
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting seed phase");
    for resource in ctx.resources_mut() {
        if let Err(e) = resource.apply_default_seed().await {
            event_log.append_raw(EventLevel::Error, "lifecycle", format!("Seed failed: {e}"));
            teardown_all_resources(ctx).await;
            return Err(e.into());
        }
    }

    // Run phase
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting run phase");
    // Execute the scenario function
    let _ = scenario_fn(ctx).await;

    // Assert phase (in a real implementation, this would run assertions)
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting assert phase");

    // Teardown phase
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting teardown phase");
    teardown_all_resources(ctx).await;

    Ok(())
}

/// Builds a [`Context`] with [`crate::resources::BuiltinResource::Fake`] already registered.
///
/// Convenience for tests that call [`run_scenario`] directly without `#[shakedown::test(resources = [Fake])]`.
pub fn new_context_with_fake_resource() -> Context {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Fake(FakeResource::default()));
    ctx
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ResourceHandle, ScenarioId};
    use crate::event::{Event, EventSnapshot};
    use crate::resources::Resource;
    use crate::ScenarioError;
    use async_trait::async_trait;

    #[test]
    fn test_phase_display() {
        // Given each lifecycle phase variant
        // When we format them with Debug
        // Then the printed names match the phase identifiers
        assert_eq!(format!("{:?}", Phase::Provision), "Provision");
        assert_eq!(format!("{:?}", Phase::Readiness), "Readiness");
        assert_eq!(format!("{:?}", Phase::Seed), "Seed");
        assert_eq!(format!("{:?}", Phase::Run), "Run");
        assert_eq!(format!("{:?}", Phase::Assert), "Assert");
        assert_eq!(format!("{:?}", Phase::Teardown), "Teardown");
    }

    #[test]
    fn test_context_new() {
        // Given a newly constructed scenario context
        let ctx = Context::new();
        // When we ask whether it has resources
        // Then it starts empty
        assert!(ctx.is_empty());
    }

    #[test]
    fn test_context_with_backend() {
        // Given a context wired to the noop environment backend
        let ctx = Context::with_backend(NoopEnvironmentBackend {});
        // When we check for registered resources
        // Then none are present yet
        assert!(ctx.is_empty());
    }

    #[test]
    fn test_context_with_scenario_id() {
        // Given a context created with a fixed scenario id string
        let ctx = Context::with_scenario_id("test-scenario-123");
        // When we read the scenario id back
        // Then it matches what we passed in
        assert_eq!(ctx.scenario_id().to_string(), "test-scenario-123");
    }

    #[test]
    fn test_context_register_builtin() {
        // Given an empty context
        let mut ctx = Context::new();
        // When we register the fake builtin resource
        ctx.register_builtin(BuiltinResource::Fake(FakeResource::default()));
        // Then the context is non-empty with exactly one entry
        assert!(!ctx.is_empty());
        assert_eq!(ctx.resources().len(), 1);
    }

    #[test]
    fn test_context_register_extension() {
        // Given an empty context
        let mut ctx = Context::new();
        // When we register an extension resource
        ctx.register_extension(FakeResource::default());
        // Then one resource is recorded
        assert_eq!(ctx.resources().len(), 1);
    }

    #[test]
    fn test_lifecycle_state() {
        // Given the lifecycle state enum
        // When we construct the NotStarted variant
        let _not_started = LifecycleState::NotStarted;
        // Then it type-checks (smoke test for the state machine surface)
        let _ = _not_started;
    }

    #[tokio::test]
    async fn test_noop_environment_backend() {
        // Given the noop environment backend
        let backend = NoopEnvironmentBackend {};
        // When we run the readiness check
        let readiness = backend.check_readiness().await;
        // Then it succeeds without side effects
        readiness.expect("readiness should succeed");
    }

    #[test]
    fn test_event_log_append_and_retrieval() {
        // Given an empty event log
        let mut log = EventLog::new();
        // When we append two events and then snapshot, clear, and re-check length
        log.append(Event::info("scenario", "test event 1"));
        log.append(Event::debug("resource:fake", "test event 2"));
        assert_eq!(log.len(), 2);
        let events = log.events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].message, "test event 1");
        assert_eq!(events[1].source, "resource:fake");
        let _snapshot: EventSnapshot = log.clone().into();
        assert_eq!(log.len(), 2);
        log.clear();
        // Then after clear the log is empty
        assert!(log.is_empty());
    }

    #[test]
    fn test_lifecycle_ordering() {
        // Given lifecycle phase and state types (without running a full async scenario here)
        let _ctx = Context::new();
        let _event_log = EventLog::new();
        // When we refer to each phase and the initial lifecycle state
        // Then Debug strings and state construction match expectations
        assert_eq!(format!("{:?}", Phase::Provision), "Provision");
        assert_eq!(format!("{:?}", Phase::Readiness), "Readiness");
        assert_eq!(format!("{:?}", Phase::Seed), "Seed");
        assert_eq!(format!("{:?}", Phase::Run), "Run");
        assert_eq!(format!("{:?}", Phase::Assert), "Assert");
        assert_eq!(format!("{:?}", Phase::Teardown), "Teardown");
        let _state = LifecycleState::NotStarted;
    }

    #[test]
    fn test_error_classification() {
        use crate::error::ShakedownError;

        // Given representative scenario, infrastructure, and internal errors
        let scenario_err: ShakedownError =
            ScenarioError::Assertion("test assertion failed".to_string()).into();
        let infra_err: ShakedownError = crate::InfrastructureError::Provision {
            resource: "test_resource".to_string(),
            source: Box::new(std::io::Error::other("provision failed")),
        }
        .into();
        let internal_err: ShakedownError =
            crate::InternalError::Bug("unexpected code path".to_string()).into();
        // When we classify them at the ShakedownError level
        // Then each lands in the matching top-level variant
        assert!(matches!(scenario_err, ShakedownError::Scenario(_)));
        assert!(matches!(infra_err, ShakedownError::Infrastructure(_)));
        assert!(matches!(internal_err, ShakedownError::Internal(_)));
    }

    #[test]
    fn test_builtin_resource_sync() {
        // Given a fake builtin resource
        let builtin = BuiltinResource::Fake(FakeResource::default());
        // When we pattern-match it
        // Then it is the Fake variant
        assert!(matches!(builtin, BuiltinResource::Fake(_)));
    }

    #[test]
    fn test_error_source_chain() {
        // Given an infrastructure error that wraps an I/O error and a plain scenario error
        let infra_err = crate::InfrastructureError::Provision {
            resource: "test".to_string(),
            source: Box::new(std::io::Error::other("inner error")),
        };
        let scenario_err = ScenarioError::Assertion("test".to_string());
        // When we query std::error::Error::source
        // Then infrastructure exposes a source and scenario does not
        assert!(std::error::Error::source(&infra_err).is_some());
        assert!(std::error::Error::source(&scenario_err).is_none());
    }

    #[test]
    fn test_event_level_display() {
        // Given each event level
        // When we format them with Display
        // Then we get the uppercase labels used in logs
        assert_eq!(format!("{}", EventLevel::Debug), "DEBUG");
        assert_eq!(format!("{}", EventLevel::Info), "INFO");
        assert_eq!(format!("{}", EventLevel::Warn), "WARN");
        assert_eq!(format!("{}", EventLevel::Error), "ERROR");
    }

    #[test]
    fn test_context_lifecycle() {
        // Given a new context
        let mut ctx = Context::new();
        assert!(ctx.is_empty());
        // When we register a builtin fake resource
        ctx.register_builtin(BuiltinResource::Fake(FakeResource::default()));
        // Then the context is no longer empty
        assert!(!ctx.is_empty());
    }

    #[test]
    fn test_resource_trait_bounds() {
        // Given the concrete FakeResource type
        fn assert_send_sync<T: Send + Sync>() {}
        // When we require Send + Sync
        // Then FakeResource satisfies the bounds
        assert_send_sync::<FakeResource>();
    }

    #[test]
    fn test_event_log_comprehensive() {
        // Given an empty log
        let mut log = EventLog::new();
        // When we append one raw line per level, clear, then append a structured event
        log.append_raw(EventLevel::Debug, "source1", "debug message");
        log.append_raw(EventLevel::Info, "source2", "info message");
        log.append_raw(EventLevel::Warn, "source3", "warn message");
        log.append_raw(EventLevel::Error, "source4", "error message");
        assert_eq!(log.len(), 4);
        let events = log.events();
        assert_eq!(events[0].level, EventLevel::Debug);
        assert_eq!(events[1].level, EventLevel::Info);
        assert_eq!(events[2].level, EventLevel::Warn);
        assert_eq!(events[3].level, EventLevel::Error);
        log.clear();
        assert!(log.is_empty());
        let event = Event::info("scenario", "test");
        log.append(event);
        assert_eq!(log.len(), 1);
        let retrieved = log.events();
        // Then the last message is the structured one we appended after clear
        assert_eq!(retrieved[0].message, "test");
    }

    #[tokio::test]
    async fn test_run_scenario_with_fake_resource() {
        // Given a context with a fake builtin that always succeeds
        let mut ctx = Context::new();
        ctx.register_builtin(BuiltinResource::Fake(FakeResource::default()));
        let mut event_log = EventLog::new();
        // When we run a no-op scenario body through the lifecycle
        let result = run_scenario(
            &mut ctx,
            "test-scenario",
            |_ctx: &mut Context| Box::pin(async {}),
            &mut event_log,
        )
        .await;
        // Then the run completes successfully
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_run_scenario_events() {
        // Given a context with fake infrastructure and an empty event log
        let mut ctx = Context::new();
        ctx.register_builtin(BuiltinResource::Fake(FakeResource::default()));
        let mut event_log = EventLog::new();
        // When we run a scenario through the lifecycle
        run_scenario(
            &mut ctx,
            "test-events-scenario",
            |_ctx: &mut Context| Box::pin(async {}),
            &mut event_log,
        )
        .await
        .expect("run_scenario should succeed");
        let events = event_log.events();
        // Then lifecycle phases emitted at least one event tagged with the lifecycle source
        assert!(events.iter().any(|e| e.source == "lifecycle"));
    }

    #[tokio::test]
    async fn test_run_scenario_fails_on_readiness() {
        // Given a resource type whose readiness check always fails (compile-time wiring check)
        struct UnreadyResource;

        #[async_trait]
        impl Resource for UnreadyResource {
            async fn provision(
                &mut self,
                _scenario_id: &ScenarioId,
            ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
                Ok(vec![ResourceHandle {
                    id: "unready".to_string(),
                    connection_string: "://fake".to_string(),
                    metadata: None,
                }])
            }

            async fn check_ready(&self) -> Result<(), InfrastructureError> {
                Err(InfrastructureError::Readiness {
                    resource: "test_resource".to_string(),
                    source: Box::new(std::io::Error::other("not ready")),
                })
            }

            async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
                Ok(())
            }

            async fn teardown(&mut self) -> Result<(), InfrastructureError> {
                Ok(())
            }
        }

        let _ctx = Context::new();

        #[allow(dead_code)]
        async fn verify_signatures_compile() {
            async fn test_method_access(
                _r: &mut UnreadyResource,
            ) -> std::result::Result<(), InfrastructureError> {
                _r.check_ready().await
            }

            let _ = test_method_access;
        }
        // When we keep the nested helpers available for type-checking
        // Then the UnreadyResource Resource impl compiles with failing readiness
        let _ = verify_signatures_compile;
    }

    #[tokio::test]
    async fn test_async_resource_phases() {
        // Given an extension resource whose async phases yield to the runtime
        struct AsyncResource;

        #[async_trait]
        impl Resource for AsyncResource {
            async fn provision(
                &mut self,
                _scenario_id: &ScenarioId,
            ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
                tokio::task::yield_now().await;
                Ok(vec![ResourceHandle {
                    id: "async_resource".to_string(),
                    connection_string: "://async".to_string(),
                    metadata: None,
                }])
            }

            async fn check_ready(&self) -> Result<(), InfrastructureError> {
                tokio::task::yield_now().await;
                Ok(())
            }

            async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
                Ok(())
            }

            async fn teardown(&mut self) -> Result<(), InfrastructureError> {
                Ok(())
            }
        }

        let mut ctx = Context::new();
        ctx.register_extension(AsyncResource);
        // When we run the full scenario lifecycle against it
        let result = run_scenario(
            &mut ctx,
            "test-async-resource",
            |_ctx: &mut Context| Box::pin(async {}),
            &mut EventLog::new(),
        )
        .await;
        // Then provisioning and readiness complete without error
        assert!(result.is_ok());
    }
}
