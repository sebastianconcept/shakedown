//! Resource providers and registration for the Shakedown harness.
//!
//! What: [`Resource`], [`BuiltinResource`], [`RegisteredResource`], and helpers such as
//! [`crate::resources::ResourceRegistry`].
//!
//! Why: tests need pluggable databases, brokers, and mocks behind one async contract with scenario
//! isolation (`scenario_id` on provision).
//!
//! How: implement [`Resource`] for your system; register via [`crate::lifecycle::Context`] or
//! `#[shakedown::test(resources = [...])]` for builtins.

use crate::error::{InfrastructureError, ResourceHandle, ScenarioId};
use async_trait::async_trait;
use std::fmt;

/// Derive a safe, unique namespace from a scenario id for any resource type.
///
/// If you're implementing [`Resource`] for your own infrastructure (Redis, Elasticsearch,
/// RabbitMQ, etc.), call this in your `provision` method to get a scenario-scoped name you
/// can use as a key prefix, database name, virtual host, index name — whatever your system
/// uses for isolation. Every scenario gets its own namespace; teardown drops it.
///
/// The built-in resources (Kafka, RabbitMQ, Postgres, MariaDB, MySQL, SQLite, Redis, Valkey, MongoDB) use specialised variants
/// of this logic internally. This helper gives you the same safety guarantees without having
/// to reimplement the sanitisation rules:
///
/// - Prefixed with `shakedown_` so names are greppable and won't collide with real data.
/// - Only lowercase alphanumerics and `_` — safe for virtually any system identifier.
/// - Truncated to `max_len` (or 63 by default, matching PostgreSQL's limit).
/// - Falls back to `"scenario"` when the id is empty.
///
/// ```
/// # use shakedown::resources::sanitize_resource_namespace;
/// let ns = sanitize_resource_namespace("My Cool Scenario!", 63);
/// assert_eq!(ns, "shakedown_my_cool_scenario_");
/// assert!(ns.len() <= 63);
/// ```
pub fn sanitize_resource_namespace(scenario_id: &str, max_len: usize) -> String {
    const PREFIX: &str = "shakedown_";
    let mut rest = String::new();
    for ch in scenario_id.chars().take(200) {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            rest.push(ch.to_ascii_lowercase());
        } else {
            rest.push('_');
        }
    }
    if rest.is_empty() {
        rest.push_str("scenario");
    }
    let mut out = String::with_capacity(max_len.min(PREFIX.len() + rest.len()));
    out.push_str(PREFIX);
    out.push_str(&rest);
    if out.len() > max_len {
        out.truncate(max_len);
    }
    out
}

/// Turn a scenario id into a safe Kafka topic prefix segment.
///
/// Prefixes with `shakedown_`, replaces disallowed characters with `_`, and caps length so topic
/// names stay valid on typical brokers. Used by the Kafka builtin (`kafka` feature) when provisioning.
pub fn sanitize_kafka_topic_prefix(scenario_id: &str) -> String {
    let mut out = String::with_capacity(scenario_id.len().saturating_add(12));
    out.push_str("shakedown_");
    for ch in scenario_id.chars().take(200) {
        if ch.is_ascii_alphanumeric() || ch == '.' || ch == '_' || ch == '-' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    out.push('_');
    out
}

/// Turn a scenario id into a safe RabbitMQ exchange/queue name prefix segment.
///
/// Uses the same character rules as [`sanitize_kafka_topic_prefix`] so topic-style and AMQP
/// routing identifiers stay consistent across Kafka and Rabbit in one process.
pub fn sanitize_rabbit_resource_prefix(scenario_id: &str) -> String {
    sanitize_kafka_topic_prefix(scenario_id)
}

/// Turn a scenario id into a safe Redis / Valkey key prefix (`shakedown:<id>:`).
///
/// Uses `:` as the segment separator (RESP key convention). Only ASCII alphanumerics survive;
/// other characters become `_`. Empty ids map to `shakedown:scenario:`.
pub fn sanitize_redis_key_prefix(scenario_id: &str) -> String {
    let mut middle = String::new();
    for ch in scenario_id.chars().take(200) {
        if ch.is_ascii_alphanumeric() {
            middle.push(ch.to_ascii_lowercase());
        } else {
            middle.push('_');
        }
    }
    if middle.is_empty() {
        middle.push_str("scenario");
    }
    format!("shakedown:{middle}:")
}

/// Turn a scenario id into a safe PostgreSQL database name (`shakedown_…`, max 63 bytes).
///
/// Prefixes with `shakedown_`, lowercases alphanumerics, replaces other characters with `_`, and
/// truncates to PostgreSQL’s 63-byte identifier limit.
pub fn sanitize_sql_database_name(scenario_id: &str) -> String {
    const PREFIX: &str = "shakedown_";
    const MAX_LEN: usize = 63;
    let mut rest = String::new();
    for ch in scenario_id.chars().take(200) {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            rest.push(ch.to_ascii_lowercase());
        } else {
            rest.push('_');
        }
    }
    if rest.is_empty() {
        rest.push_str("scenario");
    }
    let mut out = String::with_capacity(MAX_LEN);
    out.push_str(PREFIX);
    out.push_str(&rest);
    if out.len() > MAX_LEN {
        out.truncate(MAX_LEN);
    }
    out
}

/// Build a unique SQLite **named in-memory** URI for a scenario id.
///
/// Uses `file:<name>?mode=memory&cache=shared` so additional connections opened with the same URL
/// attach to one logical database; distinct scenario ids yield distinct names (isolation). The
/// harness keeps a primary [`rusqlite::Connection`] while [`crate::lifecycle::Context`] exposes the
/// same URI in [`SqliteBindings::database_url`] for user clients.
pub fn sqlite_named_memory_uri(scenario_id: &str) -> String {
    let mut safe = String::new();
    for ch in scenario_id.chars().take(120) {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            safe.push(ch.to_ascii_lowercase());
        } else {
            safe.push('_');
        }
    }
    if safe.is_empty() {
        safe.push_str("scenario");
    }
    format!("file:shakedown_{safe}?mode=memory&cache=shared")
}

/// Async resource contract — implement this trait to add a new infrastructure type.
///
/// Methods are async because provisioning (especially containers) involves I/O.
/// The `scenario_id` parameter enables logical isolation (e.g., topic prefix for Kafka).
///
/// `async_trait` is used here so third-party crates can use `Box<dyn Resource>` via
/// [`RegisteredResource::Extension`].
#[async_trait]
pub trait Resource: Send + Sync {
    /// Provision phase: create isolated state and resources for a scenario.
    async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError>;

    /// Readiness check: verify the resource is ready to use (beyond TCP).
    async fn check_ready(&self) -> Result<(), InfrastructureError>;

    /// Apply project default seed (migrations, reference data, baseline topics).
    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        Ok(())
    }

    /// Teardown: clean up scenario state, drop connections.
    async fn teardown(&mut self) -> Result<(), InfrastructureError>;
}

/// First-party resources as a closed enum for efficient internal dispatch.
#[allow(clippy::large_enum_variant)] // Kafka holds a container handle; boxing would diverge from SPECS §1a.
#[derive(Debug)]
pub enum BuiltinResource {
    /// Fake resource for non-Docker tests (unit test path)
    Fake(FakeResource),

    #[cfg(feature = "kafka")]
    /// Docker-backed Kafka (KRaft, apache image via testcontainers-modules).
    Kafka(KafkaResource),

    #[cfg(feature = "rabbitmq")]
    /// Docker-backed RabbitMQ (official image via testcontainers-modules).
    Rabbit(RabbitResource),

    #[cfg(feature = "postgres")]
    /// Docker-backed PostgreSQL (suite-shared container via testcontainers-modules).
    Postgres(PostgresResource),

    #[cfg(feature = "mariadb")]
    /// Docker-backed MariaDB (suite-shared container via testcontainers-modules).
    MariaDB(MariaDBResource),

    #[cfg(feature = "mysql")]
    /// Docker-backed Oracle MySQL (suite-shared container via testcontainers-modules).
    Mysql(MysqlResource),

    #[cfg(feature = "sqlite")]
    /// In-memory SQLite per scenario (no Docker; [`SqliteResource`]).
    Sqlite(SqliteResource),

    #[cfg(feature = "fault-injection")]
    /// Docker-backed Toxiproxy (suite-shared container via [`ToxiproxyResource`]).
    Toxiproxy(ToxiproxyResource),

    #[cfg(feature = "redis")]
    /// Docker-backed Redis (official image via testcontainers-modules; suite-shared container).
    Redis(RedisResource),

    #[cfg(feature = "valkey")]
    /// Docker-backed Valkey (official image via testcontainers-modules; suite-shared container).
    Valkey(ValkeyResource),

    #[cfg(feature = "mongodb")]
    /// Docker-backed MongoDB (official image via testcontainers-modules; suite-shared `mongod`).
    Mongo(MongoResource),
}

#[async_trait]
impl Resource for BuiltinResource {
    async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        match self {
            BuiltinResource::Fake(r) => r.provision(scenario_id).await,
            #[cfg(feature = "kafka")]
            BuiltinResource::Kafka(r) => r.provision(scenario_id).await,
            #[cfg(feature = "rabbitmq")]
            BuiltinResource::Rabbit(r) => r.provision(scenario_id).await,
            #[cfg(feature = "postgres")]
            BuiltinResource::Postgres(r) => r.provision(scenario_id).await,
            #[cfg(feature = "mariadb")]
            BuiltinResource::MariaDB(r) => r.provision(scenario_id).await,
            #[cfg(feature = "mysql")]
            BuiltinResource::Mysql(r) => r.provision(scenario_id).await,
            #[cfg(feature = "sqlite")]
            BuiltinResource::Sqlite(r) => r.provision(scenario_id).await,
            #[cfg(feature = "fault-injection")]
            BuiltinResource::Toxiproxy(r) => r.provision(scenario_id).await,
            #[cfg(feature = "redis")]
            BuiltinResource::Redis(r) => r.provision(scenario_id).await,
            #[cfg(feature = "valkey")]
            BuiltinResource::Valkey(r) => r.provision(scenario_id).await,
            #[cfg(feature = "mongodb")]
            BuiltinResource::Mongo(r) => r.provision(scenario_id).await,
        }
    }

    async fn check_ready(&self) -> Result<(), InfrastructureError> {
        match self {
            BuiltinResource::Fake(r) => r.check_ready().await,
            #[cfg(feature = "kafka")]
            BuiltinResource::Kafka(r) => r.check_ready().await,
            #[cfg(feature = "rabbitmq")]
            BuiltinResource::Rabbit(r) => r.check_ready().await,
            #[cfg(feature = "postgres")]
            BuiltinResource::Postgres(r) => r.check_ready().await,
            #[cfg(feature = "mariadb")]
            BuiltinResource::MariaDB(r) => r.check_ready().await,
            #[cfg(feature = "mysql")]
            BuiltinResource::Mysql(r) => r.check_ready().await,
            #[cfg(feature = "sqlite")]
            BuiltinResource::Sqlite(r) => r.check_ready().await,
            #[cfg(feature = "fault-injection")]
            BuiltinResource::Toxiproxy(r) => r.check_ready().await,
            #[cfg(feature = "redis")]
            BuiltinResource::Redis(r) => r.check_ready().await,
            #[cfg(feature = "valkey")]
            BuiltinResource::Valkey(r) => r.check_ready().await,
            #[cfg(feature = "mongodb")]
            BuiltinResource::Mongo(r) => r.check_ready().await,
        }
    }

    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        match self {
            BuiltinResource::Fake(r) => r.apply_default_seed().await,
            #[cfg(feature = "kafka")]
            BuiltinResource::Kafka(r) => r.apply_default_seed().await,
            #[cfg(feature = "rabbitmq")]
            BuiltinResource::Rabbit(r) => r.apply_default_seed().await,
            #[cfg(feature = "postgres")]
            BuiltinResource::Postgres(r) => r.apply_default_seed().await,
            #[cfg(feature = "mariadb")]
            BuiltinResource::MariaDB(r) => r.apply_default_seed().await,
            #[cfg(feature = "mysql")]
            BuiltinResource::Mysql(r) => r.apply_default_seed().await,
            #[cfg(feature = "sqlite")]
            BuiltinResource::Sqlite(r) => r.apply_default_seed().await,
            #[cfg(feature = "fault-injection")]
            BuiltinResource::Toxiproxy(r) => r.apply_default_seed().await,
            #[cfg(feature = "redis")]
            BuiltinResource::Redis(r) => r.apply_default_seed().await,
            #[cfg(feature = "valkey")]
            BuiltinResource::Valkey(r) => r.apply_default_seed().await,
            #[cfg(feature = "mongodb")]
            BuiltinResource::Mongo(r) => r.apply_default_seed().await,
        }
    }

    async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        match self {
            BuiltinResource::Fake(r) => r.teardown().await,
            #[cfg(feature = "kafka")]
            BuiltinResource::Kafka(r) => r.teardown().await,
            #[cfg(feature = "rabbitmq")]
            BuiltinResource::Rabbit(r) => r.teardown().await,
            #[cfg(feature = "postgres")]
            BuiltinResource::Postgres(r) => r.teardown().await,
            #[cfg(feature = "mariadb")]
            BuiltinResource::MariaDB(r) => r.teardown().await,
            #[cfg(feature = "mysql")]
            BuiltinResource::Mysql(r) => r.teardown().await,
            #[cfg(feature = "sqlite")]
            BuiltinResource::Sqlite(r) => r.teardown().await,
            #[cfg(feature = "fault-injection")]
            BuiltinResource::Toxiproxy(r) => r.teardown().await,
            #[cfg(feature = "redis")]
            BuiltinResource::Redis(r) => r.teardown().await,
            #[cfg(feature = "valkey")]
            BuiltinResource::Valkey(r) => r.teardown().await,
            #[cfg(feature = "mongodb")]
            BuiltinResource::Mongo(r) => r.teardown().await,
        }
    }
}

/// Stub [`Resource`] with no network or Docker I/O (always succeeds).
///
/// Exists so unit tests and CI jobs exercise the lifecycle without Testcontainers. Prefer
/// `#[shakedown::test(resources = [Fake])]` or [`crate::lifecycle::Context::register_builtin`] with
/// [`BuiltinResource::Fake`].
#[derive(Clone, Debug)]
pub struct FakeResource {
    _priv: std::marker::PhantomData<()>,
}

impl Default for FakeResource {
    fn default() -> Self {
        Self {
            _priv: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl Resource for FakeResource {
    async fn provision(
        &mut self,
        _scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        Ok(vec![ResourceHandle {
            id: "fake_resource".to_string(),
            connection_string: "://localhost/fake".to_string(),
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
        Ok(())
    }
}

#[cfg(feature = "kafka")]
mod kafka_impl {
    use super::{sanitize_kafka_topic_prefix, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{Consumer, DefaultConsumerContext};
    use rdkafka::types::RDKafkaErrorCode;
    use std::sync::OnceLock;
    use std::time::Duration;
    use testcontainers::core::Image;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::kafka::apache::{self, KAFKA_PORT};
    use tokio::sync::Mutex;

    /// Suite-wide broker: one Docker container per test process, shared by all [`KafkaResource`]
    /// instances until the last holder tears down (D-3).
    #[allow(clippy::large_enum_variant)] // Running holds ContainerAsync; boxing would complicate rm() handoff.
    enum SuiteBrokerState {
        Idle,
        Running {
            bootstrap: String,
            container: testcontainers::ContainerAsync<apache::Kafka>,
            refs: usize,
            /// Effective `image:tag` for mismatch detection across concurrent scenarios.
            image_key: String,
        },
    }

    static SUITE_BROKER: OnceLock<Mutex<SuiteBrokerState>> = OnceLock::new();

    fn suite_broker_mutex() -> &'static Mutex<SuiteBrokerState> {
        SUITE_BROKER.get_or_init(|| Mutex::new(SuiteBrokerState::Idle))
    }

    fn image_fingerprint(image: &apache::Kafka, tag_override: &Option<String>) -> String {
        let tag = tag_override.as_deref().unwrap_or_else(|| image.tag());
        format!("{}:{}", image.name(), tag)
    }

    /// Docker-backed Kafka using testcontainers-modules KRaft image (`apache/kafka-native` by default).
    ///
    /// **Suite lifecycle (D-3):** the first provision in the process starts a single KRaft broker;
    /// further [`KafkaResource`] instances reuse it. The last successful [`Resource::teardown`]
    /// removes the container. All instances in one process must use the same image name and tag
    /// (first configuration wins).
    #[derive(Debug)]
    pub struct KafkaResource {
        image: apache::Kafka,
        /// Optional image tag override (image name stays from [`apache::Kafka`]).
        image_tag: Option<String>,
        /// `true` after this instance acquired a reference to the suite broker.
        holds_suite_ref: bool,
        bootstrap_servers: Option<String>,
        topic_prefix: String,
    }

    impl Default for KafkaResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl KafkaResource {
        /// Create default Docker-backed Kafka settings (suite broker started on first provision).
        pub fn new() -> Self {
            Self {
                image: apache::Kafka::default(),
                image_tag: None,
                holds_suite_ref: false,
                bootstrap_servers: None,
                topic_prefix: String::new(),
            }
        }

        /// Override the image tag (e.g. pin `3.8.0` for production parity). Name remains `apache/kafka-native` / JVM variant from [`apache::Kafka`].
        pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
            self.image_tag = Some(tag.into());
            self
        }

        /// Use JVM image (`apache/kafka`) instead of native.
        pub fn with_jvm_image(mut self) -> Self {
            self.image = apache::Kafka::default().with_jvm_image();
            self
        }

        /// Apply tag override from `SHAKEDOWN_KAFKA_IMAGE_TAG` when set (suite-level parity with CI/prod pins).
        pub fn with_image_tag_from_env(mut self) -> Self {
            if let Ok(tag) = std::env::var("SHAKEDOWN_KAFKA_IMAGE_TAG") {
                let tag = tag.trim();
                if !tag.is_empty() {
                    self.image_tag = Some(tag.to_string());
                }
            }
            self
        }

        fn map_tc_err(e: impl std::fmt::Display + Send + Sync + 'static) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(e.to_string())))
        }
    }

    #[async_trait]
    impl Resource for KafkaResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.holds_suite_ref {
                return Err(InfrastructureError::AlreadyProvisioned("kafka".into()));
            }

            self.topic_prefix = sanitize_kafka_topic_prefix(scenario_id.as_str());
            let want_key = image_fingerprint(&self.image, &self.image_tag);

            let mut guard = suite_broker_mutex().lock().await;
            let bootstrap = match &mut *guard {
                SuiteBrokerState::Idle => {
                    use testcontainers::core::ImageExt;
                    let image = self.image.clone();
                    let node = if let Some(ref tag) = self.image_tag {
                        image
                            .with_tag(tag.clone())
                            .with_label(
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                            )
                            .start()
                            .await
                            .map_err(Self::map_tc_err)?
                    } else {
                        image
                            .with_label(
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                            )
                            .start()
                            .await
                            .map_err(Self::map_tc_err)?
                    };

                    let port = node
                        .get_host_port_ipv4(KAFKA_PORT)
                        .await
                        .map_err(Self::map_tc_err)?;
                    let bootstrap = format!("127.0.0.1:{port}");
                    *guard = SuiteBrokerState::Running {
                        bootstrap: bootstrap.clone(),
                        container: node,
                        refs: 1,
                        image_key: want_key.clone(),
                    };
                    bootstrap
                }
                SuiteBrokerState::Running {
                    bootstrap,
                    refs,
                    image_key,
                    ..
                } => {
                    if *image_key != want_key {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!(
                                "Kafka suite broker already running as {image_key}; \
                                 cannot start or join as {want_key}. Use one image:tag per process."
                            )),
                        )));
                    }
                    *refs += 1;
                    bootstrap.clone()
                }
            };
            drop(guard);

            self.bootstrap_servers = Some(bootstrap.clone());
            self.holds_suite_ref = true;

            Ok(vec![ResourceHandle {
                id: format!("kafka_{}", scenario_id),
                connection_string: bootstrap.clone(),
                metadata: Some(serde_json::json!({
                    "bootstrap_servers": bootstrap,
                    "topic_prefix": self.topic_prefix,
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let bootstrap = self
                .bootstrap_servers
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned("kafka".into()))?;

            let consumer: rdkafka::consumer::BaseConsumer<DefaultConsumerContext> =
                ClientConfig::new()
                    .set("bootstrap.servers", bootstrap)
                    .set("client.id", "shakedown_ready_probe")
                    .set("socket.timeout.ms", "5000")
                    .create()
                    .map_err(|e| InfrastructureError::Readiness {
                        resource: "kafka".into(),
                        source: Box::new(std::io::Error::other(e.to_string())),
                    })?;

            const ATTEMPTS: u32 = 30;
            for i in 0..ATTEMPTS {
                match consumer.fetch_metadata(None, Duration::from_secs(3)) {
                    Ok(meta) if !meta.brokers().is_empty() => return Ok(()),
                    Ok(_) => {}
                    Err(e) => {
                        if i + 1 == ATTEMPTS {
                            return Err(InfrastructureError::Readiness {
                                resource: "kafka".into(),
                                source: Box::new(std::io::Error::other(e.to_string())),
                            });
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }

            Err(InfrastructureError::Readiness {
                resource: "kafka".into(),
                source: Box::new(std::io::Error::other(
                    "Kafka metadata fetch did not succeed in time",
                )),
            })
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            let bootstrap = self
                .bootstrap_servers
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned("kafka".into()))?;

            let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
                .set("bootstrap.servers", bootstrap)
                .set("client.id", "shakedown_seed")
                .create()
                .map_err(|e| {
                    InfrastructureError::Backend(Box::new(std::io::Error::other(e.to_string())))
                })?;

            let proof_topic = format!("{}shakedown.proof", self.topic_prefix);
            let new_topics = [NewTopic::new(&proof_topic, 1, TopicReplication::Fixed(1))];

            let results = admin
                .create_topics(&new_topics, &AdminOptions::new())
                .await
                .map_err(|e| {
                    InfrastructureError::Backend(Box::new(std::io::Error::other(e.to_string())))
                })?;

            for r in results {
                if let Err((_, code)) = r {
                    if code != RDKafkaErrorCode::TopicAlreadyExists {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!("create topic: {code:?}")),
                        )));
                    }
                }
            }

            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if !self.holds_suite_ref {
                self.bootstrap_servers = None;
                return Ok(());
            }

            let mut guard = suite_broker_mutex().lock().await;
            let mut container_to_rm: Option<testcontainers::ContainerAsync<apache::Kafka>> = None;
            match &mut *guard {
                SuiteBrokerState::Idle => {}
                SuiteBrokerState::Running { refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuiteBrokerState::Idle);
                        if let SuiteBrokerState::Running { container, .. } = taken {
                            container_to_rm = Some(container);
                        }
                    }
                }
            }
            drop(guard);

            if let Some(c) = container_to_rm {
                c.rm().await.map_err(Self::map_tc_err)?;
            }

            self.holds_suite_ref = false;
            self.bootstrap_servers = None;
            Ok(())
        }
    }
}

#[cfg(feature = "rabbitmq")]
mod rabbit_impl {
    use super::{sanitize_rabbit_resource_prefix, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use lapin::{types::ShortString, Connection, ConnectionProperties};
    use std::sync::OnceLock;
    use testcontainers::core::Image;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::rabbitmq::RabbitMq;
    use tokio::sync::Mutex;

    const RABBITMQ_AMQP_PORT: u16 = 5672;

    #[allow(clippy::large_enum_variant)]
    enum SuiteBrokerState {
        Idle,
        Running {
            host: String,
            port: u16,
            container: testcontainers::ContainerAsync<RabbitMq>,
            refs: usize,
            image_key: String,
        },
    }

    static SUITE_BROKER: OnceLock<Mutex<SuiteBrokerState>> = OnceLock::new();

    fn suite_broker_mutex() -> &'static Mutex<SuiteBrokerState> {
        SUITE_BROKER.get_or_init(|| Mutex::new(SuiteBrokerState::Idle))
    }

    fn image_fingerprint(image: &RabbitMq, tag_override: &Option<String>) -> String {
        let tag = tag_override.as_deref().unwrap_or_else(|| image.tag());
        format!("{}:{}", image.name(), tag)
    }

    /// Docker-backed RabbitMQ using the official image via testcontainers-modules.
    ///
    /// **Suite lifecycle:** the first provision in the process starts a single broker; further
    /// [`RabbitResource`] instances reuse it. The last successful [`Resource::teardown`] removes the
    /// container when the refcount reaches zero.
    #[derive(Debug)]
    pub struct RabbitResource {
        image: RabbitMq,
        image_tag: Option<String>,
        holds_suite_ref: bool,
        host: Option<String>,
        port: Option<u16>,
        username: String,
        password: String,
        vhost: String,
        amqp_uri: Option<String>,
        resource_prefix: String,
    }

    impl Default for RabbitResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl RabbitResource {
        /// Create default Docker-backed RabbitMQ settings (suite broker started on first provision).
        pub fn new() -> Self {
            Self {
                image: RabbitMq::default(),
                image_tag: None,
                holds_suite_ref: false,
                host: None,
                port: None,
                username: "guest".to_string(),
                password: "guest".to_string(),
                vhost: "/".to_string(),
                amqp_uri: None,
                resource_prefix: String::new(),
            }
        }

        /// Override the image tag (e.g. pin a management image tag for parity). Name stays `rabbitmq`.
        pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
            self.image_tag = Some(tag.into());
            self
        }

        /// Apply tag override from `SHAKEDOWN_RABBITMQ_IMAGE_TAG` when set.
        pub fn with_image_tag_from_env(mut self) -> Self {
            if let Ok(tag) = std::env::var("SHAKEDOWN_RABBITMQ_IMAGE_TAG") {
                let tag = tag.trim();
                if !tag.is_empty() {
                    self.image_tag = Some(tag.to_string());
                }
            }
            self
        }

        fn map_tc_err(e: impl std::fmt::Display + Send + Sync + 'static) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(e.to_string())))
        }

        fn build_amqp_uri(host: &str, port: u16, username: &str, password: &str) -> String {
            format!("amqp://{username}:{password}@{host}:{port}")
        }
    }

    #[async_trait]
    impl Resource for RabbitResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.holds_suite_ref {
                return Err(InfrastructureError::AlreadyProvisioned("rabbitmq".into()));
            }

            self.resource_prefix = sanitize_rabbit_resource_prefix(scenario_id.as_str());
            let want_key = image_fingerprint(&self.image, &self.image_tag);

            let mut guard = suite_broker_mutex().lock().await;
            let (host, port) = match &mut *guard {
                SuiteBrokerState::Idle => {
                    use testcontainers::core::ImageExt;
                    let image = self.image.clone();
                    let node = if let Some(ref tag) = self.image_tag {
                        image
                            .with_tag(tag.clone())
                            .with_label(
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                            )
                            .start()
                            .await
                            .map_err(Self::map_tc_err)?
                    } else {
                        image
                            .with_label(
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                            )
                            .start()
                            .await
                            .map_err(Self::map_tc_err)?
                    };

                    let mapped_port = node
                        .get_host_port_ipv4(RABBITMQ_AMQP_PORT)
                        .await
                        .map_err(Self::map_tc_err)?;
                    let host = "127.0.0.1".to_string();
                    *guard = SuiteBrokerState::Running {
                        host: host.clone(),
                        port: mapped_port,
                        container: node,
                        refs: 1,
                        image_key: want_key.clone(),
                    };
                    (host, mapped_port)
                }
                SuiteBrokerState::Running {
                    host,
                    port,
                    refs,
                    image_key,
                    ..
                } => {
                    if *image_key != want_key {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!(
                                "RabbitMQ suite broker already running as {image_key}; \
                                 cannot start or join as {want_key}. Use one image:tag per process."
                            )),
                        )));
                    }
                    *refs += 1;
                    (host.clone(), *port)
                }
            };
            drop(guard);

            let amqp_uri = Self::build_amqp_uri(&host, port, &self.username, &self.password);
            self.host = Some(host.clone());
            self.port = Some(port);
            self.amqp_uri = Some(amqp_uri.clone());
            self.holds_suite_ref = true;

            Ok(vec![ResourceHandle {
                id: format!("rabbitmq_{scenario_id}"),
                connection_string: amqp_uri.clone(),
                metadata: Some(serde_json::json!({
                    "engine": "rabbitmq",
                    "amqp_uri": amqp_uri,
                    "host": host,
                    "port": port,
                    "username": self.username,
                    "password": self.password,
                    "vhost": self.vhost,
                    "resource_prefix": self.resource_prefix,
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let uri = self
                .amqp_uri
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned("rabbitmq".into()))?;

            const ATTEMPTS: u32 = 30;
            for attempt in 0..ATTEMPTS {
                match Connection::connect(uri, ConnectionProperties::default()).await {
                    Ok(conn) => match conn.create_channel().await {
                        Ok(_) => {
                            let _ = conn.close(200, ShortString::from("shakedown_ready")).await;
                            return Ok(());
                        }
                        Err(error) if attempt + 1 == ATTEMPTS => {
                            return Err(InfrastructureError::Readiness {
                                resource: "rabbitmq".into(),
                                source: Box::new(std::io::Error::other(error.to_string())),
                            });
                        }
                        Err(_) => {}
                    },
                    Err(error) if attempt + 1 == ATTEMPTS => {
                        return Err(InfrastructureError::Readiness {
                            resource: "rabbitmq".into(),
                            source: Box::new(std::io::Error::other(error.to_string())),
                        });
                    }
                    Err(_) => {}
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }

            Err(InfrastructureError::Readiness {
                resource: "rabbitmq".into(),
                source: Box::new(std::io::Error::other(
                    "RabbitMQ AMQP connection did not succeed in time",
                )),
            })
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if !self.holds_suite_ref {
                self.host = None;
                self.port = None;
                self.amqp_uri = None;
                self.resource_prefix.clear();
                return Ok(());
            }

            let mut guard = suite_broker_mutex().lock().await;
            let mut container_to_rm: Option<testcontainers::ContainerAsync<RabbitMq>> = None;
            match &mut *guard {
                SuiteBrokerState::Idle => {}
                SuiteBrokerState::Running { refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuiteBrokerState::Idle);
                        if let SuiteBrokerState::Running { container, .. } = taken {
                            container_to_rm = Some(container);
                        }
                    }
                }
            }
            drop(guard);

            if let Some(c) = container_to_rm {
                c.rm().await.map_err(Self::map_tc_err)?;
            }

            self.holds_suite_ref = false;
            self.host = None;
            self.port = None;
            self.amqp_uri = None;
            self.resource_prefix.clear();
            Ok(())
        }
    }
}

#[cfg(any(feature = "redis", feature = "valkey"))]
mod resp_family {
    use super::{sanitize_redis_key_prefix, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use testcontainers::core::Image;
    use testcontainers::core::ImageExt;
    use testcontainers::runners::AsyncRunner;
    use tokio::sync::Mutex;

    const RESP_PORT: u16 = 6379;

    #[allow(clippy::large_enum_variant)]
    pub(crate) enum SuiteState<I: Image> {
        Idle,
        Running {
            host: String,
            port: u16,
            container: testcontainers::ContainerAsync<I>,
            refs: usize,
            image_key: String,
        },
    }

    /// Suite-shared RESP server (Redis or Valkey image) with per-scenario key prefixes.
    pub struct RespResource<I>
    where
        I: Image + Clone + Send + Sync + std::fmt::Debug + 'static,
    {
        pub image: I,
        pub image_tag: Option<String>,
        pub suite: &'static Mutex<SuiteState<I>>,
        pub engine: &'static str,
        pub env_tag_var: &'static str,
        /// When set and `image_tag` is unset, use this instead of [`Image::tag`] (pins Redis to `7`).
        pub hardcoded_default_tag: Option<&'static str>,
        holds_suite_ref: bool,
        host: Option<String>,
        port: Option<u16>,
        connection_url: Option<String>,
        key_prefix: String,
    }

    impl<I: Image + Clone + Send + Sync + std::fmt::Debug + 'static> std::fmt::Debug
        for RespResource<I>
    {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("RespResource")
                .field("engine", &self.engine)
                .field("image_tag", &self.image_tag)
                .finish_non_exhaustive()
        }
    }

    impl<I: Image + Clone + Send + Sync + std::fmt::Debug + 'static> RespResource<I> {
        pub(crate) fn new(
            image: I,
            suite: &'static Mutex<SuiteState<I>>,
            engine: &'static str,
            env_tag_var: &'static str,
            hardcoded_default_tag: Option<&'static str>,
        ) -> Self {
            Self {
                image,
                image_tag: None,
                suite,
                engine,
                env_tag_var,
                hardcoded_default_tag,
                holds_suite_ref: false,
                host: None,
                port: None,
                connection_url: None,
                key_prefix: String::new(),
            }
        }

        /// Override the image tag (full `name:tag` is `Image::name` + this tag).
        pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
            self.image_tag = Some(tag.into());
            self
        }

        /// Load tag override from `self.env_tag_var` when set (e.g. `SHAKEDOWN_REDIS_IMAGE_TAG`).
        pub fn with_image_tag_from_env(mut self) -> Self {
            if let Ok(tag) = std::env::var(self.env_tag_var) {
                let tag = tag.trim();
                if !tag.is_empty() {
                    self.image_tag = Some(tag.to_string());
                }
            }
            self
        }

        fn map_tc_err(
            error: impl std::fmt::Display + Send + Sync + 'static,
        ) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
        }

        fn effective_tag(&self) -> String {
            if let Some(ref tag) = self.image_tag {
                return tag.clone();
            }
            if let Some(tag) = self.hardcoded_default_tag {
                return tag.to_string();
            }
            self.image.tag().to_string()
        }

        fn fingerprint(&self) -> String {
            format!("{}:{}", self.image.name(), self.effective_tag())
        }

        fn connection_url_display(&self) -> Result<&str, InfrastructureError> {
            self.connection_url
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned(self.engine.into()))
        }
    }

    async fn delete_keys_with_prefix(
        connection_url: &str,
        key_prefix: &str,
        resource_label: &str,
    ) -> Result<(), InfrastructureError> {
        let client =
            redis::Client::open(connection_url).map_err(|error| InfrastructureError::Teardown {
                resource: resource_label.to_string(),
                source: Box::new(std::io::Error::other(error.to_string())),
            })?;
        let mut connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| InfrastructureError::Teardown {
                resource: resource_label.to_string(),
                source: Box::new(std::io::Error::other(error.to_string())),
            })?;

        let pattern = format!("{key_prefix}*");
        let mut cursor: u64 = 0;
        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(256)
                .query_async(&mut connection)
                .await
                .map_err(|error| InfrastructureError::Teardown {
                    resource: resource_label.to_string(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;

            if !keys.is_empty() {
                redis::cmd("DEL")
                    .arg(&keys)
                    .query_async::<i64>(&mut connection)
                    .await
                    .map_err(|error| InfrastructureError::Teardown {
                        resource: resource_label.to_string(),
                        source: Box::new(std::io::Error::other(error.to_string())),
                    })?;
            }

            if next_cursor == 0 {
                break;
            }
            cursor = next_cursor;
        }

        Ok(())
    }

    #[async_trait]
    impl<I: Image + Clone + Send + Sync + std::fmt::Debug + 'static> Resource for RespResource<I> {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.holds_suite_ref {
                return Err(InfrastructureError::AlreadyProvisioned(self.engine.into()));
            }

            self.key_prefix = sanitize_redis_key_prefix(scenario_id.as_str());
            let want_key = self.fingerprint();

            let mut guard = self.suite.lock().await;
            let (host, port) = match &mut *guard {
                SuiteState::Idle => {
                    let tag = self.effective_tag();
                    let node = self
                        .image
                        .clone()
                        .with_tag(tag)
                        .with_label(
                            crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                            crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                        )
                        .start()
                        .await
                        .map_err(Self::map_tc_err)?;

                    let mapped_port = node
                        .get_host_port_ipv4(RESP_PORT)
                        .await
                        .map_err(Self::map_tc_err)?;
                    let host = "127.0.0.1".to_string();
                    *guard = SuiteState::Running {
                        host: host.clone(),
                        port: mapped_port,
                        container: node,
                        refs: 1,
                        image_key: want_key.clone(),
                    };
                    (host, mapped_port)
                }
                SuiteState::Running {
                    host,
                    port,
                    ref mut refs,
                    image_key,
                    ..
                } => {
                    if image_key != &want_key {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!(
                                "{} suite server already running as {image_key}; \
                                 cannot start or join as {want_key}. Use one image:tag per process.",
                                self.engine
                            )),
                        )));
                    }
                    *refs += 1;
                    (host.clone(), *port)
                }
            };
            drop(guard);

            let connection_url = format!("redis://{host}:{port}");
            self.host = Some(host.clone());
            self.port = Some(port);
            self.connection_url = Some(connection_url.clone());
            self.holds_suite_ref = true;

            Ok(vec![ResourceHandle {
                id: format!("{}_{scenario_id}", self.engine),
                connection_string: connection_url.clone(),
                metadata: Some(serde_json::json!({
                    "engine": self.engine,
                    "host": host,
                    "port": port,
                    "connection_url": connection_url,
                    "key_prefix": self.key_prefix,
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let url = self.connection_url_display()?;
            let client =
                redis::Client::open(url).map_err(|error| InfrastructureError::Readiness {
                    resource: self.engine.into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;

            const ATTEMPTS: u32 = 30;
            for attempt in 0..ATTEMPTS {
                match client.get_multiplexed_async_connection().await {
                    Ok(mut connection) => match redis::cmd("PING")
                        .query_async::<String>(&mut connection)
                        .await
                    {
                        Ok(response) if response.eq_ignore_ascii_case("PONG") => return Ok(()),
                        Ok(_) | Err(_) if attempt + 1 == ATTEMPTS => {
                            return Err(InfrastructureError::Readiness {
                                resource: self.engine.into(),
                                source: Box::new(std::io::Error::other(
                                    "Redis PING did not return PONG",
                                )),
                            });
                        }
                        Ok(_) | Err(_) => {}
                    },
                    Err(error) if attempt + 1 == ATTEMPTS => {
                        return Err(InfrastructureError::Readiness {
                            resource: self.engine.into(),
                            source: Box::new(std::io::Error::other(error.to_string())),
                        });
                    }
                    Err(_) => {}
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }

            Err(InfrastructureError::Readiness {
                resource: self.engine.into(),
                source: Box::new(std::io::Error::other(
                    "Redis readiness: connection attempts exhausted",
                )),
            })
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if !self.holds_suite_ref {
                self.host = None;
                self.port = None;
                self.connection_url = None;
                self.key_prefix.clear();
                return Ok(());
            }

            let url = self
                .connection_url
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned(self.engine.into()))?
                .to_string();
            let prefix = std::mem::take(&mut self.key_prefix);
            let engine = self.engine.to_string();

            delete_keys_with_prefix(url.as_str(), prefix.as_str(), engine.as_str()).await?;

            let mut guard = self.suite.lock().await;
            let mut container_to_remove: Option<testcontainers::ContainerAsync<I>> = None;
            match &mut *guard {
                SuiteState::Idle => {}
                SuiteState::Running { ref mut refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuiteState::Idle);
                        if let SuiteState::Running { container, .. } = taken {
                            container_to_remove = Some(container);
                        }
                    }
                }
            }
            drop(guard);

            if let Some(container) = container_to_remove {
                container.rm().await.map_err(Self::map_tc_err)?;
            }

            self.holds_suite_ref = false;
            self.host = None;
            self.port = None;
            self.connection_url = None;
            Ok(())
        }
    }
}

#[cfg(feature = "redis")]
mod redis_impl {
    use super::resp_family::{RespResource, SuiteState};
    use super::{Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use std::sync::OnceLock;
    use testcontainers_modules::redis::Redis;
    use tokio::sync::Mutex;

    static SUITE_REDIS: OnceLock<Mutex<SuiteState<Redis>>> = OnceLock::new();

    fn suite_redis_mutex() -> &'static Mutex<SuiteState<Redis>> {
        SUITE_REDIS.get_or_init(|| Mutex::new(SuiteState::Idle))
    }

    /// Docker-backed Redis using the official image via testcontainers-modules.
    ///
    /// Suite lifecycle matches Rabbit/Postgres: one container per process; refcounted teardown.
    /// Default image tag is **`7`** (`redis:7`). Override with [`Self::with_image_tag`] or
    /// `SHAKEDOWN_REDIS_IMAGE_TAG`.
    #[derive(Debug)]
    pub struct RedisResource(pub(super) RespResource<Redis>);

    impl Default for RedisResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl RedisResource {
        pub fn new() -> Self {
            Self(
                RespResource::new(
                    Redis::default(),
                    suite_redis_mutex(),
                    "redis",
                    "SHAKEDOWN_REDIS_IMAGE_TAG",
                    Some("7"),
                )
                .with_image_tag_from_env(),
            )
        }

        pub fn with_image_tag(self, tag: impl Into<String>) -> Self {
            Self(self.0.with_image_tag(tag))
        }

        pub fn with_image_tag_from_env(self) -> Self {
            Self(self.0.with_image_tag_from_env())
        }
    }

    #[async_trait]
    impl Resource for RedisResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            self.0.provision(scenario_id).await
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            self.0.check_ready().await
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            self.0.apply_default_seed().await
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            self.0.teardown().await
        }
    }
}

#[cfg(feature = "valkey")]
mod valkey_impl {
    use super::resp_family::{RespResource, SuiteState};
    use super::{Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use std::sync::OnceLock;
    use testcontainers_modules::valkey::Valkey;
    use tokio::sync::Mutex;

    static SUITE_VALKEY: OnceLock<Mutex<SuiteState<Valkey>>> = OnceLock::new();

    fn suite_valkey_mutex() -> &'static Mutex<SuiteState<Valkey>> {
        SUITE_VALKEY.get_or_init(|| Mutex::new(SuiteState::Idle))
    }

    /// Docker-backed Valkey using the official image via testcontainers-modules.
    ///
    /// Default tag follows the module image (Valkey 8.x). Override with [`Self::with_image_tag`] or
    /// `SHAKEDOWN_VALKEY_IMAGE_TAG`.
    #[derive(Debug)]
    pub struct ValkeyResource(pub(super) RespResource<Valkey>);

    impl Default for ValkeyResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl ValkeyResource {
        pub fn new() -> Self {
            Self(
                RespResource::new(
                    Valkey::default(),
                    suite_valkey_mutex(),
                    "valkey",
                    "SHAKEDOWN_VALKEY_IMAGE_TAG",
                    Some("8"),
                )
                .with_image_tag_from_env(),
            )
        }

        pub fn with_image_tag(self, tag: impl Into<String>) -> Self {
            Self(self.0.with_image_tag(tag))
        }

        pub fn with_image_tag_from_env(self) -> Self {
            Self(self.0.with_image_tag_from_env())
        }
    }

    #[async_trait]
    impl Resource for ValkeyResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            self.0.provision(scenario_id).await
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            self.0.check_ready().await
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            self.0.apply_default_seed().await
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            self.0.teardown().await
        }
    }
}

#[cfg(feature = "mongodb")]
mod mongo_impl {
    use super::{sanitize_sql_database_name, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use mongodb::bson::doc;
    use mongodb::Client;
    use std::sync::OnceLock;
    use testcontainers::core::{Image, ImageExt};
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::mongo::Mongo;
    use tokio::sync::Mutex;

    const MONGO_PORT: u16 = 27017;
    /// Default `mongo` image tag (official Docker Hub; extra-infra SPECS).
    pub const DEFAULT_MONGO_IMAGE_TAG: &str = "7";
    const SUITE_USER: &str = "root";
    const SUITE_PASSWORD: &str = "root";

    #[allow(clippy::large_enum_variant)]
    enum SuiteMongoState {
        Idle,
        Running {
            host: String,
            port: u16,
            container: testcontainers::ContainerAsync<Mongo>,
            refs: usize,
            image_key: String,
        },
    }

    static SUITE_MONGO: OnceLock<Mutex<SuiteMongoState>> = OnceLock::new();

    fn suite_mongo_mutex() -> &'static Mutex<SuiteMongoState> {
        SUITE_MONGO.get_or_init(|| Mutex::new(SuiteMongoState::Idle))
    }

    fn image_fingerprint(image: &Mongo, tag_override: &Option<String>) -> String {
        let tag = tag_override.as_deref().unwrap_or(DEFAULT_MONGO_IMAGE_TAG);
        format!("{}:{}", image.name(), tag)
    }

    fn admin_connection_uri(host: &str, port: u16) -> String {
        format!("mongodb://{SUITE_USER}:{SUITE_PASSWORD}@{host}:{port}/admin?authSource=admin")
    }

    fn scenario_connection_uri(host: &str, port: u16, database: &str) -> String {
        format!("mongodb://{SUITE_USER}:{SUITE_PASSWORD}@{host}:{port}/{database}?authSource=admin")
    }

    async fn mongo_client(uri: &str) -> Result<Client, mongodb::error::Error> {
        Client::with_uri_str(uri).await
    }

    /// Docker-backed MongoDB using testcontainers-modules (`mongo` image).
    ///
    /// **Suite lifecycle:** one `mongod` per process; refcounted teardown drops the scenario database
    /// and removes the container when the last reference completes teardown.
    ///
    /// Default image tag is **`7`** (`mongo:7`). Override with [`Self::with_image_tag`] or
    /// `SHAKEDOWN_MONGO_IMAGE_TAG`. Root credentials are fixed (`root` / `root`) for the suite container.
    #[derive(Debug)]
    pub struct MongoResource {
        image: Mongo,
        image_tag: Option<String>,
        holds_suite_ref: bool,
        host: Option<String>,
        port: Option<u16>,
        username: String,
        password: String,
        database_name: String,
    }

    impl Default for MongoResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl MongoResource {
        /// Create default Docker-backed MongoDB settings (suite server started on first provision).
        pub fn new() -> Self {
            Self {
                image: Mongo::default(),
                image_tag: Some(DEFAULT_MONGO_IMAGE_TAG.to_string()),
                holds_suite_ref: false,
                host: None,
                port: None,
                username: SUITE_USER.to_string(),
                password: SUITE_PASSWORD.to_string(),
                database_name: String::new(),
            }
        }

        /// Override the image tag (pin `7.0` or another tag for production parity). Image name stays `mongo`.
        pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
            self.image_tag = Some(tag.into());
            self
        }

        /// Apply tag override from `SHAKEDOWN_MONGO_IMAGE_TAG` when set.
        pub fn with_image_tag_from_env(mut self) -> Self {
            if let Ok(tag) = std::env::var("SHAKEDOWN_MONGO_IMAGE_TAG") {
                let tag = tag.trim();
                if !tag.is_empty() {
                    self.image_tag = Some(tag.to_string());
                }
            }
            self
        }

        fn map_tc_err(
            error: impl std::fmt::Display + Send + Sync + 'static,
        ) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
        }
    }

    #[async_trait]
    impl Resource for MongoResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.holds_suite_ref {
                return Err(InfrastructureError::AlreadyProvisioned("mongodb".into()));
            }

            let database_name = sanitize_sql_database_name(scenario_id.as_str());
            let want_key = image_fingerprint(&self.image, &self.image_tag);

            let mut guard = suite_mongo_mutex().lock().await;
            let (host, port) = match &mut *guard {
                SuiteMongoState::Idle => {
                    let tag = self.image_tag.as_deref().unwrap_or(DEFAULT_MONGO_IMAGE_TAG);
                    let node = self
                        .image
                        .clone()
                        .with_tag(tag.to_string())
                        .with_env_var("MONGO_INITDB_ROOT_USERNAME", SUITE_USER)
                        .with_env_var("MONGO_INITDB_ROOT_PASSWORD", SUITE_PASSWORD)
                        .with_label(
                            crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                            crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                        )
                        .start()
                        .await
                        .map_err(Self::map_tc_err)?;

                    let mapped_port = node
                        .get_host_port_ipv4(MONGO_PORT)
                        .await
                        .map_err(Self::map_tc_err)?;
                    let host = "127.0.0.1".to_string();

                    *guard = SuiteMongoState::Running {
                        host: host.clone(),
                        port: mapped_port,
                        container: node,
                        refs: 1,
                        image_key: want_key.clone(),
                    };
                    (host, mapped_port)
                }
                SuiteMongoState::Running {
                    host,
                    port,
                    refs,
                    image_key,
                    ..
                } => {
                    if *image_key != want_key {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!(
                                "MongoDB suite server already running as {image_key}; \
                                 cannot start or join as {want_key}. Use one image:tag per process."
                            )),
                        )));
                    }
                    *refs += 1;
                    (host.clone(), *port)
                }
            };
            drop(guard);

            let admin_uri = admin_connection_uri(host.as_str(), port);
            let client = mongo_client(admin_uri.as_str()).await.map_err(|error| {
                InfrastructureError::Provision {
                    resource: "mongodb".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                }
            })?;
            let scenario_db = client.database(database_name.as_str());
            let init_coll = scenario_db.collection::<mongodb::bson::Document>("_shakedown_init");
            init_coll
                .insert_one(doc! { "ok": true })
                .await
                .map_err(|error| InfrastructureError::Provision {
                    resource: "mongodb".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;

            self.holds_suite_ref = true;
            self.host = Some(host.clone());
            self.port = Some(port);
            self.database_name = database_name.clone();

            let connection_url =
                scenario_connection_uri(host.as_str(), port, database_name.as_str());
            let username = self.username.clone();
            let password = self.password.clone();

            Ok(vec![ResourceHandle {
                id: format!("mongo_{scenario_id}"),
                connection_string: connection_url.clone(),
                metadata: Some(serde_json::json!({
                    "host": host,
                    "port": port,
                    "database": database_name,
                    "username": username,
                    "password": password,
                    "connection_url": connection_url,
                    "engine": "mongodb",
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let host = self
                .host
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned("mongodb".into()))?;
            let port = self
                .port
                .ok_or_else(|| InfrastructureError::NotProvisioned("mongodb".into()))?;

            let admin_uri = admin_connection_uri(host, port);
            let client = mongo_client(admin_uri.as_str()).await.map_err(|error| {
                InfrastructureError::Readiness {
                    resource: "mongodb".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                }
            })?;
            let admin = client.database("admin");
            admin
                .run_command(doc! { "ping": 1 })
                .await
                .map_err(|error| InfrastructureError::Readiness {
                    resource: "mongodb".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;
            Ok(())
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if !self.holds_suite_ref {
                self.host = None;
                self.port = None;
                self.database_name.clear();
                return Ok(());
            }

            let database_name = self.database_name.clone();
            let host = self.host.clone().unwrap_or_default();
            let port = self.port.unwrap_or(MONGO_PORT);

            if !database_name.is_empty() {
                let admin_uri = admin_connection_uri(host.as_str(), port);
                if let Ok(client) = mongo_client(admin_uri.as_str()).await {
                    let db = client.database(database_name.as_str());
                    let _ = db.drop().await;
                }
            }

            let mut guard = suite_mongo_mutex().lock().await;
            let mut container_to_rm: Option<testcontainers::ContainerAsync<Mongo>> = None;
            match &mut *guard {
                SuiteMongoState::Idle => {}
                SuiteMongoState::Running { refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuiteMongoState::Idle);
                        if let SuiteMongoState::Running { container, .. } = taken {
                            container_to_rm = Some(container);
                        }
                    }
                }
            }
            drop(guard);

            if let Some(container) = container_to_rm {
                container.rm().await.map_err(Self::map_tc_err)?;
            }

            self.holds_suite_ref = false;
            self.host = None;
            self.port = None;
            self.database_name.clear();
            Ok(())
        }
    }
}

#[cfg(feature = "postgres")]
mod postgres_impl {
    use super::{sanitize_sql_database_name, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use std::sync::OnceLock;
    use testcontainers::core::Image;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::postgres::Postgres;
    use tokio::sync::Mutex;
    use tokio_postgres::NoTls;

    const POSTGRES_PORT: u16 = 5432;
    /// Default `postgres` image tag (Debian-based, non-alpine; see `mvp-postgres-mariadb` SPECS).
    pub const DEFAULT_POSTGRES_IMAGE_TAG: &str = "16";

    #[allow(clippy::large_enum_variant)]
    enum SuitePostgresState {
        Idle,
        Running {
            host: String,
            port: u16,
            username: String,
            password: String,
            container: testcontainers::ContainerAsync<Postgres>,
            refs: usize,
            image_key: String,
        },
    }

    static SUITE_POSTGRES: OnceLock<Mutex<SuitePostgresState>> = OnceLock::new();

    fn suite_postgres_mutex() -> &'static Mutex<SuitePostgresState> {
        SUITE_POSTGRES.get_or_init(|| Mutex::new(SuitePostgresState::Idle))
    }

    fn image_fingerprint(image: &Postgres, tag_override: &Option<String>) -> String {
        let tag = tag_override
            .as_deref()
            .unwrap_or(DEFAULT_POSTGRES_IMAGE_TAG);
        format!("{}:{}", image.name(), tag)
    }

    fn admin_connection_url(host: &str, port: u16, username: &str, password: &str) -> String {
        format!("postgresql://{username}:{password}@{host}:{port}/postgres")
    }

    fn scenario_connection_url(
        host: &str,
        port: u16,
        database: &str,
        username: &str,
        password: &str,
    ) -> String {
        format!("postgresql://{username}:{password}@{host}:{port}/{database}")
    }

    async fn connect_with_url(url: &str) -> Result<tokio_postgres::Client, InfrastructureError> {
        let (client, connection) = tokio_postgres::connect(url, NoTls).await.map_err(|e| {
            InfrastructureError::Backend(Box::new(std::io::Error::other(e.to_string())))
        })?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }

    /// Docker-backed PostgreSQL using testcontainers-modules (`postgres` image).
    ///
    /// **Suite lifecycle:** the first provision in the process starts a single server; further
    /// [`PostgresResource`] instances reuse it. The last successful [`Resource::teardown`] drops the
    /// scenario database and may remove the container when the refcount reaches zero.
    #[derive(Debug)]
    pub struct PostgresResource {
        image: Postgres,
        image_tag: Option<String>,
        holds_suite_ref: bool,
        host: Option<String>,
        port: Option<u16>,
        username: String,
        password: String,
        database_name: String,
    }

    impl Default for PostgresResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl PostgresResource {
        /// Create default Docker-backed Postgres settings (suite server started on first provision).
        pub fn new() -> Self {
            Self {
                image: Postgres::default(),
                image_tag: Some(DEFAULT_POSTGRES_IMAGE_TAG.to_string()),
                holds_suite_ref: false,
                host: None,
                port: None,
                username: "postgres".to_string(),
                password: "postgres".to_string(),
                database_name: String::new(),
            }
        }

        /// Override the image tag (e.g. pin `16` or `16.4` for production parity). Name stays `postgres`.
        pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
            self.image_tag = Some(tag.into());
            self
        }

        /// Apply tag override from `SHAKEDOWN_POSTGRES_IMAGE_TAG` when set.
        pub fn with_image_tag_from_env(mut self) -> Self {
            if let Ok(tag) = std::env::var("SHAKEDOWN_POSTGRES_IMAGE_TAG") {
                let tag = tag.trim();
                if !tag.is_empty() {
                    self.image_tag = Some(tag.to_string());
                }
            }
            self
        }

        fn map_tc_err(e: impl std::fmt::Display + Send + Sync + 'static) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(e.to_string())))
        }
    }

    #[async_trait]
    impl Resource for PostgresResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.holds_suite_ref {
                return Err(InfrastructureError::AlreadyProvisioned("postgres".into()));
            }

            let database_name = sanitize_sql_database_name(scenario_id.as_str());
            let want_key = image_fingerprint(&self.image, &self.image_tag);

            let mut guard = suite_postgres_mutex().lock().await;
            let (host, port, username, password) = match &mut *guard {
                SuitePostgresState::Idle => {
                    use testcontainers::core::ImageExt;
                    let image = self.image.clone();
                    let node = if let Some(ref tag) = self.image_tag {
                        image
                            .with_tag(tag.clone())
                            .with_label(
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                            )
                            .start()
                            .await
                            .map_err(Self::map_tc_err)?
                    } else {
                        image
                            .with_label(
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                            )
                            .start()
                            .await
                            .map_err(Self::map_tc_err)?
                    };

                    let mapped_port = node
                        .get_host_port_ipv4(POSTGRES_PORT)
                        .await
                        .map_err(Self::map_tc_err)?;
                    let host = "127.0.0.1".to_string();
                    let username = "postgres".to_string();
                    let password = "postgres".to_string();

                    *guard = SuitePostgresState::Running {
                        host: host.clone(),
                        port: mapped_port,
                        username: username.clone(),
                        password: password.clone(),
                        container: node,
                        refs: 1,
                        image_key: want_key.clone(),
                    };
                    (host, mapped_port, username, password)
                }
                SuitePostgresState::Running {
                    host,
                    port,
                    username,
                    password,
                    refs,
                    image_key,
                    ..
                } => {
                    if *image_key != want_key {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!(
                                "Postgres suite server already running as {image_key}; \
                                 cannot start or join as {want_key}. Use one image:tag per process."
                            )),
                        )));
                    }
                    *refs += 1;
                    (host.clone(), *port, username.clone(), password.clone())
                }
            };
            drop(guard);

            let admin_url = admin_connection_url(&host, port, &username, &password);
            let client = connect_with_url(&admin_url).await?;
            let create_sql = format!("CREATE DATABASE {}", database_name);
            client.batch_execute(&create_sql).await.map_err(|e| {
                InfrastructureError::Provision {
                    resource: "postgres".into(),
                    source: Box::new(std::io::Error::other(e.to_string())),
                }
            })?;

            self.holds_suite_ref = true;
            self.host = Some(host.clone());
            self.port = Some(port);
            self.username = username.clone();
            self.password = password.clone();
            self.database_name = database_name.clone();

            let connection_url =
                scenario_connection_url(&host, port, &database_name, &username, &password);

            Ok(vec![ResourceHandle {
                id: format!("postgres_{}", scenario_id),
                connection_string: connection_url.clone(),
                metadata: Some(serde_json::json!({
                    "host": host,
                    "port": port,
                    "database": database_name,
                    "username": username,
                    "password": password,
                    "connection_url": connection_url,
                    "engine": "postgres",
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let host = self
                .host
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned("postgres".into()))?;
            let port = self
                .port
                .ok_or_else(|| InfrastructureError::NotProvisioned("postgres".into()))?;
            let url = scenario_connection_url(
                host,
                port,
                &self.database_name,
                &self.username,
                &self.password,
            );
            let client =
                connect_with_url(&url)
                    .await
                    .map_err(|e| InfrastructureError::Readiness {
                        resource: "postgres".into(),
                        source: Box::new(std::io::Error::other(e.to_string())),
                    })?;
            client.query_one("SELECT 1", &[]).await.map_err(|e| {
                InfrastructureError::Readiness {
                    resource: "postgres".into(),
                    source: Box::new(std::io::Error::other(e.to_string())),
                }
            })?;
            Ok(())
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if !self.holds_suite_ref {
                self.host = None;
                self.port = None;
                self.database_name.clear();
                return Ok(());
            }

            let database_name = self.database_name.clone();
            let host = self.host.clone().unwrap_or_default();
            let port = self.port.unwrap_or(POSTGRES_PORT);
            let username = self.username.clone();
            let password = self.password.clone();

            if !database_name.is_empty() {
                let admin_url = admin_connection_url(&host, port, &username, &password);
                if let Ok(client) = connect_with_url(&admin_url).await {
                    let _ = client
                        .query(
                            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
                            &[&database_name],
                        )
                        .await;
                    let drop_sql = format!("DROP DATABASE IF EXISTS {}", database_name);
                    let _ = client.batch_execute(&drop_sql).await;
                }
            }

            let mut guard = suite_postgres_mutex().lock().await;
            let mut container_to_rm: Option<testcontainers::ContainerAsync<Postgres>> = None;
            match &mut *guard {
                SuitePostgresState::Idle => {}
                SuitePostgresState::Running { refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuitePostgresState::Idle);
                        if let SuitePostgresState::Running { container, .. } = taken {
                            container_to_rm = Some(container);
                        }
                    }
                }
            }
            drop(guard);

            if let Some(c) = container_to_rm {
                c.rm().await.map_err(Self::map_tc_err)?;
            }

            self.holds_suite_ref = false;
            self.host = None;
            self.port = None;
            self.database_name.clear();
            Ok(())
        }
    }
}

#[cfg(feature = "mariadb")]
mod mariadb_impl {
    use super::{sanitize_sql_database_name, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use mysql_async::prelude::Queryable;
    use mysql_async::{OptsBuilder, Pool};
    use std::sync::OnceLock;
    use testcontainers::core::Image;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::mariadb::Mariadb;
    use tokio::sync::Mutex;

    const MARIADB_PORT: u16 = 3306;
    /// Default `mariadb` image tag (non-alpine; see `mvp-postgres-mariadb` SPECS).
    pub const DEFAULT_MARIADB_IMAGE_TAG: &str = "11";

    #[allow(clippy::large_enum_variant)]
    enum SuiteMariaDBState {
        Idle,
        Running {
            host: String,
            port: u16,
            username: String,
            password: String,
            container: testcontainers::ContainerAsync<Mariadb>,
            refs: usize,
            image_key: String,
        },
    }

    static SUITE_MARIADB: OnceLock<Mutex<SuiteMariaDBState>> = OnceLock::new();

    fn suite_mariadb_mutex() -> &'static Mutex<SuiteMariaDBState> {
        SUITE_MARIADB.get_or_init(|| Mutex::new(SuiteMariaDBState::Idle))
    }

    fn image_fingerprint(image: &Mariadb, tag_override: &Option<String>) -> String {
        let tag = tag_override.as_deref().unwrap_or(DEFAULT_MARIADB_IMAGE_TAG);
        format!("{}:{}", image.name(), tag)
    }

    fn admin_pool(host: &str, port: u16, username: &str, password: &str) -> Pool {
        let mut builder = OptsBuilder::default()
            .user(Some(username))
            .ip_or_hostname(host)
            .tcp_port(port)
            .db_name(Some("mysql"));
        if password.is_empty() {
            builder = builder.pass::<String>(None);
        } else {
            builder = builder.pass(Some(password));
        }
        Pool::new(builder)
    }

    fn scenario_connection_url(
        host: &str,
        port: u16,
        database: &str,
        username: &str,
        password: &str,
    ) -> String {
        if password.is_empty() {
            format!("mysql://{username}@{host}:{port}/{database}")
        } else {
            format!("mysql://{username}:{password}@{host}:{port}/{database}")
        }
    }

    /// Docker-backed MariaDB using testcontainers-modules (`mariadb` image).
    ///
    /// **Suite lifecycle:** the first provision in the process starts a single server; further
    /// [`MariaDBResource`] instances reuse it. The last successful [`Resource::teardown`] drops the
    /// scenario database and may remove the container when the refcount reaches zero.
    #[derive(Debug)]
    pub struct MariaDBResource {
        image: Mariadb,
        image_tag: Option<String>,
        holds_suite_ref: bool,
        host: Option<String>,
        port: Option<u16>,
        username: String,
        password: String,
        database_name: String,
    }

    impl Default for MariaDBResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl MariaDBResource {
        /// Create default Docker-backed MariaDB settings (suite server started on first provision).
        pub fn new() -> Self {
            Self {
                image: Mariadb::default(),
                image_tag: Some(DEFAULT_MARIADB_IMAGE_TAG.to_string()),
                holds_suite_ref: false,
                host: None,
                port: None,
                username: "root".to_string(),
                password: String::new(),
                database_name: String::new(),
            }
        }

        /// Override the image tag (e.g. pin `11` or `11.4` for production parity). Name stays `mariadb`.
        pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
            self.image_tag = Some(tag.into());
            self
        }

        /// Apply tag override from `SHAKEDOWN_MARIADB_IMAGE_TAG` when set.
        pub fn with_image_tag_from_env(mut self) -> Self {
            if let Ok(tag) = std::env::var("SHAKEDOWN_MARIADB_IMAGE_TAG") {
                let tag = tag.trim();
                if !tag.is_empty() {
                    self.image_tag = Some(tag.to_string());
                }
            }
            self
        }

        fn map_tc_err(e: impl std::fmt::Display + Send + Sync + 'static) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(e.to_string())))
        }

        fn map_mysql_err(e: mysql_async::Error) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(e.to_string())))
        }
    }

    #[async_trait]
    impl Resource for MariaDBResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.holds_suite_ref {
                return Err(InfrastructureError::AlreadyProvisioned("mariadb".into()));
            }

            let database_name = sanitize_sql_database_name(scenario_id.as_str());
            let want_key = image_fingerprint(&self.image, &self.image_tag);

            let mut guard = suite_mariadb_mutex().lock().await;
            let (host, port, username, password) = match &mut *guard {
                SuiteMariaDBState::Idle => {
                    use testcontainers::core::ImageExt;
                    let image = self.image.clone();
                    let node = if let Some(ref tag) = self.image_tag {
                        image
                            .with_tag(tag.clone())
                            .with_label(
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                            )
                            .start()
                            .await
                            .map_err(Self::map_tc_err)?
                    } else {
                        image
                            .with_label(
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                            )
                            .start()
                            .await
                            .map_err(Self::map_tc_err)?
                    };

                    let mapped_port = node
                        .get_host_port_ipv4(MARIADB_PORT)
                        .await
                        .map_err(Self::map_tc_err)?;
                    let host = "127.0.0.1".to_string();
                    let username = "root".to_string();
                    let password = String::new();

                    *guard = SuiteMariaDBState::Running {
                        host: host.clone(),
                        port: mapped_port,
                        username: username.clone(),
                        password: password.clone(),
                        container: node,
                        refs: 1,
                        image_key: want_key.clone(),
                    };
                    (host, mapped_port, username, password)
                }
                SuiteMariaDBState::Running {
                    host,
                    port,
                    username,
                    password,
                    refs,
                    image_key,
                    ..
                } => {
                    if *image_key != want_key {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!(
                                "MariaDB suite server already running as {image_key}; \
                                 cannot start or join as {want_key}. Use one image:tag per process."
                            )),
                        )));
                    }
                    *refs += 1;
                    (host.clone(), *port, username.clone(), password.clone())
                }
            };
            drop(guard);

            let pool = admin_pool(&host, port, &username, &password);
            let mut conn = pool.get_conn().await.map_err(Self::map_mysql_err)?;
            let create_sql = format!("CREATE DATABASE `{}`", database_name.replace('`', ""));
            conn.query_drop(create_sql.as_str()).await.map_err(|e| {
                InfrastructureError::Provision {
                    resource: "mariadb".into(),
                    source: Box::new(std::io::Error::other(e.to_string())),
                }
            })?;

            self.holds_suite_ref = true;
            self.host = Some(host.clone());
            self.port = Some(port);
            self.username = username.clone();
            self.password = password.clone();
            self.database_name = database_name.clone();

            let connection_url =
                scenario_connection_url(&host, port, &database_name, &username, &password);

            Ok(vec![ResourceHandle {
                id: format!("mariadb_{}", scenario_id),
                connection_string: connection_url.clone(),
                metadata: Some(serde_json::json!({
                    "host": host,
                    "port": port,
                    "database": database_name,
                    "username": username,
                    "password": password,
                    "connection_url": connection_url,
                    "engine": "mariadb",
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let host = self
                .host
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned("mariadb".into()))?;
            let port = self
                .port
                .ok_or_else(|| InfrastructureError::NotProvisioned("mariadb".into()))?;
            let pool = admin_pool(host, port, &self.username, &self.password);
            let mut conn = pool
                .get_conn()
                .await
                .map_err(|e| InfrastructureError::Readiness {
                    resource: "mariadb".into(),
                    source: Box::new(std::io::Error::other(e.to_string())),
                })?;
            conn.query_drop(format!("USE `{}`", self.database_name.replace('`', "")).as_str())
                .await
                .map_err(|e| InfrastructureError::Readiness {
                    resource: "mariadb".into(),
                    source: Box::new(std::io::Error::other(e.to_string())),
                })?;
            conn.query_drop("SELECT 1")
                .await
                .map_err(|e| InfrastructureError::Readiness {
                    resource: "mariadb".into(),
                    source: Box::new(std::io::Error::other(e.to_string())),
                })?;
            Ok(())
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if !self.holds_suite_ref {
                self.host = None;
                self.port = None;
                self.database_name.clear();
                return Ok(());
            }

            let database_name = self.database_name.clone();
            let host = self.host.clone().unwrap_or_default();
            let port = self.port.unwrap_or(MARIADB_PORT);
            let username = self.username.clone();
            let password = self.password.clone();

            if !database_name.is_empty() {
                let pool = admin_pool(&host, port, &username, &password);
                if let Ok(mut conn) = pool.get_conn().await {
                    let safe = database_name.replace('`', "");
                    let kill_sql = format!(
                        "SELECT CONCAT('KILL ', id, ';') AS stmt FROM information_schema.processlist \
                         WHERE db = '{}'",
                        safe.replace('\'', "''")
                    );
                    if let Ok(rows) = conn.query::<String, _>(kill_sql).await {
                        for stmt in rows {
                            let _ = conn.query_drop(stmt.as_str()).await;
                        }
                    }
                    let drop_sql = format!("DROP DATABASE IF EXISTS `{}`", safe);
                    let _ = conn.query_drop(drop_sql.as_str()).await;
                }
            }

            let mut guard = suite_mariadb_mutex().lock().await;
            let mut container_to_rm: Option<testcontainers::ContainerAsync<Mariadb>> = None;
            match &mut *guard {
                SuiteMariaDBState::Idle => {}
                SuiteMariaDBState::Running { refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuiteMariaDBState::Idle);
                        if let SuiteMariaDBState::Running { container, .. } = taken {
                            container_to_rm = Some(container);
                        }
                    }
                }
            }
            drop(guard);

            if let Some(c) = container_to_rm {
                c.rm().await.map_err(Self::map_tc_err)?;
            }

            self.holds_suite_ref = false;
            self.host = None;
            self.port = None;
            self.database_name.clear();
            Ok(())
        }
    }
}

#[cfg(feature = "mysql")]
mod mysql_impl {
    use super::{sanitize_sql_database_name, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use mysql_async::prelude::Queryable;
    use mysql_async::{OptsBuilder, Pool};
    use std::sync::OnceLock;
    use testcontainers::core::Image;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::mysql::Mysql as MysqlImage;
    use tokio::sync::Mutex;

    const MYSQL_PORT: u16 = 3306;
    /// Default `mysql` image tag (Oracle MySQL; non-alpine; see `mvp-postgres-mariadb` SPECS).
    pub const DEFAULT_MYSQL_IMAGE_TAG: &str = "8.4";

    #[allow(clippy::large_enum_variant)]
    enum SuiteMysqlState {
        Idle,
        Running {
            host: String,
            port: u16,
            username: String,
            password: String,
            container: testcontainers::ContainerAsync<MysqlImage>,
            refs: usize,
            image_key: String,
        },
    }

    static SUITE_MYSQL: OnceLock<Mutex<SuiteMysqlState>> = OnceLock::new();

    fn suite_mysql_mutex() -> &'static Mutex<SuiteMysqlState> {
        SUITE_MYSQL.get_or_init(|| Mutex::new(SuiteMysqlState::Idle))
    }

    fn image_fingerprint(image: &MysqlImage, tag_override: &Option<String>) -> String {
        let tag = tag_override.as_deref().unwrap_or(DEFAULT_MYSQL_IMAGE_TAG);
        format!("{}:{}", image.name(), tag)
    }

    fn admin_pool(host: &str, port: u16, username: &str, password: &str) -> Pool {
        let mut builder = OptsBuilder::default()
            .user(Some(username))
            .ip_or_hostname(host)
            .tcp_port(port)
            .db_name(Some("mysql"));
        if password.is_empty() {
            builder = builder.pass::<String>(None);
        } else {
            builder = builder.pass(Some(password));
        }
        Pool::new(builder)
    }

    fn scenario_connection_url(
        host: &str,
        port: u16,
        database: &str,
        username: &str,
        password: &str,
    ) -> String {
        if password.is_empty() {
            format!("mysql://{username}@{host}:{port}/{database}")
        } else {
            format!("mysql://{username}:{password}@{host}:{port}/{database}")
        }
    }

    /// Docker-backed Oracle MySQL using testcontainers-modules (`mysql` image).
    ///
    /// **Suite lifecycle:** the first provision in the process starts a single server; further
    /// [`MysqlResource`] instances reuse it. The last successful [`Resource::teardown`] drops the
    /// scenario database and may remove the container when the refcount reaches zero.
    #[derive(Debug)]
    pub struct MysqlResource {
        image: MysqlImage,
        image_tag: Option<String>,
        holds_suite_ref: bool,
        host: Option<String>,
        port: Option<u16>,
        username: String,
        password: String,
        database_name: String,
    }

    impl Default for MysqlResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl MysqlResource {
        /// Create default Docker-backed MySQL settings (suite server started on first provision).
        pub fn new() -> Self {
            Self {
                image: MysqlImage::default(),
                image_tag: Some(DEFAULT_MYSQL_IMAGE_TAG.to_string()),
                holds_suite_ref: false,
                host: None,
                port: None,
                username: "root".to_string(),
                password: String::new(),
                database_name: String::new(),
            }
        }

        /// Override the image tag (e.g. pin `8.4` or `8.4.2` for production parity). Name stays `mysql`.
        pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
            self.image_tag = Some(tag.into());
            self
        }

        /// Apply tag override from `SHAKEDOWN_MYSQL_IMAGE_TAG` when set.
        pub fn with_image_tag_from_env(mut self) -> Self {
            if let Ok(tag) = std::env::var("SHAKEDOWN_MYSQL_IMAGE_TAG") {
                let tag = tag.trim();
                if !tag.is_empty() {
                    self.image_tag = Some(tag.to_string());
                }
            }
            self
        }

        fn map_tc_err(e: impl std::fmt::Display + Send + Sync + 'static) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(e.to_string())))
        }

        fn map_mysql_err(e: mysql_async::Error) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(e.to_string())))
        }
    }

    #[async_trait]
    impl Resource for MysqlResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.holds_suite_ref {
                return Err(InfrastructureError::AlreadyProvisioned("mysql".into()));
            }

            let database_name = sanitize_sql_database_name(scenario_id.as_str());
            let want_key = image_fingerprint(&self.image, &self.image_tag);

            let mut guard = suite_mysql_mutex().lock().await;
            let (host, port, username, password) = match &mut *guard {
                SuiteMysqlState::Idle => {
                    use testcontainers::core::ImageExt;
                    let image = self.image.clone();
                    let node = if let Some(ref tag) = self.image_tag {
                        image
                            .with_tag(tag.clone())
                            .with_label(
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                            )
                            .start()
                            .await
                            .map_err(Self::map_tc_err)?
                    } else {
                        image
                            .with_label(
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                            )
                            .start()
                            .await
                            .map_err(Self::map_tc_err)?
                    };

                    let mapped_port = node
                        .get_host_port_ipv4(MYSQL_PORT)
                        .await
                        .map_err(Self::map_tc_err)?;
                    let host = "127.0.0.1".to_string();
                    let username = "root".to_string();
                    let password = String::new();

                    *guard = SuiteMysqlState::Running {
                        host: host.clone(),
                        port: mapped_port,
                        username: username.clone(),
                        password: password.clone(),
                        container: node,
                        refs: 1,
                        image_key: want_key.clone(),
                    };
                    (host, mapped_port, username, password)
                }
                SuiteMysqlState::Running {
                    host,
                    port,
                    username,
                    password,
                    refs,
                    image_key,
                    ..
                } => {
                    if *image_key != want_key {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!(
                                "MySQL suite server already running as {image_key}; \
                                 cannot start or join as {want_key}. Use one image:tag per process."
                            )),
                        )));
                    }
                    *refs += 1;
                    (host.clone(), *port, username.clone(), password.clone())
                }
            };
            drop(guard);

            let pool = admin_pool(&host, port, &username, &password);
            let mut conn = pool.get_conn().await.map_err(Self::map_mysql_err)?;
            let create_sql = format!("CREATE DATABASE `{}`", database_name.replace('`', ""));
            conn.query_drop(create_sql.as_str()).await.map_err(|e| {
                InfrastructureError::Provision {
                    resource: "mysql".into(),
                    source: Box::new(std::io::Error::other(e.to_string())),
                }
            })?;

            self.holds_suite_ref = true;
            self.host = Some(host.clone());
            self.port = Some(port);
            self.username = username.clone();
            self.password = password.clone();
            self.database_name = database_name.clone();

            let connection_url =
                scenario_connection_url(&host, port, &database_name, &username, &password);

            Ok(vec![ResourceHandle {
                id: format!("mysql_{}", scenario_id),
                connection_string: connection_url.clone(),
                metadata: Some(serde_json::json!({
                    "host": host,
                    "port": port,
                    "database": database_name,
                    "username": username,
                    "password": password,
                    "connection_url": connection_url,
                    "engine": "mysql",
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let host = self
                .host
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned("mysql".into()))?;
            let port = self
                .port
                .ok_or_else(|| InfrastructureError::NotProvisioned("mysql".into()))?;
            let pool = admin_pool(host, port, &self.username, &self.password);
            let mut conn = pool
                .get_conn()
                .await
                .map_err(|e| InfrastructureError::Readiness {
                    resource: "mysql".into(),
                    source: Box::new(std::io::Error::other(e.to_string())),
                })?;
            conn.query_drop(format!("USE `{}`", self.database_name.replace('`', "")).as_str())
                .await
                .map_err(|e| InfrastructureError::Readiness {
                    resource: "mysql".into(),
                    source: Box::new(std::io::Error::other(e.to_string())),
                })?;
            conn.query_drop("SELECT 1")
                .await
                .map_err(|e| InfrastructureError::Readiness {
                    resource: "mysql".into(),
                    source: Box::new(std::io::Error::other(e.to_string())),
                })?;
            Ok(())
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if !self.holds_suite_ref {
                self.host = None;
                self.port = None;
                self.database_name.clear();
                return Ok(());
            }

            let database_name = self.database_name.clone();
            let host = self.host.clone().unwrap_or_default();
            let port = self.port.unwrap_or(MYSQL_PORT);
            let username = self.username.clone();
            let password = self.password.clone();

            if !database_name.is_empty() {
                let pool = admin_pool(&host, port, &username, &password);
                if let Ok(mut conn) = pool.get_conn().await {
                    let safe = database_name.replace('`', "");
                    let kill_sql = format!(
                        "SELECT CONCAT('KILL ', id, ';') AS stmt FROM information_schema.processlist \
                         WHERE db = '{}'",
                        safe.replace('\'', "''")
                    );
                    if let Ok(rows) = conn.query::<String, _>(kill_sql).await {
                        for stmt in rows {
                            let _ = conn.query_drop(stmt.as_str()).await;
                        }
                    }
                    let drop_sql = format!("DROP DATABASE IF EXISTS `{}`", safe);
                    let _ = conn.query_drop(drop_sql.as_str()).await;
                }
            }

            let mut guard = suite_mysql_mutex().lock().await;
            let mut container_to_rm: Option<testcontainers::ContainerAsync<MysqlImage>> = None;
            match &mut *guard {
                SuiteMysqlState::Idle => {}
                SuiteMysqlState::Running { refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuiteMysqlState::Idle);
                        if let SuiteMysqlState::Running { container, .. } = taken {
                            container_to_rm = Some(container);
                        }
                    }
                }
            }
            drop(guard);

            if let Some(container_async) = container_to_rm {
                container_async.rm().await.map_err(Self::map_tc_err)?;
            }

            self.holds_suite_ref = false;
            self.host = None;
            self.port = None;
            self.database_name.clear();
            Ok(())
        }
    }
}

#[cfg(feature = "sqlite")]
mod sqlite_impl {
    use super::{sqlite_named_memory_uri, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use rusqlite::{Connection, OpenFlags};
    use std::sync::{Arc, Mutex};

    const RESOURCE_NAME: &str = "sqlite";

    /// In-memory SQLite for one scenario (no Docker).
    ///
    /// Opens a **named shared-cache in-memory** database (see [`super::sqlite_named_memory_uri`]) so
    /// [`SqliteBindings::database_url`] can be used with `rusqlite` or other drivers to open further
    /// connections to the same logical database.
    ///
    /// `rusqlite::Connection` is not [`Sync`]; it is wrapped in [`Arc`] [`Mutex`] so [`BuiltinResource`]
    /// remains [`Sync`] and readiness can use [`tokio::task::spawn_blocking`] (works on the default
    /// single-threaded `#[tokio::test]` runtime).
    #[derive(Debug, Default)]
    pub struct SqliteResource {
        connection: Option<Arc<Mutex<Connection>>>,
        /// Retained for debugging; matches metadata `database_url`.
        database_url: String,
    }

    impl SqliteResource {
        /// New SQLite resource (connection opened on [`Resource::provision`]).
        pub fn new() -> Self {
            Self::default()
        }
    }

    #[async_trait]
    impl Resource for SqliteResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.connection.is_some() {
                return Err(InfrastructureError::AlreadyProvisioned(
                    RESOURCE_NAME.into(),
                ));
            }

            let database_url = sqlite_named_memory_uri(scenario_id.as_str());
            let open_url = database_url.clone();
            let connection = tokio::task::spawn_blocking(move || {
                Connection::open_with_flags(
                    open_url.as_str(),
                    OpenFlags::SQLITE_OPEN_URI
                        | OpenFlags::SQLITE_OPEN_READ_WRITE
                        | OpenFlags::SQLITE_OPEN_CREATE,
                )
            })
            .await
            .map_err(|join_error| InfrastructureError::Provision {
                resource: RESOURCE_NAME.into(),
                source: Box::new(std::io::Error::other(join_error.to_string())),
            })?
            .map_err(|source| InfrastructureError::Provision {
                resource: RESOURCE_NAME.into(),
                source: Box::new(source),
            })?;

            self.connection = Some(Arc::new(Mutex::new(connection)));
            self.database_url.clone_from(&database_url);

            Ok(vec![ResourceHandle {
                id: format!("sqlite_{scenario_id}"),
                connection_string: database_url.clone(),
                metadata: Some(serde_json::json!({
                    "database_url": database_url,
                    "engine": "sqlite",
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let arc = self
                .connection
                .as_ref()
                .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
            let arc = Arc::clone(arc);
            tokio::task::spawn_blocking(move || {
                let connection = arc
                    .lock()
                    .map_err(|poison| InfrastructureError::Readiness {
                        resource: RESOURCE_NAME.into(),
                        source: Box::new(std::io::Error::other(poison.to_string())),
                    })?;
                connection
                    .query_row("SELECT 1", [], |row| row.get::<_, i32>(0))
                    .map_err(|source| InfrastructureError::Readiness {
                        resource: RESOURCE_NAME.into(),
                        source: Box::new(source),
                    })?;
                Ok::<(), InfrastructureError>(())
            })
            .await
            .map_err(|join_error| InfrastructureError::Readiness {
                resource: RESOURCE_NAME.into(),
                source: Box::new(std::io::Error::other(join_error.to_string())),
            })?
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            self.connection.take();
            self.database_url.clear();
            Ok(())
        }
    }
}

#[cfg(feature = "fault-injection")]
mod toxiproxy_impl {
    //! Suite-shared Toxiproxy (`ghcr.io/shopify/toxiproxy`) for fault injection (SPECS §2).
    //!
    //! Container labels use [`crate::docker_labels`] (same strings as `diagnostics::labels`).

    use super::{Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use noxious_client::Client as ToxiproxyClient;
    use std::sync::OnceLock;
    use std::time::Duration;
    use testcontainers::core::IntoContainerPort;
    use testcontainers::core::WaitFor;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::GenericImage;
    use testcontainers::ImageExt;
    use tokio::sync::Mutex;

    use crate::docker_labels::{SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE};

    const RESOURCE_NAME: &str = "toxiproxy";
    const API_PORT: u16 = 8474;
    const DEFAULT_IMAGE_NAME: &str = "ghcr.io/shopify/toxiproxy";
    const DEFAULT_IMAGE_TAG: &str = "2.9.0";

    #[allow(clippy::large_enum_variant)]
    enum SuiteToxiproxyState {
        Idle,
        Running {
            api_base_url: String,
            container: testcontainers::ContainerAsync<GenericImage>,
            refs: usize,
            image_key: String,
        },
    }

    static SUITE_TOXIPROXY: OnceLock<Mutex<SuiteToxiproxyState>> = OnceLock::new();

    fn suite_toxiproxy_mutex() -> &'static Mutex<SuiteToxiproxyState> {
        SUITE_TOXIPROXY.get_or_init(|| Mutex::new(SuiteToxiproxyState::Idle))
    }

    fn image_fingerprint(image_name: &str, tag_override: &Option<String>) -> String {
        let tag = tag_override
            .as_deref()
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .unwrap_or(DEFAULT_IMAGE_TAG);
        format!("{image_name}:{tag}")
    }

    /// Docker-backed Toxiproxy using a suite-wide container (mirrors [`super::kafka_impl::KafkaResource`] / D-3).
    #[derive(Debug)]
    pub struct ToxiproxyResource {
        image_name: String,
        image_tag: Option<String>,
        holds_suite_ref: bool,
        api_base_url: Option<String>,
    }

    impl Default for ToxiproxyResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl ToxiproxyResource {
        /// Default image [`DEFAULT_IMAGE_NAME`] — suite broker starts on first provision.
        pub fn new() -> Self {
            Self {
                image_name: DEFAULT_IMAGE_NAME.to_string(),
                image_tag: None,
                holds_suite_ref: false,
                api_base_url: None,
            }
        }

        /// Override the image tag (e.g. pin a release). Image name stays [`DEFAULT_IMAGE_NAME`].
        pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
            self.image_tag = Some(tag.into());
            self
        }

        /// Apply tag from `SHAKEDOWN_TOXIPROXY_IMAGE_TAG` when set (suite-level CI/prod pins).
        pub fn with_image_tag_from_env(mut self) -> Self {
            if let Ok(tag) = std::env::var("SHAKEDOWN_TOXIPROXY_IMAGE_TAG") {
                let tag = tag.trim();
                if !tag.is_empty() {
                    self.image_tag = Some(tag.to_string());
                }
            }
            self
        }

        fn map_tc_err(
            error: impl std::fmt::Display + Send + Sync + 'static,
        ) -> InfrastructureError {
            InfrastructureError::Provision {
                resource: RESOURCE_NAME.into(),
                source: Box::new(std::io::Error::other(error.to_string())),
            }
        }

        async fn get_version_ok(api_base_url: &str) -> Result<(), InfrastructureError> {
            let url = format!("{}/version", api_base_url.trim_end_matches('/'));
            let http = reqwest::Client::builder()
                .timeout(Duration::from_secs(4))
                .build()
                .map_err(|source| InfrastructureError::Readiness {
                    resource: RESOURCE_NAME.into(),
                    source: Box::new(source),
                })?;
            const ATTEMPTS: u32 = 30;
            for attempt in 0..ATTEMPTS {
                match http.get(&url).send().await {
                    Ok(response) if response.status().is_success() => return Ok(()),
                    Ok(response) => {
                        if attempt + 1 == ATTEMPTS {
                            return Err(InfrastructureError::Readiness {
                                resource: RESOURCE_NAME.into(),
                                source: Box::new(std::io::Error::other(format!(
                                    "GET /version returned {}",
                                    response.status()
                                ))),
                            });
                        }
                    }
                    Err(err) => {
                        if attempt + 1 == ATTEMPTS {
                            return Err(InfrastructureError::Readiness {
                                resource: RESOURCE_NAME.into(),
                                source: Box::new(std::io::Error::other(err.to_string())),
                            });
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(InfrastructureError::Readiness {
                resource: RESOURCE_NAME.into(),
                source: Box::new(std::io::Error::other(
                    "GET /version did not succeed in time",
                )),
            })
        }
    }

    #[async_trait]
    impl Resource for ToxiproxyResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.holds_suite_ref {
                return Err(InfrastructureError::AlreadyProvisioned(
                    RESOURCE_NAME.into(),
                ));
            }

            let want_key = image_fingerprint(&self.image_name, &self.image_tag);

            let mut guard = suite_toxiproxy_mutex().lock().await;
            let api_base_url = match &mut *guard {
                SuiteToxiproxyState::Idle => {
                    let tag = self
                        .image_tag
                        .clone()
                        .unwrap_or_else(|| DEFAULT_IMAGE_TAG.to_string());
                    let image = GenericImage::new(self.image_name.clone(), tag)
                        .with_exposed_port(API_PORT.tcp())
                        .with_wait_for(WaitFor::seconds(2))
                        .with_label(SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE);
                    let node = image.start().await.map_err(Self::map_tc_err)?;
                    let port = node
                        .get_host_port_ipv4(API_PORT)
                        .await
                        .map_err(Self::map_tc_err)?;
                    let api_base_url = format!("http://127.0.0.1:{port}");
                    *guard = SuiteToxiproxyState::Running {
                        api_base_url: api_base_url.clone(),
                        container: node,
                        refs: 1,
                        image_key: want_key.clone(),
                    };
                    api_base_url
                }
                SuiteToxiproxyState::Running {
                    api_base_url,
                    refs,
                    image_key,
                    ..
                } => {
                    if *image_key != want_key {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!(
                                "Toxiproxy suite already running as {image_key}; \
                                 cannot start or join as {want_key}. Use one image:tag per process."
                            )),
                        )));
                    }
                    *refs += 1;
                    api_base_url.clone()
                }
            };
            drop(guard);

            self.api_base_url = Some(api_base_url.clone());
            self.holds_suite_ref = true;

            Ok(vec![ResourceHandle {
                id: format!("toxiproxy_{scenario_id}"),
                connection_string: api_base_url.clone(),
                metadata: Some(serde_json::json!({
                    "engine": "toxiproxy",
                    "api_url": api_base_url,
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let api_base_url = self
                .api_base_url
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
            Self::get_version_ok(api_base_url).await
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if !self.holds_suite_ref {
                self.api_base_url = None;
                return Ok(());
            }

            let api_base = self
                .api_base_url
                .take()
                .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;

            let client = ToxiproxyClient::new(&api_base);
            let proxies =
                client
                    .proxies()
                    .await
                    .map_err(|source| InfrastructureError::Teardown {
                        resource: RESOURCE_NAME.into(),
                        source: Box::new(source),
                    })?;
            for (_name, proxy) in proxies {
                proxy
                    .delete()
                    .await
                    .map_err(|source| InfrastructureError::Teardown {
                        resource: RESOURCE_NAME.into(),
                        source: Box::new(source),
                    })?;
            }

            let mut guard = suite_toxiproxy_mutex().lock().await;
            let mut container_to_rm: Option<testcontainers::ContainerAsync<GenericImage>> = None;
            match &mut *guard {
                SuiteToxiproxyState::Idle => {}
                SuiteToxiproxyState::Running { refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuiteToxiproxyState::Idle);
                        if let SuiteToxiproxyState::Running { container, .. } = taken {
                            container_to_rm = Some(container);
                        }
                    }
                }
            }
            drop(guard);

            if let Some(container) = container_to_rm {
                container
                    .rm()
                    .await
                    .map_err(|remove_error| InfrastructureError::Teardown {
                        resource: RESOURCE_NAME.into(),
                        source: Box::new(std::io::Error::other(remove_error.to_string())),
                    })?;
            }

            self.holds_suite_ref = false;
            self.api_base_url = None;
            Ok(())
        }
    }
}

#[cfg(feature = "fault-injection")]
pub use toxiproxy_impl::ToxiproxyResource;

#[cfg(feature = "kafka")]
pub use kafka_impl::KafkaResource;

#[cfg(feature = "rabbitmq")]
pub use rabbit_impl::RabbitResource;

#[cfg(feature = "postgres")]
pub use postgres_impl::PostgresResource;

#[cfg(feature = "mariadb")]
pub use mariadb_impl::MariaDBResource;

#[cfg(feature = "mysql")]
pub use mysql_impl::MysqlResource;

#[cfg(feature = "sqlite")]
pub use sqlite_impl::SqliteResource;

#[cfg(feature = "redis")]
pub use redis_impl::RedisResource;

#[cfg(feature = "valkey")]
pub use valkey_impl::ValkeyResource;

#[cfg(feature = "mongodb")]
pub use mongo_impl::MongoResource;

/// Connection and naming details extracted after Kafka provision for a scenario.
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from [`crate::lifecycle::Context::kafka_bindings`]
/// inside scenarios that produce/consume messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaBindings {
    /// Bootstrap servers URL.
    pub bootstrap_servers: String,
    /// Topic prefix for this scenario's namespace.
    pub topic_prefix: String,
    /// Optional client ID prefix for isolation.
    pub client_id_prefix: Option<String>,
}

impl KafkaBindings {
    /// Create bindings with the given prefix.
    pub fn new(bootstrap_servers: impl Into<String>, topic_prefix: impl Into<String>) -> Self {
        let bootstrap = bootstrap_servers.into();
        let prefix = topic_prefix.into();
        KafkaBindings {
            bootstrap_servers: bootstrap.clone(),
            topic_prefix: prefix.clone(),
            client_id_prefix: Some(format!("shakedown_{prefix}")),
        }
    }

    /// Create bindings without a client ID prefix.
    pub fn with_bootstrap_only(
        bootstrap_servers: impl Into<String>,
        topic_prefix: impl Into<String>,
    ) -> Self {
        KafkaBindings {
            bootstrap_servers: bootstrap_servers.into(),
            topic_prefix: topic_prefix.into(),
            client_id_prefix: None,
        }
    }

    /// Return the topic name under this scenario's prefix.
    pub fn topic_namespace(&self, topic_name: &str) -> String {
        format!("{}{}", self.topic_prefix, topic_name)
    }
}

/// AMQP connection details and scenario-scoped naming for RabbitMQ.
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from
/// [`crate::lifecycle::Context::rabbit_bindings`] after provision (`rabbitmq` feature).
#[cfg(feature = "rabbitmq")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RabbitBindings {
    /// Full AMQP URI (includes credentials; test images only — do not use in production).
    pub amqp_uri: String,
    /// Broker hostname or IP (typically `127.0.0.1`).
    pub host: String,
    /// Mapped host port for AMQP (`5672` in the default image).
    pub port: u16,
    /// Username (default `guest` for the official test image).
    pub username: String,
    /// Password (default `guest`).
    pub password: String,
    /// Virtual host (default `/`).
    pub vhost: String,
    /// Prefix for exchange and queue names for this scenario (isolation).
    pub resource_prefix: String,
    /// Optional client connection name prefix (mirrors [`KafkaBindings::client_id_prefix`]).
    pub client_id_prefix: Option<String>,
}

#[cfg(feature = "rabbitmq")]
impl RabbitBindings {
    /// Build bindings from provision metadata fields.
    pub fn new(
        amqp_uri: impl Into<String>,
        host: impl Into<String>,
        port: u16,
        username: impl Into<String>,
        password: impl Into<String>,
        vhost: impl Into<String>,
        resource_prefix: impl Into<String>,
    ) -> Self {
        let resource_prefix = resource_prefix.into();
        let prefix_trimmed = resource_prefix.trim_end_matches('_').to_string();
        RabbitBindings {
            amqp_uri: amqp_uri.into(),
            host: host.into(),
            port,
            username: username.into(),
            password: password.into(),
            vhost: vhost.into(),
            resource_prefix,
            client_id_prefix: Some(format!("shakedown_{prefix_trimmed}")),
        }
    }

    /// Compose a queue or exchange name under this scenario's prefix (same composition as [`KafkaBindings::topic_namespace`]).
    pub fn namespaced_name(&self, logical_name: &str) -> String {
        format!("{}{}", self.resource_prefix, logical_name)
    }
}

/// Connection details for a provisioned PostgreSQL scenario database.
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from
/// [`crate::lifecycle::Context::postgres_bindings`] after provision.
#[cfg(feature = "postgres")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresBindings {
    /// Hostname or IP (typically `127.0.0.1` for the suite container).
    pub host: String,
    /// Mapped host port for PostgreSQL.
    pub port: u16,
    /// Scenario-isolated database name.
    pub database: String,
    /// Superuser or application user for the scenario database.
    pub username: String,
    /// Password for `username`.
    pub password: String,
    /// JDBC/libpq-style URL for this scenario database.
    pub connection_url: String,
}

/// Connection details for a provisioned MariaDB scenario database.
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from
/// [`crate::lifecycle::Context::mariadb_bindings`] after provision.
#[cfg(feature = "mariadb")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MariaDBBindings {
    /// Hostname or IP (typically `127.0.0.1` for the suite container).
    pub host: String,
    /// Mapped host port for MariaDB.
    pub port: u16,
    /// Scenario-isolated database name.
    pub database: String,
    /// User for the scenario database (suite default is often `root`).
    pub username: String,
    /// Password for `username` (may be empty for test images).
    pub password: String,
    /// `mysql://` URL for this scenario database.
    pub connection_url: String,
}

/// Connection details for a provisioned Oracle MySQL scenario database.
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from
/// [`crate::lifecycle::Context::mysql_bindings`] after provision.
#[cfg(feature = "mysql")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MysqlBindings {
    /// Hostname or IP (typically `127.0.0.1` for the suite container).
    pub host: String,
    /// Mapped host port for MySQL.
    pub port: u16,
    /// Scenario-isolated database name.
    pub database: String,
    /// User for the scenario database (suite default is often `root`).
    pub username: String,
    /// Password for `username` (may be empty for test images).
    pub password: String,
    /// `mysql://` URL for this scenario database.
    pub connection_url: String,
}

/// Connection details for a provisioned in-memory SQLite database.
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from
/// [`crate::lifecycle::Context::sqlite_bindings`] after provision.
#[cfg(feature = "sqlite")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteBindings {
    /// URI string for this scenario's database (named in-memory with shared cache; see feature SPECS).
    pub database_url: String,
}

/// Redis or Valkey connection details and scenario-scoped key prefix.
///
/// Populated from provision metadata for both `redis` and `valkey` engine handles; use
/// [`crate::lifecycle::Context::redis_bindings`] after provision.
#[cfg(any(feature = "redis", feature = "valkey"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisBindings {
    /// `redis://` URL without authentication (default suite images).
    pub connection_url: String,
    pub host: String,
    pub port: u16,
    /// Prefix prepended to logical keys for isolation (`shakedown:<scenario>:`).
    pub key_prefix: String,
}

/// Connection details for a provisioned MongoDB scenario database (`mongodb` feature).
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from
/// [`crate::lifecycle::Context::mongo_bindings`] after provision.
#[cfg(feature = "mongodb")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MongoBindings {
    /// Hostname or IP (typically `127.0.0.1` for the suite container).
    pub host: String,
    /// Mapped host port for `mongod` (`27017` in the default image).
    pub port: u16,
    /// Scenario-isolated database name.
    pub database: String,
    /// Root user for the suite container (default `root`).
    pub username: String,
    /// Password for `username` (default `root` in the suite image).
    pub password: String,
    /// Full MongoDB connection URI for this scenario database (`authSource=admin`).
    pub connection_url: String,
}

/// A marker trait for resources that can be cloned.
pub trait CloneResource: Resource + Clone {}

impl<T: Resource + Clone> CloneResource for T {}

/// Pairs a [`Resource`] implementation with declarative [`ResourceConfig`] (for tooling / future CLI).
#[derive(Debug)]
pub struct ResourceBinding<T: Resource> {
    /// Live resource implementation under test.
    pub resource: T,
    /// Declared type label and connection string (documentation or driver hints).
    pub config: ResourceConfig,
}

/// Static description of a resource binding (type name and connection string).
#[derive(Debug, Clone)]
pub struct ResourceConfig {
    /// Logical type key (e.g. `"kafka"`, `"postgres"`).
    pub resource_type: String,
    /// Human-readable or driver connection string.
    pub connection_string: String,
}

impl<T: Resource> ResourceBinding<T> {
    /// Wrap `resource` with `config`.
    pub fn new(resource: T, config: ResourceConfig) -> Self {
        Self { resource, config }
    }

    /// Borrow the inner resource.
    pub fn resource(&self) -> &T {
        &self.resource
    }

    /// Borrow the configuration.
    pub fn config(&self) -> &ResourceConfig {
        &self.config
    }
}

/// Either a closed [`BuiltinResource`] or a boxed user [`Resource`], as stored on [`crate::lifecycle::Context`].
#[allow(clippy::large_enum_variant)]
pub enum RegisteredResource {
    /// Shipped-with-the-crate implementation ([`FakeResource`], Kafka, TCP SQL engines, SQLite when features are on).
    Builtin(BuiltinResource),
    /// Third-party or test-local [`Resource`] behind dynamic dispatch.
    Extension(Box<dyn Resource>),
}

impl RegisteredResource {
    /// Forward to the builtin or extension [`Resource::provision`].
    pub async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        match self {
            RegisteredResource::Builtin(b) => b.provision(scenario_id).await,
            RegisteredResource::Extension(e) => e.provision(scenario_id).await,
        }
    }

    /// Forward to [`Resource::check_ready`].
    pub async fn check_ready(&self) -> Result<(), InfrastructureError> {
        match self {
            RegisteredResource::Builtin(b) => b.check_ready().await,
            RegisteredResource::Extension(e) => e.check_ready().await,
        }
    }

    /// Forward to [`Resource::apply_default_seed`].
    pub async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        match self {
            RegisteredResource::Builtin(b) => b.apply_default_seed().await,
            RegisteredResource::Extension(e) => e.apply_default_seed().await,
        }
    }

    /// Forward to [`Resource::teardown`].
    pub async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        match self {
            RegisteredResource::Builtin(b) => b.teardown().await,
            RegisteredResource::Extension(e) => e.teardown().await,
        }
    }

    /// Dynamic downcast hook for tests and future reflection (builtin vs extension).
    pub fn as_any(&self) -> &dyn std::any::Any {
        match self {
            RegisteredResource::Builtin(b) => b,
            RegisteredResource::Extension(e) => e,
        }
    }

    /// Returns `true` if this slot holds a [`BuiltinResource`].
    pub fn is_builtin(&self) -> bool {
        matches!(self, RegisteredResource::Builtin(_))
    }

    /// Returns `true` if this slot holds a boxed extension [`Resource`].
    pub fn is_extension(&self) -> bool {
        matches!(self, RegisteredResource::Extension(_))
    }
}

impl fmt::Debug for RegisteredResource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RegisteredResource::Builtin(r) => f
                .debug_tuple("RegisteredResource::Builtin")
                .field(r)
                .finish(),
            RegisteredResource::Extension(_) => f
                .debug_tuple("RegisteredResource::Extension")
                .field(&"<dyn Resource>")
                .finish(),
        }
    }
}

impl PartialEq for RegisteredResource {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (
                RegisteredResource::Builtin(_),
                RegisteredResource::Builtin(_)
            ) | (
                RegisteredResource::Extension(_),
                RegisteredResource::Extension(_)
            )
        )
    }
}

/// Standalone list of [`RegisteredResource`] entries (alternative to using [`crate::lifecycle::Context`] alone).
#[derive(Debug, Default)]
pub struct ResourceRegistry {
    resources: Vec<RegisteredResource>,
}

impl ResourceRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a [`BuiltinResource`].
    pub fn register_builtin(&mut self, resource: BuiltinResource) {
        self.resources.push(RegisteredResource::Builtin(resource));
    }

    /// Append a boxed extension resource.
    pub fn register_extension<R: Resource + 'static>(&mut self, resource: R) {
        self.resources
            .push(RegisteredResource::Extension(Box::new(resource)));
    }

    /// Slice of all registered resources in order.
    pub fn resources(&self) -> &[RegisteredResource] {
        &self.resources
    }

    /// Remove every registered resource.
    pub fn clear(&mut self) {
        self.resources.clear();
    }

    /// `true` when there are no resources.
    pub fn is_empty(&self) -> bool {
        self.resources.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fake_resource_provision() {
        // Given a default fake resource and a scenario id
        let mut fake = FakeResource::default();
        // When we provision it
        let handles = fake
            .provision(&ScenarioId::new("test"))
            .await
            .expect("provision should succeed");
        // Then we get a single handle with the stable fake id
        assert_eq!(handles.len(), 1);
        assert_eq!(handles[0].id, "fake_resource");
    }

    #[tokio::test]
    async fn fake_resource_check_ready() {
        // Given a fake resource after provision semantics are not required here
        let fake = FakeResource::default();
        // When we check readiness
        let ready = fake.check_ready().await;
        // Then it always reports ok
        assert!(ready.is_ok());
    }

    #[tokio::test]
    async fn fake_resource_apply_default_seed() {
        // Given a mutable fake resource
        let mut fake = FakeResource::default();
        // When we apply the default seed
        let seeded = fake.apply_default_seed().await;
        // Then seeding succeeds
        assert!(seeded.is_ok());
    }

    #[tokio::test]
    async fn fake_resource_teardown() {
        // Given a fake resource that was "used" in a scenario
        let mut fake = FakeResource::default();
        // When we tear it down
        let torn_down = fake.teardown().await;
        // Then teardown succeeds
        assert!(torn_down.is_ok());
    }

    #[test]
    fn registered_resource_debug() {
        // Given a builtin registered resource wrapping a fake
        let builtin = RegisteredResource::Builtin(BuiltinResource::Fake(FakeResource::default()));
        // When we format it with Debug
        let debug_text = format!("{builtin:?}");
        // Then we get a non-empty debug representation
        assert!(!debug_text.is_empty());
    }

    #[test]
    fn registered_resource_partial_eq() {
        // Given two identical builtin fakes, a different extension, and another extension equal to the first
        let r1 = RegisteredResource::Builtin(BuiltinResource::Fake(FakeResource::default()));
        let r2 = RegisteredResource::Builtin(BuiltinResource::Fake(FakeResource::default()));
        let r3 = RegisteredResource::Extension(Box::new(FakeResource::default()));
        let r4 = RegisteredResource::Extension(Box::new(FakeResource::default()));
        // When we compare them with PartialEq
        // Then builtins match each other, builtin differs from extension, and extensions match
        assert_eq!(r1, r2);
        assert_ne!(r1, r3);
        assert_eq!(r3, r4);
    }

    #[test]
    fn resource_registry() {
        // Given an empty registry
        let mut registry = ResourceRegistry::new();
        // When we add a builtin, then an extension, then clear
        registry.register_builtin(BuiltinResource::Fake(FakeResource::default()));
        assert!(!registry.is_empty());
        assert_eq!(registry.resources().len(), 1);
        registry.register_extension(FakeResource::default());
        assert_eq!(registry.resources().len(), 2);
        registry.clear();
        // Then the registry is empty again
        assert!(registry.is_empty());
    }

    #[test]
    fn sanitize_resource_namespace_general() {
        assert_eq!(
            sanitize_resource_namespace("My Cool Scenario!", 63),
            "shakedown_my_cool_scenario_"
        );
        assert_eq!(sanitize_resource_namespace("", 63), "shakedown_scenario");
        let long = "x".repeat(200);
        let ns = sanitize_resource_namespace(&long, 40);
        assert!(ns.len() <= 40);
        assert!(ns.starts_with("shakedown_"));
    }

    #[test]
    fn sanitize_redis_key_prefix_colon_segments() {
        assert_eq!(
            sanitize_redis_key_prefix("MyScenario_1"),
            "shakedown:myscenario_1:"
        );
        assert_eq!(sanitize_redis_key_prefix(""), "shakedown:scenario:");
    }

    #[test]
    fn sanitize_sql_database_name_postgres_rules() {
        assert_eq!(
            sanitize_sql_database_name("MyScenario_1"),
            "shakedown_myscenario_1"
        );
        let long = "a".repeat(100);
        let sanitized = sanitize_sql_database_name(&long);
        assert!(sanitized.len() <= 63);
        assert!(sanitized.starts_with("shakedown_"));
        assert_eq!(sanitize_sql_database_name(""), "shakedown_scenario");
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_named_memory_uri_is_unique_per_scenario() {
        let a = super::sqlite_named_memory_uri("test_a");
        let b = super::sqlite_named_memory_uri("test_b");
        assert_ne!(a, b);
        assert!(a.starts_with("file:shakedown_"));
        assert!(a.contains("mode=memory"));
        assert!(a.contains("cache=shared"));
    }

    #[test]
    fn resource_binding() {
        // Given a fake resource and a typed connection config
        let resource = FakeResource::default();
        let config = ResourceConfig {
            resource_type: "fake".to_string(),
            connection_string: "localhost:1234".to_string(),
        };
        // When we bind them
        let binding = ResourceBinding::new(resource, config);
        // Then the binding exposes the config we passed in
        assert_eq!(binding.config.resource_type, "fake");
        assert_eq!(binding.config.connection_string, "localhost:1234");
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn kafka_bindings_topic_namespace() {
        // Given Kafka bindings for a bootstrap URL and topic prefix
        let bindings = KafkaBindings::new("localhost:9092", "shakedown_test");
        // When we read fields and derive a topic namespace suffix
        // Then servers, prefix, client id, and composed namespace match expectations
        assert_eq!(bindings.bootstrap_servers, "localhost:9092");
        assert_eq!(bindings.topic_prefix, "shakedown_test");
        assert!(bindings.client_id_prefix.is_some());
        assert_eq!(bindings.topic_namespace("proof"), "shakedown_testproof");
    }

    #[cfg(feature = "rabbitmq")]
    #[test]
    fn rabbit_bindings_namespaced_name() {
        let bindings = RabbitBindings::new(
            "amqp://guest:guest@127.0.0.1:5672",
            "127.0.0.1",
            5672,
            "guest",
            "guest",
            "/",
            "shakedown_test_",
        );
        assert_eq!(bindings.amqp_uri, "amqp://guest:guest@127.0.0.1:5672");
        assert_eq!(bindings.port, 5672);
        assert_eq!(bindings.vhost, "/");
        assert_eq!(bindings.namespaced_name("proof"), "shakedown_test_proof");
    }
}
