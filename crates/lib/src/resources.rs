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

    #[cfg(feature = "postgres")]
    /// Docker-backed PostgreSQL (suite-shared container via testcontainers-modules).
    Postgres(PostgresResource),

    #[cfg(feature = "mariadb")]
    /// Docker-backed MariaDB (suite-shared container via testcontainers-modules).
    MariaDB(MariaDBResource),
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
            #[cfg(feature = "postgres")]
            BuiltinResource::Postgres(r) => r.provision(scenario_id).await,
            #[cfg(feature = "mariadb")]
            BuiltinResource::MariaDB(r) => r.provision(scenario_id).await,
        }
    }

    async fn check_ready(&self) -> Result<(), InfrastructureError> {
        match self {
            BuiltinResource::Fake(r) => r.check_ready().await,
            #[cfg(feature = "kafka")]
            BuiltinResource::Kafka(r) => r.check_ready().await,
            #[cfg(feature = "postgres")]
            BuiltinResource::Postgres(r) => r.check_ready().await,
            #[cfg(feature = "mariadb")]
            BuiltinResource::MariaDB(r) => r.check_ready().await,
        }
    }

    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        match self {
            BuiltinResource::Fake(r) => r.apply_default_seed().await,
            #[cfg(feature = "kafka")]
            BuiltinResource::Kafka(r) => r.apply_default_seed().await,
            #[cfg(feature = "postgres")]
            BuiltinResource::Postgres(r) => r.apply_default_seed().await,
            #[cfg(feature = "mariadb")]
            BuiltinResource::MariaDB(r) => r.apply_default_seed().await,
        }
    }

    async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        match self {
            BuiltinResource::Fake(r) => r.teardown().await,
            #[cfg(feature = "kafka")]
            BuiltinResource::Kafka(r) => r.teardown().await,
            #[cfg(feature = "postgres")]
            BuiltinResource::Postgres(r) => r.teardown().await,
            #[cfg(feature = "mariadb")]
            BuiltinResource::MariaDB(r) => r.teardown().await,
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
                            .start()
                            .await
                            .map_err(Self::map_tc_err)?
                    } else {
                        image.start().await.map_err(Self::map_tc_err)?
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
                            .start()
                            .await
                            .map_err(Self::map_tc_err)?
                    } else {
                        image.start().await.map_err(Self::map_tc_err)?
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
                            .start()
                            .await
                            .map_err(Self::map_tc_err)?
                    } else {
                        image.start().await.map_err(Self::map_tc_err)?
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

#[cfg(feature = "kafka")]
pub use kafka_impl::KafkaResource;

#[cfg(feature = "postgres")]
pub use postgres_impl::PostgresResource;

#[cfg(feature = "mariadb")]
pub use mariadb_impl::MariaDBResource;

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
    /// Shipped-with-the-crate implementation ([`FakeResource`], Kafka / Postgres / MariaDB when features are on).
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
}
