use crate::error::{InfrastructureError, ScenarioId};
use crate::resources::{sanitize_sql_database_name, Resource, ResourceHandle};
use async_trait::async_trait;
use std::sync::OnceLock;
use testcontainers::core::IntoContainerPort;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::GenericImage;
use testcontainers::ImageExt;
use tokio::sync::Mutex;
use tokio_postgres::NoTls;

const RESOURCE_NAME: &str = "timescaledb";
const TIMESCALEDB_PORT: u16 = 5432;
/// Default image repository (`timescale/timescaledb`).
pub const DEFAULT_TIMESCALEDB_IMAGE_REPOSITORY: &str = "timescale/timescaledb";
/// Default image tag (`latest-pg16` — Timescale image pinned to Postgres 16 line).
pub const DEFAULT_TIMESCALEDB_IMAGE_TAG: &str = "latest-pg16";

#[allow(clippy::large_enum_variant)]
enum SuiteTimescaledbState {
    Idle,
    Running {
        host: String,
        port: u16,
        username: String,
        password: String,
        container: testcontainers::ContainerAsync<GenericImage>,
        refs: usize,
        image_key: String,
    },
}

static SUITE_TIMESCALEDB: OnceLock<Mutex<SuiteTimescaledbState>> = OnceLock::new();

fn suite_timescaledb_mutex() -> &'static Mutex<SuiteTimescaledbState> {
    SUITE_TIMESCALEDB.get_or_init(|| Mutex::new(SuiteTimescaledbState::Idle))
}

fn image_fingerprint(repository: &str, tag_override: &Option<String>) -> String {
    let tag = tag_override
        .as_deref()
        .map(str::trim)
        .filter(|tag| !tag.is_empty())
        .unwrap_or(DEFAULT_TIMESCALEDB_IMAGE_TAG);
    format!("{repository}:{tag}")
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

/// Docker-backed TimescaleDB (`timescale/timescaledb` via [`GenericImage`]).
///
/// **Suite lifecycle:** one server per process; per-scenario `CREATE DATABASE` / `DROP DATABASE`;
/// refcounted teardown. Uses the same [`super::PostgresBindings`] shape as PostgreSQL.
///
/// Default tag **`latest-pg16`**. Override with [`Self::with_image_tag`] or
/// `SHAKEDOWN_TIMESCALEDB_IMAGE_TAG`.
#[derive(Debug)]
pub struct TimescaledbResource {
    image_repository: String,
    image_tag: Option<String>,
    holds_suite_ref: bool,
    host: Option<String>,
    port: Option<u16>,
    username: String,
    password: String,
    database_name: String,
}

impl Default for TimescaledbResource {
    fn default() -> Self {
        Self::new()
    }
}

impl TimescaledbResource {
    /// Create default Docker-backed TimescaleDB settings (suite server started on first provision).
    pub fn new() -> Self {
        Self {
            image_repository: DEFAULT_TIMESCALEDB_IMAGE_REPOSITORY.to_string(),
            image_tag: Some(DEFAULT_TIMESCALEDB_IMAGE_TAG.to_string()),
            holds_suite_ref: false,
            host: None,
            port: None,
            username: "postgres".to_string(),
            password: "postgres".to_string(),
            database_name: String::new(),
        }
    }

    /// Override the image tag (pin another `timescale/timescaledb` tag). Repository stays
    /// [`DEFAULT_TIMESCALEDB_IMAGE_REPOSITORY`] unless changed via [`Self::with_image_repository`].
    pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
        self.image_tag = Some(tag.into());
        self
    }

    /// Override the image repository (advanced; default is `timescale/timescaledb`).
    pub fn with_image_repository(mut self, repository: impl Into<String>) -> Self {
        self.image_repository = repository.into();
        self
    }

    /// Apply tag override from `SHAKEDOWN_TIMESCALEDB_IMAGE_TAG` when set (non-empty).
    pub fn with_image_tag_from_env(mut self) -> Self {
        if let Ok(tag) = std::env::var("SHAKEDOWN_TIMESCALEDB_IMAGE_TAG") {
            let tag = tag.trim();
            if !tag.is_empty() {
                self.image_tag = Some(tag.to_string());
            }
        }
        self
    }

    fn map_tc_err(error: impl std::fmt::Display + Send + Sync + 'static) -> InfrastructureError {
        InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
    }
}

#[async_trait]
impl Resource for TimescaledbResource {
    async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        if self.holds_suite_ref {
            return Err(InfrastructureError::AlreadyProvisioned(
                RESOURCE_NAME.into(),
            ));
        }

        let database_name = sanitize_sql_database_name(scenario_id.as_str());
        let want_key = image_fingerprint(self.image_repository.as_str(), &self.image_tag);

        let mut guard = suite_timescaledb_mutex().lock().await;
        let (host, port, username, password) = match &mut *guard {
            SuiteTimescaledbState::Idle => {
                let repository = self.image_repository.as_str();
                let tag = self
                    .image_tag
                    .as_deref()
                    .map(str::trim)
                    .filter(|t| !t.is_empty())
                    .unwrap_or(DEFAULT_TIMESCALEDB_IMAGE_TAG);
                let start_image = GenericImage::new(repository, tag)
                    .with_exposed_port(TIMESCALEDB_PORT.tcp())
                    .with_wait_for(WaitFor::message_on_stderr(
                        "database system is ready to accept connections",
                    ))
                    .with_env_var("POSTGRES_USER", "postgres")
                    .with_env_var("POSTGRES_PASSWORD", "postgres")
                    .with_env_var("POSTGRES_DB", "postgres")
                    .with_label(
                        crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                        crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                    );
                let node = start_image.start().await.map_err(Self::map_tc_err)?;

                let mapped_port = node
                    .get_host_port_ipv4(TIMESCALEDB_PORT)
                    .await
                    .map_err(Self::map_tc_err)?;
                let host = "127.0.0.1".to_string();
                let username = "postgres".to_string();
                let password = "postgres".to_string();

                *guard = SuiteTimescaledbState::Running {
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
            SuiteTimescaledbState::Running {
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
                            "TimescaleDB suite server already running as {image_key}; \
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
        client
            .batch_execute(&create_sql)
            .await
            .map_err(|e| InfrastructureError::Provision {
                resource: RESOURCE_NAME.into(),
                source: Box::new(std::io::Error::other(e.to_string())),
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
            id: format!("timescaledb_{scenario_id}"),
            connection_string: connection_url.clone(),
            metadata: Some(serde_json::json!({
                "host": host,
                "port": port,
                "database": database_name,
                "username": username,
                "password": password,
                "connection_url": connection_url,
                "engine": "timescaledb",
            })),
        }])
    }

    async fn check_ready(&self) -> Result<(), InfrastructureError> {
        let host = self
            .host
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
        let port = self
            .port
            .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
        let url = scenario_connection_url(
            host,
            port,
            &self.database_name,
            &self.username,
            &self.password,
        );
        let client = connect_with_url(&url)
            .await
            .map_err(|e| InfrastructureError::Readiness {
                resource: RESOURCE_NAME.into(),
                source: Box::new(std::io::Error::other(e.to_string())),
            })?;
        client
            .query_one("SELECT 1", &[])
            .await
            .map_err(|e| InfrastructureError::Readiness {
                resource: RESOURCE_NAME.into(),
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
        let port = self.port.unwrap_or(TIMESCALEDB_PORT);
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

        let mut guard = suite_timescaledb_mutex().lock().await;
        let mut container_to_rm: Option<testcontainers::ContainerAsync<GenericImage>> = None;
        match &mut *guard {
            SuiteTimescaledbState::Idle => {}
            SuiteTimescaledbState::Running { refs, .. } => {
                *refs = refs.saturating_sub(1);
                if *refs == 0 {
                    let taken = std::mem::replace(&mut *guard, SuiteTimescaledbState::Idle);
                    if let SuiteTimescaledbState::Running { container, .. } = taken {
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
