use crate::error::{InfrastructureError, ScenarioId};
use crate::resources::{sanitize_sql_database_name, Resource, ResourceHandle};
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
        client
            .batch_execute(&create_sql)
            .await
            .map_err(|e| InfrastructureError::Provision {
                resource: "postgres".into(),
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
        let client = connect_with_url(&url)
            .await
            .map_err(|e| InfrastructureError::Readiness {
                resource: "postgres".into(),
                source: Box::new(std::io::Error::other(e.to_string())),
            })?;
        client
            .query_one("SELECT 1", &[])
            .await
            .map_err(|e| InfrastructureError::Readiness {
                resource: "postgres".into(),
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
