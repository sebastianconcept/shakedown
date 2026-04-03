use crate::error::{InfrastructureError, ScenarioId};
use crate::resources::{sanitize_sql_database_name, Resource, ResourceHandle};
use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use mysql_async::{OptsBuilder, Pool};
use std::sync::OnceLock;
use testcontainers::core::Image;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::mariadb::Mariadb;
use tokio::sync::Mutex;

const MARIADB_PORT: u16 = 3306;
/// Default `mariadb` image tag (non-alpine).
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
        conn.query_drop(create_sql.as_str())
            .await
            .map_err(|e| InfrastructureError::Provision {
                resource: "mariadb".into(),
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
