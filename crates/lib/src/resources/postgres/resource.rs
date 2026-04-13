use crate::error::{InfrastructureError, ScenarioId};
use crate::resources::user_seed::{take_user_seed, PostgresUserSeedFn, UserSeedSlot};
use crate::resources::{
    map_seed_error, scenario_sql_database_name, PostgresBindings, Resource, ResourceHandle,
};
use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::OnceLock;
use testcontainers::core::Image;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio::sync::Mutex;
use tokio_postgres::NoTls;

const POSTGRES_PORT: u16 = 5432;
/// Default `postgres` image tag (Debian-based, non-alpine; matches common testcontainers defaults).
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

pub(crate) fn admin_connection_url(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
) -> String {
    format!("postgresql://{username}:{password}@{host}:{port}/postgres")
}

pub(crate) fn scenario_connection_url(
    host: &str,
    port: u16,
    database: &str,
    username: &str,
    password: &str,
) -> String {
    format!("postgresql://{username}:{password}@{host}:{port}/{database}")
}

pub(crate) async fn connect_with_url(
    url: &str,
) -> Result<tokio_postgres::Client, InfrastructureError> {
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
///
/// **Seed phase** ([`Resource::apply_default_seed`]): optional sqlx migrations directory, optional
/// single `.sql` seed file (static reference data), then optional [`Self::set_seed`] closure.
/// See [`crate::resources::user_seed`] for when to use a file vs a closure.
#[cfg_attr(docsrs, doc(cfg(feature = "postgres")))]
pub struct PostgresResource {
    image: Postgres,
    image_tag: Option<String>,
    logical_binding_key: Option<String>,
    holds_suite_ref: bool,
    host: Option<String>,
    port: Option<u16>,
    username: String,
    password: String,
    database_name: String,
    /// sqlx migration tree; applied after readiness, before seed file and user closure (`postgres` + `sqlx`).
    #[cfg(all(feature = "postgres", feature = "sqlx"))]
    sqlx_migrations_path: Option<PathBuf>,
    /// Single `.sql` file executed as a batch after migrations (static reference data).
    seed_sql_path: Option<PathBuf>,
    user_seed: UserSeedSlot<PostgresBindings>,
}

impl std::fmt::Debug for PostgresResource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = formatter.debug_struct("PostgresResource");
        debug_struct
            .field("image", &self.image)
            .field("image_tag", &self.image_tag)
            .field("logical_binding_key", &self.logical_binding_key)
            .field("holds_suite_ref", &self.holds_suite_ref)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("database_name", &self.database_name);
        #[cfg(all(feature = "postgres", feature = "sqlx"))]
        debug_struct.field("sqlx_migrations_path", &self.sqlx_migrations_path);
        debug_struct
            .field("seed_sql_path", &self.seed_sql_path)
            .field(
                "user_seed",
                &self
                    .user_seed
                    .lock()
                    .ok()
                    .and_then(|guard| guard.as_ref().map(|_| "<closure>")),
            )
            .finish()
    }
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
            logical_binding_key: None,
            holds_suite_ref: false,
            host: None,
            port: None,
            username: "postgres".to_string(),
            password: "postgres".to_string(),
            database_name: String::new(),
            #[cfg(all(feature = "postgres", feature = "sqlx"))]
            sqlx_migrations_path: None,
            seed_sql_path: None,
            user_seed: UserSeedSlot::new(None),
        }
    }

    /// Logical binding from `#[shakedown::test(resources = [Postgres(name = "...")])]`.
    pub fn with_logical_binding_name(mut self, name: impl Into<String>) -> Self {
        self.logical_binding_key = Some(name.into());
        self
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

    /// On-disk sqlx migration directory (versioned DDL). Applied in [`Resource::apply_default_seed`]
    /// after resource defaults and before [`Self::with_seed_path`] / [`Self::set_seed`].
    ///
    /// Requires the `sqlx` Cargo feature on `shakedown`.
    #[cfg(all(feature = "postgres", feature = "sqlx"))]
    pub fn with_migrations_path(mut self, path: PathBuf) -> Self {
        self.sqlx_migrations_path = Some(path);
        self
    }

    /// Single `.sql` file with static reference data (DML). Executed after sqlx migrations and before [`Self::set_seed`].
    ///
    /// Prefer this for lookup tables and enums; use [`Self::set_seed`] for scenario-scoped or dynamic data.
    pub fn with_seed_path(mut self, path: PathBuf) -> Self {
        self.seed_sql_path = Some(path);
        self
    }

    /// Programmatic seed after migrations and optional [`Self::with_seed_path`] file.
    ///
    /// Receives [`PostgresBindings`] for the scenario database. Use when data depends on isolation keys,
    /// async helpers, or dynamic values.
    pub fn set_seed(&mut self, seed: PostgresUserSeedFn) {
        *self.user_seed.lock().unwrap() = Some(seed);
    }

    fn postgres_bindings(&self) -> Result<PostgresBindings, InfrastructureError> {
        let host = self
            .host
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned("postgres".into()))?;
        let port = self
            .port
            .ok_or_else(|| InfrastructureError::NotProvisioned("postgres".into()))?;
        let connection_url = scenario_connection_url(
            host,
            port,
            self.database_name.as_str(),
            &self.username,
            &self.password,
        );
        Ok(PostgresBindings {
            host: host.to_string(),
            port,
            database: self.database_name.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            connection_url,
            replica_port: None,
        })
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

        let database_name =
            scenario_sql_database_name(scenario_id.as_str(), self.logical_binding_key.as_deref());
        let binding_map_key = self.logical_binding_key.as_deref().unwrap_or("default");
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
                "binding_name": binding_map_key,
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
        let bindings = self.postgres_bindings()?;

        #[cfg(all(feature = "postgres", feature = "sqlx"))]
        if let Some(path) = self.sqlx_migrations_path.clone() {
            let migrator = sqlx::migrate::Migrator::new(path).await.map_err(|source| {
                InfrastructureError::Backend(Box::new(std::io::Error::other(source.to_string())))
            })?;
            bindings
                .run_migrations(&migrator)
                .await
                .map_err(map_seed_error)?;
        }

        if let Some(path) = self.seed_sql_path.clone() {
            let sql = tokio::fs::read_to_string(&path).await.map_err(|source| {
                InfrastructureError::Backend(Box::new(std::io::Error::other(source.to_string())))
            })?;
            let client = connect_with_url(bindings.connection_url.as_str()).await?;
            client.batch_execute(&sql).await.map_err(|source| {
                InfrastructureError::Backend(Box::new(std::io::Error::other(source.to_string())))
            })?;
        }

        if let Some(seed_fn) = take_user_seed(&self.user_seed) {
            seed_fn(bindings).await.map_err(map_seed_error)?;
        }

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
