use crate::error::{InfrastructureError, ScenarioId};
use crate::resources::sql_seed::statements_from_sql_script;
use crate::resources::user_seed::{take_user_seed, MysqlUserSeedFn, UserSeedSlot};
use crate::resources::{
    map_seed_error, scenario_sql_database_name, MysqlBindings, Resource, ResourceHandle,
};
use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use mysql_async::{OptsBuilder, Pool};
use std::path::PathBuf;
use std::sync::OnceLock;
use testcontainers::core::Image;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::mysql::Mysql as MysqlImage;
use tokio::sync::Mutex;

const MYSQL_PORT: u16 = 3306;
/// Default `mysql` image tag (Oracle MySQL; non-alpine).
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

pub(crate) fn admin_pool(host: &str, port: u16, username: &str, password: &str) -> Pool {
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

pub(crate) fn scenario_connection_url(
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
pub struct MysqlResource {
    image: MysqlImage,
    image_tag: Option<String>,
    logical_binding_key: Option<String>,
    holds_suite_ref: bool,
    host: Option<String>,
    port: Option<u16>,
    username: String,
    password: String,
    database_name: String,
    #[cfg(all(feature = "mysql", feature = "sqlx"))]
    sqlx_migrations_path: Option<PathBuf>,
    seed_sql_path: Option<PathBuf>,
    user_seed: UserSeedSlot<MysqlBindings>,
}

impl std::fmt::Debug for MysqlResource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = formatter.debug_struct("MysqlResource");
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
        #[cfg(all(feature = "mysql", feature = "sqlx"))]
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
            logical_binding_key: None,
            holds_suite_ref: false,
            host: None,
            port: None,
            username: "root".to_string(),
            password: String::new(),
            database_name: String::new(),
            #[cfg(all(feature = "mysql", feature = "sqlx"))]
            sqlx_migrations_path: None,
            seed_sql_path: None,
            user_seed: UserSeedSlot::new(None),
        }
    }

    /// Logical binding from `#[shakedown::test(resources = [Mysql(name = "...")])]`.
    pub fn with_logical_binding_name(mut self, name: impl Into<String>) -> Self {
        self.logical_binding_key = Some(name.into());
        self
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

    /// sqlx migration directory (`mysql` + `sqlx`).
    #[cfg(all(feature = "mysql", feature = "sqlx"))]
    pub fn with_migrations_path(mut self, path: PathBuf) -> Self {
        self.sqlx_migrations_path = Some(path);
        self
    }

    /// Single `.sql` seed file (`;`-split statements).
    pub fn with_seed_path(mut self, path: PathBuf) -> Self {
        self.seed_sql_path = Some(path);
        self
    }

    /// Programmatic seed after migrations and optional [`Self::with_seed_path`] file.
    ///
    /// Receives [`MysqlBindings`] for the scenario database.
    pub fn set_seed(&mut self, seed: MysqlUserSeedFn) {
        *self.user_seed.lock().unwrap() = Some(seed);
    }

    fn mysql_bindings(&self) -> Result<MysqlBindings, InfrastructureError> {
        let host = self
            .host
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned("mysql".into()))?;
        let port = self
            .port
            .ok_or_else(|| InfrastructureError::NotProvisioned("mysql".into()))?;
        let connection_url = scenario_connection_url(
            host,
            port,
            self.database_name.as_str(),
            &self.username,
            &self.password,
        );
        Ok(MysqlBindings {
            host: host.to_string(),
            port,
            database: self.database_name.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            connection_url,
        })
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

        let database_name =
            scenario_sql_database_name(scenario_id.as_str(), self.logical_binding_key.as_deref());
        let binding_map_key = self.logical_binding_key.as_deref().unwrap_or("default");
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
        conn.query_drop(create_sql.as_str())
            .await
            .map_err(|e| InfrastructureError::Provision {
                resource: "mysql".into(),
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
                "binding_name": binding_map_key,
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
        let bindings = self.mysql_bindings()?;

        #[cfg(all(feature = "mysql", feature = "sqlx"))]
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
            let opts = mysql_async::Opts::from_url(bindings.connection_url.as_str()).map_err(
                |source| {
                    InfrastructureError::Backend(Box::new(std::io::Error::other(
                        source.to_string(),
                    )))
                },
            )?;
            let pool = Pool::new(opts);
            let mut connection = pool.get_conn().await.map_err(Self::map_mysql_err)?;
            for statement in statements_from_sql_script(sql.as_str()) {
                connection
                    .query_drop(statement)
                    .await
                    .map_err(Self::map_mysql_err)?;
            }
            pool.disconnect().await.map_err(Self::map_mysql_err)?;
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
