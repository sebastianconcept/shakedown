use crate::error::{InfrastructureError, ScenarioId};
use crate::resources::user_seed::{take_user_seed, MongoUserSeedFn, UserSeedSlot};
use crate::resources::{
    map_seed_error, sanitize_sql_database_name, MongoBindings, Resource, ResourceHandle,
};
use async_trait::async_trait;
use mongodb::bson::doc;
use mongodb::Client;
use std::sync::OnceLock;
use testcontainers::core::{Image, ImageExt};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::mongo::Mongo;
use tokio::sync::Mutex;

const MONGO_PORT: u16 = 27017;
/// Default `mongo` image tag (official Docker Hub image).
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
#[cfg_attr(docsrs, doc(cfg(feature = "mongodb")))]
pub struct MongoResource {
    image: Mongo,
    image_tag: Option<String>,
    holds_suite_ref: bool,
    host: Option<String>,
    port: Option<u16>,
    username: String,
    password: String,
    database_name: String,
    user_seed: UserSeedSlot<MongoBindings>,
}

impl std::fmt::Debug for MongoResource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MongoResource")
            .field("image", &self.image)
            .field("image_tag", &self.image_tag)
            .field("holds_suite_ref", &self.holds_suite_ref)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("database_name", &self.database_name)
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
            user_seed: UserSeedSlot::new(None),
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

    /// Async seed closure receiving [`MongoBindings`] ([`Resource::apply_default_seed`]).
    pub fn set_seed(&mut self, seed: MongoUserSeedFn) {
        *self.user_seed.lock().unwrap() = Some(seed);
    }

    fn mongo_bindings(&self) -> Result<MongoBindings, InfrastructureError> {
        let host = self
            .host
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned("mongodb".into()))?;
        let port = self
            .port
            .ok_or_else(|| InfrastructureError::NotProvisioned("mongodb".into()))?;
        let connection_url = scenario_connection_uri(host, port, self.database_name.as_str());
        Ok(MongoBindings {
            host: host.to_string(),
            port,
            database: self.database_name.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            connection_url,
            replica_set_host_ports: Vec::new(),
        })
    }

    fn map_tc_err(error: impl std::fmt::Display + Send + Sync + 'static) -> InfrastructureError {
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

        let connection_url = scenario_connection_uri(host.as_str(), port, database_name.as_str());
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
        if let Some(seed_fn) = take_user_seed(&self.user_seed) {
            let bindings = self.mongo_bindings()?;
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
