use crate::error::{InfrastructureError, ScenarioId};
use crate::resources::search_operations::SearchOperations;
use crate::resources::user_seed::{take_user_seed, ElasticsearchUserSeedFn, UserSeedSlot};
use crate::resources::{
    map_seed_error, sanitize_elasticsearch_index_prefix, ElasticsearchBindings, Resource,
    ResourceHandle,
};
use async_trait::async_trait;
use std::sync::OnceLock;
use std::time::Duration;
use testcontainers::core::{Image, ImageExt, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::elastic_search::{ElasticSearch, ELASTICSEARCH_API_PORT};
use tokio::sync::Mutex;

const RESOURCE_NAME: &str = "elasticsearch";
/// Default Elasticsearch image tag (`docker.elastic.co/elasticsearch/elasticsearch:8.15.0`).
/// (Floating `8` is not published on the Elastic registry; pin a concrete 8.x patch.)
pub const DEFAULT_ELASTICSEARCH_IMAGE_TAG: &str = "8.15.0";
const SUITE_SECURITY_USER: &str = "elastic";
const SUITE_SECURITY_PASSWORD: &str = "shakedown";

#[allow(clippy::large_enum_variant)]
enum SuiteElasticsearchState {
    Idle,
    Running {
        host: String,
        port: u16,
        container: testcontainers::ContainerAsync<ElasticSearch>,
        refs: usize,
        image_key: String,
    },
}

static SUITE_ELASTICSEARCH: OnceLock<Mutex<SuiteElasticsearchState>> = OnceLock::new();

fn suite_elasticsearch_mutex() -> &'static Mutex<SuiteElasticsearchState> {
    SUITE_ELASTICSEARCH.get_or_init(|| Mutex::new(SuiteElasticsearchState::Idle))
}

fn image_fingerprint(image: &ElasticSearch, tag_override: &Option<String>) -> String {
    let tag = tag_override
        .as_deref()
        .unwrap_or(DEFAULT_ELASTICSEARCH_IMAGE_TAG);
    format!("{}:{}", image.name(), tag)
}

/// Docker-backed Elasticsearch (`docker.elastic.co/elasticsearch/elasticsearch` via testcontainers-modules).
///
/// **Suite lifecycle:** one node per process; per-scenario index prefix; refcounted teardown deletes
/// matching indices then may remove the container.
///
/// Default tag **`8.15.0`**. Override with [`Self::with_image_tag`] or `SHAKEDOWN_ELASTICSEARCH_IMAGE_TAG`.
/// Security is **off** by default (`xpack.security.enabled=false`). Use [`Self::with_security`] for HTTP basic auth (`elastic` / `shakedown`).
pub struct ElasticsearchResource {
    image: ElasticSearch,
    image_tag: Option<String>,
    security_enabled: bool,
    holds_suite_ref: bool,
    host: Option<String>,
    port: Option<u16>,
    http_url: String,
    index_prefix: String,
    username: Option<String>,
    password: Option<String>,
    user_seed: UserSeedSlot<ElasticsearchBindings>,
}

impl std::fmt::Debug for ElasticsearchResource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ElasticsearchResource")
            .field("image", &self.image)
            .field("image_tag", &self.image_tag)
            .field("security_enabled", &self.security_enabled)
            .field("holds_suite_ref", &self.holds_suite_ref)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("http_url", &self.http_url)
            .field("index_prefix", &self.index_prefix)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
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

impl Default for ElasticsearchResource {
    fn default() -> Self {
        Self::new()
    }
}

impl ElasticsearchResource {
    /// Create default settings (suite container started on first provision; security off).
    pub fn new() -> Self {
        Self {
            image: ElasticSearch::default(),
            image_tag: Some(DEFAULT_ELASTICSEARCH_IMAGE_TAG.to_string()),
            security_enabled: false,
            holds_suite_ref: false,
            host: None,
            port: None,
            http_url: String::new(),
            index_prefix: String::new(),
            username: None,
            password: None,
            user_seed: UserSeedSlot::new(None),
        }
    }

    /// Enable X-Pack security with fixed `elastic` / `shakedown` credentials for integration tests.
    pub fn with_security(mut self, enabled: bool) -> Self {
        self.security_enabled = enabled;
        self
    }

    /// Override the image tag (pin `8.11.0` or another tag). Image name stays the Elastic official registry path.
    pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
        self.image_tag = Some(tag.into());
        self
    }

    /// Apply tag override from `SHAKEDOWN_ELASTICSEARCH_IMAGE_TAG` when set.
    pub fn with_image_tag_from_env(mut self) -> Self {
        if let Ok(tag) = std::env::var("SHAKEDOWN_ELASTICSEARCH_IMAGE_TAG") {
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

    fn search_client(&self) -> SearchOperations {
        SearchOperations::new(
            self.http_url.clone(),
            self.username.clone(),
            self.password.clone(),
        )
    }

    /// Async seed closure receiving [`ElasticsearchBindings`] ([`Resource::apply_default_seed`]).
    pub fn set_seed(&mut self, seed: ElasticsearchUserSeedFn) {
        *self.user_seed.lock().unwrap() = Some(seed);
    }

    fn elasticsearch_bindings(&self) -> Result<ElasticsearchBindings, InfrastructureError> {
        let host = self
            .host
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
        let port = self
            .port
            .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
        Ok(ElasticsearchBindings {
            url: self.http_url.clone(),
            host: host.to_string(),
            port,
            index_prefix: self.index_prefix.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
        })
    }
}

#[async_trait]
impl Resource for ElasticsearchResource {
    async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        if self.holds_suite_ref {
            return Err(InfrastructureError::AlreadyProvisioned(
                RESOURCE_NAME.into(),
            ));
        }

        let index_prefix = sanitize_elasticsearch_index_prefix(scenario_id.as_str());
        let want_key = image_fingerprint(&self.image, &self.image_tag);

        let mut guard = suite_elasticsearch_mutex().lock().await;
        let (host, port) = match &mut *guard {
            SuiteElasticsearchState::Idle => {
                let tag = self
                    .image_tag
                    .as_deref()
                    .unwrap_or(DEFAULT_ELASTICSEARCH_IMAGE_TAG);
                let mut start_image = self.image.clone().with_tag(tag.to_string());
                start_image = start_image
                    .with_env_var("discovery.type", "single-node")
                    .with_env_var("ES_JAVA_OPTS", "-Xms512m -Xmx512m");
                if self.security_enabled {
                    start_image = start_image
                        .with_env_var("xpack.security.enabled", "true")
                        .with_env_var("ELASTIC_PASSWORD", SUITE_SECURITY_PASSWORD);
                } else {
                    start_image = start_image.with_env_var("xpack.security.enabled", "false");
                }
                // `testcontainers-modules` waits for ES 7's "[YELLOW] to [GREEN]" stdout line; ES 8+ uses
                // JSON logs and never prints that, so the default wait hits the 60s startup timeout.
                let start_image = start_image
                    .with_ready_conditions(vec![WaitFor::message_on_either_std(
                        "recovered [0] indices into cluster_state",
                    )])
                    .with_startup_timeout(Duration::from_secs(180));
                let node = start_image
                    .with_label(
                        crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                        crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                    )
                    .start()
                    .await
                    .map_err(Self::map_tc_err)?;

                let mapped_port = node
                    .get_host_port_ipv4(ELASTICSEARCH_API_PORT)
                    .await
                    .map_err(Self::map_tc_err)?;
                let host = "127.0.0.1".to_string();

                *guard = SuiteElasticsearchState::Running {
                    host: host.clone(),
                    port: mapped_port,
                    container: node,
                    refs: 1,
                    image_key: want_key.clone(),
                };
                (host, mapped_port)
            }
            SuiteElasticsearchState::Running {
                host,
                port,
                refs,
                image_key,
                ..
            } => {
                if *image_key != want_key {
                    return Err(InfrastructureError::Backend(Box::new(
                        std::io::Error::other(format!(
                            "Elasticsearch suite already running as {image_key}; \
                             cannot start or join as {want_key}. Use one image:tag per process."
                        )),
                    )));
                }
                *refs += 1;
                (host.clone(), *port)
            }
        };
        drop(guard);

        let http_url = format!("http://{host}:{port}");
        let (username, password) = if self.security_enabled {
            (
                Some(SUITE_SECURITY_USER.to_string()),
                Some(SUITE_SECURITY_PASSWORD.to_string()),
            )
        } else {
            (None, None)
        };

        self.holds_suite_ref = true;
        self.host = Some(host.clone());
        self.port = Some(port);
        self.http_url = http_url.clone();
        self.index_prefix = index_prefix.clone();
        self.username = username.clone();
        self.password = password.clone();

        Ok(vec![ResourceHandle {
            id: format!("elasticsearch_{scenario_id}"),
            connection_string: http_url.clone(),
            metadata: Some(serde_json::json!({
                "host": host,
                "port": port,
                "url": http_url,
                "index_prefix": index_prefix,
                "username": username,
                "password": password,
                "engine": "elasticsearch",
            })),
        }])
    }

    async fn check_ready(&self) -> Result<(), InfrastructureError> {
        let _ = self
            .host
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
        let _ = self
            .port
            .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;

        let client = self.search_client();
        let health =
            client
                .cluster_health()
                .await
                .map_err(|error| InfrastructureError::Readiness {
                    resource: RESOURCE_NAME.into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;
        let status = health
            .get("status")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        if status == "red" {
            return Err(InfrastructureError::Readiness {
                resource: RESOURCE_NAME.into(),
                source: Box::new(std::io::Error::other(format!(
                    "cluster health status is red: {health}"
                ))),
            });
        }
        Ok(())
    }

    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        if let Some(seed_fn) = take_user_seed(&self.user_seed) {
            let bindings = self.elasticsearch_bindings()?;
            seed_fn(bindings).await.map_err(map_seed_error)?;
        }
        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        if !self.holds_suite_ref {
            self.host = None;
            self.port = None;
            self.http_url.clear();
            self.index_prefix.clear();
            self.username = None;
            self.password = None;
            return Ok(());
        }

        let index_prefix = self.index_prefix.clone();
        let http_url = self.http_url.clone();
        let username = self.username.clone();
        let password = self.password.clone();

        if !index_prefix.is_empty() && !http_url.is_empty() {
            let client = SearchOperations::new(http_url, username, password);
            if let Err(error) = client
                .delete_indices_with_prefix(index_prefix.as_str())
                .await
            {
                return Err(InfrastructureError::Teardown {
                    resource: RESOURCE_NAME.into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                });
            }
        }

        let mut guard = suite_elasticsearch_mutex().lock().await;
        let mut container_to_rm: Option<testcontainers::ContainerAsync<ElasticSearch>> = None;
        match &mut *guard {
            SuiteElasticsearchState::Idle => {}
            SuiteElasticsearchState::Running { refs, .. } => {
                *refs = refs.saturating_sub(1);
                if *refs == 0 {
                    let taken = std::mem::replace(&mut *guard, SuiteElasticsearchState::Idle);
                    if let SuiteElasticsearchState::Running { container, .. } = taken {
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
        self.http_url.clear();
        self.index_prefix.clear();
        self.username = None;
        self.password = None;
        Ok(())
    }
}
