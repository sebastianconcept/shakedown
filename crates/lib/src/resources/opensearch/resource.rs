use crate::error::{InfrastructureError, ScenarioId};
use crate::resources::search_operations::SearchOperations;
use crate::resources::user_seed::{take_user_seed, OpenSearchUserSeedFn, UserSeedSlot};
use crate::resources::{
    map_seed_error, sanitize_elasticsearch_index_prefix, OpenSearchBindings, Resource,
    ResourceHandle,
};
use async_trait::async_trait;
use std::sync::OnceLock;
use std::time::Duration;
use testcontainers::core::IntoContainerPort;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::GenericImage;
use testcontainers::ImageExt;
use tokio::sync::Mutex;

const RESOURCE_NAME: &str = "opensearch";
/// Default image repository (`opensearchproject/opensearch`).
pub const DEFAULT_OPENSEARCH_IMAGE_REPOSITORY: &str = "opensearchproject/opensearch";
/// Default OpenSearch image tag (`opensearchproject/opensearch:2`).
pub const DEFAULT_OPENSEARCH_IMAGE_TAG: &str = "2";
const OPENSEARCH_HTTP_PORT: u16 = 9200;
const SUITE_SECURITY_USER: &str = "admin";
/// Meets OpenSearch 2.19+ `OPENSEARCH_INITIAL_ADMIN_PASSWORD` strength (zxcvbn + regex).
const SUITE_SECURITY_PASSWORD: &str = "Shakedown#Integration2024!";

#[allow(clippy::large_enum_variant)]
enum SuiteOpenSearchState {
    Idle,
    Running {
        host: String,
        port: u16,
        container: testcontainers::ContainerAsync<GenericImage>,
        refs: usize,
        image_key: String,
    },
}

static SUITE_OPENSEARCH: OnceLock<Mutex<SuiteOpenSearchState>> = OnceLock::new();

fn suite_opensearch_mutex() -> &'static Mutex<SuiteOpenSearchState> {
    SUITE_OPENSEARCH.get_or_init(|| Mutex::new(SuiteOpenSearchState::Idle))
}

fn image_fingerprint(repository: &str, tag_override: &Option<String>) -> String {
    let tag = tag_override
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .unwrap_or(DEFAULT_OPENSEARCH_IMAGE_TAG);
    format!("{repository}:{tag}")
}

/// Docker-backed OpenSearch (`opensearchproject/opensearch` via [`GenericImage`]).
///
/// **Suite lifecycle:** one node per process; per-scenario index prefix; refcounted teardown deletes
/// matching indices then may remove the container.
///
/// Default tag **`2`**. Override with [`Self::with_image_tag`] or `SHAKEDOWN_OPENSEARCH_IMAGE_TAG`.
/// Security is **off** by default (`plugins.security.disabled=true`). Use [`Self::with_security`] for HTTP basic auth (`admin` / fixed test password matching `OPENSEARCH_INITIAL_ADMIN_PASSWORD` policy).
pub struct OpenSearchResource {
    image_repository: String,
    image_tag: Option<String>,
    security_enabled: bool,
    holds_suite_ref: bool,
    host: Option<String>,
    port: Option<u16>,
    http_url: String,
    index_prefix: String,
    username: Option<String>,
    password: Option<String>,
    user_seed: UserSeedSlot<OpenSearchBindings>,
}

impl std::fmt::Debug for OpenSearchResource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenSearchResource")
            .field("image_repository", &self.image_repository)
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

impl Default for OpenSearchResource {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenSearchResource {
    /// Create default settings (suite container started on first provision; security off).
    pub fn new() -> Self {
        Self {
            image_repository: DEFAULT_OPENSEARCH_IMAGE_REPOSITORY.to_string(),
            image_tag: Some(DEFAULT_OPENSEARCH_IMAGE_TAG.to_string()),
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

    /// Enable the security plugin with fixed `admin` / `shakedown` credentials for integration tests.
    pub fn with_security(mut self, enabled: bool) -> Self {
        self.security_enabled = enabled;
        self
    }

    /// Override the image tag (pin `2.11.0` or another tag). Repository stays [`DEFAULT_OPENSEARCH_IMAGE_REPOSITORY`].
    pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
        self.image_tag = Some(tag.into());
        self
    }

    /// Apply tag override from `SHAKEDOWN_OPENSEARCH_IMAGE_TAG` when set.
    pub fn with_image_tag_from_env(mut self) -> Self {
        if let Ok(tag) = std::env::var("SHAKEDOWN_OPENSEARCH_IMAGE_TAG") {
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

    /// Async seed closure receiving [`OpenSearchBindings`] ([`Resource::apply_default_seed`]).
    pub fn set_seed(&mut self, seed: OpenSearchUserSeedFn) {
        *self.user_seed.lock().unwrap() = Some(seed);
    }

    fn opensearch_bindings(&self) -> Result<OpenSearchBindings, InfrastructureError> {
        let host = self
            .host
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
        let port = self
            .port
            .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
        Ok(OpenSearchBindings {
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
impl Resource for OpenSearchResource {
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
        let want_key = image_fingerprint(&self.image_repository, &self.image_tag);

        let mut guard = suite_opensearch_mutex().lock().await;
        let (host, port) = match &mut *guard {
            SuiteOpenSearchState::Idle => {
                let repository = self.image_repository.as_str();
                let tag = self
                    .image_tag
                    .as_deref()
                    .map(str::trim)
                    .filter(|t| !t.is_empty())
                    .unwrap_or(DEFAULT_OPENSEARCH_IMAGE_TAG);
                // OpenSearch 2.12+ requires `OPENSEARCH_INITIAL_ADMIN_PASSWORD` even when the
                // security *plugin* is disabled; otherwise the entrypoint exits before binding :9200.
                let mut start_image = GenericImage::new(repository, tag)
                    .with_exposed_port(OPENSEARCH_HTTP_PORT.tcp())
                    .with_wait_for(WaitFor::message_on_either_std("publish_address"))
                    .with_startup_timeout(Duration::from_secs(240))
                    .with_env_var("discovery.type", "single-node")
                    .with_env_var("OPENSEARCH_JAVA_OPTS", "-Xms256m -Xmx256m")
                    .with_env_var("OPENSEARCH_INITIAL_ADMIN_PASSWORD", SUITE_SECURITY_PASSWORD)
                    .with_label(
                        crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                        crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                    );
                if self.security_enabled {
                    start_image = start_image.with_env_var("plugins.security.disabled", "false");
                } else {
                    start_image = start_image.with_env_var("plugins.security.disabled", "true");
                }
                let node = start_image.start().await.map_err(Self::map_tc_err)?;

                let mapped_port = node
                    .get_host_port_ipv4(OPENSEARCH_HTTP_PORT.tcp())
                    .await
                    .map_err(Self::map_tc_err)?;
                let host = "127.0.0.1".to_string();

                *guard = SuiteOpenSearchState::Running {
                    host: host.clone(),
                    port: mapped_port,
                    container: node,
                    refs: 1,
                    image_key: want_key.clone(),
                };
                (host, mapped_port)
            }
            SuiteOpenSearchState::Running {
                host,
                port,
                refs,
                image_key,
                ..
            } => {
                if *image_key != want_key {
                    return Err(InfrastructureError::Backend(Box::new(
                        std::io::Error::other(format!(
                            "OpenSearch suite already running as {image_key}; \
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
            id: format!("opensearch_{scenario_id}"),
            connection_string: http_url.clone(),
            metadata: Some(serde_json::json!({
                "host": host,
                "port": port,
                "url": http_url,
                "index_prefix": index_prefix,
                "username": username,
                "password": password,
                "engine": "opensearch",
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

        const ATTEMPTS: u32 = 60;
        for attempt in 0..ATTEMPTS {
            let client = self.search_client();
            match client.cluster_health().await {
                Ok(health) => {
                    let status = health
                        .get("status")
                        .and_then(|value| value.as_str())
                        .unwrap_or("");
                    if status != "red" {
                        return Ok(());
                    }
                    if attempt + 1 == ATTEMPTS {
                        return Err(InfrastructureError::Readiness {
                            resource: RESOURCE_NAME.into(),
                            source: Box::new(std::io::Error::other(format!(
                                "cluster health status is red: {health}"
                            ))),
                        });
                    }
                }
                Err(error) if attempt + 1 == ATTEMPTS => {
                    return Err(InfrastructureError::Readiness {
                        resource: RESOURCE_NAME.into(),
                        source: Box::new(std::io::Error::other(error.to_string())),
                    });
                }
                Err(_) => {}
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Err(InfrastructureError::Readiness {
            resource: RESOURCE_NAME.into(),
            source: Box::new(std::io::Error::other(
                "OpenSearch readiness: cluster health attempts exhausted",
            )),
        })
    }

    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        if let Some(seed_fn) = take_user_seed(&self.user_seed) {
            let bindings = self.opensearch_bindings()?;
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

        let mut guard = suite_opensearch_mutex().lock().await;
        let mut container_to_rm: Option<testcontainers::ContainerAsync<GenericImage>> = None;
        match &mut *guard {
            SuiteOpenSearchState::Idle => {}
            SuiteOpenSearchState::Running { refs, .. } => {
                *refs = refs.saturating_sub(1);
                if *refs == 0 {
                    let taken = std::mem::replace(&mut *guard, SuiteOpenSearchState::Idle);
                    if let SuiteOpenSearchState::Running { container, .. } = taken {
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
