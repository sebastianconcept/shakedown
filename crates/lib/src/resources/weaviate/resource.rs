use crate::error::{InfrastructureError, ScenarioId};
use crate::resources::user_seed::{take_user_seed, UserSeedSlot, WeaviateUserSeedFn};
use crate::resources::{
    map_seed_error, sanitize_elasticsearch_index_prefix, Resource, ResourceHandle, WeaviateBindings,
};
use async_trait::async_trait;
use std::sync::OnceLock;
use testcontainers::core::IntoContainerPort;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::GenericImage;
use testcontainers::ImageExt;
use tokio::sync::Mutex;

const RESOURCE_NAME: &str = "weaviate";
/// Default image repository (`semitechnologies/weaviate`, same as testcontainers-modules/weaviate).
pub const DEFAULT_WEAVIATE_IMAGE_REPOSITORY: &str = "semitechnologies/weaviate";
/// Default image tag (`1.28.2`, aligned with testcontainers-modules/weaviate).
pub const DEFAULT_WEAVIATE_IMAGE_TAG: &str = "1.28.2";
const WEAVIATE_HTTP_PORT: u16 = 8080;
const WEAVIATE_GRPC_PORT: u16 = 50051;

#[allow(clippy::large_enum_variant)]
enum SuiteWeaviateState {
    Idle,
    Running {
        host: String,
        port: u16,
        container: testcontainers::ContainerAsync<GenericImage>,
        refs: usize,
        image_key: String,
    },
}

static SUITE_WEAVIATE: OnceLock<Mutex<SuiteWeaviateState>> = OnceLock::new();

fn suite_weaviate_mutex() -> &'static Mutex<SuiteWeaviateState> {
    SUITE_WEAVIATE.get_or_init(|| Mutex::new(SuiteWeaviateState::Idle))
}

fn image_fingerprint(tag_override: &Option<String>) -> String {
    let tag = tag_override
        .as_deref()
        .map(str::trim)
        .filter(|tag| !tag.is_empty())
        .unwrap_or(DEFAULT_WEAVIATE_IMAGE_TAG);
    format!("{}:{tag}", DEFAULT_WEAVIATE_IMAGE_REPOSITORY)
}

fn class_delete_prefix(class_prefix: &str) -> String {
    let mut chars = class_prefix.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => format!("{}{}", first.to_uppercase(), chars.as_str()),
    }
}

async fn delete_classes_with_prefix(rest_url: &str, class_prefix: &str) -> Result<(), String> {
    let delete_prefix = class_delete_prefix(class_prefix);
    if delete_prefix.is_empty() {
        return Ok(());
    }
    let client = reqwest::Client::new();
    let base = rest_url.trim_end_matches('/');
    let schema_url = format!("{base}/v1/schema");
    let response = client
        .get(&schema_url)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    if !response.status().is_success() {
        return Err(format!("list schema failed: HTTP {}", response.status()));
    }
    let body: serde_json::Value = response.json().await.map_err(|error| error.to_string())?;
    let names: Vec<String> = body
        .get("classes")
        .and_then(|classes| classes.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.get("class").and_then(|name| name.as_str()))
                .filter(|name| name.starts_with(&delete_prefix))
                .map(std::string::ToString::to_string)
                .collect()
        })
        .unwrap_or_default();
    for class_name in names {
        let delete_url = format!("{base}/v1/schema/{}", class_name);
        let delete_response = client
            .delete(&delete_url)
            .send()
            .await
            .map_err(|error| error.to_string())?;
        if !delete_response.status().is_success() {
            return Err(format!(
                "delete class {class_name} failed: HTTP {}",
                delete_response.status()
            ));
        }
    }
    Ok(())
}

/// Docker-backed Weaviate (`semitechnologies/weaviate` via [`GenericImage`], tag aligned with
/// `testcontainers-modules/weaviate`).
///
/// **Suite lifecycle:** one node per process; per-scenario class name prefix; refcounted teardown
/// deletes matching schema classes then may remove the container.
///
/// Startup uses a short fixed wait (like Qdrant); [`Resource::check_ready`] performs HTTP
/// `GET /v1/.well-known/ready` so flakes from container HTTP wait strategies are avoided.
///
/// Default tag **`1.28.2`**. Override with [`Self::with_image_tag`] or `SHAKEDOWN_WEAVIATE_IMAGE_TAG`.
/// The container enables anonymous access (`AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true`) for local test runs without auth setup.
pub struct WeaviateResource {
    image_tag: Option<String>,
    holds_suite_ref: bool,
    host: Option<String>,
    port: Option<u16>,
    rest_url: String,
    class_prefix: String,
    user_seed: UserSeedSlot<WeaviateBindings>,
}

impl std::fmt::Debug for WeaviateResource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WeaviateResource")
            .field("image_tag", &self.image_tag)
            .field("holds_suite_ref", &self.holds_suite_ref)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("rest_url", &self.rest_url)
            .field("class_prefix", &self.class_prefix)
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

impl Default for WeaviateResource {
    fn default() -> Self {
        Self::new()
    }
}

impl WeaviateResource {
    /// Create default settings (suite container started on first provision).
    pub fn new() -> Self {
        Self {
            image_tag: Some(DEFAULT_WEAVIATE_IMAGE_TAG.to_string()),
            holds_suite_ref: false,
            host: None,
            port: None,
            rest_url: String::new(),
            class_prefix: String::new(),
            user_seed: UserSeedSlot::new(None),
        }
    }

    /// Override the image tag (e.g. `1.28.2`). Image name stays `semitechnologies/weaviate`.
    pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
        self.image_tag = Some(tag.into());
        self
    }

    /// Apply tag override from `SHAKEDOWN_WEAVIATE_IMAGE_TAG` when set (tag only or `repo:tag`).
    pub fn with_image_tag_from_env(mut self) -> Self {
        if let Ok(tag) = std::env::var("SHAKEDOWN_WEAVIATE_IMAGE_TAG") {
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

    /// Async seed closure receiving [`WeaviateBindings`] ([`Resource::apply_default_seed`]).
    pub fn set_seed(&mut self, seed: WeaviateUserSeedFn) {
        *self.user_seed.lock().unwrap() = Some(seed);
    }

    fn weaviate_bindings(&self) -> Result<WeaviateBindings, InfrastructureError> {
        let host = self
            .host
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
        let port = self
            .port
            .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
        Ok(WeaviateBindings {
            rest_url: self.rest_url.clone(),
            host: host.to_string(),
            port,
            class_prefix: self.class_prefix.clone(),
        })
    }
}

#[async_trait]
impl Resource for WeaviateResource {
    async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        if self.holds_suite_ref {
            return Err(InfrastructureError::AlreadyProvisioned(
                RESOURCE_NAME.into(),
            ));
        }

        let class_prefix = sanitize_elasticsearch_index_prefix(scenario_id.as_str());
        let want_key = image_fingerprint(&self.image_tag);

        let mut guard = suite_weaviate_mutex().lock().await;
        let (host, port) = match &mut *guard {
            SuiteWeaviateState::Idle => {
                let tag = self
                    .image_tag
                    .as_deref()
                    .map(str::trim)
                    .filter(|tag| !tag.is_empty())
                    .unwrap_or(DEFAULT_WEAVIATE_IMAGE_TAG);
                let start_image = GenericImage::new(DEFAULT_WEAVIATE_IMAGE_REPOSITORY, tag)
                    .with_exposed_port(WEAVIATE_HTTP_PORT.tcp())
                    .with_exposed_port(WEAVIATE_GRPC_PORT.tcp())
                    .with_wait_for(WaitFor::seconds(10))
                    .with_env_var("AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED", "true")
                    .with_env_var("PERSISTENCE_DATA_PATH", "/tmp/weaviate")
                    .with_label(
                        crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                        crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                    );
                let node = start_image.start().await.map_err(Self::map_tc_err)?;

                let mapped_port = node
                    .get_host_port_ipv4(WEAVIATE_HTTP_PORT)
                    .await
                    .map_err(Self::map_tc_err)?;
                let host = "127.0.0.1".to_string();

                *guard = SuiteWeaviateState::Running {
                    host: host.clone(),
                    port: mapped_port,
                    container: node,
                    refs: 1,
                    image_key: want_key.clone(),
                };
                (host, mapped_port)
            }
            SuiteWeaviateState::Running {
                host,
                port,
                refs,
                image_key,
                ..
            } => {
                if *image_key != want_key {
                    return Err(InfrastructureError::Backend(Box::new(
                        std::io::Error::other(format!(
                            "Weaviate suite already running as {image_key}; \
                             cannot start or join as {want_key}. Use one image:tag per process."
                        )),
                    )));
                }
                *refs += 1;
                (host.clone(), *port)
            }
        };
        drop(guard);

        let rest_url = format!("http://{host}:{port}");

        self.holds_suite_ref = true;
        self.host = Some(host.clone());
        self.port = Some(port);
        self.rest_url = rest_url.clone();
        self.class_prefix = class_prefix.clone();

        Ok(vec![ResourceHandle {
            id: format!("weaviate_{scenario_id}"),
            connection_string: rest_url.clone(),
            metadata: Some(serde_json::json!({
                "host": host,
                "port": port,
                "rest_url": rest_url,
                "class_prefix": class_prefix,
                "engine": "weaviate",
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

        let ready_url = format!(
            "{}/v1/.well-known/ready",
            self.rest_url.trim_end_matches('/')
        );
        let response = reqwest::Client::new()
            .get(&ready_url)
            .send()
            .await
            .map_err(|error| InfrastructureError::Readiness {
                resource: RESOURCE_NAME.into(),
                source: Box::new(std::io::Error::other(error.to_string())),
            })?;
        if !response.status().is_success() {
            return Err(InfrastructureError::Readiness {
                resource: RESOURCE_NAME.into(),
                source: Box::new(std::io::Error::other(format!(
                    "GET /v1/.well-known/ready returned HTTP {}",
                    response.status()
                ))),
            });
        }
        Ok(())
    }

    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        if let Some(seed_fn) = take_user_seed(&self.user_seed) {
            let bindings = self.weaviate_bindings()?;
            seed_fn(bindings).await.map_err(map_seed_error)?;
        }
        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        if !self.holds_suite_ref {
            self.host = None;
            self.port = None;
            self.rest_url.clear();
            self.class_prefix.clear();
            return Ok(());
        }

        let class_prefix = self.class_prefix.clone();
        let rest_url = self.rest_url.clone();

        if !class_prefix.is_empty() && !rest_url.is_empty() {
            if let Err(error) =
                delete_classes_with_prefix(rest_url.as_str(), class_prefix.as_str()).await
            {
                return Err(InfrastructureError::Teardown {
                    resource: RESOURCE_NAME.into(),
                    source: Box::new(std::io::Error::other(error)),
                });
            }
        }

        let mut guard = suite_weaviate_mutex().lock().await;
        let mut container_to_rm: Option<testcontainers::ContainerAsync<GenericImage>> = None;
        match &mut *guard {
            SuiteWeaviateState::Idle => {}
            SuiteWeaviateState::Running { refs, .. } => {
                *refs = refs.saturating_sub(1);
                if *refs == 0 {
                    let taken = std::mem::replace(&mut *guard, SuiteWeaviateState::Idle);
                    if let SuiteWeaviateState::Running { container, .. } = taken {
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
        self.rest_url.clear();
        self.class_prefix.clear();
        Ok(())
    }
}
