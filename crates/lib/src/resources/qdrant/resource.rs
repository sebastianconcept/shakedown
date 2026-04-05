use crate::error::{InfrastructureError, ScenarioId};
use crate::resources::{sanitize_elasticsearch_index_prefix, Resource, ResourceHandle};
use async_trait::async_trait;
use std::sync::OnceLock;
use testcontainers::core::IntoContainerPort;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::GenericImage;
use testcontainers::ImageExt;
use tokio::sync::Mutex;

const RESOURCE_NAME: &str = "qdrant";
/// Default image repository (`qdrant/qdrant`).
pub const DEFAULT_QDRANT_IMAGE_REPOSITORY: &str = "qdrant/qdrant";
/// Default image tag (`qdrant/qdrant:latest`). (`:1` is not published on Docker Hub.)
pub const DEFAULT_QDRANT_IMAGE_TAG: &str = "latest";
const QDRANT_REST_PORT: u16 = 6333;
const QDRANT_GRPC_PORT: u16 = 6334;

#[allow(clippy::large_enum_variant)]
enum SuiteQdrantState {
    Idle,
    Running {
        host: String,
        rest_port: u16,
        grpc_port: u16,
        container: testcontainers::ContainerAsync<GenericImage>,
        refs: usize,
        image_key: String,
    },
}

static SUITE_QDRANT: OnceLock<Mutex<SuiteQdrantState>> = OnceLock::new();

fn suite_qdrant_mutex() -> &'static Mutex<SuiteQdrantState> {
    SUITE_QDRANT.get_or_init(|| Mutex::new(SuiteQdrantState::Idle))
}

fn image_fingerprint(repository: &str, tag_override: &Option<String>) -> String {
    let tag = tag_override
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .unwrap_or(DEFAULT_QDRANT_IMAGE_TAG);
    format!("{repository}:{tag}")
}

async fn delete_collections_with_prefix(rest_url: &str, prefix: &str) -> Result<(), String> {
    let client = reqwest::Client::new();
    let base = rest_url.trim_end_matches('/');
    let list_url = format!("{base}/collections");
    let response = client
        .get(&list_url)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    if !response.status().is_success() {
        return Err(format!(
            "list collections failed: HTTP {}",
            response.status()
        ));
    }
    let body: serde_json::Value = response.json().await.map_err(|error| error.to_string())?;
    let names: Vec<String> = body
        .get("result")
        .and_then(|result| result.get("collections"))
        .and_then(|collections| collections.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.get("name").and_then(|name| name.as_str()))
                .filter(|name| name.starts_with(prefix))
                .map(std::string::ToString::to_string)
                .collect()
        })
        .unwrap_or_default();
    for name in names {
        let delete_url = format!("{base}/collections/{}", name);
        let delete_response = client
            .delete(&delete_url)
            .send()
            .await
            .map_err(|error| error.to_string())?;
        if !delete_response.status().is_success() {
            return Err(format!(
                "delete collection {name} failed: HTTP {}",
                delete_response.status()
            ));
        }
    }
    Ok(())
}

/// Docker-backed Qdrant (`qdrant/qdrant` via [`GenericImage`]).
///
/// **Suite lifecycle:** one node per process; per-scenario collection prefix; refcounted teardown
/// deletes matching collections then may remove the container.
///
/// Default tag **`latest`**. Override with [`Self::with_image_tag`] or `SHAKEDOWN_QDRANT_IMAGE_TAG`.
/// Readiness probes HTTP [`GET /readyz`](https://qdrant.tech/documentation/guides/installation/) on the REST port until the node reports ready.
#[derive(Debug)]
pub struct QdrantResource {
    image_repository: String,
    image_tag: Option<String>,
    holds_suite_ref: bool,
    host: Option<String>,
    rest_port: Option<u16>,
    grpc_port: Option<u16>,
    rest_url: String,
    grpc_url: String,
    collection_prefix: String,
}

impl Default for QdrantResource {
    fn default() -> Self {
        Self::new()
    }
}

impl QdrantResource {
    /// Create default settings (suite container started on first provision).
    pub fn new() -> Self {
        Self {
            image_repository: DEFAULT_QDRANT_IMAGE_REPOSITORY.to_string(),
            image_tag: Some(DEFAULT_QDRANT_IMAGE_TAG.to_string()),
            holds_suite_ref: false,
            host: None,
            rest_port: None,
            grpc_port: None,
            rest_url: String::new(),
            grpc_url: String::new(),
            collection_prefix: String::new(),
        }
    }

    /// Override the image tag. Repository stays [`DEFAULT_QDRANT_IMAGE_REPOSITORY`].
    pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
        self.image_tag = Some(tag.into());
        self
    }

    /// Apply tag override from `SHAKEDOWN_QDRANT_IMAGE_TAG` when set (e.g. `v1.12.5` or `latest`).
    pub fn with_image_tag_from_env(mut self) -> Self {
        if let Ok(tag) = std::env::var("SHAKEDOWN_QDRANT_IMAGE_TAG") {
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
impl Resource for QdrantResource {
    async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        if self.holds_suite_ref {
            return Err(InfrastructureError::AlreadyProvisioned(
                RESOURCE_NAME.into(),
            ));
        }

        let collection_prefix = sanitize_elasticsearch_index_prefix(scenario_id.as_str());
        let want_key = image_fingerprint(&self.image_repository, &self.image_tag);

        let mut guard = suite_qdrant_mutex().lock().await;
        let (host, rest_port, grpc_port) = match &mut *guard {
            SuiteQdrantState::Idle => {
                let repository = self.image_repository.as_str();
                let tag = self
                    .image_tag
                    .as_deref()
                    .map(str::trim)
                    .filter(|t| !t.is_empty())
                    .unwrap_or(DEFAULT_QDRANT_IMAGE_TAG);
                let start_image = GenericImage::new(repository, tag)
                    .with_exposed_port(QDRANT_REST_PORT.tcp())
                    .with_exposed_port(QDRANT_GRPC_PORT.tcp())
                    .with_wait_for(WaitFor::seconds(3))
                    .with_label(
                        crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                        crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                    );
                let node = start_image.start().await.map_err(Self::map_tc_err)?;

                let mapped_rest = node
                    .get_host_port_ipv4(QDRANT_REST_PORT)
                    .await
                    .map_err(Self::map_tc_err)?;
                let mapped_grpc = node
                    .get_host_port_ipv4(QDRANT_GRPC_PORT)
                    .await
                    .map_err(Self::map_tc_err)?;
                let host = "127.0.0.1".to_string();

                *guard = SuiteQdrantState::Running {
                    host: host.clone(),
                    rest_port: mapped_rest,
                    grpc_port: mapped_grpc,
                    container: node,
                    refs: 1,
                    image_key: want_key.clone(),
                };
                (host, mapped_rest, mapped_grpc)
            }
            SuiteQdrantState::Running {
                host,
                rest_port,
                grpc_port,
                refs,
                image_key,
                ..
            } => {
                if *image_key != want_key {
                    return Err(InfrastructureError::Backend(Box::new(
                        std::io::Error::other(format!(
                            "Qdrant suite already running as {image_key}; \
                             cannot start or join as {want_key}. Use one image:tag per process."
                        )),
                    )));
                }
                *refs += 1;
                (host.clone(), *rest_port, *grpc_port)
            }
        };
        drop(guard);

        let rest_url = format!("http://{host}:{rest_port}");
        let grpc_url = format!("http://{host}:{grpc_port}");

        self.holds_suite_ref = true;
        self.host = Some(host.clone());
        self.rest_port = Some(rest_port);
        self.grpc_port = Some(grpc_port);
        self.rest_url = rest_url.clone();
        self.grpc_url = grpc_url.clone();
        self.collection_prefix = collection_prefix.clone();

        Ok(vec![ResourceHandle {
            id: format!("qdrant_{scenario_id}"),
            connection_string: grpc_url.clone(),
            metadata: Some(serde_json::json!({
                "host": host,
                "rest_port": rest_port,
                "grpc_port": grpc_port,
                "rest_url": rest_url,
                "grpc_url": grpc_url,
                "collection_prefix": collection_prefix,
                "engine": "qdrant",
            })),
        }])
    }

    async fn check_ready(&self) -> Result<(), InfrastructureError> {
        let host = self
            .host
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
        let _ = self
            .rest_port
            .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
        let _ = host;

        let ready_url = format!("{}/readyz", self.rest_url.trim_end_matches('/'));
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
                    "GET /readyz returned HTTP {}",
                    response.status()
                ))),
            });
        }
        Ok(())
    }

    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        if !self.holds_suite_ref {
            self.host = None;
            self.rest_port = None;
            self.grpc_port = None;
            self.rest_url.clear();
            self.grpc_url.clear();
            self.collection_prefix.clear();
            return Ok(());
        }

        let collection_prefix = self.collection_prefix.clone();
        let rest_url = self.rest_url.clone();

        if !collection_prefix.is_empty() && !rest_url.is_empty() {
            if let Err(error) =
                delete_collections_with_prefix(rest_url.as_str(), collection_prefix.as_str()).await
            {
                return Err(InfrastructureError::Teardown {
                    resource: RESOURCE_NAME.into(),
                    source: Box::new(std::io::Error::other(error)),
                });
            }
        }

        let mut guard = suite_qdrant_mutex().lock().await;
        let mut container_to_rm: Option<testcontainers::ContainerAsync<GenericImage>> = None;
        match &mut *guard {
            SuiteQdrantState::Idle => {}
            SuiteQdrantState::Running { refs, .. } => {
                *refs = refs.saturating_sub(1);
                if *refs == 0 {
                    let taken = std::mem::replace(&mut *guard, SuiteQdrantState::Idle);
                    if let SuiteQdrantState::Running { container, .. } = taken {
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
        self.rest_port = None;
        self.grpc_port = None;
        self.rest_url.clear();
        self.grpc_url.clear();
        self.collection_prefix.clear();
        Ok(())
    }
}
