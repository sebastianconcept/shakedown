//! Suite-shared Toxiproxy (`ghcr.io/shopify/toxiproxy`) for fault injection (latency, loss, reset on proxied TCP).
//!
//! Container labels use [`crate::docker_labels`] (same strings as `diagnostics::labels`).

use crate::error::{InfrastructureError, ScenarioId};
use crate::resources::{Resource, ResourceHandle};
use async_trait::async_trait;
use noxious_client::Client as ToxiproxyClient;
use std::sync::OnceLock;
use std::time::Duration;
use testcontainers::core::IntoContainerPort;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::GenericImage;
use testcontainers::ImageExt;
use tokio::sync::Mutex;

use crate::docker_labels::{SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE};

const RESOURCE_NAME: &str = "toxiproxy";
const API_PORT: u16 = 8474;
const DEFAULT_IMAGE_NAME: &str = "ghcr.io/shopify/toxiproxy";
const DEFAULT_IMAGE_TAG: &str = "2.9.0";

#[allow(clippy::large_enum_variant)]
enum SuiteToxiproxyState {
    Idle,
    Running {
        api_base_url: String,
        /// Docker container id (hex) for [`bollard::Docker::connect_network`] when wiring an inter-node mesh.
        container_id: String,
        container: testcontainers::ContainerAsync<GenericImage>,
        refs: usize,
        image_key: String,
    },
}

static SUITE_TOXIPROXY: OnceLock<Mutex<SuiteToxiproxyState>> = OnceLock::new();

fn suite_toxiproxy_mutex() -> &'static Mutex<SuiteToxiproxyState> {
    SUITE_TOXIPROXY.get_or_init(|| Mutex::new(SuiteToxiproxyState::Idle))
}

fn image_fingerprint(image_name: &str, tag_override: &Option<String>) -> String {
    let tag = tag_override
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .unwrap_or(DEFAULT_IMAGE_TAG);
    format!("{image_name}:{tag}")
}

/// Docker-backed Toxiproxy using a suite-wide container (same suite-shared refcount pattern as the built-in `KafkaResource`).
#[derive(Debug)]
pub struct ToxiproxyResource {
    image_name: String,
    image_tag: Option<String>,
    holds_suite_ref: bool,
    api_base_url: Option<String>,
}

impl Default for ToxiproxyResource {
    fn default() -> Self {
        Self::new()
    }
}

impl ToxiproxyResource {
    /// Default image [`DEFAULT_IMAGE_NAME`] — suite broker starts on first provision.
    pub fn new() -> Self {
        Self {
            image_name: DEFAULT_IMAGE_NAME.to_string(),
            image_tag: None,
            holds_suite_ref: false,
            api_base_url: None,
        }
    }

    /// Override the image tag (e.g. pin a release). Image name stays [`DEFAULT_IMAGE_NAME`].
    pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
        self.image_tag = Some(tag.into());
        self
    }

    /// Apply tag from `SHAKEDOWN_TOXIPROXY_IMAGE_TAG` when set (suite-level CI/prod pins).
    pub fn with_image_tag_from_env(mut self) -> Self {
        if let Ok(tag) = std::env::var("SHAKEDOWN_TOXIPROXY_IMAGE_TAG") {
            let tag = tag.trim();
            if !tag.is_empty() {
                self.image_tag = Some(tag.to_string());
            }
        }
        self
    }

    fn map_tc_err(error: impl std::fmt::Display + Send + Sync + 'static) -> InfrastructureError {
        InfrastructureError::Provision {
            resource: RESOURCE_NAME.into(),
            source: Box::new(std::io::Error::other(error.to_string())),
        }
    }

    async fn get_version_ok(api_base_url: &str) -> Result<(), InfrastructureError> {
        let url = format!("{}/version", api_base_url.trim_end_matches('/'));
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(4))
            .build()
            .map_err(|source| InfrastructureError::Readiness {
                resource: RESOURCE_NAME.into(),
                source: Box::new(source),
            })?;
        const ATTEMPTS: u32 = 30;
        for attempt in 0..ATTEMPTS {
            match http.get(&url).send().await {
                Ok(response) if response.status().is_success() => return Ok(()),
                Ok(response) => {
                    if attempt + 1 == ATTEMPTS {
                        return Err(InfrastructureError::Readiness {
                            resource: RESOURCE_NAME.into(),
                            source: Box::new(std::io::Error::other(format!(
                                "GET /version returned {}",
                                response.status()
                            ))),
                        });
                    }
                }
                Err(err) => {
                    if attempt + 1 == ATTEMPTS {
                        return Err(InfrastructureError::Readiness {
                            resource: RESOURCE_NAME.into(),
                            source: Box::new(std::io::Error::other(err.to_string())),
                        });
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Err(InfrastructureError::Readiness {
            resource: RESOURCE_NAME.into(),
            source: Box::new(std::io::Error::other(
                "GET /version did not succeed in time",
            )),
        })
    }
}

#[async_trait]
impl Resource for ToxiproxyResource {
    async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        if self.holds_suite_ref {
            return Err(InfrastructureError::AlreadyProvisioned(
                RESOURCE_NAME.into(),
            ));
        }

        let want_key = image_fingerprint(&self.image_name, &self.image_tag);

        let mut guard = suite_toxiproxy_mutex().lock().await;
        let api_base_url = match &mut *guard {
            SuiteToxiproxyState::Idle => {
                let tag = self
                    .image_tag
                    .clone()
                    .unwrap_or_else(|| DEFAULT_IMAGE_TAG.to_string());
                let image = GenericImage::new(self.image_name.clone(), tag)
                    .with_exposed_port(API_PORT.tcp())
                    .with_wait_for(WaitFor::seconds(2))
                    .with_label(SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE);
                let node = image.start().await.map_err(Self::map_tc_err)?;
                let port = node
                    .get_host_port_ipv4(API_PORT)
                    .await
                    .map_err(Self::map_tc_err)?;
                let api_base_url = format!("http://127.0.0.1:{port}");
                let container_id = node.id().to_string();
                *guard = SuiteToxiproxyState::Running {
                    api_base_url: api_base_url.clone(),
                    container_id,
                    container: node,
                    refs: 1,
                    image_key: want_key.clone(),
                };
                api_base_url
            }
            SuiteToxiproxyState::Running {
                api_base_url,
                refs,
                image_key,
                ..
            } => {
                if *image_key != want_key {
                    return Err(InfrastructureError::Backend(Box::new(
                        std::io::Error::other(format!(
                            "Toxiproxy suite already running as {image_key}; \
                             cannot start or join as {want_key}. Use one image:tag per process."
                        )),
                    )));
                }
                *refs += 1;
                api_base_url.clone()
            }
        };
        drop(guard);

        self.api_base_url = Some(api_base_url.clone());
        self.holds_suite_ref = true;

        Ok(vec![ResourceHandle {
            id: format!("toxiproxy_{scenario_id}"),
            connection_string: api_base_url.clone(),
            metadata: Some(serde_json::json!({
                "engine": "toxiproxy",
                "api_url": api_base_url,
            })),
        }])
    }

    async fn check_ready(&self) -> Result<(), InfrastructureError> {
        let api_base_url = self
            .api_base_url
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
        Self::get_version_ok(api_base_url).await
    }

    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        if !self.holds_suite_ref {
            self.api_base_url = None;
            return Ok(());
        }

        let api_base = self
            .api_base_url
            .take()
            .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;

        let client = ToxiproxyClient::new(&api_base);
        let proxies = client
            .proxies()
            .await
            .map_err(|source| InfrastructureError::Teardown {
                resource: RESOURCE_NAME.into(),
                source: Box::new(source),
            })?;
        for (_name, proxy) in proxies {
            proxy
                .delete()
                .await
                .map_err(|source| InfrastructureError::Teardown {
                    resource: RESOURCE_NAME.into(),
                    source: Box::new(source),
                })?;
        }

        let mut guard = suite_toxiproxy_mutex().lock().await;
        let mut container_to_rm: Option<testcontainers::ContainerAsync<GenericImage>> = None;
        match &mut *guard {
            SuiteToxiproxyState::Idle => {}
            SuiteToxiproxyState::Running { refs, .. } => {
                *refs = refs.saturating_sub(1);
                if *refs == 0 {
                    let taken = std::mem::replace(&mut *guard, SuiteToxiproxyState::Idle);
                    if let SuiteToxiproxyState::Running { container, .. } = taken {
                        container_to_rm = Some(container);
                    }
                }
            }
        }
        drop(guard);

        if let Some(container) = container_to_rm {
            container
                .rm()
                .await
                .map_err(|remove_error| InfrastructureError::Teardown {
                    resource: RESOURCE_NAME.into(),
                    source: Box::new(std::io::Error::other(remove_error.to_string())),
                })?;
        }

        self.holds_suite_ref = false;
        self.api_base_url = None;
        Ok(())
    }
}

pub(crate) async fn toxiproxy_suite_container_id() -> Option<String> {
    let guard = suite_toxiproxy_mutex().lock().await;
    match &*guard {
        SuiteToxiproxyState::Idle => None,
        SuiteToxiproxyState::Running { container_id, .. } => Some(container_id.clone()),
    }
}
