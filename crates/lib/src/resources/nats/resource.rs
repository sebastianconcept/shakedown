use crate::error::{InfrastructureError, ScenarioId};
use crate::resources::{sanitize_nats_subject_prefix, Resource, ResourceHandle};
use async_trait::async_trait;
use std::sync::OnceLock;
use testcontainers::core::{Image, ImageExt};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use tokio::sync::Mutex;

const NATS_CLIENT_PORT: u16 = 4222;
/// Default `nats` image tag (Docker Hub `nats:2`).
pub const DEFAULT_NATS_IMAGE_TAG: &str = "2";

#[allow(clippy::large_enum_variant)]
enum SuiteNatsState {
    Idle,
    Running {
        host: String,
        port: u16,
        container: testcontainers::ContainerAsync<Nats>,
        refs: usize,
        image_key: String,
    },
}

static SUITE_NATS: OnceLock<Mutex<SuiteNatsState>> = OnceLock::new();

fn suite_nats_mutex() -> &'static Mutex<SuiteNatsState> {
    SUITE_NATS.get_or_init(|| Mutex::new(SuiteNatsState::Idle))
}

fn image_fingerprint(image: &Nats, tag_override: &Option<String>) -> String {
    let tag = tag_override.as_deref().unwrap_or(DEFAULT_NATS_IMAGE_TAG);
    format!("{}:{}", image.name(), tag)
}

/// Docker-backed NATS using testcontainers-modules (`nats` image).
///
/// **Suite lifecycle:** one server per process; refcounted teardown removes the container when
/// the last scenario completes. **JetStream is enabled** on the server so
/// [`crate::publication::NatsPublicationVerifier`] / [`crate::outbox::Outbox::assert_nats_published`]
/// can read persisted messages for scenario-namespaced subjects.
///
/// Default image tag is **`2`** (`nats:2`). Override with [`Self::with_image_tag`] or
/// `SHAKEDOWN_NATS_IMAGE_TAG`.
#[derive(Debug)]
pub struct NatsResource {
    image: Nats,
    image_tag: Option<String>,
    holds_suite_ref: bool,
    host: Option<String>,
    port: Option<u16>,
    connection_url: Option<String>,
    subject_prefix: String,
}

impl Default for NatsResource {
    fn default() -> Self {
        Self::new()
    }
}

impl NatsResource {
    /// Create default Docker-backed NATS settings (suite broker started on first provision).
    pub fn new() -> Self {
        Self {
            image: Nats::default(),
            image_tag: Some(DEFAULT_NATS_IMAGE_TAG.to_string()),
            holds_suite_ref: false,
            host: None,
            port: None,
            connection_url: None,
            subject_prefix: String::new(),
        }
        .with_image_tag_from_env()
    }

    /// Override the image tag (pin a patch release for production parity). Image name stays `nats`.
    pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
        self.image_tag = Some(tag.into());
        self
    }

    /// Apply tag override from `SHAKEDOWN_NATS_IMAGE_TAG` when set (non-empty).
    pub fn with_image_tag_from_env(mut self) -> Self {
        if let Ok(tag) = std::env::var("SHAKEDOWN_NATS_IMAGE_TAG") {
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

    fn connection_url_display(&self) -> Result<&str, InfrastructureError> {
        self.connection_url
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned("nats".into()))
    }
}

#[async_trait]
impl Resource for NatsResource {
    async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        if self.holds_suite_ref {
            return Err(InfrastructureError::AlreadyProvisioned("nats".into()));
        }

        self.subject_prefix = sanitize_nats_subject_prefix(scenario_id.as_str());
        let want_key = image_fingerprint(&self.image, &self.image_tag);

        let mut guard = suite_nats_mutex().lock().await;
        let (host, port) = match &mut *guard {
            SuiteNatsState::Idle => {
                let tag = self.image_tag.as_deref().unwrap_or(DEFAULT_NATS_IMAGE_TAG);
                let jetstream_cmd = NatsServerCmd::default().with_jetstream();
                let node = self
                    .image
                    .clone()
                    .with_tag(tag.to_string())
                    .with_cmd(&jetstream_cmd)
                    .with_label(
                        crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                        crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                    )
                    .start()
                    .await
                    .map_err(Self::map_tc_err)?;

                let mapped_port = node
                    .get_host_port_ipv4(NATS_CLIENT_PORT)
                    .await
                    .map_err(Self::map_tc_err)?;
                let host = "127.0.0.1".to_string();

                *guard = SuiteNatsState::Running {
                    host: host.clone(),
                    port: mapped_port,
                    container: node,
                    refs: 1,
                    image_key: want_key.clone(),
                };
                (host, mapped_port)
            }
            SuiteNatsState::Running {
                host,
                port,
                refs,
                image_key,
                ..
            } => {
                if *image_key != want_key {
                    return Err(InfrastructureError::Backend(Box::new(
                        std::io::Error::other(format!(
                            "NATS suite broker already running as {image_key}; \
                             cannot start or join as {want_key}. Use one image:tag per process."
                        )),
                    )));
                }
                *refs += 1;
                (host.clone(), *port)
            }
        };
        drop(guard);

        let connection_url = format!("nats://{host}:{port}");
        self.host = Some(host.clone());
        self.port = Some(port);
        self.connection_url = Some(connection_url.clone());
        self.holds_suite_ref = true;

        let subject_prefix = self.subject_prefix.clone();

        Ok(vec![ResourceHandle {
            id: format!("nats_{scenario_id}"),
            connection_string: connection_url.clone(),
            metadata: Some(serde_json::json!({
                "engine": "nats",
                "host": host,
                "port": port,
                "connection_url": connection_url,
                "subject_prefix": subject_prefix,
            })),
        }])
    }

    async fn check_ready(&self) -> Result<(), InfrastructureError> {
        let url = self.connection_url_display()?;
        const ATTEMPTS: u32 = 30;
        for attempt in 0..ATTEMPTS {
            match async_nats::connect(url).await {
                Ok(client) => match client.flush().await {
                    Ok(()) => return Ok(()),
                    Err(error) if attempt + 1 == ATTEMPTS => {
                        return Err(InfrastructureError::Readiness {
                            resource: "nats".into(),
                            source: Box::new(std::io::Error::other(error.to_string())),
                        });
                    }
                    Err(_) => {}
                },
                Err(error) if attempt + 1 == ATTEMPTS => {
                    return Err(InfrastructureError::Readiness {
                        resource: "nats".into(),
                        source: Box::new(std::io::Error::other(error.to_string())),
                    });
                }
                Err(_) => {}
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        Err(InfrastructureError::Readiness {
            resource: "nats".into(),
            source: Box::new(std::io::Error::other(
                "NATS readiness: connection attempts exhausted",
            )),
        })
    }

    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        if !self.holds_suite_ref {
            self.host = None;
            self.port = None;
            self.connection_url = None;
            self.subject_prefix.clear();
            return Ok(());
        }

        let mut guard = suite_nats_mutex().lock().await;
        let mut container_to_rm: Option<testcontainers::ContainerAsync<Nats>> = None;
        match &mut *guard {
            SuiteNatsState::Idle => {}
            SuiteNatsState::Running { refs, .. } => {
                *refs = refs.saturating_sub(1);
                if *refs == 0 {
                    let taken = std::mem::replace(&mut *guard, SuiteNatsState::Idle);
                    if let SuiteNatsState::Running { container, .. } = taken {
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
        self.connection_url = None;
        self.subject_prefix.clear();
        Ok(())
    }
}
