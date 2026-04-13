use crate::error::{InfrastructureError, ScenarioId};
use crate::resources::user_seed::{take_user_seed, RabbitUserSeedFn, UserSeedSlot};
use crate::resources::{
    map_seed_error, sanitize_rabbit_resource_prefix, RabbitBindings, Resource, ResourceHandle,
};
use async_trait::async_trait;
use lapin::{types::ShortString, Connection, ConnectionProperties};
use std::sync::OnceLock;
use testcontainers::core::Image;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;
use tokio::sync::Mutex;

const RABBITMQ_AMQP_PORT: u16 = 5672;

#[allow(clippy::large_enum_variant)]
enum SuiteBrokerState {
    Idle,
    Running {
        host: String,
        port: u16,
        container: testcontainers::ContainerAsync<RabbitMq>,
        refs: usize,
        image_key: String,
    },
}

static SUITE_BROKER: OnceLock<Mutex<SuiteBrokerState>> = OnceLock::new();

fn suite_broker_mutex() -> &'static Mutex<SuiteBrokerState> {
    SUITE_BROKER.get_or_init(|| Mutex::new(SuiteBrokerState::Idle))
}

fn image_fingerprint(image: &RabbitMq, tag_override: &Option<String>) -> String {
    let tag = tag_override.as_deref().unwrap_or_else(|| image.tag());
    format!("{}:{}", image.name(), tag)
}

/// Docker-backed RabbitMQ using the official image via testcontainers-modules.
///
/// **Suite lifecycle:** the first provision in the process starts a single broker; further
/// [`RabbitResource`] instances reuse it. The last successful [`Resource::teardown`] removes the
/// container when the refcount reaches zero.
#[cfg_attr(docsrs, doc(cfg(feature = "rabbitmq")))]
pub struct RabbitResource {
    image: RabbitMq,
    image_tag: Option<String>,
    holds_suite_ref: bool,
    host: Option<String>,
    port: Option<u16>,
    username: String,
    password: String,
    vhost: String,
    amqp_uri: Option<String>,
    resource_prefix: String,
    user_seed: UserSeedSlot<RabbitBindings>,
}

impl std::fmt::Debug for RabbitResource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RabbitResource")
            .field("image", &self.image)
            .field("image_tag", &self.image_tag)
            .field("holds_suite_ref", &self.holds_suite_ref)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("vhost", &self.vhost)
            .field("amqp_uri", &self.amqp_uri.as_ref().map(|_| "<uri>"))
            .field("resource_prefix", &self.resource_prefix)
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

impl Default for RabbitResource {
    fn default() -> Self {
        Self::new()
    }
}

impl RabbitResource {
    /// Create default Docker-backed RabbitMQ settings (suite broker started on first provision).
    pub fn new() -> Self {
        Self {
            image: RabbitMq::default(),
            image_tag: None,
            holds_suite_ref: false,
            host: None,
            port: None,
            username: "guest".to_string(),
            password: "guest".to_string(),
            vhost: "/".to_string(),
            amqp_uri: None,
            resource_prefix: String::new(),
            user_seed: UserSeedSlot::new(None),
        }
    }

    /// Override the image tag (e.g. pin a management image tag for parity). Name stays `rabbitmq`.
    pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
        self.image_tag = Some(tag.into());
        self
    }

    /// Apply tag override from `SHAKEDOWN_RABBITMQ_IMAGE_TAG` when set.
    pub fn with_image_tag_from_env(mut self) -> Self {
        if let Ok(tag) = std::env::var("SHAKEDOWN_RABBITMQ_IMAGE_TAG") {
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

    fn build_amqp_uri(host: &str, port: u16, username: &str, password: &str) -> String {
        format!("amqp://{username}:{password}@{host}:{port}")
    }

    /// Async seed closure receiving [`RabbitBindings`] ([`Resource::apply_default_seed`]).
    pub fn set_seed(&mut self, seed: RabbitUserSeedFn) {
        *self.user_seed.lock().unwrap() = Some(seed);
    }

    fn rabbit_bindings(&self) -> Result<RabbitBindings, InfrastructureError> {
        let host = self
            .host
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned("rabbitmq".into()))?;
        let port = self
            .port
            .ok_or_else(|| InfrastructureError::NotProvisioned("rabbitmq".into()))?;
        let amqp_uri = self
            .amqp_uri
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned("rabbitmq".into()))?;
        Ok(RabbitBindings::new(
            amqp_uri,
            host,
            port,
            self.username.as_str(),
            self.password.as_str(),
            self.vhost.as_str(),
            self.resource_prefix.as_str(),
        ))
    }
}

#[async_trait]
impl Resource for RabbitResource {
    async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        if self.holds_suite_ref {
            return Err(InfrastructureError::AlreadyProvisioned("rabbitmq".into()));
        }

        self.resource_prefix = sanitize_rabbit_resource_prefix(scenario_id.as_str());
        let want_key = image_fingerprint(&self.image, &self.image_tag);

        let mut guard = suite_broker_mutex().lock().await;
        let (host, port) = match &mut *guard {
            SuiteBrokerState::Idle => {
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
                    .get_host_port_ipv4(RABBITMQ_AMQP_PORT)
                    .await
                    .map_err(Self::map_tc_err)?;
                let host = "127.0.0.1".to_string();
                *guard = SuiteBrokerState::Running {
                    host: host.clone(),
                    port: mapped_port,
                    container: node,
                    refs: 1,
                    image_key: want_key.clone(),
                };
                (host, mapped_port)
            }
            SuiteBrokerState::Running {
                host,
                port,
                refs,
                image_key,
                ..
            } => {
                if *image_key != want_key {
                    return Err(InfrastructureError::Backend(Box::new(
                        std::io::Error::other(format!(
                            "RabbitMQ suite broker already running as {image_key}; \
                             cannot start or join as {want_key}. Use one image:tag per process."
                        )),
                    )));
                }
                *refs += 1;
                (host.clone(), *port)
            }
        };
        drop(guard);

        let amqp_uri = Self::build_amqp_uri(&host, port, &self.username, &self.password);
        self.host = Some(host.clone());
        self.port = Some(port);
        self.amqp_uri = Some(amqp_uri.clone());
        self.holds_suite_ref = true;

        Ok(vec![ResourceHandle {
            id: format!("rabbitmq_{scenario_id}"),
            connection_string: amqp_uri.clone(),
            metadata: Some(serde_json::json!({
                "engine": "rabbitmq",
                "amqp_uri": amqp_uri,
                "host": host,
                "port": port,
                "username": self.username,
                "password": self.password,
                "vhost": self.vhost,
                "resource_prefix": self.resource_prefix,
            })),
        }])
    }

    async fn check_ready(&self) -> Result<(), InfrastructureError> {
        let uri = self
            .amqp_uri
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned("rabbitmq".into()))?;

        const ATTEMPTS: u32 = 30;
        for attempt in 0..ATTEMPTS {
            match Connection::connect(uri, ConnectionProperties::default()).await {
                Ok(conn) => match conn.create_channel().await {
                    Ok(_) => {
                        let _ = conn.close(200, ShortString::from("shakedown_ready")).await;
                        return Ok(());
                    }
                    Err(error) if attempt + 1 == ATTEMPTS => {
                        return Err(InfrastructureError::Readiness {
                            resource: "rabbitmq".into(),
                            source: Box::new(std::io::Error::other(error.to_string())),
                        });
                    }
                    Err(_) => {}
                },
                Err(error) if attempt + 1 == ATTEMPTS => {
                    return Err(InfrastructureError::Readiness {
                        resource: "rabbitmq".into(),
                        source: Box::new(std::io::Error::other(error.to_string())),
                    });
                }
                Err(_) => {}
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }

        Err(InfrastructureError::Readiness {
            resource: "rabbitmq".into(),
            source: Box::new(std::io::Error::other(
                "RabbitMQ AMQP connection did not succeed in time",
            )),
        })
    }

    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        if let Some(seed_fn) = take_user_seed(&self.user_seed) {
            let bindings = self.rabbit_bindings()?;
            seed_fn(bindings).await.map_err(map_seed_error)?;
        }
        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        if !self.holds_suite_ref {
            self.host = None;
            self.port = None;
            self.amqp_uri = None;
            self.resource_prefix.clear();
            return Ok(());
        }

        let mut guard = suite_broker_mutex().lock().await;
        let mut container_to_rm: Option<testcontainers::ContainerAsync<RabbitMq>> = None;
        match &mut *guard {
            SuiteBrokerState::Idle => {}
            SuiteBrokerState::Running { refs, .. } => {
                *refs = refs.saturating_sub(1);
                if *refs == 0 {
                    let taken = std::mem::replace(&mut *guard, SuiteBrokerState::Idle);
                    if let SuiteBrokerState::Running { container, .. } = taken {
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
        self.amqp_uri = None;
        self.resource_prefix.clear();
        Ok(())
    }
}
