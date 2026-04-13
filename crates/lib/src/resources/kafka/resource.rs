use crate::error::{InfrastructureError, ScenarioId};
use crate::resources::user_seed::{take_user_seed, KafkaUserSeedFn, UserSeedSlot};
use crate::resources::{
    map_seed_error, sanitize_kafka_topic_prefix, KafkaBindings, Resource, ResourceHandle,
};
use async_trait::async_trait;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, DefaultConsumerContext};
use rdkafka::types::RDKafkaErrorCode;
use std::sync::OnceLock;
use std::time::Duration;
use testcontainers::core::Image;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, KAFKA_PORT};
use tokio::sync::Mutex;

/// Suite-wide broker: one Docker container per test process, shared by all [`KafkaResource`]
/// instances until the last holder tears down — shared across scenarios in the process to avoid repeated broker startup cost.
#[allow(clippy::large_enum_variant)] // Running holds ContainerAsync; boxing would complicate rm() handoff.
enum SuiteBrokerState {
    Idle,
    Running {
        bootstrap: String,
        container: testcontainers::ContainerAsync<apache::Kafka>,
        refs: usize,
        /// Effective `image:tag` for mismatch detection across concurrent scenarios.
        image_key: String,
    },
}

static SUITE_BROKER: OnceLock<Mutex<SuiteBrokerState>> = OnceLock::new();

fn suite_broker_mutex() -> &'static Mutex<SuiteBrokerState> {
    SUITE_BROKER.get_or_init(|| Mutex::new(SuiteBrokerState::Idle))
}

fn image_fingerprint(image: &apache::Kafka, tag_override: &Option<String>) -> String {
    let tag = tag_override.as_deref().unwrap_or_else(|| image.tag());
    format!("{}:{}", image.name(), tag)
}

/// Docker-backed Kafka using testcontainers-modules KRaft image (`apache/kafka-native` by default).
///
/// **Suite lifecycle:** the first provision in the process starts a single KRaft broker;
/// further [`KafkaResource`] instances reuse it. The last successful [`Resource::teardown`]
/// removes the container. All instances in one process must use the same image name and tag
/// (first configuration wins).
#[cfg_attr(docsrs, doc(cfg(feature = "kafka")))]
pub struct KafkaResource {
    image: apache::Kafka,
    /// Optional image tag override (image name stays from [`apache::Kafka`]).
    image_tag: Option<String>,
    /// `true` after this instance acquired a reference to the suite broker.
    holds_suite_ref: bool,
    bootstrap_servers: Option<String>,
    topic_prefix: String,
    user_seed: UserSeedSlot<KafkaBindings>,
}

impl std::fmt::Debug for KafkaResource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KafkaResource")
            .field("image", &self.image)
            .field("image_tag", &self.image_tag)
            .field("holds_suite_ref", &self.holds_suite_ref)
            .field("bootstrap_servers", &self.bootstrap_servers)
            .field("topic_prefix", &self.topic_prefix)
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

impl Default for KafkaResource {
    fn default() -> Self {
        Self::new()
    }
}

impl KafkaResource {
    /// Create default Docker-backed Kafka settings (suite broker started on first provision).
    pub fn new() -> Self {
        Self {
            image: apache::Kafka::default(),
            image_tag: None,
            holds_suite_ref: false,
            bootstrap_servers: None,
            topic_prefix: String::new(),
            user_seed: UserSeedSlot::new(None),
        }
    }

    /// Override the image tag (e.g. pin `3.8.0` for production parity). Name remains `apache/kafka-native` / JVM variant from [`apache::Kafka`].
    pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
        self.image_tag = Some(tag.into());
        self
    }

    /// Use JVM image (`apache/kafka`) instead of native.
    pub fn with_jvm_image(mut self) -> Self {
        self.image = apache::Kafka::default().with_jvm_image();
        self
    }

    /// Apply tag override from `SHAKEDOWN_KAFKA_IMAGE_TAG` when set (suite-level parity with CI/prod pins).
    pub fn with_image_tag_from_env(mut self) -> Self {
        if let Ok(tag) = std::env::var("SHAKEDOWN_KAFKA_IMAGE_TAG") {
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

    /// Optional user seed after the proof topic is created ([`Resource::apply_default_seed`]).
    pub fn set_seed(&mut self, seed: KafkaUserSeedFn) {
        *self.user_seed.lock().unwrap() = Some(seed);
    }

    fn kafka_bindings(&self) -> Result<KafkaBindings, InfrastructureError> {
        let bootstrap_servers = self
            .bootstrap_servers
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned("kafka".into()))?;
        Ok(KafkaBindings::new(
            bootstrap_servers,
            self.topic_prefix.as_str(),
        ))
    }
}

#[async_trait]
impl Resource for KafkaResource {
    async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        if self.holds_suite_ref {
            return Err(InfrastructureError::AlreadyProvisioned("kafka".into()));
        }

        self.topic_prefix = sanitize_kafka_topic_prefix(scenario_id.as_str());
        let want_key = image_fingerprint(&self.image, &self.image_tag);

        let mut guard = suite_broker_mutex().lock().await;
        let bootstrap = match &mut *guard {
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

                let port = node
                    .get_host_port_ipv4(KAFKA_PORT)
                    .await
                    .map_err(Self::map_tc_err)?;
                let bootstrap = format!("127.0.0.1:{port}");
                *guard = SuiteBrokerState::Running {
                    bootstrap: bootstrap.clone(),
                    container: node,
                    refs: 1,
                    image_key: want_key.clone(),
                };
                bootstrap
            }
            SuiteBrokerState::Running {
                bootstrap,
                refs,
                image_key,
                ..
            } => {
                if *image_key != want_key {
                    return Err(InfrastructureError::Backend(Box::new(
                        std::io::Error::other(format!(
                            "Kafka suite broker already running as {image_key}; \
                             cannot start or join as {want_key}. Use one image:tag per process."
                        )),
                    )));
                }
                *refs += 1;
                bootstrap.clone()
            }
        };
        drop(guard);

        self.bootstrap_servers = Some(bootstrap.clone());
        self.holds_suite_ref = true;

        Ok(vec![ResourceHandle {
            id: format!("kafka_{}", scenario_id),
            connection_string: bootstrap.clone(),
            metadata: Some(serde_json::json!({
                "bootstrap_servers": bootstrap,
                "topic_prefix": self.topic_prefix,
            })),
        }])
    }

    async fn check_ready(&self) -> Result<(), InfrastructureError> {
        let bootstrap = self
            .bootstrap_servers
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned("kafka".into()))?;

        let consumer: rdkafka::consumer::BaseConsumer<DefaultConsumerContext> = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .set("client.id", "shakedown_ready_probe")
            .set("socket.timeout.ms", "5000")
            .create()
            .map_err(|e| InfrastructureError::Readiness {
                resource: "kafka".into(),
                source: Box::new(std::io::Error::other(e.to_string())),
            })?;

        const ATTEMPTS: u32 = 30;
        for i in 0..ATTEMPTS {
            match consumer.fetch_metadata(None, Duration::from_secs(3)) {
                Ok(meta) if !meta.brokers().is_empty() => return Ok(()),
                Ok(_) => {}
                Err(e) => {
                    if i + 1 == ATTEMPTS {
                        return Err(InfrastructureError::Readiness {
                            resource: "kafka".into(),
                            source: Box::new(std::io::Error::other(e.to_string())),
                        });
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        Err(InfrastructureError::Readiness {
            resource: "kafka".into(),
            source: Box::new(std::io::Error::other(
                "Kafka metadata fetch did not succeed in time",
            )),
        })
    }

    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        let bootstrap = self
            .bootstrap_servers
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned("kafka".into()))?;

        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .set("client.id", "shakedown_seed")
            .create()
            .map_err(|e| {
                InfrastructureError::Backend(Box::new(std::io::Error::other(e.to_string())))
            })?;

        let proof_topic = format!("{}shakedown.proof", self.topic_prefix);
        let new_topics = [NewTopic::new(&proof_topic, 1, TopicReplication::Fixed(1))];

        let results = admin
            .create_topics(&new_topics, &AdminOptions::new())
            .await
            .map_err(|e| {
                InfrastructureError::Backend(Box::new(std::io::Error::other(e.to_string())))
            })?;

        for r in results {
            if let Err((_, code)) = r {
                if code != RDKafkaErrorCode::TopicAlreadyExists {
                    return Err(InfrastructureError::Backend(Box::new(
                        std::io::Error::other(format!("create topic: {code:?}")),
                    )));
                }
            }
        }

        if let Some(seed_fn) = take_user_seed(&self.user_seed) {
            let bindings = self.kafka_bindings()?;
            seed_fn(bindings).await.map_err(map_seed_error)?;
        }

        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        if !self.holds_suite_ref {
            self.bootstrap_servers = None;
            return Ok(());
        }

        let mut guard = suite_broker_mutex().lock().await;
        let mut container_to_rm: Option<testcontainers::ContainerAsync<apache::Kafka>> = None;
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
        self.bootstrap_servers = None;
        Ok(())
    }
}
