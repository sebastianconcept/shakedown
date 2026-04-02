//! Suite-shared multi-broker KRaft Kafka (`kafka-cluster` feature).
//!
//! Provisions `N` [`apache/kafka-native`](https://hub.docker.com/r/apache/kafka-native) brokers on a
//! [`crate::cluster::create_cluster_network`] bridge with hostnames `kafka-0` … `kafka-{N-1}`.
//! Clients on the Docker host use a comma-separated bootstrap list `127.0.0.1:p1,127.0.0.1:p2,…`.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use bollard::Docker;
use futures_util::future::try_join_all;
use tokio::sync::Mutex;
use tokio::time::timeout;
use uuid::Uuid;

use crate::cluster::{create_cluster_network, remove_cluster_network};
use crate::docker_labels::{SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE};
use crate::error::{InfrastructureError, ResourceHandle, ScenarioId};
use crate::resources::{sanitize_kafka_topic_prefix, Resource};

use testcontainers::core::ImageExt;
use testcontainers::core::{ContainerPort, ContainerState, ExecCommand, Image, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::KAFKA_PORT;

const KAFKA_NATIVE_IMAGE: &str = "apache/kafka-native";
const DEFAULT_IMAGE_TAG: &str = "3.8.0";
const START_SCRIPT: &str = "/opt/kafka/testcontainers_start.sh";
const DEFAULT_RF: usize = 1;
const PROVISION_TIMEOUT: Duration = Duration::from_secs(60);

#[allow(clippy::large_enum_variant)]
enum SuiteKafkaClusterState {
    Idle,
    Running {
        network_name: String,
        nodes: Vec<testcontainers::ContainerAsync<KraftBrokerImage>>,
        host_ports: Vec<u16>,
        refs: usize,
        image_key: String,
        node_count: usize,
    },
}

static SUITE_KAFKA_CLUSTER: OnceLock<Mutex<SuiteKafkaClusterState>> = OnceLock::new();

fn suite_kafka_cluster_mutex() -> &'static Mutex<SuiteKafkaClusterState> {
    SUITE_KAFKA_CLUSTER.get_or_init(|| Mutex::new(SuiteKafkaClusterState::Idle))
}

fn map_tc_err(error: impl std::fmt::Display + Send + Sync + 'static) -> InfrastructureError {
    InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
}

fn image_fingerprint(image_name: &str, tag: &str, node_count: usize) -> String {
    format!("{image_name}:{tag}:n{node_count}")
}

/// URL-safe base64 cluster id (16 bytes), matching `kafka-storage random-uuid` output shape.
fn random_kafka_cluster_id() -> String {
    let bytes = *Uuid::new_v4().as_bytes();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Controller quorum endpoints must use each broker’s **Docker DNS name** (container name on the
/// suite bridge). `hostname` alone is not registered for inter-container resolution.
fn build_quorum_voters(suite_id: &str, node_count: usize) -> String {
    (0..node_count)
        .map(|index| {
            format!(
                "{}@{}:9094",
                index + 1,
                broker_container_name(suite_id, index)
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn broker_container_name(suite_id: &str, index: usize) -> String {
    format!("shakedown-kafka-{suite_id}-{index}")
}

/// Per-broker [`Image`] — mirrors testcontainers-modules `kafka::apache::Kafka` startup (bash wait +
/// `exec_after_start` script) with KRaft quorum voters across hostnames.
#[derive(Debug, Clone)]
struct KraftBrokerImage {
    env_vars: HashMap<String, String>,
    image_name: String,
    tag: String,
    broker_dns_name: String,
}

impl KraftBrokerImage {
    fn new(
        broker_id: u32,
        cluster_id: &str,
        quorum_voters: &str,
        broker_dns_name: &str,
        image_name: &str,
        tag: &str,
    ) -> Self {
        let mut env_vars = HashMap::new();
        env_vars.insert(
            "KAFKA_LISTENERS".to_owned(),
            "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094".to_owned(),
        );
        env_vars.insert("CLUSTER_ID".to_owned(), cluster_id.to_owned());
        env_vars.insert(
            "KAFKA_PROCESS_ROLES".to_owned(),
            "broker,controller".to_owned(),
        );
        env_vars.insert(
            "KAFKA_CONTROLLER_LISTENER_NAMES".to_owned(),
            "CONTROLLER".to_owned(),
        );
        env_vars.insert(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP".to_owned(),
            "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT".to_owned(),
        );
        env_vars.insert(
            "KAFKA_INTER_BROKER_LISTENER_NAME".to_owned(),
            "BROKER".to_owned(),
        );
        env_vars.insert(
            "KAFKA_ADVERTISED_LISTENERS".to_owned(),
            format!("PLAINTEXT://127.0.0.1:9092,BROKER://{broker_dns_name}:9093"),
        );
        env_vars.insert("KAFKA_BROKER_ID".to_owned(), broker_id.to_string());
        env_vars.insert(
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR".to_owned(),
            DEFAULT_RF.to_string(),
        );
        env_vars.insert(
            "KAFKA_CONTROLLER_QUORUM_VOTERS".to_owned(),
            quorum_voters.to_owned(),
        );

        Self {
            env_vars,
            image_name: image_name.to_owned(),
            tag: tag.to_owned(),
            broker_dns_name: broker_dns_name.to_owned(),
        }
    }
}

impl Image for KraftBrokerImage {
    fn name(&self) -> &str {
        self.image_name.as_str()
    }

    fn tag(&self) -> &str {
        self.tag.as_str()
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        &self.env_vars
    }

    fn entrypoint(&self) -> Option<&str> {
        Some("bash")
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<Cow<'_, str>>> {
        vec![
            "-c".to_string(),
            format!(
                "while [ ! -f {START_SCRIPT}  ]; do sleep 0.1; done; chmod 755 {START_SCRIPT} && {START_SCRIPT}"
            ),
        ]
        .into_iter()
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[KAFKA_PORT]
    }

    fn exec_after_start(
        &self,
        container_state: ContainerState,
    ) -> Result<Vec<ExecCommand>, testcontainers::TestcontainersError> {
        let host_plaintext_port = container_state.host_port_ipv4(KAFKA_PORT)?;
        let cmd = vec![
            "sh".to_string(),
            "-c".to_string(),
                format!(
                "echo '#!/usr/bin/env bash\nexport KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:{host_plaintext_port},BROKER://{dns}:9093\n/etc/kafka/docker/run \n' > {START_SCRIPT}",
                dns = self.broker_dns_name,
            ),
        ];
        let ready_conditions = vec![WaitFor::message_on_stdout("Kafka Server started")];
        Ok(vec![
            ExecCommand::new(cmd).with_container_ready_conditions(ready_conditions)
        ])
    }
}

/// Docker-backed **KRaft Kafka cluster** (multiple brokers on a suite bridge network).
///
/// Default is **3** brokers. Image defaults to **`apache/kafka-native:3.8.0`**; override with
/// [`Self::with_image_tag`] or **`SHAKEDOWN_KAFKA_CLUSTER_IMAGE_TAG`**.
#[derive(Debug)]
pub struct KafkaClusterResource {
    node_count: usize,
    image_tag: Option<String>,
    use_jvm_image: bool,
    holds_suite_ref: bool,
    bootstrap_servers: Option<String>,
    topic_prefix: String,
}

impl Default for KafkaClusterResource {
    fn default() -> Self {
        Self::new()
    }
}

impl KafkaClusterResource {
    /// New cluster resource with **3** brokers.
    pub fn new() -> Self {
        Self {
            node_count: 3,
            image_tag: Some(DEFAULT_IMAGE_TAG.to_string()),
            use_jvm_image: false,
            holds_suite_ref: false,
            bootstrap_servers: None,
            topic_prefix: String::new(),
        }
    }

    /// Override broker count (minimum **1**).
    pub fn with_node_count(mut self, node_count: usize) -> Self {
        self.node_count = node_count.max(1);
        self
    }

    /// Override the image tag; name remains `apache/kafka-native` unless [`Self::with_jvm_image`] is used.
    pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
        self.image_tag = Some(tag.into());
        self
    }

    /// Use JVM image (`apache/kafka`) instead of native.
    pub fn with_jvm_image(mut self) -> Self {
        self.use_jvm_image = true;
        self
    }

    /// Apply tag from `SHAKEDOWN_KAFKA_CLUSTER_IMAGE_TAG` when set.
    pub fn with_image_tag_from_env(mut self) -> Self {
        if let Ok(tag) = std::env::var("SHAKEDOWN_KAFKA_CLUSTER_IMAGE_TAG") {
            let tag = tag.trim();
            if !tag.is_empty() {
                self.image_tag = Some(tag.to_string());
            }
        }
        self
    }
}

async fn wait_for_kafka_cluster_metadata(
    bootstrap_servers: &str,
    expected_brokers: usize,
) -> Result<(), InfrastructureError> {
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{Consumer, DefaultConsumerContext};

    let consumer: rdkafka::consumer::BaseConsumer<DefaultConsumerContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("client.id", "shakedown_kafka_cluster_ready")
        .set("socket.timeout.ms", "5000")
        .create()
        .map_err(|error| InfrastructureError::Readiness {
            resource: "kafka-cluster".into(),
            source: Box::new(std::io::Error::other(error.to_string())),
        })?;

    const ATTEMPTS: u32 = 90;
    for attempt in 0..ATTEMPTS {
        match consumer.fetch_metadata(None, Duration::from_secs(3)) {
            Ok(meta) => {
                let broker_count = meta.brokers().len();
                if broker_count >= expected_brokers && broker_count > 0 {
                    return Ok(());
                }
            }
            Err(error) => {
                if attempt + 1 == ATTEMPTS {
                    return Err(InfrastructureError::Readiness {
                        resource: "kafka-cluster".into(),
                        source: Box::new(std::io::Error::other(error.to_string())),
                    });
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }

    Err(InfrastructureError::Readiness {
        resource: "kafka-cluster".into(),
        source: Box::new(std::io::Error::other(
            "Kafka cluster metadata did not report expected brokers in time",
        )),
    })
}

async fn provision_suite_inner(
    want_key: &str,
    node_count: usize,
    image_name: &str,
    tag: &str,
) -> Result<String, InfrastructureError> {
    let harness_host = "127.0.0.1";

    let mut guard = suite_kafka_cluster_mutex().lock().await;
    let bootstrap = match &mut *guard {
        SuiteKafkaClusterState::Idle => {
            let cluster_id = random_kafka_cluster_id();
            let docker = Docker::connect_with_defaults().map_err(|error| {
                InfrastructureError::Provision {
                    resource: "kafka-cluster".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                }
            })?;

            let suite_id = format!("pid{}", std::process::id());
            let quorum_voters = build_quorum_voters(suite_id.as_str(), node_count);
            let network_name = create_cluster_network(&docker, "kafka", suite_id.as_str())
                .await
                .map_err(|error| InfrastructureError::Provision {
                    resource: "kafka-cluster".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;

            // Start every broker concurrently: sequential startup deadlocks KRaft — broker 0 waits for
            // "Kafka Server started" while brokers 1..N are not yet on the network to join the quorum.
            let nodes: Vec<testcontainers::ContainerAsync<KraftBrokerImage>> =
                try_join_all((0..node_count).map(|index| {
                    let dns_name = broker_container_name(suite_id.as_str(), index);
                    let broker_id = (index + 1) as u32;
                    let image = KraftBrokerImage::new(
                        broker_id,
                        cluster_id.as_str(),
                        quorum_voters.as_str(),
                        dns_name.as_str(),
                        image_name,
                        tag,
                    );
                    let network_name = network_name.clone();
                    async move {
                        image
                            .with_network(network_name)
                            .with_container_name(dns_name.as_str())
                            .with_label(SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE)
                            .start()
                            .await
                            .map_err(map_tc_err)
                    }
                }))
                .await?;

            let mut host_ports = Vec::with_capacity(node_count);
            for node in &nodes {
                let mapped = node
                    .get_host_port_ipv4(KAFKA_PORT)
                    .await
                    .map_err(map_tc_err)?;
                host_ports.push(mapped);
            }

            let bootstrap = host_ports
                .iter()
                .map(|port| format!("{harness_host}:{port}"))
                .collect::<Vec<_>>()
                .join(",");

            timeout(
                Duration::from_secs(45),
                wait_for_kafka_cluster_metadata(bootstrap.as_str(), node_count),
            )
            .await
            .map_err(|_| InfrastructureError::Provision {
                resource: "kafka-cluster".into(),
                source: Box::new(std::io::Error::other(
                    "timed out waiting for Kafka cluster metadata (controller quorum)",
                )),
            })??;

            *guard = SuiteKafkaClusterState::Running {
                network_name,
                nodes,
                host_ports: host_ports.clone(),
                refs: 1,
                image_key: want_key.to_string(),
                node_count,
            };
            bootstrap
        }
        SuiteKafkaClusterState::Running {
            host_ports,
            refs,
            image_key,
            node_count: existing_count,
            ..
        } => {
            if *image_key != want_key {
                return Err(InfrastructureError::Backend(Box::new(
                    std::io::Error::other(format!(
                        "Kafka cluster suite already running as {image_key}; \
                         cannot start or join as {want_key}. Use one image:tag:node_count per process."
                    )),
                )));
            }
            if *existing_count != node_count {
                return Err(InfrastructureError::Backend(Box::new(
                    std::io::Error::other(format!(
                        "Kafka cluster suite already running with {existing_count} brokers; \
                         requested {node_count}. Use one node count per process."
                    )),
                )));
            }
            *refs += 1;
            host_ports
                .iter()
                .map(|port| format!("{harness_host}:{port}"))
                .collect::<Vec<_>>()
                .join(",")
        }
    };
    drop(guard);
    Ok(bootstrap)
}

#[async_trait]
impl Resource for KafkaClusterResource {
    async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        if self.holds_suite_ref {
            return Err(InfrastructureError::AlreadyProvisioned(
                "kafka-cluster".into(),
            ));
        }

        self.topic_prefix = sanitize_kafka_topic_prefix(scenario_id.as_str());
        let tag = self.image_tag.as_deref().unwrap_or(DEFAULT_IMAGE_TAG);
        let image_name = if self.use_jvm_image {
            "apache/kafka"
        } else {
            KAFKA_NATIVE_IMAGE
        };
        let want_key = image_fingerprint(image_name, tag, self.node_count);
        let node_count = self.node_count;

        let bootstrap = timeout(
            PROVISION_TIMEOUT,
            provision_suite_inner(want_key.as_str(), node_count, image_name, tag),
        )
        .await
        .map_err(|_| InfrastructureError::Provision {
            resource: "kafka-cluster".into(),
            source: Box::new(std::io::Error::other(format!(
                "Kafka cluster provisioning exceeded {:?}",
                PROVISION_TIMEOUT
            ))),
        })??;

        self.bootstrap_servers = Some(bootstrap.clone());
        self.holds_suite_ref = true;

        Ok(vec![ResourceHandle {
            id: format!("kafka_cluster_{scenario_id}"),
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
            .ok_or_else(|| InfrastructureError::NotProvisioned("kafka-cluster".into()))?;
        wait_for_kafka_cluster_metadata(bootstrap, self.node_count).await
    }

    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        let bootstrap = self
            .bootstrap_servers
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned("kafka-cluster".into()))?;

        use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
        use rdkafka::client::DefaultClientContext;
        use rdkafka::config::ClientConfig;
        use rdkafka::types::RDKafkaErrorCode;

        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .set("client.id", "shakedown_kafka_cluster_seed")
            .create()
            .map_err(|error| {
                InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
            })?;

        let proof_topic = format!("{}shakedown.proof", self.topic_prefix);
        let new_topics = [NewTopic::new(&proof_topic, 1, TopicReplication::Fixed(1))];

        let results = admin
            .create_topics(&new_topics, &AdminOptions::new())
            .await
            .map_err(|error| {
                InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
            })?;

        for result in results {
            if let Err((_, code)) = result {
                if code != RDKafkaErrorCode::TopicAlreadyExists {
                    return Err(InfrastructureError::Backend(Box::new(
                        std::io::Error::other(format!("create topic: {code:?}")),
                    )));
                }
            }
        }

        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        if !self.holds_suite_ref {
            self.bootstrap_servers = None;
            return Ok(());
        }

        let mut guard = suite_kafka_cluster_mutex().lock().await;
        let mut teardown: Option<(
            String,
            Vec<testcontainers::ContainerAsync<KraftBrokerImage>>,
        )> = None;
        match &mut *guard {
            SuiteKafkaClusterState::Idle => {}
            SuiteKafkaClusterState::Running { refs, .. } => {
                *refs = refs.saturating_sub(1);
                if *refs == 0 {
                    let taken = std::mem::replace(&mut *guard, SuiteKafkaClusterState::Idle);
                    if let SuiteKafkaClusterState::Running {
                        network_name,
                        nodes,
                        ..
                    } = taken
                    {
                        teardown = Some((network_name, nodes));
                    }
                }
            }
        }
        drop(guard);

        if let Some((network_name, nodes)) = teardown {
            let docker =
                Docker::connect_with_defaults().map_err(|error| InfrastructureError::Teardown {
                    resource: "kafka-cluster".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;
            for node in nodes {
                let _ = node.stop_with_timeout(Some(30)).await;
                let _ = node.rm().await;
            }
            remove_cluster_network(&docker, network_name.as_str())
                .await
                .map_err(|error| InfrastructureError::Teardown {
                    resource: "kafka-cluster".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;
        }

        self.holds_suite_ref = false;
        self.bootstrap_servers = None;
        Ok(())
    }
}
