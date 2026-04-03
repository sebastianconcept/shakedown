//! Suite-shared RabbitMQ Erlang cluster (`rabbitmq-cluster` feature).
//!
//! Provisions **N** [`rabbitmq`](https://hub.docker.com/_/rabbitmq) nodes on a
//! [`crate::cluster::create_cluster_network`] bridge, joins them with `rabbitmqctl join_cluster`,
//! and exposes a **multi-host** AMQP URI on `127.0.0.1` mapped ports for the harness process.

use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

use async_trait::async_trait;
use bollard::Docker;
use futures_util::future::try_join_all;
use lapin::{types::ShortString, Connection, ConnectionProperties};
use tokio::sync::Mutex;
use tokio::time::timeout;
use uuid::Uuid;

use crate::cluster::{create_cluster_network, remove_cluster_network};
use crate::docker_labels::{SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE};
use crate::error::{InfrastructureError, ResourceHandle, ScenarioId};
use crate::resources::{sanitize_rabbit_resource_prefix, Resource};

use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const RABBIT_AMQP_PORT: u16 = 5672;
/// RabbitMQ management plugin / HTTP API (queue details, quorum leader election).
const RABBIT_MGMT_PORT: u16 = 15672;
/// **`rabbitmq:<tag>-management`** so the management API is available for quorum leadership queries.
const DEFAULT_IMAGE_TAG: &str = "4.0-management";
const DEFAULT_NODE_COUNT: usize = 3;
const PROVISION_TIMEOUT: Duration = Duration::from_secs(120);

#[allow(clippy::large_enum_variant)]
enum SuiteRabbitClusterState {
    Idle,
    Running {
        network_name: String,
        nodes: Vec<testcontainers::ContainerAsync<GenericImage>>,
        host_ports: Vec<u16>,
        /// Mapped host ports for `15672` (management API), aligned with `nodes` / `host_ports`.
        management_host_ports: Vec<u16>,
        /// Docker hostnames for each node (`shakedown-rabbit-{suite}-{i}`), for mapping Raft leaders.
        hostnames: Vec<String>,
        refs: usize,
        image_key: String,
        node_count: usize,
        /// Host `rabbitmq.conf` drop-in; removed when the suite tears down.
        queue_type_conf_path: PathBuf,
    },
}

static SUITE_RABBIT_CLUSTER: OnceLock<Mutex<SuiteRabbitClusterState>> = OnceLock::new();

fn suite_rabbit_cluster_mutex() -> &'static Mutex<SuiteRabbitClusterState> {
    SUITE_RABBIT_CLUSTER.get_or_init(|| Mutex::new(SuiteRabbitClusterState::Idle))
}

fn map_tc_err(error: impl std::fmt::Display + Send + Sync + 'static) -> InfrastructureError {
    InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
}

fn image_fingerprint(tag: &str, node_count: usize) -> String {
    format!("rabbitmq:{tag}:n{node_count}")
}

fn rabbit_hostname(suite_id: &str, index: usize) -> String {
    format!("shakedown-rabbit-{suite_id}-{index}")
}

fn disc_node_name(suite_id: &str) -> String {
    format!("rabbit@{}", rabbit_hostname(suite_id, 0))
}

/// Write `default_queue_type = quorum` for the official `rabbitmq` image (`conf.d` drop-in).
fn default_queue_type_conf_path(suite_id: &str) -> Result<PathBuf, InfrastructureError> {
    let path = std::env::temp_dir().join(format!(
        "shakedown-rabbitmq-default-queue-type-{suite_id}.conf"
    ));
    std::fs::write(path.as_path(), b"default_queue_type = quorum\n").map_err(|error| {
        InfrastructureError::Provision {
            resource: "rabbitmq-cluster".into(),
            source: Box::new(std::io::Error::other(format!(
                "write RabbitMQ default queue type conf: {error}"
            ))),
        }
    })?;
    Ok(path)
}

fn build_multi_host_amqp_uri(
    username: &str,
    password: &str,
    harness_host: &str,
    ports: &[u16],
) -> String {
    let hosts = ports
        .iter()
        .map(|port| format!("{harness_host}:{port}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("amqp://{username}:{password}@{hosts}/%2f")
}

async fn wait_amqp_uri_ready(uri: &str, resource_label: &str) -> Result<(), InfrastructureError> {
    const ATTEMPTS: u32 = 90;
    for attempt in 0..ATTEMPTS {
        match Connection::connect(uri, ConnectionProperties::default()).await {
            Ok(conn) => match conn.create_channel().await {
                Ok(_) => {
                    let _ = conn.close(200, ShortString::from("shakedown_ready")).await;
                    return Ok(());
                }
                Err(_) if attempt + 1 == ATTEMPTS => {
                    return Err(InfrastructureError::Readiness {
                        resource: resource_label.to_string(),
                        source: Box::new(std::io::Error::other(
                            "RabbitMQ AMQP channel did not open in time",
                        )),
                    });
                }
                Err(_) => {}
            },
            Err(error) if attempt + 1 == ATTEMPTS => {
                return Err(InfrastructureError::Readiness {
                    resource: resource_label.to_string(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                });
            }
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    Err(InfrastructureError::Readiness {
        resource: resource_label.to_string(),
        source: Box::new(std::io::Error::other(
            "RabbitMQ AMQP connection did not succeed in time",
        )),
    })
}

async fn wait_management_api_ready(port: u16) -> Result<(), InfrastructureError> {
    let url = format!("http://127.0.0.1:{port}/api/overview");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|error| InfrastructureError::Readiness {
            resource: "rabbitmq-cluster".into(),
            source: Box::new(std::io::Error::other(error.to_string())),
        })?;
    for _ in 0..90u32 {
        match client
            .get(&url)
            .basic_auth("guest", Some("guest"))
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => return Ok(()),
            _ => {}
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    Err(InfrastructureError::Readiness {
        resource: "rabbitmq-cluster".into(),
        source: Box::new(std::io::Error::other(
            "RabbitMQ management API did not become ready in time",
        )),
    })
}

/// Run `docker exec <container> rabbitmqctl …` (avoids testcontainers `ExecResult` `Send` issues on async).
async fn rabbitmqctl_exec(
    container_id: &str,
    arguments: &[&str],
) -> Result<(), InfrastructureError> {
    let mut command = tokio::process::Command::new("docker");
    command.arg("exec").arg(container_id).arg("rabbitmqctl");
    for argument in arguments {
        command.arg(argument);
    }
    let output = command
        .output()
        .await
        .map_err(|error| InfrastructureError::Provision {
            resource: "rabbitmq-cluster".into(),
            source: Box::new(std::io::Error::other(error.to_string())),
        })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(InfrastructureError::Provision {
            resource: "rabbitmq-cluster".into(),
            source: Box::new(std::io::Error::other(format!(
                "rabbitmqctl failed (status {:?}): stdout={stdout} stderr={stderr}",
                output.status.code()
            ))),
        });
    }
    Ok(())
}

async fn join_follower_to_cluster(
    follower: &testcontainers::ContainerAsync<GenericImage>,
    disc_node: &str,
) -> Result<(), InfrastructureError> {
    let container_id = follower.id();
    rabbitmqctl_exec(container_id, &["stop_app"]).await?;
    rabbitmqctl_exec(container_id, &["reset"]).await?;
    rabbitmqctl_exec(container_id, &["join_cluster", disc_node]).await?;
    rabbitmqctl_exec(container_id, &["start_app"]).await?;
    Ok(())
}

async fn verify_cluster_formed(
    first_node: &testcontainers::ContainerAsync<GenericImage>,
) -> Result<(), InfrastructureError> {
    rabbitmqctl_exec(first_node.id(), &["cluster_status"]).await
}

/// Docker-backed **RabbitMQ cluster** (Erlang clustering on a suite bridge; quorum default queue type).
///
/// Default is **3** nodes. Image defaults to **`rabbitmq:4.0-management`**; override with
/// [`Self::with_image_tag`] or **`SHAKEDOWN_RABBITMQ_CLUSTER_IMAGE_TAG`**.
#[derive(Debug)]
pub struct RabbitClusterResource {
    node_count: usize,
    image_tag: Option<String>,
    holds_suite_ref: bool,
    amqp_uri: Option<String>,
    resource_prefix: String,
    username: String,
    password: String,
}

impl Default for RabbitClusterResource {
    fn default() -> Self {
        Self::new()
    }
}

impl RabbitClusterResource {
    /// New cluster resource with **3** nodes.
    pub fn new() -> Self {
        Self {
            node_count: DEFAULT_NODE_COUNT,
            image_tag: Some(DEFAULT_IMAGE_TAG.to_string()),
            holds_suite_ref: false,
            amqp_uri: None,
            resource_prefix: String::new(),
            username: "guest".to_string(),
            password: "guest".to_string(),
        }
    }

    /// Override node count (minimum **2** for a cluster; defaults to **3**).
    pub fn with_node_count(mut self, node_count: usize) -> Self {
        self.node_count = node_count.max(2);
        self
    }

    /// Override the image tag; repository stays `rabbitmq` (management variant recommended).
    pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
        self.image_tag = Some(tag.into());
        self
    }

    /// Apply tag from `SHAKEDOWN_RABBITMQ_CLUSTER_IMAGE_TAG` when set.
    pub fn with_image_tag_from_env(mut self) -> Self {
        if let Ok(tag) = std::env::var("SHAKEDOWN_RABBITMQ_CLUSTER_IMAGE_TAG") {
            let tag = tag.trim();
            if !tag.is_empty() {
                self.image_tag = Some(tag.to_string());
            }
        }
        self
    }
}

async fn provision_suite_inner(
    want_key: &str,
    tag: &str,
    node_count: usize,
) -> Result<(String, String, u16), InfrastructureError> {
    let harness_host = "127.0.0.1";
    let username = "guest";
    let password = "guest";

    let mut guard = suite_rabbit_cluster_mutex().lock().await;
    let (single_amqp_uri, multi_connection_url, first_port) = match &mut *guard {
        SuiteRabbitClusterState::Idle => {
            let docker = Docker::connect_with_defaults().map_err(|error| {
                InfrastructureError::Provision {
                    resource: "rabbitmq-cluster".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                }
            })?;

            let suite_id = format!("pid{}", std::process::id());
            let network_name = create_cluster_network(&docker, "rabbitmq", suite_id.as_str())
                .await
                .map_err(|error| InfrastructureError::Provision {
                    resource: "rabbitmq-cluster".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;

            let cookie = Uuid::new_v4().as_simple().to_string();
            let disc = disc_node_name(suite_id.as_str());
            let queue_type_conf_path = default_queue_type_conf_path(suite_id.as_str())?;
            let queue_type_conf_host = queue_type_conf_path.display().to_string();

            // Start the seed node first so we do not spike RAM with N concurrent broker boots (OOM risk on CI).
            let hostname0 = rabbit_hostname(suite_id.as_str(), 0);
            let nodename0 = format!("rabbit@{hostname0}");
            let start0 = GenericImage::new("rabbitmq", tag)
                .with_exposed_port(RABBIT_AMQP_PORT.tcp())
                .with_exposed_port(RABBIT_MGMT_PORT.tcp())
                .with_wait_for(WaitFor::seconds(2))
                .with_env_var("RABBITMQ_ERLANG_COOKIE", cookie.as_str())
                .with_env_var("RABBITMQ_NODENAME", nodename0.as_str())
                .with_mount(Mount::bind_mount(
                    queue_type_conf_host.as_str(),
                    "/etc/rabbitmq/conf.d/20-shakedown-default-queue-type.conf",
                ))
                .with_network(network_name.clone())
                .with_container_name(hostname0.as_str())
                .with_hostname(hostname0.as_str())
                .with_label(SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE);
            let node0 = start0.start().await.map_err(map_tc_err)?;
            let port0 = node0
                .get_host_port_ipv4(RABBIT_AMQP_PORT)
                .await
                .map_err(map_tc_err)?;
            let mgmt0 = node0
                .get_host_port_ipv4(RABBIT_MGMT_PORT)
                .await
                .map_err(map_tc_err)?;
            let single0 = format!("amqp://{username}:{password}@{harness_host}:{port0}/%2f");
            wait_amqp_uri_ready(single0.as_str(), "rabbitmq-cluster").await?;

            let mut nodes = vec![node0];
            let mut host_ports = vec![port0];
            let mut management_host_ports = vec![mgmt0];

            if node_count > 1 {
                let start_futures = (1..node_count).map(|index| {
                    let network_name = network_name.clone();
                    let suite_id = suite_id.clone();
                    let cookie = cookie.clone();
                    let tag = tag.to_string();
                    let queue_type_conf_host = queue_type_conf_host.clone();
                    async move {
                        let hostname = rabbit_hostname(suite_id.as_str(), index);
                        let nodename = format!("rabbit@{hostname}");
                        let start_image = GenericImage::new("rabbitmq", tag.as_str())
                            .with_exposed_port(RABBIT_AMQP_PORT.tcp())
                            .with_exposed_port(RABBIT_MGMT_PORT.tcp())
                            .with_wait_for(WaitFor::seconds(2))
                            .with_env_var("RABBITMQ_ERLANG_COOKIE", cookie.as_str())
                            .with_env_var("RABBITMQ_NODENAME", nodename.as_str())
                            .with_mount(Mount::bind_mount(
                                queue_type_conf_host.as_str(),
                                "/etc/rabbitmq/conf.d/20-shakedown-default-queue-type.conf",
                            ))
                            .with_network(network_name)
                            .with_container_name(hostname.as_str())
                            .with_hostname(hostname.as_str())
                            .with_label(SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE);
                        start_image.start().await.map_err(map_tc_err)
                    }
                });
                let extra: Vec<_> = try_join_all(start_futures).await?;
                for node in extra {
                    let mapped = node
                        .get_host_port_ipv4(RABBIT_AMQP_PORT)
                        .await
                        .map_err(map_tc_err)?;
                    let mgmt_mapped = node
                        .get_host_port_ipv4(RABBIT_MGMT_PORT)
                        .await
                        .map_err(map_tc_err)?;
                    host_ports.push(mapped);
                    management_host_ports.push(mgmt_mapped);
                    nodes.push(node);
                }
            }

            for port in host_ports.iter() {
                let single = format!("amqp://{username}:{password}@{harness_host}:{port}/%2f");
                wait_amqp_uri_ready(single.as_str(), "rabbitmq-cluster").await?;
            }

            for management_port in management_host_ports.iter() {
                wait_management_api_ready(*management_port).await?;
            }

            let hostnames: Vec<String> = (0..node_count)
                .map(|index| rabbit_hostname(suite_id.as_str(), index))
                .collect();

            for follower in nodes.iter().skip(1) {
                join_follower_to_cluster(follower, disc.as_str()).await?;
            }

            verify_cluster_formed(&nodes[0]).await?;

            let multi_connection_url =
                build_multi_host_amqp_uri(username, password, harness_host, host_ports.as_slice());
            let single_amqp_uri = format!(
                "amqp://{username}:{password}@{harness_host}:{}/%2f",
                host_ports[0]
            );
            wait_amqp_uri_ready(single_amqp_uri.as_str(), "rabbitmq-cluster").await?;

            *guard = SuiteRabbitClusterState::Running {
                network_name,
                nodes,
                host_ports: host_ports.clone(),
                management_host_ports: management_host_ports.clone(),
                hostnames,
                refs: 1,
                image_key: want_key.to_string(),
                node_count,
                queue_type_conf_path,
            };
            let first_port = host_ports[0];
            (single_amqp_uri, multi_connection_url, first_port)
        }
        SuiteRabbitClusterState::Running {
            host_ports,
            refs,
            image_key,
            node_count: existing_count,
            ..
        } => {
            if *image_key != want_key {
                return Err(InfrastructureError::Backend(Box::new(
                    std::io::Error::other(format!(
                        "RabbitMQ cluster suite already running as {image_key}; \
                         cannot start or join as {want_key}."
                    )),
                )));
            }
            if *existing_count != node_count {
                return Err(InfrastructureError::Backend(Box::new(
                    std::io::Error::other(format!(
                        "RabbitMQ cluster suite already running with {existing_count} nodes; \
                         requested {node_count}."
                    )),
                )));
            }
            *refs += 1;
            let first_port = host_ports.first().copied().ok_or_else(|| {
                InfrastructureError::Backend(Box::new(std::io::Error::other(
                    "RabbitMQ cluster suite missing mapped port",
                )))
            })?;
            let multi_connection_url =
                build_multi_host_amqp_uri(username, password, harness_host, host_ports.as_slice());
            let single_amqp_uri =
                format!("amqp://{username}:{password}@{harness_host}:{first_port}/%2f");
            (single_amqp_uri, multi_connection_url, first_port)
        }
    };
    drop(guard);
    Ok((single_amqp_uri, multi_connection_url, first_port))
}

#[async_trait]
impl Resource for RabbitClusterResource {
    async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        if self.holds_suite_ref {
            return Err(InfrastructureError::AlreadyProvisioned(
                "rabbitmq-cluster".into(),
            ));
        }

        self.resource_prefix = sanitize_rabbit_resource_prefix(scenario_id.as_str());
        let tag = self.image_tag.as_deref().unwrap_or(DEFAULT_IMAGE_TAG);
        let want_key = image_fingerprint(tag, self.node_count);
        let node_count = self.node_count;

        let (single_amqp_uri, multi_connection_url, first_port) = timeout(
            PROVISION_TIMEOUT,
            provision_suite_inner(want_key.as_str(), tag, node_count),
        )
        .await
        .map_err(|_| InfrastructureError::Provision {
            resource: "rabbitmq-cluster".into(),
            source: Box::new(std::io::Error::other(format!(
                "RabbitMQ cluster provisioning exceeded {:?}",
                PROVISION_TIMEOUT
            ))),
        })??;

        self.amqp_uri = Some(single_amqp_uri.clone());
        self.holds_suite_ref = true;

        Ok(vec![ResourceHandle {
            id: format!("rabbitmq_cluster_{scenario_id}"),
            connection_string: multi_connection_url.clone(),
            metadata: Some(serde_json::json!({
                "engine": "rabbitmq",
                "rabbitmq_cluster": "suite",
                "amqp_uri": single_amqp_uri.clone(),
                "connection_url": multi_connection_url.clone(),
                "host": "127.0.0.1",
                "port": first_port,
                "username": self.username,
                "password": self.password,
                "vhost": "/",
                "resource_prefix": self.resource_prefix,
            })),
        }])
    }

    async fn check_ready(&self) -> Result<(), InfrastructureError> {
        let uri = self
            .amqp_uri
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned("rabbitmq-cluster".into()))?;
        wait_amqp_uri_ready(uri, "rabbitmq-cluster").await
    }

    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        if !self.holds_suite_ref {
            self.amqp_uri = None;
            self.resource_prefix.clear();
            return Ok(());
        }

        let mut guard = suite_rabbit_cluster_mutex().lock().await;
        let mut teardown: Option<(
            String,
            Vec<testcontainers::ContainerAsync<GenericImage>>,
            PathBuf,
        )> = None;
        match &mut *guard {
            SuiteRabbitClusterState::Idle => {}
            SuiteRabbitClusterState::Running { refs, .. } => {
                *refs = refs.saturating_sub(1);
                if *refs == 0 {
                    let taken = std::mem::replace(&mut *guard, SuiteRabbitClusterState::Idle);
                    if let SuiteRabbitClusterState::Running {
                        network_name,
                        nodes,
                        queue_type_conf_path,
                        ..
                    } = taken
                    {
                        teardown = Some((network_name, nodes, queue_type_conf_path));
                    }
                }
            }
        }
        drop(guard);

        if let Some((network_name, nodes, queue_type_conf_path)) = teardown {
            let docker =
                Docker::connect_with_defaults().map_err(|error| InfrastructureError::Teardown {
                    resource: "rabbitmq-cluster".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;
            for node in nodes {
                let _ = node.stop_with_timeout(Some(30)).await;
                let _ = node.rm().await;
            }
            remove_cluster_network(&docker, network_name.as_str())
                .await
                .map_err(|error| InfrastructureError::Teardown {
                    resource: "rabbitmq-cluster".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;
            let _ = std::fs::remove_file(queue_type_conf_path);
        }

        self.holds_suite_ref = false;
        self.amqp_uri = None;
        self.resource_prefix.clear();
        Ok(())
    }
}

/// Suite layout for RabbitMQ cluster topology control (management API + Docker node IDs).
pub(crate) struct RabbitClusterSuiteSnapshot {
    pub(crate) node_count: usize,
    pub(crate) container_ids: Vec<String>,
    pub(crate) hostnames: Vec<String>,
    pub(crate) management_host_ports: Vec<u16>,
    pub(crate) username: String,
    pub(crate) password: String,
}

pub(crate) async fn rabbit_cluster_suite_snapshot() -> Option<RabbitClusterSuiteSnapshot> {
    let guard = suite_rabbit_cluster_mutex().lock().await;
    match &*guard {
        SuiteRabbitClusterState::Idle => None,
        SuiteRabbitClusterState::Running {
            nodes,
            management_host_ports,
            hostnames,
            node_count,
            ..
        } => {
            let container_ids = nodes.iter().map(|node| node.id().to_string()).collect();
            Some(RabbitClusterSuiteSnapshot {
                node_count: *node_count,
                container_ids,
                hostnames: hostnames.clone(),
                management_host_ports: management_host_ports.clone(),
                username: "guest".to_string(),
                password: "guest".to_string(),
            })
        }
    }
}
