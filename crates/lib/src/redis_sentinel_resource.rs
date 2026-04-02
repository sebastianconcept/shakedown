//! Suite-shared Redis with Sentinel (`redis-sentinel` feature).
//!
//! Provisions **3** [`redis`](https://hub.docker.com/_/redis) servers (1 primary + 2 replicas) and **3**
//! [`redis-sentinel`](https://redis.io/docs/management/sentinel/) processes on a
//! [`crate::cluster::create_cluster_network`] bridge. Hostnames `redis-0` … `redis-2` and `sentinel-0` …
//! `sentinel-2` resolve on the user-defined network.
//!
//! [`RedisBindings::connection_url`] targets the **current primary** on the Docker host (`127.0.0.1` +
//! published port). [`RedisSentinelBindings::sentinel_hosts`] lists host-mapped Sentinel endpoints for
//! client-side discovery.

use std::sync::OnceLock;
use std::time::Duration;

use async_trait::async_trait;
use bollard::Docker;
use futures_util::future::try_join_all;
use tokio::sync::Mutex;
use tokio::time::timeout;

use crate::cluster::{create_cluster_network, remove_cluster_network};
use crate::docker_labels::{SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE};
use crate::error::{InfrastructureError, ResourceHandle, ScenarioId};
use crate::resources::{sanitize_redis_key_prefix, Resource};

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const REDIS_PORT: u16 = 6379;
const SENTINEL_PORT: u16 = 26379;
const DEFAULT_IMAGE_TAG: &str = "7";
const DEFAULT_MASTER_NAME: &str = "mymaster";
const REDIS_NODE_COUNT: usize = 3;
const SENTINEL_COUNT: usize = 3;
const PROVISION_TIMEOUT: Duration = Duration::from_secs(60);

#[allow(clippy::large_enum_variant)]
enum SuiteRedisSentinelState {
    Idle,
    Running {
        network_name: String,
        redis_nodes: Vec<testcontainers::ContainerAsync<GenericImage>>,
        sentinel_nodes: Vec<testcontainers::ContainerAsync<GenericImage>>,
        redis_host_ports: Vec<u16>,
        sentinel_host_ports: Vec<u16>,
        refs: usize,
        image_key: String,
    },
}

static SUITE_REDIS_SENTINEL: OnceLock<Mutex<SuiteRedisSentinelState>> = OnceLock::new();

fn suite_redis_sentinel_mutex() -> &'static Mutex<SuiteRedisSentinelState> {
    SUITE_REDIS_SENTINEL.get_or_init(|| Mutex::new(SuiteRedisSentinelState::Idle))
}

fn map_tc_err(error: impl std::fmt::Display + Send + Sync + 'static) -> InfrastructureError {
    InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
}

fn image_fingerprint(tag: &str) -> String {
    format!("redis:{tag}:sentinel")
}

fn harness_host() -> &'static str {
    "127.0.0.1"
}

fn redis_container_name(suite_id: &str, index: usize) -> String {
    format!("shakedown-rs-{suite_id}-redis-{index}")
}

fn sentinel_container_name(suite_id: &str, index: usize) -> String {
    format!("shakedown-rs-{suite_id}-sentinel-{index}")
}

/// Docker-backed **Redis replication + Sentinel** (3 nodes + 3 sentinels on a suite bridge network).
///
/// Default image tag is **`7`** (`redis:7`). Override with [`Self::with_image_tag`] or
/// **`SHAKEDOWN_REDIS_IMAGE_TAG`** (same env var as standalone [`crate::RedisResource`]).
#[derive(Debug)]
pub struct RedisSentinelResource {
    image_tag: Option<String>,
    master_name: String,
    holds_suite_ref: bool,
    connection_url: Option<String>,
    sentinel_hosts: Option<String>,
    key_prefix: String,
}

impl Default for RedisSentinelResource {
    fn default() -> Self {
        Self::new()
    }
}

impl RedisSentinelResource {
    /// New resource with default master name **`mymaster`**.
    pub fn new() -> Self {
        Self {
            image_tag: None,
            master_name: DEFAULT_MASTER_NAME.to_string(),
            holds_suite_ref: false,
            connection_url: None,
            sentinel_hosts: None,
            key_prefix: String::new(),
        }
    }

    /// Override the `redis` image tag (repository remains **`redis`**).
    pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
        self.image_tag = Some(tag.into());
        self
    }

    /// Load tag from **`SHAKEDOWN_REDIS_IMAGE_TAG`** when set (aligned with [`crate::RedisResource`]).
    pub fn with_image_tag_from_env(mut self) -> Self {
        if let Ok(tag) = std::env::var("SHAKEDOWN_REDIS_IMAGE_TAG") {
            let tag = tag.trim();
            if !tag.is_empty() {
                self.image_tag = Some(tag.to_string());
            }
        }
        self
    }

    /// Sentinel [`SENTINEL MONITOR`](https://redis.io/docs/management/sentinel/#sentinel-monitor) master name (default **`mymaster`**).
    pub fn with_master_name(mut self, master_name: impl Into<String>) -> Self {
        self.master_name = master_name.into();
        self
    }
}

async fn delete_keys_with_prefix(
    connection_url: &str,
    key_prefix: &str,
) -> Result<(), InfrastructureError> {
    let client =
        redis::Client::open(connection_url).map_err(|error| InfrastructureError::Teardown {
            resource: "redis-sentinel".into(),
            source: Box::new(std::io::Error::other(error.to_string())),
        })?;
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|error| InfrastructureError::Teardown {
            resource: "redis-sentinel".into(),
            source: Box::new(std::io::Error::other(error.to_string())),
        })?;

    let pattern = format!("{key_prefix}*");
    let mut cursor: u64 = 0;
    loop {
        let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(256)
            .query_async(&mut connection)
            .await
            .map_err(|error| InfrastructureError::Teardown {
                resource: "redis-sentinel".into(),
                source: Box::new(std::io::Error::other(error.to_string())),
            })?;

        if !keys.is_empty() {
            redis::cmd("DEL")
                .arg(&keys)
                .query_async::<i64>(&mut connection)
                .await
                .map_err(|error| InfrastructureError::Teardown {
                    resource: "redis-sentinel".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;
        }

        if next_cursor == 0 {
            break;
        }
        cursor = next_cursor;
    }

    Ok(())
}

fn sentinel_config_shell_command(master_name: &str, master_ip: &str) -> String {
    format!(
        r#"printf '%s\n' \
'bind 0.0.0.0' \
'port {SENTINEL_PORT}' \
'sentinel monitor {master_name} {master_ip} {REDIS_PORT} 2' \
'sentinel down-after-milliseconds {master_name} 5000' \
'sentinel parallel-syncs {master_name} 1' \
'sentinel failover-timeout {master_name} 10000' \
> /tmp/sentinel.conf && exec redis-sentinel /tmp/sentinel.conf"#
    )
}

async fn container_ipv4_on_user_network(
    docker: &Docker,
    container_id: &str,
    network_name: &str,
) -> Result<String, InfrastructureError> {
    let inspect = docker
        .inspect_container(container_id, None)
        .await
        .map_err(|error| InfrastructureError::Provision {
            resource: "redis-sentinel".into(),
            source: Box::new(std::io::Error::other(error.to_string())),
        })?;
    let networks = inspect
        .network_settings
        .and_then(|settings| settings.networks)
        .unwrap_or_default();
    for (name, endpoint) in networks {
        if name == network_name {
            if let Some(ip) = endpoint.ip_address.filter(|value| !value.is_empty()) {
                return Ok(ip);
            }
        }
    }
    Err(InfrastructureError::Provision {
        resource: "redis-sentinel".into(),
        source: Box::new(std::io::Error::other(format!(
            "container {container_id} has no IPv4 on Docker network {network_name}"
        ))),
    })
}

async fn wait_master_replication(
    master_url: &str,
    expected_slaves: usize,
) -> Result<(), InfrastructureError> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    let client =
        redis::Client::open(master_url).map_err(|error| InfrastructureError::Readiness {
            resource: "redis-sentinel".into(),
            source: Box::new(std::io::Error::other(error.to_string())),
        })?;
    loop {
        if tokio::time::Instant::now() > deadline {
            return Err(InfrastructureError::Readiness {
                resource: "redis-sentinel".into(),
                source: Box::new(std::io::Error::other(
                    "timed out waiting for replicas to link to primary",
                )),
            });
        }
        let mut connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| InfrastructureError::Readiness {
                resource: "redis-sentinel".into(),
                source: Box::new(std::io::Error::other(error.to_string())),
            })?;
        let info: String = redis::cmd("INFO")
            .arg("REPLICATION")
            .query_async(&mut connection)
            .await
            .map_err(|error| InfrastructureError::Readiness {
                resource: "redis-sentinel".into(),
                source: Box::new(std::io::Error::other(error.to_string())),
            })?;
        let mut role_ok = false;
        let mut slaves = 0usize;
        for line in info.lines() {
            if line.starts_with("role:") {
                role_ok = line.contains("master");
            }
            if line.starts_with("connected_slaves:") {
                slaves = line
                    .split(':')
                    .nth(1)
                    .and_then(|value| value.trim().parse().ok())
                    .unwrap_or(0);
            }
        }
        if role_ok && slaves >= expected_slaves {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

async fn sentinel_ckquorum(
    sentinel_url: &str,
    master_name: &str,
) -> Result<(), InfrastructureError> {
    let client =
        redis::Client::open(sentinel_url).map_err(|error| InfrastructureError::Readiness {
            resource: "redis-sentinel".into(),
            source: Box::new(std::io::Error::other(error.to_string())),
        })?;
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|error| InfrastructureError::Readiness {
            resource: "redis-sentinel".into(),
            source: Box::new(std::io::Error::other(error.to_string())),
        })?;
    let _: String = redis::cmd("SENTINEL")
        .arg("CKQUORUM")
        .arg(master_name)
        .query_async(&mut connection)
        .await
        .map_err(|error| InfrastructureError::Readiness {
            resource: "redis-sentinel".into(),
            source: Box::new(std::io::Error::other(error.to_string())),
        })?;
    Ok(())
}

async fn wait_sentinel_quorum(
    sentinel_ports: &[u16],
    master_name: &str,
) -> Result<(), InfrastructureError> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    let host = harness_host();
    loop {
        if tokio::time::Instant::now() > deadline {
            return Err(InfrastructureError::Readiness {
                resource: "redis-sentinel".into(),
                source: Box::new(std::io::Error::other(
                    "timed out waiting for Sentinel quorum (CKQUORUM)",
                )),
            });
        }
        for port in sentinel_ports {
            let url = format!("redis://{host}:{port}/");
            if sentinel_ckquorum(url.as_str(), master_name).await.is_ok() {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
}

async fn provision_suite_inner(
    want_key: &str,
    tag: &str,
    master_name: &str,
) -> Result<(String, Vec<u16>, Vec<u16>), InfrastructureError> {
    let host = harness_host().to_string();

    let mut guard = suite_redis_sentinel_mutex().lock().await;
    let (connection_url, redis_host_ports, sentinel_host_ports) = match &mut *guard {
        SuiteRedisSentinelState::Idle => {
            let docker = Docker::connect_with_defaults().map_err(|error| {
                InfrastructureError::Provision {
                    resource: "redis-sentinel".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                }
            })?;

            let suite_id = format!("pid{}", std::process::id());
            let network_name = create_cluster_network(&docker, "redis-sentinel", suite_id.as_str())
                .await
                .map_err(|error| InfrastructureError::Provision {
                    resource: "redis-sentinel".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;

            let redis_wait = WaitFor::message_on_stdout("Ready to accept connections");
            let master_container_name = redis_container_name(suite_id.as_str(), 0);
            let master_image = GenericImage::new("redis", tag)
                .with_exposed_port(REDIS_PORT.tcp())
                .with_wait_for(redis_wait.clone())
                .with_cmd(["redis-server".to_string()])
                .with_network(network_name.clone())
                .with_container_name(master_container_name.as_str())
                .with_hostname(master_container_name.as_str())
                .with_label(SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE);

            let master_container = master_image.start().await.map_err(map_tc_err)?;
            let master_mapped = master_container
                .get_host_port_ipv4(REDIS_PORT)
                .await
                .map_err(map_tc_err)?;

            let master_ip = container_ipv4_on_user_network(
                &docker,
                master_container.id(),
                network_name.as_str(),
            )
            .await?;

            let master_url = format!("redis://{host}:{master_mapped}/");
            let mut redis_nodes = vec![master_container];
            let mut redis_host_ports_vec = vec![master_mapped];

            let replica_futures = (1..REDIS_NODE_COUNT).map(|index| {
                let network_name = network_name.clone();
                let replica_wait = redis_wait.clone();
                let master_ip = master_ip.clone();
                let suite_id = suite_id.clone();
                let cmd = format!(
                    "sleep 1 && redis-server --replicaof {} {}",
                    master_ip, REDIS_PORT
                );
                async move {
                    let replica_name = redis_container_name(suite_id.as_str(), index);
                    let start_image = GenericImage::new("redis", tag)
                        .with_exposed_port(REDIS_PORT.tcp())
                        .with_wait_for(replica_wait)
                        .with_cmd(["sh".to_string(), "-c".to_string(), cmd])
                        .with_network(network_name)
                        .with_container_name(replica_name.as_str())
                        .with_hostname(replica_name.as_str())
                        .with_label(SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE);
                    start_image.start().await.map_err(map_tc_err)
                }
            });
            let replicas: Vec<_> = try_join_all(replica_futures).await?;
            for replica in replicas {
                let mapped = replica
                    .get_host_port_ipv4(REDIS_PORT)
                    .await
                    .map_err(map_tc_err)?;
                redis_host_ports_vec.push(mapped);
                redis_nodes.push(replica);
            }

            wait_master_replication(master_url.as_str(), REDIS_NODE_COUNT - 1).await?;

            let sentinel_shell = sentinel_config_shell_command(master_name, master_ip.as_str());
            let sentinel_wait = WaitFor::message_on_stdout("+monitor master");

            let sentinel_futures = (0..SENTINEL_COUNT).map(|index| {
                let network_name = network_name.clone();
                let sentinel_wait = sentinel_wait.clone();
                let shell = sentinel_shell.clone();
                let suite_id = suite_id.clone();
                async move {
                    let name = sentinel_container_name(suite_id.as_str(), index);
                    let start_image = GenericImage::new("redis", tag)
                        .with_exposed_port(SENTINEL_PORT.tcp())
                        .with_wait_for(sentinel_wait)
                        .with_cmd(["sh".to_string(), "-c".to_string(), shell])
                        .with_network(network_name)
                        .with_container_name(name.as_str())
                        .with_hostname(name.as_str())
                        .with_label(SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE);
                    start_image.start().await.map_err(map_tc_err)
                }
            });
            let sentinels: Vec<_> = try_join_all(sentinel_futures).await?;
            let mut sentinel_host_ports_vec = Vec::with_capacity(SENTINEL_COUNT);
            let mut sentinel_nodes = Vec::new();
            for sentinel in sentinels {
                let mapped = sentinel
                    .get_host_port_ipv4(SENTINEL_PORT)
                    .await
                    .map_err(map_tc_err)?;
                sentinel_host_ports_vec.push(mapped);
                sentinel_nodes.push(sentinel);
            }

            wait_sentinel_quorum(sentinel_host_ports_vec.as_slice(), master_name).await?;

            let connection = format!(
                "redis://{host}:{}/",
                redis_host_ports_vec.first().copied().ok_or_else(|| {
                    InfrastructureError::Provision {
                        resource: "redis-sentinel".into(),
                        source: Box::new(std::io::Error::other("missing primary mapped port")),
                    }
                },)?
            );

            *guard = SuiteRedisSentinelState::Running {
                network_name,
                redis_nodes,
                sentinel_nodes,
                redis_host_ports: redis_host_ports_vec.clone(),
                sentinel_host_ports: sentinel_host_ports_vec.clone(),
                refs: 1,
                image_key: want_key.to_string(),
            };

            (connection, redis_host_ports_vec, sentinel_host_ports_vec)
        }
        SuiteRedisSentinelState::Running {
            redis_host_ports,
            sentinel_host_ports,
            refs,
            image_key,
            ..
        } => {
            if *image_key != want_key {
                return Err(InfrastructureError::Backend(Box::new(
                    std::io::Error::other(format!(
                        "Redis Sentinel suite already running as {image_key}; \
                         cannot start or join as {want_key}."
                    )),
                )));
            }
            *refs += 1;
            let primary_port = redis_host_ports.first().copied().ok_or_else(|| {
                InfrastructureError::Backend(Box::new(std::io::Error::other(
                    "Redis Sentinel suite missing primary port",
                )))
            })?;
            (
                format!("redis://{host}:{primary_port}/"),
                redis_host_ports.clone(),
                sentinel_host_ports.clone(),
            )
        }
    };
    drop(guard);

    Ok((connection_url, redis_host_ports, sentinel_host_ports))
}

#[async_trait]
impl Resource for RedisSentinelResource {
    async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        if self.holds_suite_ref {
            return Err(InfrastructureError::AlreadyProvisioned(
                "redis-sentinel".into(),
            ));
        }

        self.key_prefix = sanitize_redis_key_prefix(scenario_id.as_str());
        let tag = self.image_tag.as_deref().unwrap_or(DEFAULT_IMAGE_TAG);
        let want_key = image_fingerprint(tag);
        let master_name = self.master_name.clone();

        let (connection_url, redis_ports, sentinel_ports) = timeout(
            PROVISION_TIMEOUT,
            provision_suite_inner(want_key.as_str(), tag, master_name.as_str()),
        )
        .await
        .map_err(|_| InfrastructureError::Provision {
            resource: "redis-sentinel".into(),
            source: Box::new(std::io::Error::other(format!(
                "Redis Sentinel provisioning exceeded {:?}",
                PROVISION_TIMEOUT
            ))),
        })??;

        let sentinel_hosts = sentinel_ports
            .iter()
            .map(|port| format!("{}:{}", harness_host(), port))
            .collect::<Vec<_>>()
            .join(",");

        let replica_connection_urls: Vec<String> = redis_ports
            .iter()
            .skip(1)
            .map(|port| format!("redis://{}:{}/", harness_host(), port))
            .collect();

        self.connection_url = Some(connection_url.clone());
        self.sentinel_hosts = Some(sentinel_hosts.clone());
        self.holds_suite_ref = true;

        let primary_port =
            redis_ports
                .first()
                .copied()
                .ok_or_else(|| InfrastructureError::Provision {
                    resource: "redis-sentinel".into(),
                    source: Box::new(std::io::Error::other("missing primary port")),
                })?;

        Ok(vec![ResourceHandle {
            id: format!("redis_sentinel_{scenario_id}"),
            connection_string: connection_url.clone(),
            metadata: Some(serde_json::json!({
                "engine": "redis-sentinel",
                "host": harness_host(),
                "port": primary_port,
                "connection_url": connection_url,
                "key_prefix": self.key_prefix,
                "sentinel_hosts": sentinel_hosts,
                "master_name": master_name,
                "replica_connection_urls": replica_connection_urls,
            })),
        }])
    }

    async fn check_ready(&self) -> Result<(), InfrastructureError> {
        let url = self
            .connection_url
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned("redis-sentinel".into()))?;
        let client = redis::Client::open(url).map_err(|error| InfrastructureError::Readiness {
            resource: "redis-sentinel".into(),
            source: Box::new(std::io::Error::other(error.to_string())),
        })?;
        let mut connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| InfrastructureError::Readiness {
                resource: "redis-sentinel".into(),
                source: Box::new(std::io::Error::other(error.to_string())),
            })?;
        let _: String = redis::cmd("PING")
            .query_async(&mut connection)
            .await
            .map_err(|error| InfrastructureError::Readiness {
                resource: "redis-sentinel".into(),
                source: Box::new(std::io::Error::other(error.to_string())),
            })?;
        Ok(())
    }

    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        let connection_url = self.connection_url.clone();
        let key_prefix = self.key_prefix.clone();

        if self.holds_suite_ref {
            if let Some(url) = connection_url.as_ref() {
                delete_keys_with_prefix(url, key_prefix.as_str()).await?;
            }

            let mut guard = suite_redis_sentinel_mutex().lock().await;
            let mut teardown: Option<(
                String,
                Vec<testcontainers::ContainerAsync<GenericImage>>,
                Vec<testcontainers::ContainerAsync<GenericImage>>,
            )> = None;
            match &mut *guard {
                SuiteRedisSentinelState::Idle => {}
                SuiteRedisSentinelState::Running { refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuiteRedisSentinelState::Idle);
                        if let SuiteRedisSentinelState::Running {
                            network_name,
                            redis_nodes,
                            sentinel_nodes,
                            ..
                        } = taken
                        {
                            teardown = Some((network_name, redis_nodes, sentinel_nodes));
                        }
                    }
                }
            }
            drop(guard);

            if let Some((network_name, redis_nodes, sentinel_nodes)) = teardown {
                let docker = Docker::connect_with_defaults().map_err(|error| {
                    InfrastructureError::Teardown {
                        resource: "redis-sentinel".into(),
                        source: Box::new(std::io::Error::other(error.to_string())),
                    }
                })?;
                for node in sentinel_nodes {
                    let _ = node.stop_with_timeout(Some(30)).await;
                    let _ = node.rm().await;
                }
                for node in redis_nodes {
                    let _ = node.stop_with_timeout(Some(30)).await;
                    let _ = node.rm().await;
                }
                remove_cluster_network(&docker, network_name.as_str())
                    .await
                    .map_err(|error| InfrastructureError::Teardown {
                        resource: "redis-sentinel".into(),
                        source: Box::new(std::io::Error::other(error.to_string())),
                    })?;
            }
        }

        self.holds_suite_ref = false;
        self.connection_url = None;
        self.sentinel_hosts = None;
        Ok(())
    }
}
