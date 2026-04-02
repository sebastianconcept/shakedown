//! Suite-shared MongoDB replica set (`mongodb-cluster` feature).
//!
//! Provisions `N` [`mongod`](https://hub.docker.com/_/mongo) containers on a [`crate::cluster::create_cluster_network`]
//! bridge network (with per-node `mongo-{i}` hostnames), runs `replSetInitiate`, and waits for a primary.
//!
//! **Member addresses:** `replSetInitiate` uses **`mongo-0:27017`, …** so peers reach each other on the
//! user-defined network.
//!
//! **Host-process connection URL:** the Rust driver would otherwise follow server-advertised hostnames
//! (`mongo-*`) that do not resolve on the machine running `cargo test`. Published
//! [`ResourceHandle::connection_string`] therefore targets the **current primary’s** published port on
//! **`127.0.0.1`** with **`directConnection=true`** (and `replicaSet=…` for metadata). Use a driver that
//! respects that URI from the test host; for multi-seed discovery from the host, additional DNS or
//! network setup would be required.
//!
//! **Auth:** the official `mongo` image entrypoint’s init path binds a temporary `mongod` to
//! `127.0.0.1` only, which breaks published-port access until the real process starts. This resource
//! therefore starts **`mongod` without** `MONGO_INITDB_*` (no root user); the suite is suitable for
//! harness integration tests. Use standalone [`crate::MongoResource`] when you need authenticated
//! single-node MongoDB.

use std::time::Duration;

use async_trait::async_trait;
use bollard::Docker;
use mongodb::bson::{doc, Document};
use tokio::sync::Mutex;
use tokio::time::timeout;

use crate::cluster::{create_cluster_network, remove_cluster_network};
use crate::docker_labels::{SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE};
use crate::error::{InfrastructureError, ResourceHandle, ScenarioId};
use crate::resources::{sanitize_sql_database_name, Resource};

use std::sync::OnceLock;

const MONGO_PORT: u16 = 27017;
const DEFAULT_IMAGE_TAG: &str = "7";
const DEFAULT_REPLICA_SET_ID: &str = "rs0";
const PROVISION_TIMEOUT: Duration = Duration::from_secs(60);

#[allow(clippy::large_enum_variant)]
enum SuiteMongoRsState {
    Idle,
    Running {
        network_name: String,
        nodes: Vec<testcontainers::ContainerAsync<testcontainers::GenericImage>>,
        host_ports: Vec<u16>,
        refs: usize,
        image_key: String,
        node_count: usize,
    },
}

static SUITE_MONGO_RS: OnceLock<Mutex<SuiteMongoRsState>> = OnceLock::new();

fn suite_mongo_rs_mutex() -> &'static Mutex<SuiteMongoRsState> {
    SUITE_MONGO_RS.get_or_init(|| Mutex::new(SuiteMongoRsState::Idle))
}

fn map_tc_err(error: impl std::fmt::Display + Send + Sync + 'static) -> InfrastructureError {
    InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
}

fn image_fingerprint(tag: &str) -> String {
    format!("mongo:{tag}")
}

fn admin_uri_for_host_and_port(host: &str, port: u16) -> String {
    format!("mongodb://{host}:{port}/admin?directConnection=true")
}

/// Connection URL for the harness / user code running on the **Docker host** (not inside the bridge network).
///
/// Uses **`directConnection=true` only** (no `replicaSet=` query param): including `replicaSet` caused the
/// Rust driver to stay in replica-set discovery mode and surface `RsGhost` while nodes were still
/// converging, even with `directConnection`. Replica set id remains in [`ResourceHandle`] metadata as
/// `replica_set`.
fn harness_connection_url(host: &str, primary_mapped_port: u16, database_name: &str) -> String {
    format!("mongodb://{host}:{primary_mapped_port}/{database_name}?directConnection=true&w=1")
}

fn client_host_for_harness() -> &'static str {
    "127.0.0.1"
}

async fn mongo_client(uri: &str) -> Result<mongodb::Client, mongodb::error::Error> {
    mongodb::Client::with_uri_str(uri).await
}

fn primary_mapped_port_from_status(
    status: &Document,
    host_ports: &[u16],
) -> Result<u16, InfrastructureError> {
    let members = status
        .get_array("members")
        .map_err(|_| InfrastructureError::Provision {
            resource: "mongodb-cluster".into(),
            source: Box::new(std::io::Error::other(
                "replSetGetStatus: missing members array",
            )),
        })?;
    for member in members {
        let Some(member_doc) = member.as_document() else {
            continue;
        };
        if member_doc.get_str("stateStr").unwrap_or("") != "PRIMARY" {
            continue;
        }
        let name = member_doc
            .get_str("name")
            .map_err(|_| InfrastructureError::Provision {
                resource: "mongodb-cluster".into(),
                source: Box::new(std::io::Error::other(
                    "replSetGetStatus: PRIMARY member missing name",
                )),
            })?;
        let index = name
            .strip_prefix("mongo-")
            .and_then(|rest| rest.split(':').next())
            .and_then(|index| index.parse::<usize>().ok())
            .ok_or_else(|| InfrastructureError::Provision {
                resource: "mongodb-cluster".into(),
                source: Box::new(std::io::Error::other(format!(
                    "unrecognized member host in replSetGetStatus: {name}"
                ))),
            })?;
        let port =
            host_ports
                .get(index)
                .copied()
                .ok_or_else(|| InfrastructureError::Provision {
                    resource: "mongodb-cluster".into(),
                    source: Box::new(std::io::Error::other(format!(
                        "member index {index} out of range for host port list"
                    ))),
                })?;
        return Ok(port);
    }
    Err(InfrastructureError::Provision {
        resource: "mongodb-cluster".into(),
        source: Box::new(std::io::Error::other(
            "replSetGetStatus contained no PRIMARY member",
        )),
    })
}

async fn wait_for_primary_election(
    host: &str,
    first_host_port: u16,
    host_ports: &[u16],
    node_count: usize,
) -> Result<u16, InfrastructureError> {
    let uri = admin_uri_for_host_and_port(host, first_host_port);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let client =
            mongo_client(uri.as_str())
                .await
                .map_err(|error| InfrastructureError::Provision {
                    resource: "mongodb-cluster".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;
        let admin = client.database("admin");
        let status = admin
            .run_command(doc! { "replSetGetStatus": 1 })
            .await
            .map_err(|error| InfrastructureError::Provision {
                resource: "mongodb-cluster".into(),
                source: Box::new(std::io::Error::other(error.to_string())),
            })?;
        if primary_and_secondaries_ok(&status, node_count) {
            return primary_mapped_port_from_status(&status, host_ports);
        }
        if tokio::time::Instant::now() > deadline {
            return Err(InfrastructureError::Provision {
                resource: "mongodb-cluster".into(),
                source: Box::new(std::io::Error::other(
                    "timed out waiting for replica set primary election",
                )),
            });
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
}

fn primary_and_secondaries_ok(status: &Document, node_count: usize) -> bool {
    let Some(members) = status.get_array("members").ok() else {
        return false;
    };
    if members.len() != node_count {
        return false;
    }
    let mut primary_count = 0usize;
    let mut secondary_count = 0usize;
    for member in members {
        let Some(member_doc) = member.as_document() else {
            continue;
        };
        let state = member_doc
            .get_str("stateStr")
            .unwrap_or("")
            .to_ascii_uppercase();
        match state.as_str() {
            "PRIMARY" => primary_count += 1,
            "SECONDARY" => secondary_count += 1,
            _ => {}
        }
    }
    primary_count == 1 && secondary_count == node_count.saturating_sub(1)
}

/// Docker-backed MongoDB **replica set** (multiple `mongod` containers on a suite-scoped bridge network).
///
/// Default is **3** nodes and replica set id **`rs0`**. Image defaults to **`mongo:7`**; override with
/// [`Self::with_image_tag`] or **`SHAKEDOWN_MONGO_CLUSTER_IMAGE_TAG`**.
#[derive(Debug)]
pub struct MongoReplicaSetResource {
    node_count: usize,
    image_tag: Option<String>,
    replica_set_id: String,
    holds_suite_ref: bool,
    client_host: Option<String>,
    host_ports: Vec<u16>,
    database_name: String,
}

impl Default for MongoReplicaSetResource {
    fn default() -> Self {
        Self::new()
    }
}

impl MongoReplicaSetResource {
    /// New replica set resource with **3** nodes and replica set id **`rs0`**.
    pub fn new() -> Self {
        Self {
            node_count: 3,
            image_tag: Some(DEFAULT_IMAGE_TAG.to_string()),
            replica_set_id: DEFAULT_REPLICA_SET_ID.to_string(),
            holds_suite_ref: false,
            client_host: None,
            host_ports: Vec::new(),
            database_name: String::new(),
        }
    }

    /// Override member count (minimum **3** for a majority replica set).
    pub fn with_node_count(mut self, node_count: usize) -> Self {
        self.node_count = node_count.max(3);
        self
    }

    /// Override the image tag (`mongo:<tag>`).
    pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
        self.image_tag = Some(tag.into());
        self
    }

    /// Apply tag from `SHAKEDOWN_MONGO_CLUSTER_IMAGE_TAG` when set.
    pub fn with_image_tag_from_env(mut self) -> Self {
        if let Ok(tag) = std::env::var("SHAKEDOWN_MONGO_CLUSTER_IMAGE_TAG") {
            let tag = tag.trim();
            if !tag.is_empty() {
                self.image_tag = Some(tag.to_string());
            }
        }
        self
    }
}

#[async_trait]
impl Resource for MongoReplicaSetResource {
    async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        if self.holds_suite_ref {
            return Err(InfrastructureError::AlreadyProvisioned(
                "mongodb-cluster".into(),
            ));
        }

        let database_name = sanitize_sql_database_name(scenario_id.as_str());
        let tag = self.image_tag.as_deref().unwrap_or(DEFAULT_IMAGE_TAG);
        let want_key = image_fingerprint(tag);
        let node_count = self.node_count;
        let replica_set_id = self.replica_set_id.clone();

        timeout(
            PROVISION_TIMEOUT,
            provision_suite_inner(
                &database_name,
                tag,
                want_key.as_str(),
                node_count,
                replica_set_id.as_str(),
            ),
        )
        .await
        .map_err(|_| InfrastructureError::Provision {
            resource: "mongodb-cluster".into(),
            source: Box::new(std::io::Error::other(format!(
                "mongodb replica set provisioning exceeded {:?}",
                PROVISION_TIMEOUT
            ))),
        })?
        .map(|(client_host, host_ports, primary_mapped_port)| {
            self.holds_suite_ref = true;
            self.client_host = Some(client_host.clone());
            self.host_ports = host_ports.clone();
            self.database_name = database_name.clone();

            let connection_url = harness_connection_url(
                client_host.as_str(),
                primary_mapped_port,
                database_name.as_str(),
            );

            vec![ResourceHandle {
                id: format!("mongo_rs_{scenario_id}"),
                connection_string: connection_url.clone(),
                metadata: Some(serde_json::json!({
                    "host": client_host,
                    "port": primary_mapped_port,
                    "database": database_name,
                    "username": "",
                    "password": "",
                    "connection_url": connection_url,
                    "engine": "mongodb",
                    "replica_set": replica_set_id,
                    "mongodb_cluster": "replica_set",
                })),
            }]
        })
    }

    async fn check_ready(&self) -> Result<(), InfrastructureError> {
        let host = self
            .client_host
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned("mongodb-cluster".into()))?;
        let first_port = self
            .host_ports
            .first()
            .copied()
            .ok_or_else(|| InfrastructureError::NotProvisioned("mongodb-cluster".into()))?;

        let uri = admin_uri_for_host_and_port(host, first_port);
        let client =
            mongo_client(uri.as_str())
                .await
                .map_err(|error| InfrastructureError::Readiness {
                    resource: "mongodb-cluster".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;
        let admin = client.database("admin");
        admin
            .run_command(doc! { "ping": 1 })
            .await
            .map_err(|error| InfrastructureError::Readiness {
                resource: "mongodb-cluster".into(),
                source: Box::new(std::io::Error::other(error.to_string())),
            })?;
        Ok(())
    }

    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        if !self.holds_suite_ref {
            self.client_host = None;
            self.host_ports.clear();
            self.database_name.clear();
            return Ok(());
        }

        let database_name = self.database_name.clone();
        let host = self.client_host.clone().unwrap_or_default();
        let first_port = self.host_ports.first().copied().unwrap_or(MONGO_PORT);

        if !database_name.is_empty() && !host.is_empty() {
            let uri = admin_uri_for_host_and_port(host.as_str(), first_port);
            if let Ok(client) = mongo_client(uri.as_str()).await {
                let database = client.database(database_name.as_str());
                let _ = database.drop().await;
            }
        }

        let mut guard = suite_mongo_rs_mutex().lock().await;
        let mut teardown: Option<(
            String,
            Vec<testcontainers::ContainerAsync<testcontainers::GenericImage>>,
        )> = None;
        match &mut *guard {
            SuiteMongoRsState::Idle => {}
            SuiteMongoRsState::Running { refs, .. } => {
                *refs = refs.saturating_sub(1);
                if *refs == 0 {
                    let taken = std::mem::replace(&mut *guard, SuiteMongoRsState::Idle);
                    if let SuiteMongoRsState::Running {
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
                    resource: "mongodb-cluster".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;
            for node in nodes {
                let _ = node.stop_with_timeout(Some(30)).await;
                let _ = node.rm().await;
            }
            remove_cluster_network(&docker, network_name.as_str())
                .await
                .map_err(|error| InfrastructureError::Teardown {
                    resource: "mongodb-cluster".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;
        }

        self.holds_suite_ref = false;
        self.client_host = None;
        self.host_ports.clear();
        self.database_name.clear();
        Ok(())
    }
}

async fn provision_suite_inner(
    _database_name: &str,
    tag: &str,
    want_key: &str,
    node_count: usize,
    replica_set_id: &str,
) -> Result<(String, Vec<u16>, u16), InfrastructureError> {
    use testcontainers::core::{IntoContainerPort, WaitFor};
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{GenericImage, ImageExt};

    let harness_host = client_host_for_harness().to_string();

    let mut guard = suite_mongo_rs_mutex().lock().await;
    let (client_host, host_ports) = match &mut *guard {
        SuiteMongoRsState::Idle => {
            let docker = Docker::connect_with_defaults().map_err(|error| {
                InfrastructureError::Provision {
                    resource: "mongodb-cluster".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                }
            })?;

            let suite_id = format!("pid{}", std::process::id());
            let network_name = create_cluster_network(&docker, "mongo", suite_id.as_str())
                .await
                .map_err(|error| InfrastructureError::Provision {
                    resource: "mongodb-cluster".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;

            let mut nodes = Vec::with_capacity(node_count);
            let mut host_ports = Vec::with_capacity(node_count);

            for index in 0..node_count {
                let hostname = format!("mongo-{index}");
                let start_image = GenericImage::new("mongo", tag)
                    .with_exposed_port(MONGO_PORT.tcp())
                    .with_wait_for(WaitFor::message_on_stdout("Waiting for connections"))
                    .with_cmd([
                        "mongod".to_string(),
                        "--replSet".to_string(),
                        replica_set_id.to_string(),
                        "--bind_ip_all".to_string(),
                    ])
                    .with_network(network_name.clone())
                    .with_hostname(hostname.as_str())
                    .with_label(SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE);

                let container = start_image.start().await.map_err(map_tc_err)?;
                let mapped = container
                    .get_host_port_ipv4(MONGO_PORT)
                    .await
                    .map_err(map_tc_err)?;
                host_ports.push(mapped);
                nodes.push(container);
            }

            let init_uri = admin_uri_for_host_and_port(client_host_for_harness(), host_ports[0]);
            let client = mongo_client(init_uri.as_str()).await.map_err(|error| {
                InfrastructureError::Provision {
                    resource: "mongodb-cluster".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                }
            })?;
            let admin = client.database("admin");

            let members: Vec<Document> = (0..node_count)
                .map(|index| {
                    doc! {
                        "_id": index as i32,
                        "host": format!("mongo-{index}:{MONGO_PORT}"),
                    }
                })
                .collect();

            admin
                .run_command(doc! {
                    "replSetInitiate": {
                        "_id": replica_set_id,
                        "members": members,
                    }
                })
                .await
                .map_err(|error| InfrastructureError::Provision {
                    resource: "mongodb-cluster".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;

            tokio::time::sleep(Duration::from_secs(2)).await;

            *guard = SuiteMongoRsState::Running {
                network_name,
                nodes,
                host_ports: host_ports.clone(),
                refs: 1,
                image_key: want_key.to_string(),
                node_count,
            };
            (harness_host.clone(), host_ports)
        }
        SuiteMongoRsState::Running {
            host_ports,
            refs,
            image_key,
            node_count: existing_count,
            ..
        } => {
            if *image_key != want_key {
                return Err(InfrastructureError::Backend(Box::new(
                    std::io::Error::other(format!(
                        "MongoDB replica set suite already running as {image_key}; \
                         cannot start or join as {want_key}. Use one image:tag per process."
                    )),
                )));
            }
            if *existing_count != node_count {
                return Err(InfrastructureError::Backend(Box::new(
                    std::io::Error::other(format!(
                        "MongoDB replica set suite already running with {} nodes; \
                         requested {node_count}. Use one node count per process.",
                        existing_count
                    )),
                )));
            }
            *refs += 1;
            (harness_host.clone(), host_ports.clone())
        }
    };
    drop(guard);

    let primary_mapped_port = wait_for_primary_election(
        client_host_for_harness(),
        host_ports[0],
        host_ports.as_slice(),
        node_count,
    )
    .await?;

    Ok((client_host, host_ports, primary_mapped_port))
}
