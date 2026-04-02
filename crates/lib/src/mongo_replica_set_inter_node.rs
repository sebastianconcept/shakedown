//! Inter-node Toxiproxy mesh for MongoDB replica sets (US-018; `fault-injection` + `mongodb-cluster`).
//!
//! When both [`crate::resources::BuiltinResource::Toxiproxy`] and [`crate::MongoReplicaSetResource`]
//! are registered, replica set initiation is deferred until after Toxiproxy provisions; the harness
//! attaches the suite Toxiproxy container to the cluster bridge, creates **N×(N−1)** directed
//! proxies, then runs `replSetInitiate` with member hosts `toxiproxy:<port>` (canonical ingress
//! [`proxy_name_for_edge`] `(0,j)` for each member `j`).

use std::sync::{Arc, OnceLock};

use bollard::models::{EndpointSettings, NetworkConnectRequest};
use bollard::Docker;
use mongodb::bson::{doc, Document};
use tokio::sync::Mutex;

use crate::error::InfrastructureError;
use crate::lifecycle::Context;
use crate::resources::toxiproxy_suite_container_id;
use crate::resources::MongoBindings;
use noxious_client::Client as ToxiproxyClient;

const MONGO_PORT: u16 = 27017;
const TOXIPROXY_DNS_NAME: &str = "toxiproxy";

/// Suite-scoped inter-node mesh metadata for MongoDB (`disconnect_node` / `partition_nodes` / `reset_topology`).
pub(crate) struct MongoInterNodeMeshState {
    pub(crate) suite_key: String,
    pub(crate) node_count: usize,
    pub(crate) proxy_names: Vec<String>,
    pub(crate) toxiproxy_api_base: String,
}

static MONGO_INTER_NODE_MESH: OnceLock<Mutex<Option<Arc<MongoInterNodeMeshState>>>> =
    OnceLock::new();

fn mongo_inter_node_mesh_mutex() -> &'static Mutex<Option<Arc<MongoInterNodeMeshState>>> {
    MONGO_INTER_NODE_MESH.get_or_init(|| Mutex::new(None))
}

pub(crate) async fn mongo_inter_node_mesh_snapshot() -> Option<Arc<MongoInterNodeMeshState>> {
    let guard = mongo_inter_node_mesh_mutex().lock().await;
    guard.as_ref().map(Arc::clone)
}

pub(crate) fn proxy_name_for_edge(
    suite_key: &str,
    source_index: usize,
    dest_index: usize,
) -> String {
    format!("shakedown_mongo_{suite_key}_{source_index}_to_{dest_index}")
}

/// Build every directed edge name for `node_count` (excluding self-edges).
pub(crate) fn all_mesh_proxy_names(suite_key: &str, node_count: usize) -> Vec<String> {
    let mut names = Vec::with_capacity(node_count.saturating_sub(1) * node_count);
    for source_index in 0..node_count {
        for dest_index in 0..node_count {
            if source_index == dest_index {
                continue;
            }
            names.push(proxy_name_for_edge(suite_key, source_index, dest_index));
        }
    }
    names
}

fn parse_listen_port(listen: &str) -> Result<u16, InfrastructureError> {
    let port_str = listen
        .rsplit(':')
        .next()
        .ok_or_else(|| InfrastructureError::Provision {
            resource: "mongodb-cluster".into(),
            source: Box::new(std::io::Error::other(format!(
                "could not parse Toxiproxy listen address: {listen}"
            ))),
        })?;
    port_str
        .parse()
        .map_err(|error| InfrastructureError::Provision {
            resource: "mongodb-cluster".into(),
            source: Box::new(std::io::Error::other(format!("listen port parse: {error}"))),
        })
}

async fn connect_toxiproxy_to_cluster_network(
    network_name: &str,
    container_id: &str,
) -> Result<(), InfrastructureError> {
    let docker =
        Docker::connect_with_defaults().map_err(|source| InfrastructureError::Provision {
            resource: "mongodb-cluster".into(),
            source: Box::new(std::io::Error::other(source.to_string())),
        })?;
    let config = NetworkConnectRequest {
        container: container_id.to_string(),
        endpoint_config: Some(EndpointSettings {
            aliases: Some(vec![TOXIPROXY_DNS_NAME.to_string()]),
            ..Default::default()
        }),
    };
    docker
        .connect_network(network_name, config)
        .await
        .map_err(|source| InfrastructureError::Provision {
            resource: "mongodb-cluster".into(),
            source: Box::new(std::io::Error::other(source.to_string())),
        })
}

/// After all resources provision, attach Toxiproxy to the Mongo bridge, create the mesh, and run `replSetInitiate`.
pub(crate) async fn finalize_mongo_replica_set_inter_node_mesh(
    ctx: &mut Context,
) -> Result<(), InfrastructureError> {
    let pending = crate::mongo_replica_set_resource::take_inter_node_finalize_pending().await;
    let Some(pending) = pending else {
        return Ok(());
    };

    let container_id =
        toxiproxy_suite_container_id()
            .await
            .ok_or_else(|| InfrastructureError::Provision {
                resource: "mongodb-cluster".into(),
                source: Box::new(std::io::Error::other(
                    "Toxiproxy suite container id missing for inter-node mesh",
                )),
            })?;

    connect_toxiproxy_to_cluster_network(pending.network_name.as_str(), container_id.as_str())
        .await?;

    let suite_key = pending.suite_key.clone();
    let node_count = pending.node_count;

    let mut canonical_member_hosts: Vec<String> = Vec::with_capacity(node_count);
    {
        let fault_handle = ctx.fault_handle_mut().ok_or_else(|| InfrastructureError::Provision {
            resource: "mongodb-cluster".into(),
            source: Box::new(std::io::Error::other(
                "FaultHandle missing: register Toxiproxy before MongoReplicaSet inter-node finalize",
            )),
        })?;
        for source_index in 0..node_count {
            for dest_index in 0..node_count {
                if source_index == dest_index {
                    continue;
                }
                let name = proxy_name_for_edge(&suite_key, source_index, dest_index);
                let upstream = format!("mongo-{dest_index}:{MONGO_PORT}");
                fault_handle
                    .create_proxy(name.as_str(), "0.0.0.0:0", upstream.as_str())
                    .await
                    .map_err(|source| InfrastructureError::Provision {
                        resource: "mongodb-cluster".into(),
                        source: Box::new(std::io::Error::other(source.to_string())),
                    })?;
            }
        }

        for dest_index in 0..node_count {
            let proxy_name = proxy_name_for_edge(&suite_key, 0, dest_index);
            let listen = fault_handle
                .proxy_listen_address(proxy_name.as_str())
                .ok_or_else(|| InfrastructureError::Provision {
                    resource: "mongodb-cluster".into(),
                    source: Box::new(std::io::Error::other(format!(
                        "missing listen for proxy {proxy_name}"
                    ))),
                })?;
            let port = parse_listen_port(listen)?;
            canonical_member_hosts.push(format!("{TOXIPROXY_DNS_NAME}:{port}"));
        }
    }

    let init_uri = format!(
        "mongodb://127.0.0.1:{}/admin?directConnection=true",
        pending.host_ports[0]
    );
    let client = mongodb::Client::with_uri_str(init_uri.as_str())
        .await
        .map_err(|error| InfrastructureError::Provision {
            resource: "mongodb-cluster".into(),
            source: Box::new(std::io::Error::other(error.to_string())),
        })?;
    let admin = client.database("admin");

    let members: Vec<Document> = (0..node_count)
        .map(|index| {
            doc! {
                "_id": index as i32,
                "host": canonical_member_hosts[index].clone(),
            }
        })
        .collect();

    admin
        .run_command(doc! {
            "replSetInitiate": {
                "_id": pending.replica_set_id.as_str(),
                "members": members,
            }
        })
        .await
        .map_err(|error| InfrastructureError::Provision {
            resource: "mongodb-cluster".into(),
            source: Box::new(std::io::Error::other(error.to_string())),
        })?;

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let primary_mapped_port =
        crate::mongo_replica_set_resource::wait_for_primary_election_after_init(
            pending.host_ports.as_slice(),
            node_count,
        )
        .await?;

    let harness_host = crate::mongo_replica_set_resource::harness_client_host();
    let connection_url = crate::mongo_replica_set_resource::harness_connection_url(
        harness_host,
        primary_mapped_port,
        pending.database_name.as_str(),
    );
    ctx.set_mongo_bindings(MongoBindings {
        host: harness_host.to_string(),
        port: primary_mapped_port,
        database: pending.database_name.clone(),
        username: String::new(),
        password: String::new(),
        connection_url: connection_url.clone(),
        replica_set_host_ports: pending.host_ports.clone(),
    });

    let proxy_names = all_mesh_proxy_names(&suite_key, node_count);
    let toxiproxy_api_base = ctx
        .fault_handle()
        .map(|handle| handle.api_base_url().to_string())
        .ok_or_else(|| InfrastructureError::Provision {
            resource: "mongodb-cluster".into(),
            source: Box::new(std::io::Error::other(
                "FaultHandle missing after Mongo inter-node mesh provisioning",
            )),
        })?;

    let mesh = Arc::new(MongoInterNodeMeshState {
        suite_key,
        node_count,
        proxy_names,
        toxiproxy_api_base,
    });
    {
        let mut guard = mongo_inter_node_mesh_mutex().lock().await;
        *guard = Some(Arc::clone(&mesh));
    }

    Ok(())
}

async fn toxiproxy_set_proxy_enabled(
    api_base: &str,
    proxy_name: &str,
    enabled: bool,
) -> Result<(), crate::cluster::ClusterError> {
    let client = ToxiproxyClient::new(api_base);
    let mut proxy = client.proxy(proxy_name).await.map_err(|source| {
        crate::cluster::ClusterError::EngineOperation {
            message: format!("toxiproxy get proxy {proxy_name}"),
            source: Box::new(source),
        }
    })?;
    if enabled {
        proxy
            .enable()
            .await
            .map_err(|source| crate::cluster::ClusterError::EngineOperation {
                message: format!("toxiproxy enable {proxy_name}"),
                source: Box::new(source),
            })?;
    } else {
        proxy
            .disable()
            .await
            .map_err(|source| crate::cluster::ClusterError::EngineOperation {
                message: format!("toxiproxy disable {proxy_name}"),
                source: Box::new(source),
            })?;
    }
    Ok(())
}

pub(crate) async fn mesh_set_edges_enabled_for_node(
    mesh: &MongoInterNodeMeshState,
    node_index: usize,
    enabled: bool,
) -> Result<(), crate::cluster::ClusterError> {
    for other in 0..mesh.node_count {
        if other == node_index {
            continue;
        }
        let name_ij = proxy_name_for_edge(mesh.suite_key.as_str(), node_index, other);
        let name_ji = proxy_name_for_edge(mesh.suite_key.as_str(), other, node_index);
        toxiproxy_set_proxy_enabled(mesh.toxiproxy_api_base.as_str(), name_ij.as_str(), enabled)
            .await?;
        toxiproxy_set_proxy_enabled(mesh.toxiproxy_api_base.as_str(), name_ji.as_str(), enabled)
            .await?;
    }
    Ok(())
}

pub(crate) async fn mesh_set_partition(
    mesh: &MongoInterNodeMeshState,
    a: usize,
    b: usize,
    enabled: bool,
) -> Result<(), crate::cluster::ClusterError> {
    let name_ab = proxy_name_for_edge(mesh.suite_key.as_str(), a, b);
    let name_ba = proxy_name_for_edge(mesh.suite_key.as_str(), b, a);
    toxiproxy_set_proxy_enabled(mesh.toxiproxy_api_base.as_str(), name_ab.as_str(), enabled)
        .await?;
    toxiproxy_set_proxy_enabled(mesh.toxiproxy_api_base.as_str(), name_ba.as_str(), enabled)
        .await?;
    Ok(())
}

pub(crate) async fn mesh_enable_all(
    mesh: &MongoInterNodeMeshState,
) -> Result<(), crate::cluster::ClusterError> {
    for name in &mesh.proxy_names {
        toxiproxy_set_proxy_enabled(mesh.toxiproxy_api_base.as_str(), name.as_str(), true).await?;
    }
    Ok(())
}

pub(crate) async fn clear_mongo_inter_node_mesh_state() {
    let mut guard = mongo_inter_node_mesh_mutex().lock().await;
    *guard = None;
}
