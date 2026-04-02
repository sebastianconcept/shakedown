//! Per-scenario PostgreSQL primary + streaming replica (`postgres-cluster` feature).
//!
//! Provisions **two** containers per scenario (suite sharing would break irreversible promotion —
//! D-9). A bridge network connects primary (`pg-primary`) and replica (`pg-replica`); the replica
//! data directory is populated with [`pg_basebackup`](https://www.postgresql.org/docs/current/app-pgbasebackup.html)
//! into a Docker named volume, then a replica container mounts that volume.
//!
//! [`PostgresBindings::connection_url`](crate::PostgresBindings) targets the **primary**; helpers
//! behave like standalone [`crate::PostgresResource`].

use std::time::Duration;

use async_trait::async_trait;
use bollard::exec::{CreateExecOptions, StartExecOptions, StartExecResults};
use bollard::models::VolumeCreateRequest;
use bollard::query_parameters::RemoveVolumeOptionsBuilder;
use bollard::Docker;
use futures_util::StreamExt;
use testcontainers::core::wait::ExitWaitStrategy;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::time::timeout;
use tokio_postgres::NoTls;

use crate::cluster::{create_cluster_network, remove_cluster_network};
use crate::docker_labels::{SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE};
use crate::error::{InfrastructureError, ResourceHandle, ScenarioId};
use crate::resources::{sanitize_sql_database_name, Resource};

pub(crate) const DEFAULT_POSTGRES_CLUSTER_IMAGE_TAG: &str = "16";
const POSTGRES_PORT: u16 = 5432;
const PRIMARY_HOSTNAME: &str = "pg-primary";
const REPLICA_HOSTNAME: &str = "pg-replica";
const PROVISION_TIMEOUT: Duration = Duration::from_secs(120);
const REPLICATION_USER: &str = "replicator";
const REPLICATION_PASSWORD: &str = "replicator";

fn map_tc_err(error: impl std::fmt::Display + Send + Sync + 'static) -> InfrastructureError {
    InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
}

fn map_bollard_err(error: bollard::errors::Error) -> InfrastructureError {
    InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
}

fn network_suite_id(scenario_id: &str) -> String {
    let mut segment = sanitize_sql_database_name(scenario_id);
    if segment.len() > 48 {
        segment.truncate(48);
    }
    segment
}

fn volume_name_for(scenario_id: &str) -> String {
    let suite = network_suite_id(scenario_id);
    format!(
        "shakedown_pgrepl_{}_{}",
        std::process::id(),
        suite.trim_start_matches("shakedown_")
    )
}

fn scenario_connection_url(
    host: &str,
    port: u16,
    database: &str,
    username: &str,
    password: &str,
) -> String {
    format!("postgresql://{username}:{password}@{host}:{port}/{database}")
}

/// Allow `pg_basebackup` from the Docker bridge subnet (`pg_hba` replication entries are not set
/// for remote hosts by the stock `postgres` image when using `POSTGRES_HOST_AUTH_METHOD=trust`).
async fn append_pg_hba_replication_trust(
    docker: &Docker,
    container_id: &str,
) -> Result<(), InfrastructureError> {
    let exec = docker
        .create_exec(
            container_id,
            CreateExecOptions {
                cmd: Some(vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    "echo 'host replication replicator 0.0.0.0/0 trust' >> /var/lib/postgresql/data/pg_hba.conf".to_string(),
                ]),
                attach_stdout: Some(true),
                attach_stderr: Some(true),
                ..Default::default()
            },
        )
        .await
        .map_err(map_bollard_err)?;

    match docker
        .start_exec(&exec.id, None::<StartExecOptions>)
        .await
        .map_err(map_bollard_err)?
    {
        StartExecResults::Attached { mut output, .. } => while output.next().await.is_some() {},
        StartExecResults::Detached => {}
    }
    Ok(())
}

async fn connect_with_url(url: &str) -> Result<tokio_postgres::Client, InfrastructureError> {
    let (client, connection) = tokio_postgres::connect(url, NoTls).await.map_err(|e| {
        InfrastructureError::Backend(Box::new(std::io::Error::other(e.to_string())))
    })?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

async fn wait_for_streaming_primary(
    primary_url: &str,
    deadline: tokio::time::Instant,
) -> Result<(), InfrastructureError> {
    loop {
        let client = connect_with_url(primary_url).await?;
        let row = client
            .query_one(
                "SELECT COUNT(*)::bigint AS c FROM pg_stat_replication WHERE state = 'streaming'",
                &[],
            )
            .await
            .map_err(|e| InfrastructureError::Provision {
                resource: "postgres-cluster".into(),
                source: Box::new(std::io::Error::other(e.to_string())),
            })?;
        let count: i64 = row.get("c");
        if count >= 1 {
            return Ok(());
        }
        if tokio::time::Instant::now() > deadline {
            return Err(InfrastructureError::Provision {
                resource: "postgres-cluster".into(),
                source: Box::new(std::io::Error::other(
                    "timed out waiting for streaming replication from replica",
                )),
            });
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Docker-backed **primary + streaming replica** PostgreSQL topology (per-scenario containers).
///
/// D-9: containers are **not** suite-shared — each scenario provisions fresh primary and replica so
/// future promotion/failover tests stay meaningful.
#[derive(Debug)]
pub struct PostgresReplicaSetResource {
    image_tag: Option<String>,
    holds_provision: bool,
    network_name: Option<String>,
    volume_name: Option<String>,
    primary: Option<testcontainers::ContainerAsync<GenericImage>>,
    replica: Option<testcontainers::ContainerAsync<GenericImage>>,
    primary_host: String,
    primary_port: u16,
    replica_port: u16,
    database_name: String,
    username: String,
    password: String,
}

impl Default for PostgresReplicaSetResource {
    fn default() -> Self {
        Self::new()
    }
}

impl PostgresReplicaSetResource {
    /// New resource with default image tag [`DEFAULT_POSTGRES_CLUSTER_IMAGE_TAG`].
    pub fn new() -> Self {
        Self {
            image_tag: Some(DEFAULT_POSTGRES_CLUSTER_IMAGE_TAG.to_string()),
            holds_provision: false,
            network_name: None,
            volume_name: None,
            primary: None,
            replica: None,
            primary_host: "127.0.0.1".to_string(),
            primary_port: 0,
            replica_port: 0,
            database_name: String::new(),
            username: "postgres".to_string(),
            password: "postgres".to_string(),
        }
    }

    /// Override the image tag (`postgres:<tag>`).
    pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
        self.image_tag = Some(tag.into());
        self
    }

    /// Apply tag from `SHAKEDOWN_POSTGRES_CLUSTER_IMAGE_TAG` when set.
    pub fn with_image_tag_from_env(mut self) -> Self {
        if let Ok(tag) = std::env::var("SHAKEDOWN_POSTGRES_CLUSTER_IMAGE_TAG") {
            let tag = tag.trim();
            if !tag.is_empty() {
                self.image_tag = Some(tag.to_string());
            }
        }
        self
    }
}

#[async_trait]
impl Resource for PostgresReplicaSetResource {
    async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        if self.holds_provision {
            return Err(InfrastructureError::AlreadyProvisioned(
                "postgres-cluster".into(),
            ));
        }

        let database_name = sanitize_sql_database_name(scenario_id.as_str());
        let tag_owned = self
            .image_tag
            .clone()
            .unwrap_or_else(|| DEFAULT_POSTGRES_CLUSTER_IMAGE_TAG.to_string());

        timeout(
            PROVISION_TIMEOUT,
            provision_inner(self, scenario_id, &database_name, tag_owned.as_str()),
        )
        .await
        .map_err(|_| InfrastructureError::Provision {
            resource: "postgres-cluster".into(),
            source: Box::new(std::io::Error::other(format!(
                "postgres replica set provisioning exceeded {:?}",
                PROVISION_TIMEOUT
            ))),
        })?
    }

    async fn check_ready(&self) -> Result<(), InfrastructureError> {
        if !self.holds_provision {
            return Err(InfrastructureError::NotProvisioned(
                "postgres-cluster".into(),
            ));
        }
        let url = scenario_connection_url(
            self.primary_host.as_str(),
            self.primary_port,
            self.database_name.as_str(),
            self.username.as_str(),
            self.password.as_str(),
        );
        let client = connect_with_url(&url)
            .await
            .map_err(|e| InfrastructureError::Readiness {
                resource: "postgres-cluster".into(),
                source: Box::new(std::io::Error::other(e.to_string())),
            })?;
        client
            .query_one("SELECT 1", &[])
            .await
            .map_err(|e| InfrastructureError::Readiness {
                resource: "postgres-cluster".into(),
                source: Box::new(std::io::Error::other(e.to_string())),
            })?;

        let primary_url = scenario_connection_url(
            self.primary_host.as_str(),
            self.primary_port,
            "postgres",
            self.username.as_str(),
            self.password.as_str(),
        );
        wait_for_streaming_primary(
            primary_url.as_str(),
            tokio::time::Instant::now() + Duration::from_secs(60),
        )
        .await
    }

    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        if !self.holds_provision {
            self.database_name.clear();
            return Ok(());
        }

        let network_name = self.network_name.take();
        let volume_name = self.volume_name.take();

        if let Some(replica) = self.replica.take() {
            let _ = replica.stop_with_timeout(Some(30)).await;
            let _ = replica.rm().await;
        }
        if let Some(primary) = self.primary.take() {
            let _ = primary.stop_with_timeout(Some(30)).await;
            let _ = primary.rm().await;
        }

        let docker =
            Docker::connect_with_defaults().map_err(|e| InfrastructureError::Teardown {
                resource: "postgres-cluster".into(),
                source: Box::new(std::io::Error::other(e.to_string())),
            })?;

        if let Some(ref volume_name) = volume_name {
            let _ = docker
                .remove_volume(
                    volume_name.as_str(),
                    Some(RemoveVolumeOptionsBuilder::default().force(true).build()),
                )
                .await;
        }

        if let Some(ref name) = network_name {
            let _ = remove_cluster_network(&docker, name.as_str())
                .await
                .map_err(|e| InfrastructureError::Teardown {
                    resource: "postgres-cluster".into(),
                    source: Box::new(std::io::Error::other(e.to_string())),
                });
        }

        self.holds_provision = false;
        self.primary_port = 0;
        self.replica_port = 0;
        self.database_name.clear();
        Ok(())
    }
}

async fn provision_inner(
    resource: &mut PostgresReplicaSetResource,
    scenario_id: &ScenarioId,
    database_name: &str,
    tag: &str,
) -> Result<Vec<ResourceHandle>, InfrastructureError> {
    let docker = Docker::connect_with_defaults().map_err(|e| InfrastructureError::Provision {
        resource: "postgres-cluster".into(),
        source: Box::new(std::io::Error::other(e.to_string())),
    })?;

    let suite_segment = network_suite_id(scenario_id.as_str());
    let network_name = create_cluster_network(&docker, "pgrepl", suite_segment.as_str())
        .await
        .map_err(|e| InfrastructureError::Provision {
            resource: "postgres-cluster".into(),
            source: Box::new(std::io::Error::other(e.to_string())),
        })?;

    let volume_name = volume_name_for(scenario_id.as_str());
    docker
        .create_volume(VolumeCreateRequest {
            name: Some(volume_name.clone()),
            ..Default::default()
        })
        .await
        .map_err(map_bollard_err)?;

    let primary_image = GenericImage::new("postgres", tag)
        .with_exposed_port(POSTGRES_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout(
            "PostgreSQL init process complete; ready for start up.",
        ))
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "postgres",
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=10",
            "-c",
            "max_replication_slots=10",
            "-c",
            "hot_standby=on",
            "-c",
            "listen_addresses=*",
        ])
        .with_network(network_name.as_str())
        .with_hostname(PRIMARY_HOSTNAME)
        .with_label(SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE);

    let primary = primary_image.start().await.map_err(map_tc_err)?;

    let primary_port = primary
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .map_err(map_tc_err)?;

    let primary_host = "127.0.0.1".to_string();
    let admin_url = scenario_connection_url(
        primary_host.as_str(),
        primary_port,
        "postgres",
        "postgres",
        "postgres",
    );

    append_pg_hba_replication_trust(&docker, primary.id()).await?;

    let admin_client = connect_with_url(admin_url.as_str()).await?;
    admin_client
        .batch_execute("SELECT pg_reload_conf()")
        .await
        .map_err(|e| InfrastructureError::Provision {
            resource: "postgres-cluster".into(),
            source: Box::new(std::io::Error::other(e.to_string())),
        })?;
    let create_db_sql = format!("CREATE DATABASE {database_name}");
    admin_client
        .batch_execute(&create_db_sql)
        .await
        .map_err(|e| InfrastructureError::Provision {
            resource: "postgres-cluster".into(),
            source: Box::new(std::io::Error::other(e.to_string())),
        })?;

    let create_role_sql = format!(
        "CREATE USER {REPLICATION_USER} WITH REPLICATION PASSWORD '{REPLICATION_PASSWORD}'"
    );
    admin_client
        .batch_execute(&create_role_sql)
        .await
        .map_err(|e| InfrastructureError::Provision {
            resource: "postgres-cluster".into(),
            source: Box::new(std::io::Error::other(e.to_string())),
        })?;

    let basebackup_image = GenericImage::new("postgres", tag)
        .with_wait_for(WaitFor::exit(ExitWaitStrategy::new().with_exit_code(0)))
        .with_entrypoint("pg_basebackup")
        .with_cmd([
            "-h",
            PRIMARY_HOSTNAME,
            "-p",
            "5432",
            "-U",
            REPLICATION_USER,
            "-D",
            "/var/lib/postgresql/data",
            "-Fp",
            "-Xs",
            "-R",
            "-P",
        ])
        .with_env_var("PGPASSWORD", REPLICATION_PASSWORD)
        .with_mount(Mount::volume_mount(
            volume_name.as_str(),
            "/var/lib/postgresql/data",
        ))
        .with_network(network_name.as_str())
        .with_label(SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE)
        .with_startup_timeout(Duration::from_secs(180));

    let basebackup = basebackup_image.start().await.map_err(map_tc_err)?;
    let _ = basebackup.rm().await;

    let replica_image = GenericImage::new("postgres", tag)
        .with_exposed_port(POSTGRES_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept read-only connections",
        ))
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_mount(Mount::volume_mount(
            volume_name.as_str(),
            "/var/lib/postgresql/data",
        ))
        .with_network(network_name.as_str())
        .with_hostname(REPLICA_HOSTNAME)
        .with_label(SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE)
        .with_startup_timeout(Duration::from_secs(180));

    let replica = replica_image.start().await.map_err(map_tc_err)?;
    let replica_port = replica
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .map_err(map_tc_err)?;

    let primary_url = scenario_connection_url(
        primary_host.as_str(),
        primary_port,
        "postgres",
        "postgres",
        "postgres",
    );
    wait_for_streaming_primary(
        primary_url.as_str(),
        tokio::time::Instant::now() + Duration::from_secs(90),
    )
    .await?;

    let primary_container_id = primary.id().to_string();
    let replica_container_id = replica.id().to_string();

    let connection_url = scenario_connection_url(
        primary_host.as_str(),
        primary_port,
        database_name,
        "postgres",
        "postgres",
    );

    resource.holds_provision = true;
    resource.network_name = Some(network_name);
    resource.volume_name = Some(volume_name);
    resource.primary = Some(primary);
    resource.replica = Some(replica);
    resource.primary_host = primary_host.clone();
    resource.primary_port = primary_port;
    resource.replica_port = replica_port;
    resource.database_name = database_name.to_string();

    Ok(vec![ResourceHandle {
        id: format!("postgres_rs_{}", scenario_id.as_str()),
        connection_string: connection_url.clone(),
        metadata: Some(serde_json::json!({
            "host": primary_host,
            "port": primary_port,
            "database": database_name,
            "username": "postgres",
            "password": "postgres",
            "connection_url": connection_url,
            "engine": "postgres",
            "postgres_cluster": "primary_replica",
            "replica_port": replica_port,
            "primary_container_id": primary_container_id,
            "replica_container_id": replica_container_id,
        })),
    }])
}
