use crate::error::{InfrastructureError, ScenarioId};
use crate::resources::{sanitize_sql_database_name, Resource, ResourceHandle};
use async_trait::async_trait;
use neo4rs::{query, ConfigBuilder, Graph};
use std::sync::OnceLock;
use testcontainers::core::{ImageExt, IntoContainerPort};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::neo4j::{Neo4j, Neo4jImage};
use tokio::sync::Mutex;

const NEO4J_BOLT_PORT: u16 = 7687;
const NEO4J_HTTP_PORT: u16 = 7474;
/// Default image tag: Enterprise edition so each scenario can use a separate database on one broker.
pub const DEFAULT_NEO4J_IMAGE_TAG: &str = "5-enterprise";
const SUITE_USER: &str = "neo4j";
const SUITE_PASSWORD: &str = "shakedown";

#[allow(clippy::large_enum_variant)]
enum SuiteNeo4jState {
    Idle,
    Running {
        host: String,
        bolt_port: u16,
        http_port: u16,
        container: testcontainers::ContainerAsync<Neo4jImage>,
        refs: usize,
        image_key: String,
    },
}

static SUITE_NEO4J: OnceLock<Mutex<SuiteNeo4jState>> = OnceLock::new();

fn suite_neo4j_mutex() -> &'static Mutex<SuiteNeo4jState> {
    SUITE_NEO4J.get_or_init(|| Mutex::new(SuiteNeo4jState::Idle))
}

fn image_fingerprint(tag_override: &Option<String>) -> String {
    let tag = tag_override.as_deref().unwrap_or(DEFAULT_NEO4J_IMAGE_TAG);
    format!("neo4j:{tag}")
}

fn bolt_uri(host: &str, bolt_port: u16) -> String {
    format!("bolt://{host}:{bolt_port}")
}

fn map_neo_err(error: neo4rs::Error) -> InfrastructureError {
    InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
}

async fn graph_for_database(
    host: &str,
    bolt_port: u16,
    database: &str,
) -> Result<Graph, InfrastructureError> {
    let uri = bolt_uri(host, bolt_port);
    let configuration = ConfigBuilder::default()
        .uri(uri)
        .user(SUITE_USER)
        .password(SUITE_PASSWORD)
        .db(database)
        .build()
        .map_err(|error| InfrastructureError::Provision {
            resource: "neo4j".into(),
            source: Box::new(std::io::Error::other(error.to_string())),
        })?;
    Graph::connect(configuration).await.map_err(map_neo_err)
}

async fn graph_system(host: &str, bolt_port: u16) -> Result<Graph, InfrastructureError> {
    graph_for_database(host, bolt_port, "system").await
}

async fn run_return_one_system(host: &str, bolt_port: u16) -> Result<(), InfrastructureError> {
    let graph = graph_system(host, bolt_port).await?;
    let mut rows = graph
        .execute(query("RETURN 1 AS n"))
        .await
        .map_err(map_neo_err)?;
    let row_opt = rows.next().await.map_err(map_neo_err)?;
    let Some(row) = row_opt else {
        return Err(InfrastructureError::Readiness {
            resource: "neo4j".into(),
            source: Box::new(std::io::Error::other(
                "Neo4j readiness: RETURN 1 produced no row",
            )),
        });
    };
    let _: i64 = row
        .get("n")
        .map_err(|error| InfrastructureError::Readiness {
            resource: "neo4j".into(),
            source: Box::new(std::io::Error::other(error.to_string())),
        })?;
    Ok(())
}

/// Docker-backed Neo4j using testcontainers-modules (`neo4j` image).
///
/// **Suite lifecycle:** one server per process; per-scenario **database** via `CREATE DATABASE`
/// (Neo4j 5 **Enterprise**). Default tag **`5-enterprise`** with license acceptance env.
/// Override with [`Self::with_image_tag`] or `SHAKEDOWN_NEO4J_IMAGE_TAG`. Community Edition images
/// do not support multiple databases — provisioning fails unless you use a single shared DB
/// (not supported by this resource).
#[derive(Debug)]
pub struct Neo4jResource {
    image_tag: Option<String>,
    holds_suite_ref: bool,
    host: Option<String>,
    bolt_port: Option<u16>,
    http_port: Option<u16>,
    username: String,
    password: String,
    database_name: String,
}

impl Default for Neo4jResource {
    fn default() -> Self {
        Self::new()
    }
}

impl Neo4jResource {
    /// Create default Docker-backed Neo4j settings (suite server started on first provision).
    pub fn new() -> Self {
        Self {
            image_tag: Some(DEFAULT_NEO4J_IMAGE_TAG.to_string()),
            holds_suite_ref: false,
            host: None,
            bolt_port: None,
            http_port: None,
            username: SUITE_USER.to_string(),
            password: SUITE_PASSWORD.to_string(),
            database_name: String::new(),
        }
        .with_image_tag_from_env()
    }

    /// Override the image tag (e.g. `5-enterprise` or a digest-pinned tag). Image name stays `neo4j`.
    pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
        self.image_tag = Some(tag.into());
        self
    }

    /// Apply tag override from `SHAKEDOWN_NEO4J_IMAGE_TAG` when set (non-empty).
    pub fn with_image_tag_from_env(mut self) -> Self {
        if let Ok(tag) = std::env::var("SHAKEDOWN_NEO4J_IMAGE_TAG") {
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
}

#[async_trait]
impl Resource for Neo4jResource {
    async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        if self.holds_suite_ref {
            return Err(InfrastructureError::AlreadyProvisioned("neo4j".into()));
        }

        let database_name = sanitize_sql_database_name(scenario_id.as_str());
        let want_key = image_fingerprint(&self.image_tag);

        let mut guard = suite_neo4j_mutex().lock().await;
        let (host, bolt_port, http_port) = match &mut *guard {
            SuiteNeo4jState::Idle => {
                let tag = self.image_tag.as_deref().unwrap_or(DEFAULT_NEO4J_IMAGE_TAG);
                let neo4j_image: Neo4jImage = Neo4j::new()
                    .with_version(tag.to_string())
                    .with_user(SUITE_USER)
                    .with_password(SUITE_PASSWORD)
                    .into();

                let node = neo4j_image
                    .with_env_var("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
                    .with_label(
                        crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                        crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                    )
                    .start()
                    .await
                    .map_err(Self::map_tc_err)?;

                let mapped_bolt = node
                    .get_host_port_ipv4(NEO4J_BOLT_PORT.tcp())
                    .await
                    .map_err(Self::map_tc_err)?;
                let mapped_http = node
                    .get_host_port_ipv4(NEO4J_HTTP_PORT.tcp())
                    .await
                    .map_err(Self::map_tc_err)?;
                let host = "127.0.0.1".to_string();

                *guard = SuiteNeo4jState::Running {
                    host: host.clone(),
                    bolt_port: mapped_bolt,
                    http_port: mapped_http,
                    container: node,
                    refs: 1,
                    image_key: want_key.clone(),
                };
                (host, mapped_bolt, mapped_http)
            }
            SuiteNeo4jState::Running {
                host,
                bolt_port,
                http_port,
                refs,
                image_key,
                ..
            } => {
                if *image_key != want_key {
                    return Err(InfrastructureError::Backend(Box::new(
                        std::io::Error::other(format!(
                            "Neo4j suite server already running as {image_key}; \
                             cannot start or join as {want_key}. Use one image:tag per process."
                        )),
                    )));
                }
                *refs += 1;
                (host.clone(), *bolt_port, *http_port)
            }
        };
        drop(guard);

        let system_graph = graph_system(host.as_str(), bolt_port)
            .await
            .map_err(|error| InfrastructureError::Provision {
                resource: "neo4j".into(),
                source: Box::new(std::io::Error::other(error.to_string())),
            })?;

        let create_db_cypher = format!("CREATE DATABASE `{database_name}` WAIT");
        if let Err(error) = system_graph.run(query(&create_db_cypher)).await {
            return Err(InfrastructureError::Provision {
                resource: "neo4j".into(),
                source: Box::new(std::io::Error::other(format!(
                    "{error} (hint: per-scenario databases require Neo4j Enterprise; \
                     override image with SHAKEDOWN_NEO4J_IMAGE_TAG=5-enterprise or use a \
                     Community image only if you accept degraded single-database isolation)"
                ))),
            });
        }

        self.holds_suite_ref = true;
        self.host = Some(host.clone());
        self.bolt_port = Some(bolt_port);
        self.http_port = Some(http_port);
        self.database_name = database_name.clone();

        let username = self.username.clone();
        let password = self.password.clone();
        let bolt_url = format!("bolt://{}:{}@{}:{}", username, password, host, bolt_port);
        let http_url = format!("http://{}:{}@{}:{}", username, password, host, http_port);

        Ok(vec![ResourceHandle {
            id: format!("neo4j_{scenario_id}"),
            connection_string: bolt_url.clone(),
            metadata: Some(serde_json::json!({
                "engine": "neo4j",
                "host": host,
                "port": bolt_port,
                "http_port": http_port,
                "database": database_name,
                "username": username,
                "password": password,
                "bolt_url": bolt_url,
                "http_url": http_url,
            })),
        }])
    }

    async fn check_ready(&self) -> Result<(), InfrastructureError> {
        let host = self
            .host
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned("neo4j".into()))?;
        let bolt_port = self
            .bolt_port
            .ok_or_else(|| InfrastructureError::NotProvisioned("neo4j".into()))?;

        const ATTEMPTS: u32 = 30;
        for attempt in 0..ATTEMPTS {
            match run_return_one_system(host, bolt_port).await {
                Ok(()) => return Ok(()),
                Err(error) if attempt + 1 == ATTEMPTS => return Err(error),
                Err(_) => {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
            }
        }
        Err(InfrastructureError::Readiness {
            resource: "neo4j".into(),
            source: Box::new(std::io::Error::other("Neo4j readiness: attempts exhausted")),
        })
    }

    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        if !self.holds_suite_ref {
            self.host = None;
            self.bolt_port = None;
            self.http_port = None;
            self.database_name.clear();
            return Ok(());
        }

        let database_name = self.database_name.clone();
        let host = self.host.clone().unwrap_or_default();
        let bolt_port = self.bolt_port.unwrap_or(NEO4J_BOLT_PORT);

        if !database_name.is_empty() {
            if let Ok(graph) = graph_system(host.as_str(), bolt_port).await {
                let drop_cypher = format!("DROP DATABASE `{database_name}` IF EXISTS");
                let _ = graph.run(query(&drop_cypher)).await;
            }
        }

        let mut guard = suite_neo4j_mutex().lock().await;
        let mut container_to_rm: Option<testcontainers::ContainerAsync<Neo4jImage>> = None;
        match &mut *guard {
            SuiteNeo4jState::Idle => {}
            SuiteNeo4jState::Running { refs, .. } => {
                *refs = refs.saturating_sub(1);
                if *refs == 0 {
                    let taken = std::mem::replace(&mut *guard, SuiteNeo4jState::Idle);
                    if let SuiteNeo4jState::Running { container, .. } = taken {
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
        self.bolt_port = None;
        self.http_port = None;
        self.database_name.clear();
        Ok(())
    }
}
