use crate::error::{InfrastructureError, ScenarioId};
use crate::resources::{sanitize_sql_database_name, Resource, ResourceHandle};
use async_trait::async_trait;
use std::sync::OnceLock;
use testcontainers::core::IntoContainerPort;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::GenericImage;
use testcontainers::ImageExt;
use tokio::sync::Mutex;

const RESOURCE_NAME: &str = "influxdb";
/// Default image repository (`influxdb`).
pub const DEFAULT_INFLUXDB_IMAGE_REPOSITORY: &str = "influxdb";
/// Default image tag (`2` — InfluxDB 2.x).
pub const DEFAULT_INFLUXDB_IMAGE_TAG: &str = "2";
const INFLUX_HTTP_PORT: u16 = 8086;
/// Fixed org name from container init (OQ-4.1 / SPECS).
pub const SUITE_ORGANIZATION: &str = "shakedown";
/// Fixed admin token from `DOCKER_INFLUXDB_INIT_ADMIN_TOKEN` (OQ-4.1).
pub const SUITE_ADMIN_TOKEN: &str = "shakedown-admin-token";

#[allow(clippy::large_enum_variant)]
enum SuiteInfluxDbState {
    Idle,
    Running {
        host: String,
        port: u16,
        container: testcontainers::ContainerAsync<GenericImage>,
        refs: usize,
        image_key: String,
    },
}

static SUITE_INFLUXDB: OnceLock<Mutex<SuiteInfluxDbState>> = OnceLock::new();

fn suite_influxdb_mutex() -> &'static Mutex<SuiteInfluxDbState> {
    SUITE_INFLUXDB.get_or_init(|| Mutex::new(SuiteInfluxDbState::Idle))
}

fn image_fingerprint(repository: &str, tag_override: &Option<String>) -> String {
    let tag = tag_override
        .as_deref()
        .map(str::trim)
        .filter(|tag| !tag.is_empty())
        .unwrap_or(DEFAULT_INFLUXDB_IMAGE_TAG);
    format!("{repository}:{tag}")
}

fn authorization_value(token: &str) -> String {
    format!("Token {token}")
}

async fn influx_health_passes(base_url: &str) -> Result<(), String> {
    let client = reqwest::Client::new();
    let url = format!("{}/health", base_url.trim_end_matches('/'));
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    if !response.status().is_success() {
        return Err(format!("GET /health returned HTTP {}", response.status()));
    }
    let body: serde_json::Value = response.json().await.map_err(|error| error.to_string())?;
    let status = body
        .get("status")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    if status != "pass" {
        return Err(format!("influx health status is not pass: {body}"));
    }
    Ok(())
}

async fn fetch_organization_id(
    base_url: &str,
    admin_token: &str,
    org_name: &str,
) -> Result<String, String> {
    let client = reqwest::Client::new();
    let url = format!(
        "{}/api/v2/orgs?org={}",
        base_url.trim_end_matches('/'),
        org_name
    );
    let response = client
        .get(&url)
        .header(
            reqwest::header::AUTHORIZATION,
            authorization_value(admin_token),
        )
        .send()
        .await
        .map_err(|error| error.to_string())?;
    if !response.status().is_success() {
        return Err(format!(
            "GET /api/v2/orgs returned HTTP {}",
            response.status()
        ));
    }
    let body: serde_json::Value = response.json().await.map_err(|error| error.to_string())?;
    let org_id = body
        .get("orgs")
        .and_then(|orgs| orgs.as_array())
        .and_then(|items| items.first())
        .and_then(|org| org.get("id"))
        .and_then(|id| id.as_str())
        .ok_or_else(|| format!("orgs response missing id: {body}"))?;
    Ok(org_id.to_string())
}

async fn create_bucket(
    base_url: &str,
    admin_token: &str,
    organization_id: &str,
    bucket_name: &str,
) -> Result<String, String> {
    let client = reqwest::Client::new();
    let url = format!("{}/api/v2/buckets", base_url.trim_end_matches('/'));
    let payload = serde_json::json!({
        "orgID": organization_id,
        "name": bucket_name,
        "retentionRules": []
    });
    let response = client
        .post(&url)
        .header(
            reqwest::header::AUTHORIZATION,
            authorization_value(admin_token),
        )
        .json(&payload)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        return Err(format!("POST /api/v2/buckets failed: HTTP {status} {text}",));
    }
    let body: serde_json::Value = response.json().await.map_err(|error| error.to_string())?;
    let bucket_id = body
        .get("id")
        .and_then(|id| id.as_str())
        .ok_or_else(|| format!("bucket create response missing id: {body}"))?;
    Ok(bucket_id.to_string())
}

async fn delete_bucket_by_id(
    base_url: &str,
    admin_token: &str,
    bucket_id: &str,
) -> Result<(), String> {
    let client = reqwest::Client::new();
    let url = format!(
        "{}/api/v2/buckets/{}",
        base_url.trim_end_matches('/'),
        bucket_id
    );
    let response = client
        .delete(&url)
        .header(
            reqwest::header::AUTHORIZATION,
            authorization_value(admin_token),
        )
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        return Err(format!(
            "DELETE /api/v2/buckets failed: HTTP {status} {text}",
        ));
    }
    Ok(())
}

/// Docker-backed InfluxDB 2.x (`influxdb:2` via [`GenericImage`]).
///
/// **Suite lifecycle:** one node per process; per-scenario bucket via HTTP API; refcounted teardown
/// deletes the bucket then may remove the container.
///
/// Default tag **`2`**. Override with [`Self::with_image_tag`] or **`SHAKEDOWN_INFLUXDB_IMAGE_TAG`**
/// via [`Self::with_image_tag_from_env`]. Container init uses fixed org **`shakedown`** and admin
/// token **`shakedown-admin-token`** (see `DOCKER_INFLUXDB_INIT_*` env in [`Resource::provision`]).
#[derive(Debug)]
pub struct InfluxDbResource {
    image_repository: String,
    image_tag: Option<String>,
    holds_suite_ref: bool,
    host: Option<String>,
    port: Option<u16>,
    /// API base URL, e.g. `http://127.0.0.1:8086`.
    url: String,
    organization: String,
    token: String,
    bucket_name: String,
    bucket_id: String,
}

impl Default for InfluxDbResource {
    fn default() -> Self {
        Self::new()
    }
}

impl InfluxDbResource {
    /// Create default settings (suite container started on first provision).
    pub fn new() -> Self {
        Self {
            image_repository: DEFAULT_INFLUXDB_IMAGE_REPOSITORY.to_string(),
            image_tag: Some(DEFAULT_INFLUXDB_IMAGE_TAG.to_string()),
            holds_suite_ref: false,
            host: None,
            port: None,
            url: String::new(),
            organization: SUITE_ORGANIZATION.to_string(),
            token: SUITE_ADMIN_TOKEN.to_string(),
            bucket_name: String::new(),
            bucket_id: String::new(),
        }
    }

    /// Override the image tag. Repository stays [`DEFAULT_INFLUXDB_IMAGE_REPOSITORY`].
    pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
        self.image_tag = Some(tag.into());
        self
    }

    /// Apply tag override from **`SHAKEDOWN_INFLUXDB_IMAGE_TAG`** when set (e.g. `2` or `2.7`).
    pub fn with_image_tag_from_env(mut self) -> Self {
        if let Ok(tag) = std::env::var("SHAKEDOWN_INFLUXDB_IMAGE_TAG") {
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
impl Resource for InfluxDbResource {
    async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        if self.holds_suite_ref {
            return Err(InfrastructureError::AlreadyProvisioned(
                RESOURCE_NAME.into(),
            ));
        }

        let bucket_name = sanitize_sql_database_name(scenario_id.as_str());
        let want_key = image_fingerprint(&self.image_repository, &self.image_tag);

        let mut guard = suite_influxdb_mutex().lock().await;
        let (host, port) = match &mut *guard {
            SuiteInfluxDbState::Idle => {
                let repository = self.image_repository.as_str();
                let tag = self
                    .image_tag
                    .as_deref()
                    .map(str::trim)
                    .filter(|tag| !tag.is_empty())
                    .unwrap_or(DEFAULT_INFLUXDB_IMAGE_TAG);
                let start_image = GenericImage::new(repository, tag)
                    .with_exposed_port(INFLUX_HTTP_PORT.tcp())
                    .with_wait_for(WaitFor::seconds(20))
                    .with_env_var("DOCKER_INFLUXDB_INIT_MODE", "setup")
                    .with_env_var("DOCKER_INFLUXDB_INIT_ORG", SUITE_ORGANIZATION)
                    .with_env_var("DOCKER_INFLUXDB_INIT_BUCKET", "default")
                    .with_env_var("DOCKER_INFLUXDB_INIT_USERNAME", "admin")
                    .with_env_var("DOCKER_INFLUXDB_INIT_PASSWORD", "shakedown123")
                    .with_env_var("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN", SUITE_ADMIN_TOKEN)
                    .with_label(
                        crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                        crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                    );
                let node = start_image.start().await.map_err(Self::map_tc_err)?;

                let mapped_port = node
                    .get_host_port_ipv4(INFLUX_HTTP_PORT)
                    .await
                    .map_err(Self::map_tc_err)?;
                let host = "127.0.0.1".to_string();

                *guard = SuiteInfluxDbState::Running {
                    host: host.clone(),
                    port: mapped_port,
                    container: node,
                    refs: 1,
                    image_key: want_key.clone(),
                };
                (host, mapped_port)
            }
            SuiteInfluxDbState::Running {
                host,
                port,
                refs,
                image_key,
                ..
            } => {
                if *image_key != want_key {
                    return Err(InfrastructureError::Backend(Box::new(
                        std::io::Error::other(format!(
                            "InfluxDB suite already running as {image_key}; \
                             cannot start or join as {want_key}. Use one image:tag per process."
                        )),
                    )));
                }
                *refs += 1;
                (host.clone(), *port)
            }
        };
        drop(guard);

        let base_url = format!("http://{host}:{port}");
        let organization_id =
            fetch_organization_id(base_url.as_str(), SUITE_ADMIN_TOKEN, SUITE_ORGANIZATION)
                .await
                .map_err(|message| InfrastructureError::Provision {
                    resource: RESOURCE_NAME.into(),
                    source: Box::new(std::io::Error::other(message)),
                })?;

        let bucket_id = create_bucket(
            base_url.as_str(),
            SUITE_ADMIN_TOKEN,
            organization_id.as_str(),
            bucket_name.as_str(),
        )
        .await
        .map_err(|message| InfrastructureError::Provision {
            resource: RESOURCE_NAME.into(),
            source: Box::new(std::io::Error::other(message)),
        })?;

        self.holds_suite_ref = true;
        self.host = Some(host.clone());
        self.port = Some(port);
        self.url = base_url.clone();
        self.organization = SUITE_ORGANIZATION.to_string();
        self.token = SUITE_ADMIN_TOKEN.to_string();
        self.bucket_name = bucket_name.clone();
        self.bucket_id = bucket_id.clone();

        Ok(vec![ResourceHandle {
            id: format!("influxdb_{scenario_id}"),
            connection_string: base_url.clone(),
            metadata: Some(serde_json::json!({
                "host": host,
                "port": port,
                "url": base_url,
                "token": SUITE_ADMIN_TOKEN,
                "organization": SUITE_ORGANIZATION,
                "bucket": bucket_name,
                "engine": "influxdb",
            })),
        }])
    }

    async fn check_ready(&self) -> Result<(), InfrastructureError> {
        let _ = self
            .host
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
        let _ = self
            .port
            .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;

        influx_health_passes(self.url.as_str())
            .await
            .map_err(|message| InfrastructureError::Readiness {
                resource: RESOURCE_NAME.into(),
                source: Box::new(std::io::Error::other(message)),
            })
    }

    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        if !self.holds_suite_ref {
            self.host = None;
            self.port = None;
            self.url.clear();
            self.organization.clear();
            self.token.clear();
            self.bucket_name.clear();
            self.bucket_id.clear();
            return Ok(());
        }

        let base_url = self.url.clone();
        let bucket_id = self.bucket_id.clone();
        let token = self.token.clone();

        if !bucket_id.is_empty() && !base_url.is_empty() {
            if let Err(message) =
                delete_bucket_by_id(base_url.as_str(), token.as_str(), bucket_id.as_str()).await
            {
                return Err(InfrastructureError::Teardown {
                    resource: RESOURCE_NAME.into(),
                    source: Box::new(std::io::Error::other(message)),
                });
            }
        }

        let mut guard = suite_influxdb_mutex().lock().await;
        let mut container_to_rm: Option<testcontainers::ContainerAsync<GenericImage>> = None;
        match &mut *guard {
            SuiteInfluxDbState::Idle => {}
            SuiteInfluxDbState::Running { refs, .. } => {
                *refs = refs.saturating_sub(1);
                if *refs == 0 {
                    let taken = std::mem::replace(&mut *guard, SuiteInfluxDbState::Idle);
                    if let SuiteInfluxDbState::Running { container, .. } = taken {
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
        self.url.clear();
        self.organization.clear();
        self.token.clear();
        self.bucket_name.clear();
        self.bucket_id.clear();
        Ok(())
    }
}
