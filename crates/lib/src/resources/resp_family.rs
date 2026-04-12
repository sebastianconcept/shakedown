//! Shared [`Resource`] implementation for Redis and Valkey (RESP on port 6379).
//!
//! [`RespResource`] is generic over a [`testcontainers::core::Image`] so the `redis` and
//! `valkey` features each supply their own image while reusing one code path.
//!
//! One Docker container is started per test **process** and shared
//! across scenarios. Isolation is logical—each scenario gets a distinct key prefix via
//! [`sanitize_redis_key_prefix`]—so you avoid paying a new container for every scenario while
//! keeping data apart.
//!
//! A process-static [`Mutex`] wraps [`SuiteState`]: first provision starts the container
//! and sets `refs = 1`; further provisions for the same `image:name:tag` increment `refs`.
//! [`Resource::teardown`] deletes keys matching this scenario’s prefix ([`delete_keys_with_prefix`]
//! uses `SCAN` + `DEL`), decrements `refs`, and removes the container when the last reference drops.
//! Mismatched image fingerprints while the Shakedown server is running return an error (one image per
//! process).

use super::user_seed::RedisUserSeedFn;
use super::{map_seed_error, sanitize_redis_key_prefix, RedisBindings, Resource, ResourceHandle};
use crate::error::{InfrastructureError, ScenarioId};
use async_trait::async_trait;
use testcontainers::core::Image;
use testcontainers::core::ImageExt;
use testcontainers::runners::AsyncRunner;
use tokio::sync::Mutex;

const RESP_PORT: u16 = 6379;

#[allow(clippy::large_enum_variant)]
pub(crate) enum SuiteState<I: Image> {
    Idle,
    Running {
        host: String,
        port: u16,
        container: testcontainers::ContainerAsync<I>,
        refs: usize,
        image_key: String,
    },
}

/// Suite-shared RESP server (Redis or Valkey image) with per-scenario key prefixes.
pub struct RespResource<I>
where
    I: Image + Clone + Send + Sync + std::fmt::Debug + 'static,
{
    pub image: I,
    pub image_tag: Option<String>,
    pub suite: &'static Mutex<SuiteState<I>>,
    pub engine: &'static str,
    pub env_tag_var: &'static str,
    /// When set and `image_tag` is unset, use this instead of [`Image::tag`] (pins Redis to `7`).
    pub hardcoded_default_tag: Option<&'static str>,
    holds_suite_ref: bool,
    host: Option<String>,
    port: Option<u16>,
    connection_url: Option<String>,
    key_prefix: String,
    user_seed: Option<RedisUserSeedFn>,
}

impl<I: Image + Clone + Send + Sync + std::fmt::Debug + 'static> std::fmt::Debug
    for RespResource<I>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RespResource")
            .field("engine", &self.engine)
            .field("image_tag", &self.image_tag)
            .field("user_seed", &self.user_seed.as_ref().map(|_| "<closure>"))
            .finish_non_exhaustive()
    }
}

impl<I: Image + Clone + Send + Sync + std::fmt::Debug + 'static> RespResource<I> {
    pub(crate) fn new(
        image: I,
        suite: &'static Mutex<SuiteState<I>>,
        engine: &'static str,
        env_tag_var: &'static str,
        hardcoded_default_tag: Option<&'static str>,
    ) -> Self {
        Self {
            image,
            image_tag: None,
            suite,
            engine,
            env_tag_var,
            hardcoded_default_tag,
            holds_suite_ref: false,
            host: None,
            port: None,
            connection_url: None,
            key_prefix: String::new(),
            user_seed: None,
        }
    }

    /// User-provided async seed ([`Resource::apply_default_seed`]) after readiness.
    pub fn set_seed(&mut self, seed: RedisUserSeedFn) {
        self.user_seed = Some(seed);
    }

    fn redis_bindings(&self) -> Result<RedisBindings, InfrastructureError> {
        let host = self
            .host
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned(self.engine.into()))?;
        let port = self
            .port
            .ok_or_else(|| InfrastructureError::NotProvisioned(self.engine.into()))?;
        let connection_url = self
            .connection_url
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned(self.engine.into()))?;
        Ok(RedisBindings {
            connection_url: connection_url.to_string(),
            host: host.to_string(),
            port,
            key_prefix: self.key_prefix.clone(),
        })
    }

    /// Override the image tag (full `name:tag` is `Image::name` + this tag).
    pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
        self.image_tag = Some(tag.into());
        self
    }

    /// Load tag override from `self.env_tag_var` when set (e.g. `SHAKEDOWN_REDIS_IMAGE_TAG`).
    pub fn with_image_tag_from_env(mut self) -> Self {
        if let Ok(tag) = std::env::var(self.env_tag_var) {
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

    fn effective_tag(&self) -> String {
        if let Some(ref tag) = self.image_tag {
            return tag.clone();
        }
        if let Some(tag) = self.hardcoded_default_tag {
            return tag.to_string();
        }
        self.image.tag().to_string()
    }

    fn fingerprint(&self) -> String {
        format!("{}:{}", self.image.name(), self.effective_tag())
    }

    fn connection_url_display(&self) -> Result<&str, InfrastructureError> {
        self.connection_url
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned(self.engine.into()))
    }
}

async fn delete_keys_with_prefix(
    connection_url: &str,
    key_prefix: &str,
    resource_label: &str,
) -> Result<(), InfrastructureError> {
    let client =
        redis::Client::open(connection_url).map_err(|error| InfrastructureError::Teardown {
            resource: resource_label.to_string(),
            source: Box::new(std::io::Error::other(error.to_string())),
        })?;
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|error| InfrastructureError::Teardown {
            resource: resource_label.to_string(),
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
                resource: resource_label.to_string(),
                source: Box::new(std::io::Error::other(error.to_string())),
            })?;

        if !keys.is_empty() {
            redis::cmd("DEL")
                .arg(&keys)
                .query_async::<i64>(&mut connection)
                .await
                .map_err(|error| InfrastructureError::Teardown {
                    resource: resource_label.to_string(),
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

#[async_trait]
impl<I: Image + Clone + Send + Sync + std::fmt::Debug + 'static> Resource for RespResource<I> {
    async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        if self.holds_suite_ref {
            return Err(InfrastructureError::AlreadyProvisioned(self.engine.into()));
        }

        self.key_prefix = sanitize_redis_key_prefix(scenario_id.as_str());
        let want_key = self.fingerprint();

        let mut guard = self.suite.lock().await;
        let (host, port) = match &mut *guard {
            SuiteState::Idle => {
                let tag = self.effective_tag();
                let node = self
                    .image
                    .clone()
                    .with_tag(tag)
                    .with_label(
                        crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                        crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                    )
                    .start()
                    .await
                    .map_err(Self::map_tc_err)?;

                let mapped_port = node
                    .get_host_port_ipv4(RESP_PORT)
                    .await
                    .map_err(Self::map_tc_err)?;
                let host = "127.0.0.1".to_string();
                *guard = SuiteState::Running {
                    host: host.clone(),
                    port: mapped_port,
                    container: node,
                    refs: 1,
                    image_key: want_key.clone(),
                };
                (host, mapped_port)
            }
            SuiteState::Running {
                host,
                port,
                ref mut refs,
                image_key,
                ..
            } => {
                if image_key != &want_key {
                    return Err(InfrastructureError::Backend(Box::new(
                        std::io::Error::other(format!(
                            "{} suite server already running as {image_key}; \
                             cannot start or join as {want_key}. Use one image:tag per process.",
                            self.engine
                        )),
                    )));
                }
                *refs += 1;
                (host.clone(), *port)
            }
        };
        drop(guard);

        let connection_url = format!("redis://{host}:{port}");
        self.host = Some(host.clone());
        self.port = Some(port);
        self.connection_url = Some(connection_url.clone());
        self.holds_suite_ref = true;

        Ok(vec![ResourceHandle {
            id: format!("{}_{scenario_id}", self.engine),
            connection_string: connection_url.clone(),
            metadata: Some(serde_json::json!({
                "engine": self.engine,
                "host": host,
                "port": port,
                "connection_url": connection_url,
                "key_prefix": self.key_prefix,
            })),
        }])
    }

    async fn check_ready(&self) -> Result<(), InfrastructureError> {
        let url = self.connection_url_display()?;
        let client = redis::Client::open(url).map_err(|error| InfrastructureError::Readiness {
            resource: self.engine.into(),
            source: Box::new(std::io::Error::other(error.to_string())),
        })?;

        const ATTEMPTS: u32 = 30;
        for attempt in 0..ATTEMPTS {
            match client.get_multiplexed_async_connection().await {
                Ok(mut connection) => match redis::cmd("PING")
                    .query_async::<String>(&mut connection)
                    .await
                {
                    Ok(response) if response.eq_ignore_ascii_case("PONG") => return Ok(()),
                    Ok(_) | Err(_) if attempt + 1 == ATTEMPTS => {
                        return Err(InfrastructureError::Readiness {
                            resource: self.engine.into(),
                            source: Box::new(std::io::Error::other(
                                "Redis PING did not return PONG",
                            )),
                        });
                    }
                    Ok(_) | Err(_) => {}
                },
                Err(error) if attempt + 1 == ATTEMPTS => {
                    return Err(InfrastructureError::Readiness {
                        resource: self.engine.into(),
                        source: Box::new(std::io::Error::other(error.to_string())),
                    });
                }
                Err(_) => {}
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }

        Err(InfrastructureError::Readiness {
            resource: self.engine.into(),
            source: Box::new(std::io::Error::other(
                "Redis readiness: connection attempts exhausted",
            )),
        })
    }

    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        if let Some(seed_fn) = self.user_seed.take() {
            let bindings = self.redis_bindings()?;
            seed_fn(bindings).await.map_err(map_seed_error)?;
        }
        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        if !self.holds_suite_ref {
            self.host = None;
            self.port = None;
            self.connection_url = None;
            self.key_prefix.clear();
            return Ok(());
        }

        let url = self
            .connection_url
            .as_deref()
            .ok_or_else(|| InfrastructureError::NotProvisioned(self.engine.into()))?
            .to_string();
        let prefix = std::mem::take(&mut self.key_prefix);
        let engine = self.engine.to_string();

        delete_keys_with_prefix(url.as_str(), prefix.as_str(), engine.as_str()).await?;

        let mut guard = self.suite.lock().await;
        let mut container_to_remove: Option<testcontainers::ContainerAsync<I>> = None;
        match &mut *guard {
            SuiteState::Idle => {}
            SuiteState::Running { ref mut refs, .. } => {
                *refs = refs.saturating_sub(1);
                if *refs == 0 {
                    let taken = std::mem::replace(&mut *guard, SuiteState::Idle);
                    if let SuiteState::Running { container, .. } = taken {
                        container_to_remove = Some(container);
                    }
                }
            }
        }
        drop(guard);

        if let Some(container) = container_to_remove {
            container.rm().await.map_err(Self::map_tc_err)?;
        }

        self.holds_suite_ref = false;
        self.host = None;
        self.port = None;
        self.connection_url = None;
        Ok(())
    }
}
