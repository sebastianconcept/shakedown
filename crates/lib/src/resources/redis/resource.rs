use crate::error::{InfrastructureError, ScenarioId};
use crate::resources::resp_family::{RespResource, SuiteState};
use crate::resources::{Resource, ResourceHandle};
use async_trait::async_trait;
use std::sync::OnceLock;
use testcontainers_modules::redis::Redis;
use tokio::sync::Mutex;

static SUITE_REDIS: OnceLock<Mutex<SuiteState<Redis>>> = OnceLock::new();

fn suite_redis_mutex() -> &'static Mutex<SuiteState<Redis>> {
    SUITE_REDIS.get_or_init(|| Mutex::new(SuiteState::Idle))
}

/// Docker-backed Redis using the official image via testcontainers-modules.
///
/// Suite lifecycle matches Rabbit/Postgres: one container per process; refcounted teardown.
/// Default image tag is **`7`** (`redis:7`). Override with [`Self::with_image_tag`] or
/// `SHAKEDOWN_REDIS_IMAGE_TAG`.
#[cfg_attr(docsrs, doc(cfg(feature = "redis")))]
#[derive(Debug)]
pub struct RedisResource(pub(super) RespResource<Redis>);

impl Default for RedisResource {
    fn default() -> Self {
        Self::new()
    }
}

impl RedisResource {
    /// Create a new `RedisResource` using the default image tag (`redis:7`), overridable via `SHAKEDOWN_REDIS_IMAGE_TAG`.
    pub fn new() -> Self {
        Self(
            RespResource::new(
                Redis::default(),
                suite_redis_mutex(),
                "redis",
                "SHAKEDOWN_REDIS_IMAGE_TAG",
                Some("7"),
            )
            .with_image_tag_from_env(),
        )
    }

    /// Override the Redis image tag (e.g. `"6.2"` for an older LTS).
    pub fn with_image_tag(self, tag: impl Into<String>) -> Self {
        Self(self.0.with_image_tag(tag))
    }

    /// Apply the image tag from `SHAKEDOWN_REDIS_IMAGE_TAG` if set (called automatically by [`Self::new`]).
    pub fn with_image_tag_from_env(self) -> Self {
        Self(self.0.with_image_tag_from_env())
    }
}

#[async_trait]
impl Resource for RedisResource {
    async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        self.0.provision(scenario_id).await
    }

    async fn check_ready(&self) -> Result<(), InfrastructureError> {
        self.0.check_ready().await
    }

    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        self.0.apply_default_seed().await
    }

    async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        self.0.teardown().await
    }
}
