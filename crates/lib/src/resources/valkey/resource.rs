use crate::error::{InfrastructureError, ScenarioId};
use crate::resources::resp_family::{RespResource, SuiteState};
use crate::resources::{Resource, ResourceHandle};
use async_trait::async_trait;
use std::sync::OnceLock;
use testcontainers_modules::valkey::Valkey;
use tokio::sync::Mutex;

static SUITE_VALKEY: OnceLock<Mutex<SuiteState<Valkey>>> = OnceLock::new();

fn suite_valkey_mutex() -> &'static Mutex<SuiteState<Valkey>> {
    SUITE_VALKEY.get_or_init(|| Mutex::new(SuiteState::Idle))
}

/// Docker-backed Valkey using the official image via testcontainers-modules.
///
/// Default tag follows the module image (Valkey 8.x). Override with [`Self::with_image_tag`] or
/// `SHAKEDOWN_VALKEY_IMAGE_TAG`.
#[derive(Debug)]
pub struct ValkeyResource(pub(super) RespResource<Valkey>);

impl Default for ValkeyResource {
    fn default() -> Self {
        Self::new()
    }
}

impl ValkeyResource {
    /// Create a new `ValkeyResource` using the default image tag (`valkey:8`), overridable via `SHAKEDOWN_VALKEY_IMAGE_TAG`.
    pub fn new() -> Self {
        Self(
            RespResource::new(
                Valkey::default(),
                suite_valkey_mutex(),
                "valkey",
                "SHAKEDOWN_VALKEY_IMAGE_TAG",
                Some("8"),
            )
            .with_image_tag_from_env(),
        )
    }

    /// Override the Valkey image tag (e.g. `"7.2"` for an earlier version).
    pub fn with_image_tag(self, tag: impl Into<String>) -> Self {
        Self(self.0.with_image_tag(tag))
    }

    /// Apply the image tag from `SHAKEDOWN_VALKEY_IMAGE_TAG` if set (called automatically by [`Self::new`]).
    pub fn with_image_tag_from_env(self) -> Self {
        Self(self.0.with_image_tag_from_env())
    }
}

#[async_trait]
impl Resource for ValkeyResource {
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
