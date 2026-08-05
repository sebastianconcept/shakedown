//! Custom [`Resource`] values in the `resources = [ ... ]` list for `#[shakedown::test]` (in addition
//! to builtin resource names).

use async_trait::async_trait;
use shakedown::error::InfrastructureError;
use shakedown::resources::Resource;
use shakedown::RegisteredResource;
use shakedown::ResourceHandle;
use shakedown::ScenarioId;

/// Minimal custom resource for macro expansion tests (no Docker).
#[derive(Default)]
pub struct MinimalMacroCustomResource;

impl MinimalMacroCustomResource {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Resource for MinimalMacroCustomResource {
    async fn provision(
        &mut self,
        _scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        Ok(vec![ResourceHandle {
            id: "macro_custom".to_string(),
            connection_string: "://custom".to_string(),
            metadata: None,
        }])
    }

    async fn check_ready(&self) -> Result<(), InfrastructureError> {
        Ok(())
    }

    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        Ok(())
    }
}

#[shakedown::test(
    resources = [Fake, crate::MinimalMacroCustomResource::new()],
    ignore = false
)]
async fn macro_lists_builtin_and_custom_resource(ctx: &mut shakedown::Context) {
    let extension_count = ctx
        .resources()
        .iter()
        .filter(|registered| matches!(registered, RegisteredResource::Extension(_)))
        .count();
    assert_eq!(extension_count, 1);
    assert!(
        ctx.resources()
            .iter()
            .any(|registered| matches!(registered, RegisteredResource::Builtin(_))),
        "expected at least one builtin from Fake",
    );
}
