//! Async hooks run on a service spec after resource seeding and before that service's container
//! starts (`services` feature).

use std::future::Future;

use async_trait::async_trait;

use crate::error::Result;
use crate::lifecycle::Context;

/// Runs after infrastructure seeding and before the Docker container for this [`crate::service::ServiceSpec`]
/// is created. Receives read-only [`Context`] (bindings, no resource registration).
#[async_trait]
pub trait ServicePreStart: Send + Sync {
    /// User hook: e.g. produce to Kafka so topics exist before the service container starts.
    async fn invoke(&self, context: &Context) -> Result<()>;
}

/// Wraps a closure `Fn(&Context) -> Fut` for storage on [`crate::service::types::ServiceSpec`].
pub(crate) struct PreStartFn<F>(pub(crate) F);

#[async_trait]
impl<F, Fut> ServicePreStart for PreStartFn<F>
where
    F: Fn(&Context) -> Fut + Send + Sync,
    Fut: Future<Output = Result<()>> + Send,
{
    async fn invoke(&self, context: &Context) -> Result<()> {
        (self.0)(context).await
    }
}
