//! Resource teardown for a scenario [`super::Context`].
//!
//! This module is useful when teardown must also clear a new resource or fault handle.
//! This module always attempts teardown on registered resources and the environment backend.
//! This module does not classify provision or readiness failures.

use super::context::Context;

pub(crate) async fn teardown_all_resources(ctx: &mut Context) {
    for resource in ctx.resources_mut() {
        let _ = resource.teardown().await;
    }
    let _ = ctx.environment_backend_mut().teardown().await;
    #[cfg(feature = "fault-injection")]
    ctx.clear_fault_handle();
}

#[cfg(all(feature = "fault-injection", feature = "mongodb-cluster"))]
pub(crate) fn prepare_mongo_replica_set_inter_node_defer(ctx: &mut Context) {
    use crate::resources::{BuiltinResource, RegisteredResource};
    let defer = ctx.resources().iter().any(|registered| {
        matches!(
            registered,
            RegisteredResource::Builtin(BuiltinResource::Toxiproxy(_))
        )
    }) && ctx.resources().iter().any(|registered| {
        matches!(
            registered,
            RegisteredResource::Builtin(BuiltinResource::MongoReplicaSet(_))
        )
    });
    for registered in ctx.resources_mut() {
        if let RegisteredResource::Builtin(BuiltinResource::MongoReplicaSet(mongo)) = registered {
            mongo.set_defer_repl_set_init(defer);
        }
    }
}
