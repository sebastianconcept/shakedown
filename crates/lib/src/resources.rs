//! Resource provider traits and types.

use crate::lifecycle::Context;

/// A resource that can be provisioned and torn down.
///
/// Resources represent external systems like databases, message queues, etc.
pub trait Resource: Send + Sync {
    /// Provision the resource and return an instance.
    ///
    /// This method is called during the provision phase of the scenario lifecycle.
    fn provision(ctx: &mut Context) -> Self
    where
        Self: Sized;

    /// Apply default seed data to the resource.
    fn apply_default_seed(&self);

    /// Check if the resource is ready for use.
    ///
    /// This check should verify that the resource is fully initialized
    /// and ready to handle requests.
    fn check_ready(&self) -> bool;

    /// Teardown the resource.
    ///
    /// This method should clean up any resources and remove the resource
    /// from the environment.
    fn teardown(self)
    where
        Self: Sized;
}

/// A marker trait for resources that can be cloned.
pub trait CloneResource: Resource + Clone {}

/// Represents a resource binding with its configuration.
#[derive(Debug, Clone)]
pub struct ResourceBinding<T: Resource> {
    /// The resource instance
    pub resource: T,
    /// Configuration used to create this binding
    pub config: ResourceConfig,
}

/// Configuration for a resource.
#[derive(Debug, Clone)]
pub struct ResourceConfig {
    /// The resource type name
    pub resource_type: String,
    /// Connection string or identifier
    pub connection_string: String,
}

impl<T: Resource + Clone> ResourceBinding<T> {
    /// Create a new resource binding.
    pub fn new(resource: T, config: ResourceConfig) -> Self {
        Self { resource, config }
    }

    /// Get the underlying resource.
    pub fn resource(&self) -> &T {
        &self.resource
    }

    /// Get the configuration.
    pub fn config(&self) -> &ResourceConfig {
        &self.config
    }
}

/// A fake/noop resource implementation for testing.
///
/// This resource does nothing and always reports as ready.
#[derive(Debug, Clone)]
pub struct FakeResource;

impl Resource for FakeResource {
    fn provision(_ctx: &mut Context) -> Self {
        FakeResource
    }

    fn apply_default_seed(&self) {}

    fn check_ready(&self) -> bool {
        true
    }

    fn teardown(self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fake_resource() {
        let mut ctx = Context::new();
        let resource = FakeResource::provision(&mut ctx);
        assert!(resource.check_ready());
        // Teardown should complete without error
        resource.teardown();
    }

    #[test]
    fn test_resource_trait() {
        let mut ctx = Context::new();
        let resource = FakeResource::provision(&mut ctx);
        assert!(resource.check_ready());
    }

    #[test]
    fn test_resource_binding() {
        let mut ctx = Context::new();
        let resource = FakeResource::provision(&mut ctx);
        let config = ResourceConfig {
            resource_type: "fake".to_string(),
            connection_string: "localhost:1234".to_string(),
        };
        let binding = ResourceBinding::new(resource, config);

        assert_eq!(binding.config.resource_type, "fake");
        assert_eq!(binding.config.connection_string, "localhost:1234");
    }
}
