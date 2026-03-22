//! Resource provider traits and types.

/// A resource that can be provisioned and torn down.
///
/// Resources represent external systems like databases, message queues, etc.
pub trait Resource: Send + Sync {
    /// Apply default seed data to the resource.
    fn apply_default_seed(&self);

    /// Check if the resource is ready for use.
    fn check_ready(&self) -> bool;

    /// Teardown the resource.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct FakeResource;

    impl Resource for FakeResource {
        fn apply_default_seed(&self) {}

        fn check_ready(&self) -> bool {
            true
        }

        fn teardown(self) {}
    }

    #[test]
    fn test_resource_trait() {
        let resource = FakeResource;
        assert!(resource.check_ready());
    }

    #[test]
    fn test_resource_binding() {
        let resource = FakeResource;
        let config = ResourceConfig {
            resource_type: "fake".to_string(),
            connection_string: "localhost:1234".to_string(),
        };
        let binding = ResourceBinding::new(resource, config);

        assert_eq!(binding.config.resource_type, "fake");
        assert_eq!(binding.config.connection_string, "localhost:1234");
    }
}
