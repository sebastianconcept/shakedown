//! Built-in Valkey resource (`valkey` feature). Use [`crate::resources::redis::RedisHelpers`] for typed access (same as Redis).
mod resource;
#[cfg(test)]
mod tests;

pub use resource::ValkeyResource;
