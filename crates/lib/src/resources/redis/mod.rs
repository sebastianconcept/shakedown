//! Built-in Redis resource and typed helpers (`redis` / `valkey` feature for helpers).
mod helpers;
mod resource;
#[cfg(test)]
mod tests;

pub use helpers::RedisHelpers;
pub use resource::RedisResource;
