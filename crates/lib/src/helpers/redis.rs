//! Redis / Valkey typed helpers — [`RedisHelpers`] for scenario-scoped keys.

use redis::AsyncCommands;

use crate::error::{AssertionError, InfrastructureError, ScenarioError, ShakedownError};
use crate::helpers::map_backend_error;
use crate::lifecycle::Context;
use crate::resources::RedisBindings;

async fn connect(
    redis_bindings: &RedisBindings,
) -> Result<redis::aio::MultiplexedConnection, ShakedownError> {
    let client =
        redis::Client::open(redis_bindings.connection_url.as_str()).map_err(map_backend_error)?;
    client
        .get_multiplexed_async_connection()
        .await
        .map_err(map_backend_error)
}

/// Facade for Redis / Valkey helpers bound to [`Context`].
pub struct RedisHelpers<'ctx> {
    context: &'ctx Context,
}

impl<'ctx> RedisHelpers<'ctx> {
    pub(crate) fn new(context: &'ctx Context) -> Self {
        Self { context }
    }

    fn redis_bindings(&self) -> Result<&RedisBindings, ShakedownError> {
        self.context.redis_bindings().ok_or_else(|| {
            ShakedownError::Infrastructure(InfrastructureError::NotProvisioned(
                "redis bindings".to_string(),
            ))
        })
    }

    fn namespaced_key(&self, logical_key: &str) -> Result<String, ShakedownError> {
        let bindings = self.redis_bindings()?;
        Ok(format!("{}{}", bindings.key_prefix, logical_key))
    }

    /// `SET` with the scenario key prefix applied to `logical_key`.
    pub async fn set(&self, logical_key: &str, value: &str) -> Result<(), ShakedownError> {
        let bindings = self.redis_bindings()?;
        let full_key = self.namespaced_key(logical_key)?;
        let mut connection = connect(bindings).await?;
        connection
            .set::<_, _, ()>(full_key.as_str(), value)
            .await
            .map_err(map_backend_error)
    }

    /// `GET` after applying the key prefix.
    pub async fn get(&self, logical_key: &str) -> Result<Option<String>, ShakedownError> {
        let bindings = self.redis_bindings()?;
        let full_key = self.namespaced_key(logical_key)?;
        let mut connection = connect(bindings).await?;
        let value: Option<String> = connection.get(full_key).await.map_err(map_backend_error)?;
        Ok(value)
    }

    /// `DEL` after applying the key prefix.
    pub async fn del(&self, logical_key: &str) -> Result<u64, ShakedownError> {
        let bindings = self.redis_bindings()?;
        let full_key = self.namespaced_key(logical_key)?;
        let mut connection = connect(bindings).await?;
        let removed: u64 = connection.del(full_key).await.map_err(map_backend_error)?;
        Ok(removed)
    }

    /// Assert that a key exists under the scenario prefix.
    pub async fn assert_key_exists(&self, logical_key: &str) -> Result<(), ShakedownError> {
        let present = self.get(logical_key).await?.is_some();
        if !present {
            return Err(ShakedownError::Scenario(ScenarioError::Assertion(
                AssertionError::Unspecified {
                    detail: format!("assert_key_exists: key '{logical_key}' is absent"),
                },
            )));
        }
        Ok(())
    }

    /// Assert that a key's value equals `expected`.
    pub async fn assert_key_equals(
        &self,
        logical_key: &str,
        expected: &str,
    ) -> Result<(), ShakedownError> {
        let actual = self.get(logical_key).await?.ok_or_else(|| {
            ShakedownError::Scenario(ScenarioError::Assertion(AssertionError::Unspecified {
                detail: format!("assert_key_equals: key '{logical_key}' is absent"),
            }))
        })?;
        if actual != expected {
            return Err(ShakedownError::Scenario(ScenarioError::Assertion(
                AssertionError::Unspecified {
                    detail: format!(
                        "assert_key_equals: key '{logical_key}' expected {expected:?}, got {actual:?}"
                    ),
                },
            )));
        }
        Ok(())
    }

    /// Assert that a key does not exist under the scenario prefix.
    pub async fn assert_key_absent(&self, logical_key: &str) -> Result<(), ShakedownError> {
        if self.get(logical_key).await?.is_some() {
            return Err(ShakedownError::Scenario(ScenarioError::Assertion(
                AssertionError::Unspecified {
                    detail: format!("assert_key_absent: key '{logical_key}' exists"),
                },
            )));
        }
        Ok(())
    }

    /// `EXPIRE` in seconds for the namespaced key.
    pub async fn expire(&self, logical_key: &str, seconds: i64) -> Result<bool, ShakedownError> {
        let bindings = self.redis_bindings()?;
        let full_key = self.namespaced_key(logical_key)?;
        let mut connection = connect(bindings).await?;
        connection
            .expire(full_key, seconds)
            .await
            .map_err(map_backend_error)
    }

    /// `TTL` in seconds for the namespaced key (`None` if the key has no TTL or is missing).
    pub async fn ttl(&self, logical_key: &str) -> Result<Option<i64>, ShakedownError> {
        let bindings = self.redis_bindings()?;
        let full_key = self.namespaced_key(logical_key)?;
        let mut connection = connect(bindings).await?;
        let ttl: i64 = connection.ttl(full_key).await.map_err(map_backend_error)?;
        if ttl < 0 {
            Ok(None)
        } else {
            Ok(Some(ttl))
        }
    }
}
