use crate::error::{InfrastructureError, ScenarioId};
use crate::resources::{sqlite_named_memory_uri, Resource, ResourceHandle};
use async_trait::async_trait;
use rusqlite::{Connection, OpenFlags};
use std::sync::{Arc, Mutex};

const RESOURCE_NAME: &str = "sqlite";

/// In-memory SQLite for one scenario (no Docker).
///
/// Opens a **named shared-cache in-memory** database (see [`super::sqlite_named_memory_uri`]) so
/// [`SqliteBindings::database_url`] can be used with `rusqlite` or other drivers to open further
/// connections to the same logical database.
///
/// `rusqlite::Connection` is not [`Sync`]; it is wrapped in [`Arc`] [`Mutex`] so [`BuiltinResource`]
/// remains [`Sync`] and readiness can use [`tokio::task::spawn_blocking`] (works on the default
/// single-threaded Tokio test executor used by `#[tokio::test]`).
#[derive(Debug, Default)]
pub struct SqliteResource {
    connection: Option<Arc<Mutex<Connection>>>,
    /// Retained for debugging; matches metadata `database_url`.
    database_url: String,
}

impl SqliteResource {
    /// New SQLite resource (connection opened on [`Resource::provision`]).
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Resource for SqliteResource {
    async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        if self.connection.is_some() {
            return Err(InfrastructureError::AlreadyProvisioned(
                RESOURCE_NAME.into(),
            ));
        }

        let database_url = sqlite_named_memory_uri(scenario_id.as_str());
        let open_url = database_url.clone();
        let connection = tokio::task::spawn_blocking(move || {
            Connection::open_with_flags(
                open_url.as_str(),
                OpenFlags::SQLITE_OPEN_URI
                    | OpenFlags::SQLITE_OPEN_READ_WRITE
                    | OpenFlags::SQLITE_OPEN_CREATE,
            )
        })
        .await
        .map_err(|join_error| InfrastructureError::Provision {
            resource: RESOURCE_NAME.into(),
            source: Box::new(std::io::Error::other(join_error.to_string())),
        })?
        .map_err(|source| InfrastructureError::Provision {
            resource: RESOURCE_NAME.into(),
            source: Box::new(source),
        })?;

        self.connection = Some(Arc::new(Mutex::new(connection)));
        self.database_url.clone_from(&database_url);

        Ok(vec![ResourceHandle {
            id: format!("sqlite_{scenario_id}"),
            connection_string: database_url.clone(),
            metadata: Some(serde_json::json!({
                "database_url": database_url,
                "engine": "sqlite",
            })),
        }])
    }

    async fn check_ready(&self) -> Result<(), InfrastructureError> {
        let arc = self
            .connection
            .as_ref()
            .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
        let arc = Arc::clone(arc);
        tokio::task::spawn_blocking(move || {
            let connection = arc
                .lock()
                .map_err(|poison| InfrastructureError::Readiness {
                    resource: RESOURCE_NAME.into(),
                    source: Box::new(std::io::Error::other(poison.to_string())),
                })?;
            connection
                .query_row("SELECT 1", [], |row| row.get::<_, i32>(0))
                .map_err(|source| InfrastructureError::Readiness {
                    resource: RESOURCE_NAME.into(),
                    source: Box::new(source),
                })?;
            Ok::<(), InfrastructureError>(())
        })
        .await
        .map_err(|join_error| InfrastructureError::Readiness {
            resource: RESOURCE_NAME.into(),
            source: Box::new(std::io::Error::other(join_error.to_string())),
        })?
    }

    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        self.connection.take();
        self.database_url.clear();
        Ok(())
    }
}
