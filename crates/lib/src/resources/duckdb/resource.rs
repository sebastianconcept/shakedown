use crate::error::{InfrastructureError, ScenarioId};
use crate::resources::user_seed::{take_user_seed, DuckDbUserSeedFn, UserSeedSlot};
use crate::resources::{
    duckdb_named_memory_database_path, map_seed_error, DuckDbBindings, Resource, ResourceHandle,
};
use async_trait::async_trait;
use duckdb::Connection;
use std::path::Path;
use std::sync::{Arc, Mutex};

const RESOURCE_NAME: &str = "duckdb";

/// In-process DuckDB for one scenario (no Docker).
///
/// Opens a scenario-scoped database file (see [`super::duckdb_named_memory_database_path`]) so
/// [`DuckDbBindings::database_url`] can be used to open further connections to the same logical
/// database (helpers use [`tokio::task::spawn_blocking`]).
///
/// `duckdb::Connection` is not [`Sync`]; it is wrapped in [`Arc`] [`Mutex`] so [`BuiltinResource`]
/// remains [`Sync`].
#[derive(Default)]
pub struct DuckDbResource {
    connection: Option<Arc<Mutex<Connection>>>,
    database_url: String,
    user_seed: UserSeedSlot<DuckDbBindings>,
}

impl std::fmt::Debug for DuckDbResource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DuckDbResource")
            .field("connection", &self.connection.as_ref().map(|_| "<open>"))
            .field("database_url", &self.database_url)
            .field(
                "user_seed",
                &self
                    .user_seed
                    .lock()
                    .ok()
                    .and_then(|guard| guard.as_ref().map(|_| "<closure>")),
            )
            .finish()
    }
}

impl DuckDbResource {
    /// New DuckDB resource (connection opened on [`Resource::provision`]).
    pub fn new() -> Self {
        Self::default()
    }

    /// Async seed closure receiving [`DuckDbBindings`] ([`Resource::apply_default_seed`]).
    pub fn set_seed(&mut self, seed: DuckDbUserSeedFn) {
        *self.user_seed.lock().unwrap() = Some(seed);
    }

    fn duckdb_bindings(&self) -> Result<DuckDbBindings, InfrastructureError> {
        if self.database_url.is_empty() {
            return Err(InfrastructureError::NotProvisioned(RESOURCE_NAME.into()));
        }
        Ok(DuckDbBindings {
            database_url: self.database_url.clone(),
        })
    }
}

#[async_trait]
impl Resource for DuckDbResource {
    async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        if self.connection.is_some() {
            return Err(InfrastructureError::AlreadyProvisioned(
                RESOURCE_NAME.into(),
            ));
        }

        let database_url = duckdb_named_memory_database_path(scenario_id.as_str());
        let open_path = database_url.clone();
        let connection =
            tokio::task::spawn_blocking(move || Connection::open(Path::new(open_path.as_str())))
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
            id: format!("duckdb_{scenario_id}"),
            connection_string: database_url.clone(),
            metadata: Some(serde_json::json!({
                "database_url": database_url,
                "engine": "duckdb",
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
        if let Some(seed_fn) = take_user_seed(&self.user_seed) {
            let bindings = self.duckdb_bindings()?;
            seed_fn(bindings).await.map_err(map_seed_error)?;
        }
        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        self.connection.take();
        if !self.database_url.is_empty() {
            let _ = std::fs::remove_file(self.database_url.as_str());
        }
        self.database_url.clear();
        Ok(())
    }
}
