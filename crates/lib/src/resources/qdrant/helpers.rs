//! Qdrant typed helpers — [`QdrantHelpers`] for scenario-scoped collections (`qdrant` feature).

use qdrant_client::qdrant::{
    CountPointsBuilder, CreateCollectionBuilder, Distance, GetPointsBuilder, PointStruct,
    ScoredPoint, SearchPointsBuilder, UpsertPointsBuilder, VectorParamsBuilder,
};
use qdrant_client::Qdrant;

use crate::error::{AssertionError, InfrastructureError, ScenarioError, ShakedownError};
use crate::helper_prelude::map_backend_error;
use crate::lifecycle::Context;
use crate::resources::QdrantBindings;

/// Facade for Qdrant helpers bound to [`Context`] (`qdrant` feature).
pub struct QdrantHelpers<'ctx> {
    context: &'ctx Context,
}

impl<'ctx> QdrantHelpers<'ctx> {
    pub(crate) fn new(context: &'ctx Context) -> Self {
        Self { context }
    }

    fn qdrant_bindings(&self) -> Result<&QdrantBindings, ShakedownError> {
        self.context.qdrant_bindings().ok_or_else(|| {
            ShakedownError::Infrastructure(InfrastructureError::NotProvisioned(
                "qdrant bindings".to_string(),
            ))
        })
    }

    fn client(&self) -> Result<Qdrant, ShakedownError> {
        let bindings = self.qdrant_bindings()?;
        Qdrant::from_url(bindings.grpc_url.as_str())
            .build()
            .map_err(map_backend_error)
    }

    /// Create a collection with a single named vector configuration (auto-prefixed collection name).
    pub async fn create_collection(
        &self,
        logical_name: &str,
        vector_size: u64,
        distance: Distance,
    ) -> Result<(), ShakedownError> {
        let bindings = self.qdrant_bindings()?;
        let collection = bindings.collection_namespace(logical_name);
        let client = self.client()?;
        client
            .create_collection(
                CreateCollectionBuilder::new(collection.as_str())
                    .vectors_config(VectorParamsBuilder::new(vector_size, distance)),
            )
            .await
            .map_err(map_backend_error)?;
        Ok(())
    }

    /// Upsert points into a logical collection (auto-prefixed collection name).
    pub async fn upsert_points(
        &self,
        logical_collection: &str,
        points: Vec<PointStruct>,
    ) -> Result<(), ShakedownError> {
        let bindings = self.qdrant_bindings()?;
        let collection = bindings.collection_namespace(logical_collection);
        let client = self.client()?;
        client
            .upsert_points(UpsertPointsBuilder::new(collection, points))
            .await
            .map_err(map_backend_error)?;
        Ok(())
    }

    /// Vector similarity search; returns scored hits (newest search API).
    pub async fn search(
        &self,
        logical_collection: &str,
        query_vector: Vec<f32>,
        limit: u64,
    ) -> Result<Vec<ScoredPoint>, ShakedownError> {
        let bindings = self.qdrant_bindings()?;
        let collection = bindings.collection_namespace(logical_collection);
        let client = self.client()?;
        let response = client
            .search_points(SearchPointsBuilder::new(collection, query_vector, limit))
            .await
            .map_err(map_backend_error)?;
        Ok(response.result)
    }

    /// Assert a point id exists in the logical collection.
    pub async fn assert_point_exists(
        &self,
        logical_collection: &str,
        point_id: u64,
    ) -> Result<(), ShakedownError> {
        let bindings = self.qdrant_bindings()?;
        let collection = bindings.collection_namespace(logical_collection);
        let client = self.client()?;
        let response = client
            .get_points(GetPointsBuilder::new(collection, vec![point_id.into()]))
            .await
            .map_err(map_backend_error)?;
        if response.result.is_empty() {
            return Err(ShakedownError::Scenario(ScenarioError::Assertion(
                AssertionError::Unspecified {
                    detail: format!(
                        "expected point id={point_id} in collection {logical_collection}"
                    ),
                },
            )));
        }
        Ok(())
    }

    /// Assert exact point count in the logical collection.
    pub async fn assert_collection_count(
        &self,
        logical_collection: &str,
        expected: u64,
    ) -> Result<(), ShakedownError> {
        let bindings = self.qdrant_bindings()?;
        let collection = bindings.collection_namespace(logical_collection);
        let client = self.client()?;
        let response = client
            .count(CountPointsBuilder::new(collection).exact(true))
            .await
            .map_err(map_backend_error)?;
        let count = response.result.map(|result| result.count).unwrap_or(0);
        if count != expected {
            return Err(ShakedownError::Scenario(ScenarioError::Assertion(
                AssertionError::Unspecified {
                    detail: format!(
                        "expected {expected} points in collection {logical_collection}, got {count}"
                    ),
                },
            )));
        }
        Ok(())
    }
}
