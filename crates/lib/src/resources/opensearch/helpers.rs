//! OpenSearch typed helpers — [`OpenSearchHelpers`] for scenario-scoped indices (`opensearch` feature).

use crate::error::named_binding::not_provisioned_named_binding;
use crate::error::{AssertionError, ScenarioError, ShakedownError};
use crate::helper_prelude::map_backend_error;
use crate::lifecycle::{Context, DEFAULT_RESOURCE_BINDING_NAME};
use crate::resources::search_operations::{SearchOperations, SearchOperationsResult};
use crate::resources::OpenSearchBindings;

/// Facade for OpenSearch helpers bound to [`Context`] (`opensearch` feature).
pub struct OpenSearchHelpers<'ctx> {
    context: &'ctx Context,
    binding_name: String,
}

impl<'ctx> OpenSearchHelpers<'ctx> {
    /// Helpers scoped to the [`DEFAULT_RESOURCE_BINDING_NAME`] binding.
    pub fn new(context: &'ctx Context) -> Result<Self, ShakedownError> {
        Self::named(context, DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// Helpers scoped to the binding registered under `name` in [`Context`].
    pub fn named(context: &'ctx Context, name: &str) -> Result<Self, ShakedownError> {
        if context.opensearch_bindings_named(name).is_none() {
            return Err(not_provisioned_named_binding("opensearch bindings", name));
        }
        Ok(Self {
            context,
            binding_name: name.to_string(),
        })
    }

    fn opensearch_bindings(&self) -> Result<&OpenSearchBindings, ShakedownError> {
        self.context
            .opensearch_bindings_named(self.binding_name.as_str())
            .ok_or_else(|| {
                not_provisioned_named_binding("opensearch bindings", self.binding_name.as_str())
            })
    }

    fn operations(&self) -> Result<SearchOperations, ShakedownError> {
        let bindings = self.opensearch_bindings()?;
        Ok(SearchOperations::new(
            bindings.url.clone(),
            bindings.username.clone(),
            bindings.password.clone(),
        ))
    }

    /// `PUT` document under the scenario index prefix.
    pub async fn index_document(
        &self,
        logical_index: &str,
        document_id: &str,
        document: serde_json::Value,
    ) -> Result<(), ShakedownError> {
        let bindings = self.opensearch_bindings()?;
        let index = bindings.index_namespace(logical_index);
        self.operations()?
            .index_document(index.as_str(), document_id, &document)
            .await
            .map_err(map_backend_error)
    }

    /// `POST .../_refresh` for near-real-time visibility (FR-11).
    pub async fn refresh_index(&self, logical_index: &str) -> Result<(), ShakedownError> {
        let bindings = self.opensearch_bindings()?;
        let index = bindings.index_namespace(logical_index);
        self.operations()?
            .refresh_index(index.as_str())
            .await
            .map_err(map_backend_error)
    }

    /// Search after optional refresh is handled by callers; this issues `POST .../_search` only.
    pub async fn search(
        &self,
        logical_index: &str,
        query: serde_json::Value,
    ) -> Result<SearchOperationsResult, ShakedownError> {
        let bindings = self.opensearch_bindings()?;
        let index = bindings.index_namespace(logical_index);
        self.operations()?
            .search(index.as_str(), &query)
            .await
            .map_err(map_backend_error)
    }

    /// Refresh, then assert the document exists (`found` in GET response).
    pub async fn assert_document_indexed(
        &self,
        logical_index: &str,
        document_id: &str,
    ) -> Result<(), ShakedownError> {
        self.refresh_index(logical_index).await?;
        let bindings = self.opensearch_bindings()?;
        let index = bindings.index_namespace(logical_index);
        let body = self
            .operations()?
            .get_document(index.as_str(), document_id)
            .await
            .map_err(map_backend_error)?;
        let found = body
            .get("found")
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        if !found {
            return Err(ShakedownError::Scenario(ScenarioError::Assertion(
                AssertionError::Unspecified {
                    detail: format!(
                        "expected indexed document id={document_id} in index {index}, got found=false"
                    ),
                },
            )));
        }
        Ok(())
    }

    /// Refresh, then assert search hit count ≥ `expected_min`.
    pub async fn assert_search_hits(
        &self,
        logical_index: &str,
        query: serde_json::Value,
        expected_min: u64,
    ) -> Result<(), ShakedownError> {
        self.refresh_index(logical_index).await?;
        let result = self.search(logical_index, query).await?;
        if result.total_hits < expected_min {
            return Err(ShakedownError::Scenario(ScenarioError::Assertion(
                AssertionError::Unspecified {
                    detail: format!(
                        "expected at least {expected_min} search hits, got {}",
                        result.total_hits
                    ),
                },
            )));
        }
        Ok(())
    }
}
