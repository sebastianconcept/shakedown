//! Elasticsearch typed helpers — [`ElasticsearchHelpers`] for scenario-scoped indices (`elasticsearch` feature).

use crate::error::{AssertionError, InfrastructureError, ScenarioError, ShakedownError};
use crate::helpers::map_backend_error;
use crate::lifecycle::Context;
use crate::resources::ElasticsearchBindings;
use crate::search_operations::{SearchOperations, SearchOperationsResult};

/// Facade for Elasticsearch helpers bound to [`Context`] (`elasticsearch` feature).
pub struct ElasticsearchHelpers<'ctx> {
    context: &'ctx Context,
}

impl<'ctx> ElasticsearchHelpers<'ctx> {
    pub(crate) fn new(context: &'ctx Context) -> Self {
        Self { context }
    }

    fn elasticsearch_bindings(&self) -> Result<&ElasticsearchBindings, ShakedownError> {
        self.context.elasticsearch_bindings().ok_or_else(|| {
            ShakedownError::Infrastructure(InfrastructureError::NotProvisioned(
                "elasticsearch bindings".to_string(),
            ))
        })
    }

    fn operations(&self) -> Result<SearchOperations, ShakedownError> {
        let bindings = self.elasticsearch_bindings()?;
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
        let bindings = self.elasticsearch_bindings()?;
        let index = bindings.index_namespace(logical_index);
        self.operations()?
            .index_document(index.as_str(), document_id, &document)
            .await
            .map_err(map_backend_error)
    }

    /// `POST .../_refresh` for near-real-time visibility (FR-11).
    pub async fn refresh_index(&self, logical_index: &str) -> Result<(), ShakedownError> {
        let bindings = self.elasticsearch_bindings()?;
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
        let bindings = self.elasticsearch_bindings()?;
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
        let bindings = self.elasticsearch_bindings()?;
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
