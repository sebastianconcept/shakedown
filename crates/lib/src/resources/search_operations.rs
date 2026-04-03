//! HTTP-based Elasticsearch/OpenSearch index operations (reqwest only; OQ-6.1).
//!
//! Shared by [`crate::resources::elasticsearch::ElasticsearchHelpers`],
//! [`crate::resources::opensearch::OpenSearchHelpers`], and Elasticsearch/OpenSearch resource
//! teardown/readiness paths.

use reqwest::Client;
use serde_json::Value;

/// Elasticsearch `_search` response shape used by helpers (total hit count).
#[derive(Debug, Clone)]
pub struct SearchOperationsResult {
    /// Raw JSON body (for advanced assertions).
    pub raw: Value,
    /// Best-effort total hits (`hits.total.value` or `hits.total` number).
    pub total_hits: u64,
}

/// Minimal HTTP client for index CRUD and cluster checks (no official ES client crate).
#[derive(Debug, Clone)]
pub struct SearchOperations {
    client: Client,
    base_url: String,
    username: Option<String>,
    password: Option<String>,
}

impl SearchOperations {
    /// `base_url` is the HTTP API root (e.g. `http://127.0.0.1:9200`) without a trailing slash.
    pub fn new(
        base_url: impl Into<String>,
        username: Option<String>,
        password: Option<String>,
    ) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            username,
            password,
        }
    }

    fn apply_auth(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match (&self.username, &self.password) {
            (Some(user), Some(pass)) => request.basic_auth(user, Some(pass.as_str())),
            _ => request,
        }
    }

    /// `GET /_cluster/health`
    pub async fn cluster_health(&self) -> Result<Value, reqwest::Error> {
        let url = format!("{}/_cluster/health", self.base_url);
        let response = self
            .apply_auth(self.client.get(url))
            .send()
            .await?
            .error_for_status()?;
        response.json().await
    }

    /// `DELETE /{stem}*` — delete all indices whose names start with `stem` (wildcard suffix).
    pub async fn delete_indices_with_prefix(
        &self,
        index_prefix: &str,
    ) -> Result<(), reqwest::Error> {
        let stem = index_prefix.trim_end_matches('_');
        let path = format!("/{}_*", stem);
        let url = format!("{}{}", self.base_url, path);
        let response = self.apply_auth(self.client.delete(url)).send().await?;
        // 404 = no matching index (ok for idempotent teardown)
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(());
        }
        response.error_for_status().map(|_| ())
    }

    /// `PUT /{index}/_doc/{id}` with JSON body.
    pub async fn index_document(
        &self,
        index: &str,
        document_id: &str,
        document: &Value,
    ) -> Result<(), reqwest::Error> {
        let url = format!("{}/{}/_doc/{}", self.base_url, index, document_id);
        self.apply_auth(self.client.put(url).json(document))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    /// `POST /{index}/_refresh`
    pub async fn refresh_index(&self, index: &str) -> Result<(), reqwest::Error> {
        let url = format!("{}/{}/_refresh", self.base_url, index);
        self.apply_auth(self.client.post(url))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    /// `POST /{index}/_search` with JSON body.
    pub async fn search(
        &self,
        index: &str,
        query: &Value,
    ) -> Result<SearchOperationsResult, reqwest::Error> {
        let url = format!("{}/{}/_search", self.base_url, index);
        let response = self
            .apply_auth(self.client.post(url).json(query))
            .send()
            .await?
            .error_for_status()?;
        let raw: Value = response.json().await?;
        let total_hits = parse_total_hits(&raw);
        Ok(SearchOperationsResult { raw, total_hits })
    }

    /// `GET /{index}/_doc/{id}` — returns parsed JSON; use `found` field for existence.
    pub async fn get_document(
        &self,
        index: &str,
        document_id: &str,
    ) -> Result<Value, reqwest::Error> {
        let url = format!("{}/{}/_doc/{}", self.base_url, index, document_id);
        self.apply_auth(self.client.get(url))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
    }
}

fn parse_total_hits(response: &Value) -> u64 {
    response
        .get("hits")
        .and_then(|hits| {
            hits.get("total").and_then(|total| {
                if let Some(value) = total.get("value").and_then(|v| v.as_u64()) {
                    Some(value)
                } else {
                    total.as_u64()
                }
            })
        })
        .unwrap_or(0)
}
