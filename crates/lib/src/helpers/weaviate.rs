//! Weaviate typed helpers — [`WeaviateHelpers`] for scenario-scoped classes (`weaviate` feature).

use crate::error::{AssertionError, InfrastructureError, ScenarioError, ShakedownError};
use crate::helpers::map_backend_error;
use crate::lifecycle::Context;
use crate::resources::WeaviateBindings;

/// Facade for Weaviate helpers bound to [`Context`] (`weaviate` feature).
pub struct WeaviateHelpers<'ctx> {
    context: &'ctx Context,
}

impl<'ctx> WeaviateHelpers<'ctx> {
    pub(crate) fn new(context: &'ctx Context) -> Self {
        Self { context }
    }

    fn weaviate_bindings(&self) -> Result<&WeaviateBindings, ShakedownError> {
        self.context.weaviate_bindings().ok_or_else(|| {
            ShakedownError::Infrastructure(InfrastructureError::NotProvisioned(
                "weaviate bindings".to_string(),
            ))
        })
    }

    fn rest_url(&self) -> Result<String, ShakedownError> {
        Ok(self.weaviate_bindings()?.rest_url.clone())
    }

    /// `POST /v1/schema` with `class` rewritten to the scenario-prefixed name.
    pub async fn create_class(
        &self,
        mut class_definition: serde_json::Value,
    ) -> Result<(), ShakedownError> {
        let bindings = self.weaviate_bindings()?;
        let logical = class_definition
            .get("class")
            .and_then(|value| value.as_str())
            .ok_or_else(|| {
                ShakedownError::Scenario(ScenarioError::Assertion(AssertionError::Unspecified {
                    detail: String::from(
                        "Weaviate class schema JSON must include a string \"class\" field",
                    ),
                }))
            })?;
        let full_class = bindings.class_namespace(logical);
        if let Some(object) = class_definition.as_object_mut() {
            object.insert("class".to_string(), serde_json::Value::String(full_class));
        }
        let base = self.rest_url()?;
        let url = format!("{}/v1/schema", base.trim_end_matches('/'));
        let response = reqwest::Client::new()
            .post(&url)
            .json(&class_definition)
            .send()
            .await
            .map_err(map_backend_error)?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(ShakedownError::Infrastructure(
                InfrastructureError::Backend(Box::new(std::io::Error::other(format!(
                    "POST /v1/schema failed: HTTP {status} {body}"
                )))),
            ));
        }
        Ok(())
    }

    /// `POST /v1/objects` with class namespaced; returns the new object id.
    pub async fn create_object(
        &self,
        logical_class: &str,
        properties: serde_json::Value,
    ) -> Result<String, ShakedownError> {
        let bindings = self.weaviate_bindings()?;
        let class_name = bindings.class_namespace(logical_class);
        let body = serde_json::json!({
            "class": class_name,
            "properties": properties,
        });
        let base = self.rest_url()?;
        let url = format!("{}/v1/objects", base.trim_end_matches('/'));
        let response = reqwest::Client::new()
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(map_backend_error)?;
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(ShakedownError::Infrastructure(
                InfrastructureError::Backend(Box::new(std::io::Error::other(format!(
                    "POST /v1/objects failed: HTTP {status} {text}"
                )))),
            ));
        }
        let value: serde_json::Value = response.json().await.map_err(map_backend_error)?;
        let id = value
            .get("id")
            .and_then(|value| value.as_str())
            .ok_or_else(|| {
                ShakedownError::Infrastructure(InfrastructureError::Backend(Box::new(
                    std::io::Error::other("POST /v1/objects: missing id in response"),
                )))
            })?;
        Ok(id.to_string())
    }

    /// `GET /v1/objects/{id}` (id is globally unique in Weaviate).
    pub async fn get_object(&self, object_id: &str) -> Result<serde_json::Value, ShakedownError> {
        let base = self.rest_url()?;
        let url = format!("{}/v1/objects/{}", base.trim_end_matches('/'), object_id);
        let response = reqwest::Client::new()
            .get(&url)
            .send()
            .await
            .map_err(map_backend_error)?;
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(ShakedownError::Infrastructure(
                InfrastructureError::Backend(Box::new(std::io::Error::other(format!(
                    "GET /v1/objects/{object_id} failed: HTTP {status} {text}"
                )))),
            ));
        }
        response.json().await.map_err(map_backend_error)
    }

    /// Asserts `GET /v1/objects/{id}` succeeds.
    pub async fn assert_object_exists(&self, object_id: &str) -> Result<(), ShakedownError> {
        self.get_object(object_id).await?;
        Ok(())
    }

    /// Asserts object count for the namespaced class via `GET /v1/objects` (`class` query).
    pub async fn assert_class_count(
        &self,
        logical_class: &str,
        expected: usize,
    ) -> Result<(), ShakedownError> {
        let bindings = self.weaviate_bindings()?;
        let class_name = bindings.class_namespace(logical_class);
        let base = self.rest_url()?;
        let response = reqwest::Client::new()
            .get(format!("{}/v1/objects", base.trim_end_matches('/')))
            .query(&[("class", class_name.as_str())])
            .query(&[("limit", "1000")])
            .send()
            .await
            .map_err(map_backend_error)?;
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(ShakedownError::Infrastructure(
                InfrastructureError::Backend(Box::new(std::io::Error::other(format!(
                    "GET /v1/objects (class filter) failed: HTTP {status} {text}"
                )))),
            ));
        }
        let body: serde_json::Value = response.json().await.map_err(map_backend_error)?;
        let count = body
            .get("totalResults")
            .and_then(|value| value.as_u64())
            .map(|value| value as usize)
            .or_else(|| {
                body.get("objects")
                    .and_then(|value| value.as_array())
                    .map(Vec::len)
            })
            .unwrap_or(0);
        if count != expected {
            return Err(ShakedownError::Scenario(ScenarioError::Assertion(
                AssertionError::Unspecified {
                    detail: format!(
                        "expected {expected} Weaviate objects in class {class_name}, got {count}"
                    ),
                },
            )));
        }
        Ok(())
    }
}
