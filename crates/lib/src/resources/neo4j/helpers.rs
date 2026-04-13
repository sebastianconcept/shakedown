//! Neo4j typed helpers — [`Neo4jHelpers`] for scenario-scoped databases (`neo4j` feature).

use neo4rs::{
    query, BoltBoolean, BoltFloat, BoltInteger, BoltNull, BoltString, BoltType, ConfigBuilder,
    Graph, Row,
};

use crate::error::named_binding::not_provisioned_named_binding;
use crate::error::{AssertionError, InfrastructureError, ScenarioError, ShakedownError};
use crate::helper_prelude::map_backend_error;
use crate::lifecycle::{Context, DEFAULT_RESOURCE_BINDING_NAME};
use crate::resources::Neo4jBindings;

fn sanitize_cypher_token(token: &str) -> Result<String, ShakedownError> {
    if token.is_empty()
        || !token
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(ShakedownError::Scenario(ScenarioError::Assertion(
            AssertionError::Unspecified {
                detail: format!(
                    "Neo4j helper: label/type/property names must be non-empty ASCII alphanumeric or underscore: {token:?}"
                ),
            },
        )));
    }
    Ok(token.to_string())
}

fn json_value_to_bolt(value: &serde_json::Value) -> Result<BoltType, ShakedownError> {
    match value {
        serde_json::Value::Null => Ok(BoltType::Null(BoltNull)),
        serde_json::Value::Bool(boolean) => Ok(BoltType::Boolean(BoltBoolean::new(*boolean))),
        serde_json::Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                Ok(BoltType::Integer(BoltInteger::new(integer)))
            } else if let Some(float) = number.as_f64() {
                Ok(BoltType::Float(BoltFloat::new(float)))
            } else {
                Err(ShakedownError::Infrastructure(
                    InfrastructureError::Backend(Box::new(std::io::Error::other(
                        "Neo4j helper: numeric parameter out of range",
                    ))),
                ))
            }
        }
        serde_json::Value::String(text) => Ok(BoltType::String(BoltString::from(text.as_str()))),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Err(
            ShakedownError::Infrastructure(InfrastructureError::Backend(Box::new(
                std::io::Error::other(
                    "Neo4j helper: nested array/object parameters are not supported in this milestone",
                ),
            ))),
        ),
    }
}

async fn graph_for_bindings(neo4j_bindings: &Neo4jBindings) -> Result<Graph, ShakedownError> {
    let uri = format!(
        "bolt://{}:{}",
        neo4j_bindings.host.as_str(),
        neo4j_bindings.port
    );
    let configuration = ConfigBuilder::default()
        .uri(uri)
        .user(neo4j_bindings.username.as_str())
        .password(neo4j_bindings.password.as_str())
        .db(neo4j_bindings.database.as_str())
        .build()
        .map_err(|error| map_backend_error(std::io::Error::other(error.to_string())))?;
    Graph::connect(configuration)
        .await
        .map_err(map_backend_error)
}

/// Facade for Neo4j Cypher helpers bound to [`Context`] (`neo4j` feature).
pub struct Neo4jHelpers<'ctx> {
    context: &'ctx Context,
    binding_name: String,
}

impl<'ctx> Neo4jHelpers<'ctx> {
    /// Helpers scoped to the [`DEFAULT_RESOURCE_BINDING_NAME`] binding.
    pub fn new(context: &'ctx Context) -> Result<Self, ShakedownError> {
        Self::named(context, DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// Helpers scoped to the binding registered under `name` in [`Context`].
    pub fn named(context: &'ctx Context, name: &str) -> Result<Self, ShakedownError> {
        if context.neo4j_bindings_named(name).is_none() {
            return Err(not_provisioned_named_binding("neo4j bindings", name));
        }
        Ok(Self {
            context,
            binding_name: name.to_string(),
        })
    }

    fn neo4j_bindings(&self) -> Result<&Neo4jBindings, ShakedownError> {
        self.context
            .neo4j_bindings_named(self.binding_name.as_str())
            .ok_or_else(|| {
                not_provisioned_named_binding("neo4j bindings", self.binding_name.as_str())
            })
    }

    /// Run a write Cypher statement with named parameters (`$key` in Cypher matches `key` here).
    ///
    /// Supported parameter value types: null, bool, integer (i64), float (f64), string.
    /// Arrays and nested objects are not yet supported — pass them as separate parameters or
    /// serialise to a JSON string if the server-side Cypher can parse it.
    pub async fn execute_cypher(
        &self,
        query_text: &str,
        parameters: &[(&str, serde_json::Value)],
    ) -> Result<(), ShakedownError> {
        let neo4j_bindings = self.neo4j_bindings()?;
        let graph = graph_for_bindings(neo4j_bindings).await?;
        let mut built = query(query_text);
        for (name, value) in parameters {
            built = built.param(name, json_value_to_bolt(value)?);
        }
        graph.run(built).await.map_err(map_backend_error)
    }

    /// Run a read Cypher query and collect all rows.
    ///
    /// Supported parameter value types: null, bool, integer (i64), float (f64), string.
    /// Arrays and nested objects are not yet supported — pass them as separate parameters or
    /// serialise to a JSON string if the server-side Cypher can parse it.
    pub async fn query_cypher(
        &self,
        query_text: &str,
        parameters: &[(&str, serde_json::Value)],
    ) -> Result<Vec<Row>, ShakedownError> {
        let neo4j_bindings = self.neo4j_bindings()?;
        let graph = graph_for_bindings(neo4j_bindings).await?;
        let mut built = query(query_text);
        for (name, value) in parameters {
            built = built.param(name, json_value_to_bolt(value)?);
        }
        let mut stream = graph.execute(built).await.map_err(map_backend_error)?;
        let mut rows = Vec::new();
        while let Some(row) = stream.next().await.map_err(map_backend_error)? {
            rows.push(row);
        }
        Ok(rows)
    }

    /// Assert at least one node with label `label` matches all entries in `properties` (AND).
    pub async fn assert_node_exists(
        &self,
        label: &str,
        properties: serde_json::Value,
    ) -> Result<(), ShakedownError> {
        let label_token = sanitize_cypher_token(label)?;
        let object = properties.as_object().ok_or_else(|| {
            ShakedownError::Scenario(ScenarioError::Assertion(AssertionError::Unspecified {
                detail: "assert_node_exists: properties must be a JSON object".to_string(),
            }))
        })?;
        let mut where_parts = Vec::new();
        let mut param_pairs: Vec<(String, serde_json::Value)> = Vec::new();
        for (index, (key, value)) in object.iter().enumerate() {
            let key_token = sanitize_cypher_token(key.as_str())?;
            let param_name = format!("p{index}");
            where_parts.push(format!("n.`{key_token}` = ${param_name}"));
            param_pairs.push((param_name, value.clone()));
        }
        let where_clause = if where_parts.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", where_parts.join(" AND "))
        };
        let cypher = format!("MATCH (n:{label_token}){where_clause} RETURN n LIMIT 1");
        let param_refs: Vec<(&str, serde_json::Value)> = param_pairs
            .iter()
            .map(|(name, value)| (name.as_str(), value.clone()))
            .collect();
        let rows = self.query_cypher(cypher.as_str(), &param_refs).await?;
        if rows.is_empty() {
            return Err(ShakedownError::Scenario(ScenarioError::Assertion(
                AssertionError::Unspecified {
                    detail: "assert_node_exists: no matching node".to_string(),
                },
            )));
        }
        Ok(())
    }

    /// Assert at least one relationship with type `relationship_type` matches all `properties` on `r` (AND).
    pub async fn assert_relationship_exists(
        &self,
        relationship_type: &str,
        properties: serde_json::Value,
    ) -> Result<(), ShakedownError> {
        let type_token = sanitize_cypher_token(relationship_type)?;
        let object = properties.as_object().ok_or_else(|| {
            ShakedownError::Scenario(ScenarioError::Assertion(AssertionError::Unspecified {
                detail: "assert_relationship_exists: properties must be a JSON object".to_string(),
            }))
        })?;
        let mut where_parts = Vec::new();
        let mut param_pairs: Vec<(String, serde_json::Value)> = Vec::new();
        for (index, (key, value)) in object.iter().enumerate() {
            let key_token = sanitize_cypher_token(key.as_str())?;
            let param_name = format!("p{index}");
            where_parts.push(format!("r.`{key_token}` = ${param_name}"));
            param_pairs.push((param_name, value.clone()));
        }
        let where_clause = if where_parts.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", where_parts.join(" AND "))
        };
        let cypher = format!("MATCH ()-[r:`{type_token}`]-(){where_clause} RETURN r LIMIT 1");
        let param_refs: Vec<(&str, serde_json::Value)> = param_pairs
            .iter()
            .map(|(name, value)| (name.as_str(), value.clone()))
            .collect();
        let rows = self.query_cypher(cypher.as_str(), &param_refs).await?;
        if rows.is_empty() {
            return Err(ShakedownError::Scenario(ScenarioError::Assertion(
                AssertionError::Unspecified {
                    detail: "assert_relationship_exists: no matching relationship".to_string(),
                },
            )));
        }
        Ok(())
    }
}
