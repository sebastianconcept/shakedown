//! [`ResourceHandle`] — connection details after resource provision.

/// Connection details returned after a resource is provisioned for a scenario.
///
/// Exists so scenario code and clients use one typed handle (id, connection string, optional JSON
/// metadata) instead of ad hoc tuples. Produced by [`crate::resources::Resource::provision`] and
/// consumed when configuring clients or asserting isolation (e.g. Kafka bootstrap + topic prefix
/// in `metadata`).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ResourceHandle {
    /// Unique identifier for this logical slice of the resource.
    /// May include scenario prefix, database name, or other namespace info.
    pub id: String,
    /// Connection string that clients use to connect.
    pub connection_string: String,
    /// Optional extra metadata (e.g., port mapping, container ID).
    pub metadata: Option<serde_json::Value>,
}
