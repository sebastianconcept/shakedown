//! MongoDB typed helpers — [`MongoHelpers`] for scenario-scoped databases.

use futures_util::TryStreamExt;
use mongodb::bson::Document;
use mongodb::Client;

use crate::error::named_binding::not_provisioned_named_binding;
use crate::error::{AssertionError, ScenarioError, ShakedownError};
use crate::helper_prelude::map_backend_error;
use crate::lifecycle::{Context, DEFAULT_RESOURCE_BINDING_NAME};
use crate::resources::MongoBindings;

async fn connect_client(mongo_bindings: &MongoBindings) -> Result<Client, ShakedownError> {
    Client::with_uri_str(mongo_bindings.connection_url.as_str())
        .await
        .map_err(map_backend_error)
}

/// Facade for MongoDB helpers bound to [`Context`] (`mongodb` feature).
pub struct MongoHelpers<'ctx> {
    context: &'ctx Context,
    binding_name: String,
}

impl<'ctx> MongoHelpers<'ctx> {
    /// Helpers scoped to the [`DEFAULT_RESOURCE_BINDING_NAME`] binding.
    pub fn new(context: &'ctx Context) -> Result<Self, ShakedownError> {
        Self::named(context, DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// Helpers scoped to the binding registered under `name` in [`Context`].
    pub fn named(context: &'ctx Context, name: &str) -> Result<Self, ShakedownError> {
        if context.mongo_bindings_named(name).is_none() {
            return Err(not_provisioned_named_binding("mongodb bindings", name));
        }
        Ok(Self {
            context,
            binding_name: name.to_string(),
        })
    }

    fn mongo_bindings(&self) -> Result<&MongoBindings, ShakedownError> {
        self.context
            .mongo_bindings_named(self.binding_name.as_str())
            .ok_or_else(|| {
                not_provisioned_named_binding("mongodb bindings", self.binding_name.as_str())
            })
    }

    /// Insert one document into `collection` (logical name; uses the scenario database from bindings).
    pub async fn insert_document(
        &self,
        collection_name: &str,
        document: serde_json::Value,
    ) -> Result<(), ShakedownError> {
        let mongo_bindings = self.mongo_bindings()?;
        let bson_document = mongodb::bson::to_document(&document)
            .map_err(|error| map_backend_error(std::io::Error::other(error.to_string())))?;
        let client = connect_client(mongo_bindings).await?;
        let collection = client
            .database(mongo_bindings.database.as_str())
            .collection::<Document>(collection_name);
        collection
            .insert_one(bson_document)
            .await
            .map_err(map_backend_error)?;
        Ok(())
    }

    /// Run `find` with `filter` and return all matching documents (no implicit limit beyond driver defaults).
    pub async fn find_documents(
        &self,
        collection_name: &str,
        filter: Document,
    ) -> Result<Vec<Document>, ShakedownError> {
        let mongo_bindings = self.mongo_bindings()?;
        let client = connect_client(mongo_bindings).await?;
        let collection = client
            .database(mongo_bindings.database.as_str())
            .collection::<Document>(collection_name);
        let cursor = collection.find(filter).await.map_err(map_backend_error)?;
        let documents: Vec<Document> = cursor.try_collect().await.map_err(map_backend_error)?;
        Ok(documents)
    }

    /// Assert at least one document matches `filter`.
    pub async fn assert_document_exists(
        &self,
        collection_name: &str,
        filter: Document,
    ) -> Result<(), ShakedownError> {
        let mongo_bindings = self.mongo_bindings()?;
        let client = connect_client(mongo_bindings).await?;
        let collection = client
            .database(mongo_bindings.database.as_str())
            .collection::<Document>(collection_name);
        let count = collection
            .count_documents(filter)
            .await
            .map_err(map_backend_error)?;
        if count == 0 {
            return Err(ShakedownError::Scenario(ScenarioError::Assertion(
                AssertionError::Unspecified {
                    detail: "assert_document_exists: no matching documents".to_string(),
                },
            )));
        }
        Ok(())
    }

    /// Assert `count_documents(filter)` equals `expected`.
    pub async fn assert_document_count(
        &self,
        collection_name: &str,
        filter: Document,
        expected: u64,
    ) -> Result<(), ShakedownError> {
        let mongo_bindings = self.mongo_bindings()?;
        let client = connect_client(mongo_bindings).await?;
        let collection = client
            .database(mongo_bindings.database.as_str())
            .collection::<Document>(collection_name);
        let actual = collection
            .count_documents(filter)
            .await
            .map_err(map_backend_error)?;
        if actual != expected {
            return Err(ShakedownError::Scenario(ScenarioError::Assertion(
                AssertionError::Unspecified {
                    detail: format!(
                        "assert_document_count: expected {expected} documents, got {actual}"
                    ),
                },
            )));
        }
        Ok(())
    }
}
