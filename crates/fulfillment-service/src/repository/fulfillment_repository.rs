//! [`FulfillmentRepository`] — `fulfillments` collection access.

use mongodb::bson::doc;
use mongodb::bson::Document;
use mongodb::Database;

/// Document store for fulfillment records (`order_id` idempotency lookups).
#[derive(Clone)]
pub struct FulfillmentRepository {
    database: Database,
}

impl FulfillmentRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    /// Returns `_id` of an existing fulfillment for this `order_id`, if any.
    pub async fn find_fulfillment_id_by_order_id(
        &self,
        order_id: &str,
    ) -> Result<Option<String>, mongodb::error::Error> {
        let collection = self.database.collection::<Document>("fulfillments");
        let filter = doc! { "order_id": order_id };
        let document = collection.find_one(filter).await?;
        Ok(document.and_then(|document| {
            document
                .get_str("_id")
                .ok()
                .map(std::string::ToString::to_string)
        }))
    }

    pub async fn insert_fulfillment(
        &self,
        fulfillment_id: &str,
        order_id: &str,
        items_bson: mongodb::bson::Bson,
    ) -> Result<(), mongodb::error::Error> {
        let collection = self.database.collection::<Document>("fulfillments");
        let insert_document = doc! {
            "_id": fulfillment_id,
            "order_id": order_id,
            "items": items_bson,
            "status": "fulfilled",
        };
        collection.insert_one(insert_document).await?;
        Ok(())
    }

    pub async fn find_document_by_id(
        &self,
        fulfillment_id: &str,
    ) -> Result<Option<Document>, mongodb::error::Error> {
        let collection = self.database.collection::<Document>("fulfillments");
        collection.find_one(doc! { "_id": fulfillment_id }).await
    }
}
