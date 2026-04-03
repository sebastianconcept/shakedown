//! MongoDB `outbox_event` collection ensure and row mapping.

use crate::error::ShakedownError;
use crate::resources::MongoBindings;
use crate::OutboxError;

use super::util::{
    as_backend_error, outbox_expected_status_miss_error, parse_outbox_row_state, row_not_found_any,
};
use super::OutboxEntry;

pub(crate) async fn ensure_outbox_collection(
    mongo_bindings: &MongoBindings,
) -> Result<(), ShakedownError> {
    let client = mongodb::Client::with_uri_str(mongo_bindings.connection_url.as_str())
        .await
        .map_err(as_backend_error)?;
    let database = client.database(mongo_bindings.database.as_str());
    let names = database
        .list_collection_names()
        .await
        .map_err(as_backend_error)?;
    if !names.iter().any(|name| name == "outbox_event") {
        database
            .create_collection("outbox_event")
            .await
            .map_err(as_backend_error)?;
    }
    Ok(())
}

pub(crate) async fn fetch_entry_with_status(
    mongo_bindings: &MongoBindings,
    aggregate_id: &str,
    event_type: &str,
    expected_status: &str,
) -> Result<OutboxEntry, ShakedownError> {
    use mongodb::bson::doc;
    use mongodb::options::FindOneOptions;

    let client = mongodb::Client::with_uri_str(mongo_bindings.connection_url.as_str())
        .await
        .map_err(as_backend_error)?;
    let collection = client
        .database(mongo_bindings.database.as_str())
        .collection::<mongodb::bson::Document>("outbox_event");

    let filter = doc! {
        "aggregate_id": aggregate_id,
        "type": event_type,
        "status": expected_status,
    };
    let find_options = FindOneOptions::builder()
        .sort(doc! { "created_at": -1, "_id": -1 })
        .build();
    let maybe_document = collection
        .find_one(filter)
        .with_options(find_options)
        .await
        .map_err(as_backend_error)?;
    if let Some(document) = maybe_document {
        return outbox_entry_from_mongo_document(&document).map_err(Into::into);
    }

    let filter_latest = doc! {
        "aggregate_id": aggregate_id,
        "type": event_type,
    };
    let latest_options = FindOneOptions::builder()
        .sort(doc! { "created_at": -1, "_id": -1 })
        .projection(doc! { "status": 1 })
        .build();
    let latest_document = collection
        .find_one(filter_latest)
        .with_options(latest_options)
        .await
        .map_err(as_backend_error)?;
    let latest_status = latest_document.and_then(|document| {
        document
            .get_str("status")
            .ok()
            .map(std::string::ToString::to_string)
    });
    Err(
        outbox_expected_status_miss_error(aggregate_id, event_type, expected_status, latest_status)
            .into(),
    )
}

pub(crate) async fn fetch_latest_entry(
    mongo_bindings: &MongoBindings,
    aggregate_id: &str,
    event_type: &str,
) -> Result<OutboxEntry, ShakedownError> {
    use mongodb::bson::doc;
    use mongodb::options::FindOneOptions;

    let client = mongodb::Client::with_uri_str(mongo_bindings.connection_url.as_str())
        .await
        .map_err(as_backend_error)?;
    let collection = client
        .database(mongo_bindings.database.as_str())
        .collection::<mongodb::bson::Document>("outbox_event");

    let filter = doc! {
        "aggregate_id": aggregate_id,
        "type": event_type,
    };
    let find_options = FindOneOptions::builder()
        .sort(doc! { "created_at": -1, "_id": -1 })
        .build();
    let maybe_document = collection
        .find_one(filter)
        .with_options(find_options)
        .await
        .map_err(as_backend_error)?;
    if let Some(document) = maybe_document {
        return outbox_entry_from_mongo_document(&document).map_err(Into::into);
    }
    Err(row_not_found_any(aggregate_id, event_type).into())
}

fn outbox_entry_from_mongo_document(
    document: &mongodb::bson::Document,
) -> Result<OutboxEntry, OutboxError> {
    use mongodb::bson::Bson;

    let id = mongo_bson_id_to_string(document.get("_id").ok_or_else(|| {
        OutboxError::PayloadInvalid {
            message: "outbox_event document missing _id".to_string(),
        }
    })?)?;
    let aggregate_type = document
        .get_str("aggregate_type")
        .map_err(|error| OutboxError::PayloadInvalid {
            message: error.to_string(),
        })?
        .to_string();
    let aggregate_id = document
        .get_str("aggregate_id")
        .map_err(|error| OutboxError::PayloadInvalid {
            message: error.to_string(),
        })?
        .to_string();
    let event_type = match document.get("type") {
        Some(Bson::String(value)) => value.clone(),
        Some(other) => {
            return Err(OutboxError::PayloadInvalid {
                message: format!("expected string event type, got {other:?}"),
            });
        }
        None => {
            return Err(OutboxError::PayloadInvalid {
                message: "outbox_event document missing type".to_string(),
            });
        }
    };
    let status_str = document
        .get_str("status")
        .map_err(|error| OutboxError::PayloadInvalid {
            message: error.to_string(),
        })?;
    let payload = match document.get("payload") {
        Some(value) => mongo_payload_bson_to_json(value)?,
        None => {
            return Err(OutboxError::PayloadInvalid {
                message: "outbox_event document missing payload".to_string(),
            });
        }
    };
    let attempts = mongo_read_i32(document, "attempts")?;
    let created_at = document
        .get_str("created_at")
        .map_err(|error| OutboxError::PayloadInvalid {
            message: error.to_string(),
        })?
        .to_string();
    let sent_at = match document.get("sent_at") {
        None | Some(Bson::Null) => None,
        Some(Bson::String(value)) => Some(value.clone()),
        Some(other) => {
            return Err(OutboxError::PayloadInvalid {
                message: format!("sent_at: expected string or null, got {other:?}"),
            });
        }
    };
    let status = parse_outbox_row_state(status_str, aggregate_id.as_str(), event_type.as_str())?;
    Ok(OutboxEntry {
        id,
        aggregate_type,
        aggregate_id,
        event_type,
        payload,
        status,
        attempts,
        created_at,
        sent_at,
    })
}

fn mongo_bson_id_to_string(value: &mongodb::bson::Bson) -> Result<String, OutboxError> {
    use mongodb::bson::Bson;
    match value {
        Bson::String(value) => Ok(value.clone()),
        Bson::ObjectId(identifier) => Ok(identifier.to_hex()),
        other => Err(OutboxError::PayloadInvalid {
            message: format!("unsupported _id BSON type: {other:?}"),
        }),
    }
}

fn mongo_read_i32(document: &mongodb::bson::Document, key: &str) -> Result<i32, OutboxError> {
    use mongodb::bson::Bson;
    match document.get(key) {
        Some(Bson::Int32(value)) => Ok(*value),
        Some(Bson::Int64(value)) => {
            i32::try_from(*value).map_err(|_| OutboxError::PayloadInvalid {
                message: format!("{key} out of i32 range"),
            })
        }
        Some(other) => Err(OutboxError::PayloadInvalid {
            message: format!("{key}: expected int, got {other:?}"),
        }),
        None => Err(OutboxError::PayloadInvalid {
            message: format!("missing {key}"),
        }),
    }
}

fn mongo_payload_bson_to_json(
    value: &mongodb::bson::Bson,
) -> Result<serde_json::Value, OutboxError> {
    use mongodb::bson::Bson;
    match value {
        Bson::String(text) => super::util::parse_payload_json(text),
        Bson::Document(document) => {
            mongodb::bson::from_document::<serde_json::Value>(document.clone()).map_err(|error| {
                OutboxError::PayloadInvalid {
                    message: error.to_string(),
                }
            })
        }
        other => mongodb::bson::from_bson::<serde_json::Value>(other.clone()).map_err(|error| {
            OutboxError::PayloadInvalid {
                message: error.to_string(),
            }
        }),
    }
}
