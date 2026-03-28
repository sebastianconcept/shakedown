//! Shared [`message_matches_aggregate_id`] for publication verifiers (Kafka, RabbitMQ, …).

/// Returns true when the message key equals `aggregate_id` as UTF-8, or when the payload is
/// UTF-8 JSON with an `aggregate_id` field equal to `aggregate_id`.
///
/// Used by first-party verifiers and available for custom [`super::PublicationVerifier`]
/// implementations that follow the same outbox publication conventions.
pub fn message_matches_aggregate_id(
    message_key: Option<&[u8]>,
    message_payload: Option<&[u8]>,
    aggregate_id: &str,
) -> bool {
    if let Some(key_bytes) = message_key {
        if let Ok(key_text) = std::str::from_utf8(key_bytes) {
            if key_text == aggregate_id {
                return true;
            }
        }
    }

    if let Some(payload_bytes) = message_payload {
        if let Ok(payload_text) = std::str::from_utf8(payload_bytes) {
            if let Ok(payload_json) = serde_json::from_str::<serde_json::Value>(payload_text) {
                return payload_json
                    .get("aggregate_id")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|payload_aggregate_id| payload_aggregate_id == aggregate_id);
            }
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::message_matches_aggregate_id;

    #[test]
    fn matches_utf8_key_equal_to_aggregate_id() {
        assert!(message_matches_aggregate_id(
            Some(b"order-1".as_ref()),
            None,
            "order-1"
        ));
        assert!(!message_matches_aggregate_id(
            Some(b"order-2".as_ref()),
            None,
            "order-1"
        ));
    }

    #[test]
    fn matches_json_payload_aggregate_id_field() {
        let payload = br#"{"aggregate_id":"order-1","total":1}"#;
        assert!(message_matches_aggregate_id(None, Some(payload), "order-1"));
        assert!(!message_matches_aggregate_id(
            None,
            Some(payload),
            "order-2"
        ));
    }

    #[test]
    fn key_takes_precedence_over_payload() {
        assert!(message_matches_aggregate_id(
            Some(b"order-1".as_ref()),
            Some(br#"{"aggregate_id":"other"}"#),
            "order-1"
        ));
    }

    #[test]
    fn invalid_utf8_key_skipped_json_payload_still_matches() {
        assert!(message_matches_aggregate_id(
            Some(&[0xff, 0xfe]),
            Some(br#"{"aggregate_id":"x"}"#),
            "x"
        ));
    }
}
