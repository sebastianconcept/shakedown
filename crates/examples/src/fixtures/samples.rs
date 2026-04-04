//! Sample JSON bodies for HTTP examples (`POST /orders`, `POST /fulfill`).

/// Sample `POST /orders` JSON body for a customer id.
pub fn sample_order(customer_id: &str) -> serde_json::Value {
    serde_json::json!({
        "customer_id": customer_id,
        "items": [
            { "product_id": "widget-1", "quantity": 2 },
            { "product_id": "gadget-3", "quantity": 1 }
        ]
    })
}

/// Sample `POST /fulfill` JSON body for an order id.
pub fn sample_fulfillment(order_id: &str) -> serde_json::Value {
    serde_json::json!({
        "order_id": order_id,
        "items": [
            { "product_id": "widget-1", "quantity": 2 }
        ]
    })
}
