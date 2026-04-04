//! Appendix B: Neo4j — Cypher writes, reads, and graph assertions
//!
//! **Resources:** Neo4j (Docker)
//! **Assertions:** `execute_cypher`, `query_cypher`, `assert_node_exists`, `assert_relationship_exists`
//! **Teaching intent:** Model orders and customers as nodes and express fulfillment relationships
//!   with the same Cypher you would use in application code.
//!
//! Run: `cargo test -p shakedown-examples --features neo4j --test appendix_neo4j -- --ignored`

use serde_json::json;

#[shakedown::test(resources = [Neo4j])]
#[ignore = "requires Docker; run: cargo test -p shakedown-examples --features neo4j --test appendix_neo4j -- --ignored"]
async fn neo4j_orders_customers_and_fulfillment_relationship(ctx: &mut shakedown::Context) {
    ctx.neo4j()
        .execute_cypher(
            "CREATE (c:Customer { customer_id: $cid, name: $name })",
            &[
                ("cid", json!("customer-neo4j-1")),
                ("name", json!("Acme Retail")),
            ],
        )
        .await
        .expect("create customer node");

    ctx.neo4j()
        .execute_cypher(
            "CREATE (o:Order { order_id: $oid, status: $status })",
            &[
                ("oid", json!("order-neo4j-1")),
                ("status", json!("created")),
            ],
        )
        .await
        .expect("create order node");

    ctx.neo4j()
        .execute_cypher(
            "MATCH (c:Customer { customer_id: $cid }), (o:Order { order_id: $oid }) \
             CREATE (o)-[r:PLACED_BY { channel: $channel }]->(c)",
            &[
                ("cid", json!("customer-neo4j-1")),
                ("oid", json!("order-neo4j-1")),
                ("channel", json!("web")),
            ],
        )
        .await
        .expect("link order to customer");

    let rows = ctx
        .neo4j()
        .query_cypher(
            "MATCH (o:Order { order_id: $oid }) RETURN o.status AS status LIMIT 1",
            &[("oid", json!("order-neo4j-1"))],
        )
        .await
        .expect("read order status");

    assert_eq!(rows.len(), 1);
    let status: String = rows[0].get("status").expect("status field");
    assert_eq!(status, "created");

    ctx.neo4j()
        .assert_node_exists("Order", json!({ "order_id": "order-neo4j-1" }))
        .await
        .expect("order node exists");

    ctx.neo4j()
        .assert_relationship_exists("PLACED_BY", json!({ "channel": "web" }))
        .await
        .expect("PLACED_BY relationship exists");
}
