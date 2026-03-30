//! Neo4j typed helpers: Cypher write + assert (Docker).
//!
//! Built with `--features neo4j`. **`#[ignore]`** unless `--ignored`.
//!
//! ```text
//! cargo test -p shakedown --features neo4j --test helpers_neo4j -- --ignored
//! ```

use serde_json::json;

#[shakedown::test(resources = [Neo4j])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features neo4j --test helpers_neo4j -- --ignored"]
async fn neo4j_helpers_create_then_assert(ctx: &mut shakedown::Context) {
    ctx.neo4j()
        .execute_cypher(
            "CREATE (p:Person { name: $name, role: $role })",
            &[("name", json!("Ada")), ("role", json!("engineer"))],
        )
        .await
        .expect("execute_cypher");
    ctx.neo4j()
        .execute_cypher(
            "CREATE (p:Person { name: $name })",
            &[("name", json!("Bob"))],
        )
        .await
        .expect("second person");
    ctx.neo4j()
        .execute_cypher(
            "MATCH (a:Person { name: $a }), (b:Person { name: $b }) CREATE (a)-[r:KNOWS { since: $since }]->(b)",
            &[
                ("a", json!("Ada")),
                ("b", json!("Bob")),
                ("since", json!(2024)),
            ],
        )
        .await
        .expect("create relationship");
    let rows = ctx
        .neo4j()
        .query_cypher(
            "MATCH (p:Person { name: $name }) RETURN p.name AS name LIMIT 1",
            &[("name", json!("Ada"))],
        )
        .await
        .expect("query_cypher");
    assert_eq!(rows.len(), 1);
    let name: String = rows[0].get("name").expect("name column");
    assert_eq!(name, "Ada");
    ctx.neo4j()
        .assert_node_exists("Person", json!({ "name": "Ada" }))
        .await
        .expect("assert_node_exists");
    ctx.neo4j()
        .assert_relationship_exists("KNOWS", json!({ "since": 2024 }))
        .await
        .expect("assert_relationship_exists");
}
