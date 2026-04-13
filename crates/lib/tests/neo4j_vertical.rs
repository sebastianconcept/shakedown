//! Neo4j suite container + per-scenario database + Context bindings (`neo4j` feature).
//!
//! Built with `--features neo4j`. **`#[ignore]`** so default `cargo test` skips Docker.
//!
//! ```text
//! cargo test -p shakedown --features neo4j --test neo4j_vertical -- --ignored
//! ```

use serde_json::json;

use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, Neo4jResource, RegisteredResource,
};

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features neo4j --test neo4j_vertical -- --ignored"]
async fn neo4j_provisions_and_sets_bindings() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Neo4j(
        Neo4jResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "neo4j_vertical_proof",
        |ctx: &mut Context| {
            Box::pin(async {
                let bindings = ctx
                    .neo4j_bindings()
                    .expect("neo4j bindings after provision");
                assert_eq!(bindings.host, "127.0.0.1");
                assert!(bindings.port > 0);
                assert!(bindings.http_port > 0);
                assert!(
                    bindings.database.starts_with("shakedown-"),
                    "expected scenario database prefix (Neo4j names disallow underscores)"
                );
                assert!(bindings.bolt_url.contains("bolt://"));
                assert!(bindings.bolt_url.contains(&bindings.host));
                ctx.neo4j()
                    .expect("neo4j helpers")
                    .execute_cypher(
                        "CREATE (n:VerticalProof { proof: $proof })",
                        &[("proof", json!("neo4j_vertical"))],
                    )
                    .await
                    .expect("create node");
                ctx.neo4j()
                    .expect("neo4j helpers")
                    .assert_node_exists("VerticalProof", json!({ "proof": "neo4j_vertical" }))
                    .await
                    .expect("assert node");
                assert!(
                    matches!(
                        ctx.resources().first(),
                        Some(RegisteredResource::Builtin(BuiltinResource::Neo4j(_)))
                    ),
                    "expected Neo4j builtin"
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario should succeed");
}
