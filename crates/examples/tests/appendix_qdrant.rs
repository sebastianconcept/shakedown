//! Appendix E: Qdrant — collections, vector upsert, and count assertions
//!
//! **Resources:** Qdrant (Docker)
//! **Assertions:** `create_collection`, `upsert_points`, `assert_point_exists`, `assert_collection_count`
//! **Teaching intent:** Store embedding-sized vectors for catalog SKUs and assert the harness
//!   namespaced collection contains exactly the points you upserted.
//!
//! Run: `cargo test -p shakedown-examples --features qdrant --test appendix_qdrant -- --ignored`

use shakedown::{Distance, PointStruct};

#[shakedown::test(resources = [Qdrant])]
#[ignore = "requires Docker; run: cargo test -p shakedown-examples --features qdrant --test appendix_qdrant -- --ignored"]
async fn qdrant_order_embedding_point_round_trip(ctx: &mut shakedown::Context) {
    let logical_collection = "order_embeddings";
    let point_id: u64 = 9001;
    let vector = vec![0.1_f32, 0.2_f32, 0.3_f32, 0.4_f32];

    ctx.qdrant()
        .create_collection(logical_collection, 4, Distance::Cosine)
        .await
        .expect("create namespaced collection");

    ctx.qdrant()
        .upsert_points(
            logical_collection,
            vec![PointStruct::new(
                point_id,
                vector.clone(),
                qdrant_client::Payload::default(),
            )],
        )
        .await
        .expect("upsert order embedding");

    ctx.qdrant()
        .assert_point_exists(logical_collection, point_id)
        .await
        .expect("point should exist");

    ctx.qdrant()
        .assert_collection_count(logical_collection, 1)
        .await
        .expect("exactly one vector stored");

    let hits = ctx
        .qdrant()
        .search(logical_collection, vector, 3)
        .await
        .expect("similarity search");
    assert!(
        !hits.is_empty(),
        "expected a nearest-neighbor hit for the inserted order embedding"
    );
}
