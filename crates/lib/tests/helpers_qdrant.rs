//! Qdrant typed helpers: create collection, upsert, search (Docker).
//!
//! Built with `--features qdrant`. **`#[ignore]`** unless `--ignored`.
//!
//! ```text
//! cargo test -p shakedown --features qdrant --test helpers_qdrant -- --ignored
//! ```

use qdrant_client::qdrant::Distance;
use qdrant_client::Payload;

#[shakedown::test(resources = [Qdrant])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features qdrant --test helpers_qdrant -- --ignored"]
async fn qdrant_helpers_upsert_search(ctx: &mut shakedown::Context) {
    ctx.qdrant()
        .create_collection("hcol", 4, Distance::Cosine)
        .await
        .expect("create_collection");
    ctx.qdrant()
        .upsert_points(
            "hcol",
            vec![qdrant_client::qdrant::PointStruct::new(
                42,
                vec![0.25_f32, 0.25_f32, 0.25_f32, 0.25_f32],
                Payload::default(),
            )],
        )
        .await
        .expect("upsert");
    ctx.qdrant()
        .assert_point_exists("hcol", 42)
        .await
        .expect("assert_point_exists");
    ctx.qdrant()
        .assert_collection_count("hcol", 1)
        .await
        .expect("assert_collection_count");
    let hits = ctx
        .qdrant()
        .search("hcol", vec![0.25_f32, 0.25_f32, 0.25_f32, 0.25_f32], 2)
        .await
        .expect("search");
    assert!(!hits.is_empty(), "expected at least one search hit");
}
