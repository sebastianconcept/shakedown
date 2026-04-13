//! DuckDB typed helpers: `execute` for setup, then `assert_row_exists` / `assert_row_count` (no Docker).
//!
//! This target is listed in `Cargo.toml` with `required-features = ["duckdb"]`. Tests are **not**
//! ignored — DuckDB uses in-process bindings and does not require a Docker container.
//!
//! ```text
//! cargo test -p shakedown --features duckdb --test helpers_duckdb
//! ```

#[shakedown::test(resources = [DuckDb])]
async fn duckdb_helpers_execute_create_insert_then_assert(ctx: &mut shakedown::Context) {
    // Given: a scenario with DuckDB provisioned and an isolated named in-memory database path.

    // When: we create a table and insert a row using only `DuckDbHelpers` (async API over spawn_blocking).
    ctx.duckdb()
        .expect("duckdb helpers")
        .execute("DROP TABLE IF EXISTS helper_items", ())
        .await
        .expect("execute DROP TABLE via helpers");
    ctx.duckdb()
        .expect("duckdb helpers")
        .execute(
            "CREATE TABLE helper_items (value_text VARCHAR NOT NULL)",
            (),
        )
        .await
        .expect("execute CREATE TABLE via helpers");
    let inserted_value = "hello-duckdb";
    ctx.duckdb()
        .expect("duckdb helpers")
        .execute(
            "INSERT INTO helper_items (value_text) VALUES (?)",
            (inserted_value,),
        )
        .await
        .expect("execute INSERT via helpers");

    // Then: the row exists and the table has exactly one row.
    ctx.duckdb()
        .expect("duckdb helpers")
        .assert_row_exists(
            "SELECT 1 FROM helper_items WHERE value_text = ?",
            (inserted_value,),
        )
        .await
        .expect("assert_row_exists");
    ctx.duckdb()
        .expect("duckdb helpers")
        .assert_row_count("SELECT * FROM helper_items", (), 1)
        .await
        .expect("assert_row_count");
}
