//! Postgres typed helpers: `execute` for setup, then `assert_row_exists` / `assert_row_count` (Docker).
//!
//! This target is listed in `Cargo.toml` with `required-features = ["postgres"]`. Tests are
//! **`#[ignore]`** so `cargo test --workspace` skips Docker by default.
//!
//! With Docker available:
//!
//! ```text
//! cargo test -p shakedown --features postgres --test helpers_postgres -- --ignored
//! ```

#[shakedown::test(resources = [Postgres])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features postgres --test helpers_postgres -- --ignored"]
async fn postgres_helpers_execute_create_insert_then_assert(ctx: &mut shakedown::Context) {
    // Given: a scenario with Postgres provisioned and a fresh scenario-scoped database.

    // When: we create a table and insert a row using only `PostgresHelpers` (no raw client wiring).
    ctx.postgres()
        .execute(
            "CREATE TABLE helper_items (id SERIAL PRIMARY KEY, value_text TEXT NOT NULL)",
            &[],
        )
        .await
        .expect("execute CREATE TABLE via helpers");
    ctx.postgres()
        .execute(
            "INSERT INTO helper_items (value_text) VALUES ($1)",
            &[&"hello-postgres"],
        )
        .await
        .expect("execute INSERT via helpers");

    // Then: the row exists and the table has exactly one row.
    ctx.postgres()
        .assert_row_exists(
            "SELECT 1 FROM helper_items WHERE value_text = $1",
            &[&"hello-postgres"],
        )
        .await
        .expect("assert_row_exists");
    ctx.postgres()
        .assert_row_count("SELECT * FROM helper_items", &[], 1)
        .await
        .expect("assert_row_count");
}
