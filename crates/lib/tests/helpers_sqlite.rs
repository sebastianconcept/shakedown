//! SQLite typed helpers: `execute` for setup, then `assert_row_exists` / `assert_row_count` (no Docker).
//!
//! This target is listed in `Cargo.toml` with `required-features = ["sqlite"]`. Tests are **not**
//! ignored — SQLite uses in-memory bindings and does not require a Docker container.
//!
//! ```text
//! cargo test -p shakedown --features sqlite --test helpers_sqlite
//! ```

#[shakedown::test(resources = [Sqlite])]
async fn sqlite_helpers_execute_create_insert_then_assert(ctx: &mut shakedown::Context) {
    // Given: a scenario with SQLite provisioned and an isolated in-memory database URI.

    // When: we create a table and insert a row using only `SqliteHelpers` (async API over spawn_blocking).
    ctx.sqlite()
        .execute(
            "CREATE TABLE helper_items (id INTEGER PRIMARY KEY AUTOINCREMENT, value_text TEXT NOT NULL)",
            (),
        )
        .await
        .expect("execute CREATE TABLE via helpers");
    let inserted_value = "hello-sqlite";
    ctx.sqlite()
        .execute(
            "INSERT INTO helper_items (value_text) VALUES (?1)",
            (inserted_value,),
        )
        .await
        .expect("execute INSERT via helpers");

    // Then: the row exists and the table has exactly one row.
    ctx.sqlite()
        .assert_row_exists(
            "SELECT 1 FROM helper_items WHERE value_text = ?1",
            (inserted_value,),
        )
        .await
        .expect("assert_row_exists");
    ctx.sqlite()
        .assert_row_count("SELECT * FROM helper_items", (), 1)
        .await
        .expect("assert_row_count");
}
