//! SQLite typed helpers: `execute` for setup, then `assert_row_exists` / `assert_row_count` (no Docker).
//!
//! This target is listed in `Cargo.toml` with `required-features = ["sqlite"]`. Tests are **not**
//! ignored — SQLite uses in-memory bindings and does not require a Docker container.
//!
//! ```text
//! cargo test -p shakedown --features sqlite --test helpers_sqlite
//! ```

use async_trait::async_trait;
use rusqlite::{Connection, OpenFlags};
use shakedown::MigrationRunner;

/// Custom migrator (no sqlx) — proves [`SqliteBindings::run_migrations`] is feature-gated only on the optional sqlx path.
struct SqliteDdlRunner(&'static str);

#[async_trait]
impl MigrationRunner for SqliteDdlRunner {
    async fn run_migrations(
        &self,
        connection_url: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ddl = self.0.to_string();
        let url = connection_url.to_string();
        tokio::task::spawn_blocking(move || {
            let connection = Connection::open_with_flags(
                url.as_str(),
                OpenFlags::SQLITE_OPEN_URI
                    | OpenFlags::SQLITE_OPEN_READ_WRITE
                    | OpenFlags::SQLITE_OPEN_CREATE,
            )?;
            connection.execute(ddl.as_str(), [])?;
            Ok::<(), rusqlite::Error>(())
        })
        .await
        .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) })?
        .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) })
    }
}

#[shakedown::test(resources = [Sqlite])]
async fn sqlite_helpers_execute_create_insert_then_assert(ctx: &mut shakedown::Context) {
    // Given: a scenario with SQLite provisioned and an isolated in-memory database URI.

    // When: we create a table and insert a row using only `SqliteHelpers` (async API over spawn_blocking).
    ctx.sqlite().expect("sqlite helpers")
        .execute(
            "CREATE TABLE helper_items (id INTEGER PRIMARY KEY AUTOINCREMENT, value_text TEXT NOT NULL)",
            (),
        )
        .await
        .expect("execute CREATE TABLE via helpers");
    let inserted_value = "hello-sqlite";
    ctx.sqlite()
        .expect("sqlite helpers")
        .execute(
            "INSERT INTO helper_items (value_text) VALUES (?1)",
            (inserted_value,),
        )
        .await
        .expect("execute INSERT via helpers");

    // Then: the row exists and the table has exactly one row.
    ctx.sqlite()
        .expect("sqlite helpers")
        .assert_row_exists(
            "SELECT 1 FROM helper_items WHERE value_text = ?1",
            (inserted_value,),
        )
        .await
        .expect("assert_row_exists");
    ctx.sqlite()
        .expect("sqlite helpers")
        .assert_row_count("SELECT * FROM helper_items", (), 1)
        .await
        .expect("assert_row_count");
}

#[shakedown::test(resources = [Sqlite])]
async fn sqlite_helpers_custom_migration_runner(ctx: &mut shakedown::Context) {
    ctx.sqlite()
        .expect("sqlite helpers")
        .run_migrations(&SqliteDdlRunner(
            "CREATE TABLE migration_runner_proof (x INTEGER NOT NULL)",
        ))
        .await
        .expect("custom MigrationRunner");
    ctx.sqlite()
        .expect("sqlite helpers")
        .execute("INSERT INTO migration_runner_proof (x) VALUES (7)", ())
        .await
        .expect("insert after migration");
    ctx.sqlite()
        .expect("sqlite helpers")
        .assert_row_exists("SELECT 1 FROM migration_runner_proof WHERE x = 7", ())
        .await
        .expect("assert row");
}
