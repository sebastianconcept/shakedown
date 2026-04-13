//! Postgres + sqlx [`sqlx::migrate::Migrator`] via [`shakedown::MigrationRunner`] (Docker).
//!
//! ```text
//! cargo test -p shakedown --features postgres,sqlx --test helpers_postgres_sqlx -- --ignored
//! ```

#[shakedown::test(resources = [Postgres])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features postgres,sqlx --test helpers_postgres_sqlx -- --ignored"]
async fn postgres_helpers_run_sqlx_migrations(ctx: &mut shakedown::Context) {
    ctx.postgres()
        .expect("postgres helpers")
        .run_migrations(&sqlx::migrate!("tests/postgres_sqlx_migrations/migrations"))
        .await
        .expect("apply sqlx migrations via PostgresHelpers");

    ctx.postgres()
        .expect("postgres helpers")
        .assert_row_exists("SELECT 1 FROM sqlx_migration_proof LIMIT 1", &[])
        .await
        .expect("migration created sqlx_migration_proof");
}
