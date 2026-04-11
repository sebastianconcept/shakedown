//! End-to-end check for `#[shakedown::test(migrations = ...)]`: the harness applies sqlx migrations
//! after `apply_default_seed` and before mock services, using the path staged via
//! `Context::set_pending_postgres_sqlx_migrations_path`.
//!
//! ```text
//! cargo test -p shakedown --features postgres,sqlx --test helpers_postgres_migrations_macro -- --ignored
//! ```

#[shakedown::test(
    resources = [Postgres],
    migrations = "./tests/postgres_sqlx_migrations/migrations"
)]
#[ignore = "requires Docker; run: cargo test -p shakedown --features postgres,sqlx --test helpers_postgres_migrations_macro -- --ignored"]
async fn postgres_macro_migrations_attribute(ctx: &mut shakedown::Context) {
    ctx.postgres()
        .assert_row_exists("SELECT 1 FROM sqlx_migration_proof LIMIT 1", &[])
        .await
        .expect("migration created sqlx_migration_proof");
}
