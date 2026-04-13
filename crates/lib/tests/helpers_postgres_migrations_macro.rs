//! End-to-end check for `Postgres(migrations = ...)` on `#[shakedown::test]`: sqlx migrations run inside
//! [`shakedown::PostgresResource::apply_default_seed`](shakedown::PostgresResource) during the seed phase.
//!
//! ```text
//! cargo test -p shakedown --features postgres,sqlx --test helpers_postgres_migrations_macro -- --ignored
//! ```

#[shakedown::test(resources = [Postgres(migrations = "./tests/postgres_sqlx_migrations/migrations")])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features postgres,sqlx --test helpers_postgres_migrations_macro -- --ignored"]
async fn postgres_macro_migrations_attribute(ctx: &mut shakedown::Context) {
    ctx.postgres()
        .expect("postgres helpers")
        .assert_row_exists("SELECT 1 FROM sqlx_migration_proof LIMIT 1", &[])
        .await
        .expect("migration created sqlx_migration_proof");
}
