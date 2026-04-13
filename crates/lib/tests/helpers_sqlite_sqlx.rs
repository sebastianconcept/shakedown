//! SQLite + sqlx [`sqlx::migrate::Migrator`] and [`shakedown::resources::SqliteBindings::sqlite_pool`] (no Docker).
//!
//! Uses the same sqlx pool for both migration and assertions: rusqlite and sqlx share the same
//! named in-memory URI but open different library connections, so cross-client assertions are
//! not guaranteed to observe the same shared cache state.

#[shakedown::test(resources = [Sqlite])]
async fn sqlite_sqlx_migrate_and_sqlite_pool(ctx: &mut shakedown::Context) {
    // Given a pool opened from the same database_url as the harness bindings.
    let pool = ctx
        .sqlite()
        .expect("sqlite helpers")
        .sqlite_pool()
        .await
        .expect("sqlite_pool");

    // When we run sqlx migrations through the pool.
    let migrator = sqlx::migrate!("tests/sqlite_sqlx_migrations/migrations");
    migrator.run(&pool).await.expect("sqlx migrate on pool");

    // Then the migrated table is visible through the same pool.
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sqlx_sqlite_proof")
        .fetch_one(&pool)
        .await
        .expect("scalar query via pool");

    assert_eq!(count, 0);
}

#[shakedown::test(resources = [Sqlite])]
async fn sqlite_sqlx_migration_runner_via_helper(ctx: &mut shakedown::Context) {
    // Given: run_migrations via SqliteHelpers delegates to the MigrationRunner trait.
    ctx.sqlite()
        .expect("sqlite helpers")
        .run_migrations(&sqlx::migrate!("tests/sqlite_sqlx_migrations/migrations"))
        .await
        .expect("sqlx migrate on sqlite bindings via helper");

    // When we open a pool from the same database_url.
    let pool = ctx
        .sqlite()
        .expect("sqlite helpers")
        .sqlite_pool()
        .await
        .expect("sqlite_pool");

    // Then the migrated table is visible through the pool.
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sqlx_sqlite_proof")
        .fetch_one(&pool)
        .await
        .expect("scalar query via pool after run_migrations");

    assert_eq!(count, 0);
}
