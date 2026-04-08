-- Proof migration for `MigrationRunner` + sqlx (`helpers_postgres_sqlx` integration test).
CREATE TABLE sqlx_migration_proof (
    id SERIAL PRIMARY KEY
);
