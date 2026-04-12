//! Built-in PostgreSQL resource and typed helpers (`postgres` feature).
//!
//! The `helpers` submodule is also built with the `timescaledb` feature alone (shared wire helpers).
//!
//! # Multi-database scenarios
//!
//! Some services use a **primary** database plus a **second** logical database on the same Postgres
//! server (for example an upstream read model, a shared catalog, or a replica-shaped schema you
//! control in tests). The harness provisions one scenario-isolated database as the primary
//! [`crate::PostgresBindings`]. Anything beyond that is **explicit composition** in your prelude
//! or seed code.
//!
//! **`Postgres(migrations = "./migrations")` on [`crate::test`] applies only to the primary database** — the
//! one bound to [`crate::lifecycle::Context::postgres`] / [`crate::lifecycle::Context::postgres_bindings`].
//! Each additional database has its own migration history and path; Shakedown does not run multiple
//! migration roots for you on one attribute. That is **by design** so secondary schemas stay
//! explicit and do not silently share the primary tree.
//!
//! **Recipe:** provision another database with [`crate::Isolatable::isolate`] on the primary
//! [`crate::PostgresBindings`] (this issues `CREATE DATABASE` on the suite container and returns
//! bindings to the new DB). Run [`crate::PostgresBindings::run_migrations`] on **those** returned
//! bindings with a [`crate::MigrationRunner`] (for example [`sqlx::migrate::Migrator`] when the
//! `sqlx` feature is enabled). Inject the second URL into a service under test with
//! [`crate::service::ServiceUnderTest::env`] (requires the `services` feature).
//!
//! ```ignore
//! use shakedown::{Context, EnvMapping, Isolatable, ServiceUnderTest};
//!
//! async fn example_prelude(ctx: &mut Context) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     // Primary: your service’s own migrations (or rely on `Postgres(migrations = "./migrations")` on the macro).
//!     let primary_path = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"));
//!     let primary_migrator = sqlx::migrate::Migrator::new(primary_path).await?;
//!     ctx.postgres().run_migrations(&primary_migrator).await?;
//!
//!     // Secondary: explicit database + migrations (e.g. upstream crate’s migration tree).
//!     let primary_bindings = ctx.postgres_bindings().ok_or("postgres not provisioned")?;
//!     let second_bindings = primary_bindings.isolate("upstream").await?;
//!
//!     let upstream_path =
//!         std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../upstream-crate/migrations"));
//!     let upstream_migrator = sqlx::migrate::Migrator::new(upstream_path).await?;
//!     second_bindings.run_migrations(&upstream_migrator).await?;
//!
//!     let _spec = ServiceUnderTest::docker("myservice:latest")
//!         .name("myservice")
//!         .env_mapping(EnvMapping::postgres("DATABASE_URL"))
//!         .env("SECOND_DB_URL", second_bindings.connection_url.as_str())
//!         .build()?;
//!     Ok(())
//! }
//! ```
//!
//! When the second schema is another Postgres database on the same suite container, **`isolate` +
//! `run_migrations` on the returned [`crate::PostgresBindings`]** is the usual approach. Register a
//! separate resource only when the second store truly lives on different infrastructure.
#[cfg(any(feature = "postgres", feature = "timescaledb"))]
mod helpers;
#[cfg(feature = "postgres")]
mod resource;
#[cfg(all(test, any(feature = "postgres", feature = "timescaledb")))]
mod tests;

#[cfg(feature = "postgres")]
pub use resource::PostgresResource;

#[cfg(feature = "postgres")]
pub use helpers::PostgresHelpers;

#[cfg(feature = "postgres")]
pub(crate) use resource::{admin_connection_url, connect_with_url, scenario_connection_url};

// Re-export for `timescaledb` helpers only (`postgres` callers use `helpers` directly).
#[cfg(feature = "timescaledb")]
pub(crate) use helpers::{
    postgres_assert_row_count, postgres_assert_row_exists, postgres_execute, postgres_query_all,
};
