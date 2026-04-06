//! Integration tests for the [`Isolatable`] trait across all Bindings types.
//!
//! Prefix-based tests run without Docker. Entity-based tests (Postgres, MariaDB) require Docker
//! and follow the same `#[ignore]` / SHAKEDOWN_REUSE convention as the other vertical tests.

#[cfg(feature = "sqlite")]
mod sqlite_isolate {
    use rusqlite::{Connection, OpenFlags};
    use shakedown::{run_scenario, BuiltinResource, Context, EventLog, Isolatable, SqliteResource};

    #[tokio::test]
    async fn sqlite_bindings_isolate_yields_independent_database() {
        // Given a scenario with a SQLite resource provisioned
        let mut ctx = Context::new();
        ctx.register_builtin(BuiltinResource::Sqlite(SqliteResource::new()));
        let mut event_log = EventLog::new();
        run_scenario(
            &mut ctx,
            "isolatable_sqlite_proof",
            |context| {
                Box::pin(async move {
                    let bindings = context
                        .sqlite_bindings()
                        .expect("sqlite bindings after provision")
                        .clone();

                    // When we isolate a sub-context named "analytics"
                    let sub = bindings
                        .isolate("analytics")
                        .await
                        .expect("isolate should succeed");

                    // Then the sub URL is different (independent database)
                    assert_ne!(sub.database_url, bindings.database_url);
                    assert!(
                        sub.database_url.contains("analytics"),
                        "expected 'analytics' in URL, got: {}",
                        sub.database_url
                    );

                    // And we can open and write to the isolated database independently
                    let url = sub.database_url.clone();
                    tokio::task::spawn_blocking(move || {
                        let conn = Connection::open_with_flags(
                            url.as_str(),
                            OpenFlags::SQLITE_OPEN_URI
                                | OpenFlags::SQLITE_OPEN_READ_WRITE
                                | OpenFlags::SQLITE_OPEN_CREATE,
                        )
                        .expect("open isolated SQLite in-memory database");
                        conn.execute("CREATE TABLE analytics_events (id INTEGER)", [])
                            .expect("create table in isolated DB");
                        conn.execute("INSERT INTO analytics_events VALUES (1)", [])
                            .expect("insert into isolated DB");
                        let count: i32 = conn
                            .query_row("SELECT COUNT(*) FROM analytics_events", [], |row| {
                                row.get(0)
                            })
                            .expect("count from isolated DB");
                        assert_eq!(count, 1, "expected one row in isolated sub-context");
                    })
                    .await
                    .expect("blocking task");
                })
            },
            &mut event_log,
        )
        .await
        .expect("scenario should pass");
    }
}

#[cfg(all(feature = "sqlite", feature = "kafka"))]
mod prefix_based_isolate {
    use shakedown::{Isolatable, KafkaBindings};

    #[tokio::test]
    async fn kafka_bindings_isolate_deepens_topic_prefix() {
        // Given Kafka Bindings with a scenario prefix
        let bindings = KafkaBindings::new("localhost:9092", "shakedown_test_");

        // When we create an isolated sub-context
        let sub = bindings
            .isolate("payments")
            .await
            .expect("isolate should succeed");

        // Then the sub-prefix incorporates the given name and bootstrap is preserved
        assert_eq!(sub.topic_prefix, "shakedown_test_payments_");
        assert_eq!(sub.bootstrap_servers, "localhost:9092");

        // And we can isolate further (composable)
        let sub_sub = sub
            .isolate("retry")
            .await
            .expect("nested isolate should succeed");
        assert_eq!(sub_sub.topic_prefix, "shakedown_test_payments_retry_");
    }
}
