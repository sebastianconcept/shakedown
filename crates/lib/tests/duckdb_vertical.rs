//! DuckDB in-process resource: no Docker. Run with `cargo test -p shakedown --features duckdb`.

use std::path::Path;

use duckdb::Connection;
use shakedown::{run_scenario, BuiltinResource, Context, DuckDbResource, EventLog};

#[tokio::test]
async fn duckdb_writes_and_reads_via_bindings_without_docker() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::DuckDb(DuckDbResource::new()));
    let mut event_log = EventLog::new();
    run_scenario(
        &mut ctx,
        "duckdb_vertical_proof",
        |c| {
            Box::pin(async move {
                let url = c
                    .duckdb_bindings()
                    .expect("duckdb bindings after provision")
                    .database_url
                    .clone();
                tokio::task::spawn_blocking(move || {
                    let conn =
                        Connection::open(Path::new(url.as_str())).expect("open second connection");
                    conn.execute_batch(
                        "CREATE TABLE t (x INTEGER);
                         INSERT INTO t VALUES (42);",
                    )
                    .expect("create and insert");
                    let value: i32 = conn
                        .query_row("SELECT x FROM t", [], |row| row.get(0))
                        .expect("select");
                    assert_eq!(value, 42);
                })
                .await
                .expect("blocking task");
            })
        },
        &mut event_log,
    )
    .await
    .expect("scenario");
}

#[tokio::test]
async fn duckdb_isolates_each_scenario_without_docker() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::DuckDb(DuckDbResource::new()));
    let mut first_event_log = EventLog::new();
    run_scenario(
        &mut ctx,
        "duckdb_isolation_first",
        |c| {
            Box::pin(async move {
                let url = c
                    .duckdb_bindings()
                    .expect("duckdb bindings after first provision")
                    .database_url
                    .clone();
                tokio::task::spawn_blocking(move || {
                    let conn = Connection::open(Path::new(url.as_str()))
                        .expect("open first scenario duckdb connection");
                    conn.execute_batch(
                        "CREATE TABLE isolated_t (x INTEGER);
                         INSERT INTO isolated_t VALUES (7);",
                    )
                    .expect("create isolated table");
                })
                .await
                .expect("first blocking task");
            })
        },
        &mut first_event_log,
    )
    .await
    .expect("first scenario");

    let mut second_event_log = EventLog::new();
    run_scenario(
        &mut ctx,
        "duckdb_isolation_second",
        |c| {
            Box::pin(async move {
                let url = c
                    .duckdb_bindings()
                    .expect("duckdb bindings after second provision")
                    .database_url
                    .clone();
                tokio::task::spawn_blocking(move || {
                    let conn = Connection::open(Path::new(url.as_str()))
                        .expect("open second scenario duckdb connection");
                    let table_count: i64 = conn
                        .query_row(
                            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main' AND table_name = 'isolated_t'",
                            [],
                            |row| row.get(0),
                        )
                        .expect("query information_schema.tables");
                    assert_eq!(table_count, 0, "second scenario should start isolated");
                })
                .await
                .expect("second blocking task");
            })
        },
        &mut second_event_log,
    )
    .await
    .expect("second scenario");
}

#[shakedown::test(resources = [DuckDb])]
async fn duckdb_macro_registers_duckdb(ctx: &mut shakedown::Context) {
    assert!(
        ctx.duckdb_bindings().is_some(),
        "lifecycle should apply duckdb metadata to Context"
    );
}
