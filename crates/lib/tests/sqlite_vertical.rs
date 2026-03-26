//! SQLite in-memory resource: no Docker. Run with `cargo test -p shakedown --features sqlite`.

use rusqlite::{Connection, OpenFlags};
use shakedown::{run_scenario, BuiltinResource, Context, EventLog, SqliteResource};

#[tokio::test]
async fn sqlite_writes_and_reads_via_bindings_without_docker() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Sqlite(SqliteResource::new()));
    let mut event_log = EventLog::new();
    run_scenario(
        &mut ctx,
        "sqlite_vertical_proof",
        |c| {
            Box::pin(async move {
                let url = c
                    .sqlite_bindings()
                    .expect("sqlite bindings after provision")
                    .database_url
                    .clone();
                tokio::task::spawn_blocking(move || {
                    let conn = Connection::open_with_flags(
                        url.as_str(),
                        OpenFlags::SQLITE_OPEN_URI
                            | OpenFlags::SQLITE_OPEN_READ_WRITE
                            | OpenFlags::SQLITE_OPEN_CREATE,
                    )
                    .expect("open second connection with same named in-memory URI");
                    conn.execute("CREATE TABLE t (x INTEGER)", [])
                        .expect("create table");
                    conn.execute("INSERT INTO t VALUES (42)", [])
                        .expect("insert row");
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
async fn sqlite_isolates_each_scenario_without_docker() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Sqlite(SqliteResource::new()));
    let mut first_event_log = EventLog::new();
    run_scenario(
        &mut ctx,
        "sqlite_isolation_first",
        |c| {
            Box::pin(async move {
                let url = c
                    .sqlite_bindings()
                    .expect("sqlite bindings after first provision")
                    .database_url
                    .clone();
                tokio::task::spawn_blocking(move || {
                    let conn = Connection::open_with_flags(
                        url.as_str(),
                        OpenFlags::SQLITE_OPEN_URI
                            | OpenFlags::SQLITE_OPEN_READ_WRITE
                            | OpenFlags::SQLITE_OPEN_CREATE,
                    )
                    .expect("open first scenario sqlite connection");
                    conn.execute("CREATE TABLE isolated_t (x INTEGER)", [])
                        .expect("create isolated table");
                    conn.execute("INSERT INTO isolated_t VALUES (7)", [])
                        .expect("insert isolated row");
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
        "sqlite_isolation_second",
        |c| {
            Box::pin(async move {
                let url = c
                    .sqlite_bindings()
                    .expect("sqlite bindings after second provision")
                    .database_url
                    .clone();
                tokio::task::spawn_blocking(move || {
                    let conn = Connection::open_with_flags(
                        url.as_str(),
                        OpenFlags::SQLITE_OPEN_URI
                            | OpenFlags::SQLITE_OPEN_READ_WRITE
                            | OpenFlags::SQLITE_OPEN_CREATE,
                    )
                    .expect("open second scenario sqlite connection");
                    let table_count: i64 = conn
                        .query_row(
                            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='isolated_t'",
                            [],
                            |row| row.get(0),
                        )
                        .expect("query sqlite_master");
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

#[shakedown::test(resources = [Sqlite])]
async fn sqlite_macro_registers_sqlite(ctx: &mut shakedown::Context) {
    assert!(
        ctx.sqlite_bindings().is_some(),
        "lifecycle should apply sqlite metadata to Context"
    );
}
