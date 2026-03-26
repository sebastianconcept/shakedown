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

#[shakedown::test(resources = [Sqlite])]
async fn sqlite_macro_registers_sqlite(ctx: &mut shakedown::Context) {
    assert!(
        ctx.sqlite_bindings().is_some(),
        "lifecycle should apply sqlite metadata to Context"
    );
}
