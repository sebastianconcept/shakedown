//! Oracle MySQL suite container + per-scenario database + Context bindings (optional).
//!
//! This target is built only with `--features mysql`. The test is **`#[ignore]`** so
//! `cargo test --workspace` skips Docker by default.
//!
//! With Docker available:
//!
//! ```text
//! cargo test -p shakedown --features mysql mysql_vertical -- --ignored
//! ```

use mysql_async::prelude::Queryable;
use shakedown::{run_scenario, BuiltinResource, Context, EventLog, MysqlResource};

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features mysql mysql_vertical -- --ignored"]
async fn mysql_provisions_and_writes_reads_row() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Mysql(
        MysqlResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();
    run_scenario(
        &mut ctx,
        "mysql_vertical_proof",
        |c| {
            Box::pin(async move {
                let bindings = c.mysql_bindings().expect("mysql bindings after provision");
                assert_eq!(bindings.host, "127.0.0.1");
                assert!(bindings.port > 0);
                let database_url = bindings.connection_url.clone();

                let pool = mysql_async::Pool::new(database_url.as_str());
                let mut mysql_connection = pool.get_conn().await.expect("mysql connection");
                mysql_connection
                    .query_drop(
                        "CREATE TABLE shakedown_items (id INT PRIMARY KEY AUTO_INCREMENT, value_text TEXT NOT NULL)",
                    )
                    .await
                    .expect("create table");
                mysql_connection
                    .exec_drop(
                        "INSERT INTO shakedown_items (value_text) VALUES (?)",
                        ("hello-mysql",),
                    )
                    .await
                    .expect("insert row");
                let selected_value: Option<String> = mysql_connection
                    .exec_first("SELECT value_text FROM shakedown_items WHERE id = 1", ())
                    .await
                    .expect("select row");
                assert_eq!(selected_value.as_deref(), Some("hello-mysql"));

                drop(mysql_connection);
                pool.disconnect().await.expect("close mysql pool");
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario should succeed");
}
