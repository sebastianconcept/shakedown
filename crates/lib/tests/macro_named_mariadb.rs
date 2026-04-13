//! Named MariaDB resources via `#[shakedown::test(resources = [MariaDB(name = \"...\")])]`.
//!
//! Built only with `--features mariadb`. **`#[ignore]`** by default (macro adds ignore when
//! `resources` is non-empty). With Docker:
//!
//! ```text
//! cargo test -p shakedown --all-features macro_named_mariadb_two_bindings -- --ignored
//! ```

#[cfg(feature = "mariadb")]
#[shakedown::test(resources = [MariaDB(name = "primary"), MariaDB(name = "shard")])]
async fn macro_named_mariadb_two_bindings(ctx: &mut shakedown::Context) {
    let primary = ctx
        .mariadb_bindings_named("primary")
        .expect("primary binding after provision");
    let shard = ctx
        .mariadb_bindings_named("shard")
        .expect("shard binding after provision");
    assert_ne!(
        primary.database, shard.database,
        "named MariaDB registrations should isolate to distinct databases on one suite server"
    );
}
