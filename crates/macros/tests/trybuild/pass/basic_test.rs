//! Pass: minimal `#[shakedown::test]` — expansion references `run_scenario`, `Context`, `EventLog`.
//!
//! trybuild compiles this file as the root of a small binary crate; `main` is required to link.

#[shakedown::test(resources = [Fake])]
async fn basic_scenario(ctx: &mut shakedown::Context) {
    let _ = ctx;
}

fn main() {}
