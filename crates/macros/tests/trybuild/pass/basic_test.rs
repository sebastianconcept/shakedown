//! Pass: minimal `#[shakedown::test(resources = [...])]`.
//!
//! **Given** the smallest valid usage of the test attribute
//! **When** `trybuild` compiles this file as a small binary crate
//! **Then** the macro expansion produces valid Rust that links `shakedown::run_scenario`, `Context`, and `EventLog`.
//! (The `main` function is required for `trybuild` to treat this as a complete binary.)

#[shakedown::test(resources = [Fake])]
async fn basic_scenario(ctx: &mut shakedown::Context) {
    let _ = ctx;
}

fn main() {}
