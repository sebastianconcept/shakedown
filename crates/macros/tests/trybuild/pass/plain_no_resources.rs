//! Pass: `#[shakedown::test]` with no arguments.
//!
//! **Given** a test that does not specify any `resources` or `prelude`
//! **When** the macro expands it
//! **Then** it still correctly wires `Context::new()`, `EventLog::new()`, and `run_scenario`.
//! The test author is responsible for registering any resources inside the scenario body if needed.

#[shakedown::test]
async fn plain_scenario(ctx: &mut shakedown::Context) {
    let _ = ctx;
}

fn main() {}
