//! Pass: `#[shakedown::test]` with no `resources` arg — expansion still wires
//! `Context`, `EventLog`, and `run_scenario` (user may register resources in the body).

#[shakedown::test]
async fn plain_scenario(ctx: &mut shakedown::Context) {
    let _ = ctx;
}

fn main() {}
