//! Fail test: #[shakedown::test] on a non-async function
//! 
//! This should fail compilation with a clear error message.

#[shakedown::test]
fn sync_test(ctx: &mut shakedown::Context) {
    // This is wrong - must be async
}

fn main() {}
